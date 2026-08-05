//! M16 acceptance: fetch sessions and the streaming fetcher.
//!
//! `cargo test -p kafka-consume --test fetcher -- --ignored`
//!
//! The record count is the easy half. The assertion that actually decides
//! whether this milestone happened is that a **steady-state fetch carries an
//! empty topics array** — a consumer that re-sends its whole assignment on
//! every fetch delivers every record, in order, and looks perfect while
//! costing the broker the thing KIP-227 exists to save.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::HashSet;
use std::time::Duration;

use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_conn::StatsSnapshot;
use kafka_consume::{Consumer, ConsumerConfig, Position};
use kafka_produce::{Producer, ProducerConfig, ProducerRecord};
use kafka_read::{Cluster, Visibility};
use testkit::{Cluster as _, KafkaCluster};

/// The one cluster this binary's tests share.
///
/// Four tests booting four 3-node clusters is twelve KRaft brokers competing
/// for one runner to assert things that do not care whose broker they run on.
///
/// Never dropped, because a `static` is not: the containers go with the
/// ephemeral runner pod on CI, and may want `docker container prune` locally.
static SHARED: tokio::sync::OnceCell<Shared> = tokio::sync::OnceCell::const_new();

struct Shared {
    _fixture: KafkaCluster,
    admin: Admin,
    cluster: Cluster,
}

async fn shared() -> &'static Shared {
    SHARED
        .get_or_init(|| async {
            let fixture = testkit::cluster(3).await.expect("cluster");
            let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
                .await
                .expect("admin");
            let cluster = admin.cluster().clone();
            Shared {
                _fixture: fixture,
                admin,
                cluster,
            }
        })
        .await
}

/// Topics of this test's own, one per entry in `partitions`.
///
/// The naming is load-bearing beyond avoiding crosstalk. These tests all
/// created `fetcher-a`, with **different partition counts** — six in two of
/// them, two in the others — which was harmless while each had its own
/// cluster. Sharing one, the first creation wins and every later test
/// silently gets someone else's shape, so `pause`-ing partition 1 of a
/// six-partition topic asserts nothing it means to.
async fn setup(name: &str, partitions: &[i32]) -> (Cluster, Vec<String>) {
    let shared = shared().await;
    let topics: Vec<String> = (0..partitions.len())
        .map(|i| format!("fetcher-{name}-{i}"))
        .collect();
    shared
        .admin
        .create_topics(
            topics
                .iter()
                .zip(partitions)
                .map(|(topic, count)| NewTopic::new(topic.clone(), *count, 3)),
        )
        .await
        .expect("topics");
    for topic in &topics {
        await_topic(&shared.admin, topic).await;
    }
    (shared.cluster.clone(), topics)
}

async fn await_topic(admin: &Admin, topic: &str) {
    for _ in 0..50 {
        if let Ok(results) = admin.describe_topics([topic.to_owned()]).await
            && results.iter().any(|(_, result)| result.is_ok())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("{topic} never became describable");
}

/// Produce `count` records spread over a topic's partitions.
async fn produce(cluster: &Cluster, topic: &str, partitions: i32, count: usize) {
    let producer = Producer::new(cluster.clone(), ProducerConfig::new());
    let mut pending = Vec::with_capacity(count);
    for i in 0..count {
        let partition = i32::try_from(i).unwrap_or(0) % partitions;
        pending.push(
            producer
                .enqueue(
                    ProducerRecord::new(topic)
                        .partition(partition)
                        .value(format!("{topic}-{i}")),
                )
                .await
                .expect("enqueued"),
        );
    }
    for delivery in pending {
        delivery.await.expect("delivered");
    }
}

async fn traffic(cluster: &Cluster, brokers: &[i32]) -> StatsSnapshot {
    let mut total = StatsSnapshot::default();
    for node_id in brokers {
        if let Ok(connection) = cluster.pool().get(*node_id).await {
            let stats = connection.stats_snapshot();
            total.bytes_sent += stats.bytes_sent;
            total.requests_sent += stats.requests_sent;
        }
    }
    total
}

async fn broker_ids(cluster: &Cluster) -> Vec<i32> {
    for _ in 0..10 {
        if let Ok(snapshot) = cluster.refresh().await {
            let ids: Vec<i32> = snapshot
                .brokers()
                .iter()
                .map(|broker| broker.node_id)
                .collect();
            if !ids.is_empty() {
                return ids;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("the cluster reported no brokers");
}

/// The acceptance case: 12 partitions across 2 topics, 100k records, exact
/// count and per-partition order.
#[tokio::test]
#[ignore = "needs Docker"]
async fn twelve_partitions_across_two_topics_stream_in_order() {
    const PER_TOPIC: usize = 50_000;

    let (cluster, topics) = setup("twelve", &[6, 6]).await;
    let (topic_a, topic_b) = (topics[0].as_str(), topics[1].as_str());
    produce(&cluster, topic_a, 6, PER_TOPIC).await;
    produce(&cluster, topic_b, 6, PER_TOPIC).await;

    let mut consumer = Consumer::new(
        cluster.clone(),
        ConsumerConfig::new().visibility(Visibility::All),
    );
    let assignment: Vec<(String, i32)> = [topic_a, topic_b]
        .iter()
        .flat_map(|topic| (0..6).map(move |p| ((*topic).to_owned(), p)))
        .collect();
    consumer
        .assign(assignment.clone(), Position::Earliest)
        .await
        .expect("assigned");
    assert_eq!(consumer.assignment().len(), 12);

    // Per-partition order, checked per partition because that is the only
    // place Kafka promises it.
    let mut last: std::collections::HashMap<(String, i32), i64> = std::collections::HashMap::new();
    let mut total = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(180);

    while total < PER_TOPIC * 2 && std::time::Instant::now() < deadline {
        for record in consumer.poll().await.expect("poll") {
            let key = (record.topic.clone(), record.partition);
            if let Some(previous) = last.get(&key) {
                assert!(
                    record.offset > *previous,
                    "{key:?} went backwards: {} after {previous}",
                    record.offset
                );
            }
            last.insert(key, record.offset);
            total += 1;
        }
    }

    assert_eq!(
        total,
        PER_TOPIC * 2,
        "expected every record across both topics"
    );
}

/// The assertion that proves the session is live rather than a full fetch
/// wearing a session id.
#[tokio::test]
#[ignore = "needs Docker"]
async fn a_steady_state_fetch_stops_re_sending_the_assignment() {
    const RECORDS: usize = 2_000;

    let (cluster, topics) = setup("steady", &[6]).await;
    let topic_a = topics[0].as_str();
    produce(&cluster, topic_a, 6, RECORDS).await;

    let mut consumer = Consumer::new(
        cluster.clone(),
        ConsumerConfig::new()
            .visibility(Visibility::All)
            .max_wait_ms(100),
    );
    consumer
        .assign(
            (0..6).map(|p| (topic_a.to_owned(), p)).collect::<Vec<_>>(),
            Position::Earliest,
        )
        .await
        .expect("assigned");

    // Drain everything, which establishes the session and advances every
    // partition to the log end.
    let mut drained = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while drained < RECORDS && std::time::Instant::now() < deadline {
        drained += consumer.poll().await.expect("poll").len();
    }
    assert_eq!(drained, RECORDS);

    // Now, caught up and unchanged, the requests should carry almost nothing.
    let brokers = broker_ids(&cluster).await;
    let mark = traffic(&cluster, &brokers).await;
    for _ in 0..10 {
        assert!(
            consumer.poll().await.expect("poll").is_empty(),
            "the log has not moved, so a poll must return nothing"
        );
    }
    let used = traffic(&cluster, &brokers).await;
    let delta = used.since(&mark);

    assert!(
        delta.requests_sent > 0,
        "measurement is broken: ten polls cannot have taken zero requests"
    );
    // A full fetch of six partitions carries topic ids, partition indexes,
    // offsets and byte budgets — on the order of 40 bytes per partition per
    // request. An incremental one carries the session id and epoch and
    // nothing else. The bound is loose because the exact framing is not the
    // point; the order of magnitude is.
    let per_request = delta.bytes_sent / delta.requests_sent;
    assert!(
        per_request < 120,
        "steady-state fetches average {per_request} bytes, which is a full \
         fetch wearing a session id rather than an incremental one"
    );
}

/// `seek`, `pause` and `resume` — the operations a bounded scan never needs.
#[tokio::test]
#[ignore = "needs Docker"]
async fn seek_pause_and_resume_change_the_stream_mid_flight() {
    const RECORDS: usize = 500;

    let (cluster, topics) = setup("seek", &[2]).await;
    let topic_a = topics[0].as_str();
    produce(&cluster, topic_a, 2, RECORDS).await;

    let mut consumer = Consumer::new(
        cluster.clone(),
        ConsumerConfig::new()
            .visibility(Visibility::All)
            .max_wait_ms(100),
    );
    consumer
        .assign(
            vec![(topic_a.to_owned(), 0), (topic_a.to_owned(), 1)],
            Position::Earliest,
        )
        .await
        .expect("assigned");

    // Pause partition 1: everything that arrives must be partition 0.
    consumer.pause(topic_a, 1);
    assert!(consumer.is_paused(topic_a, 1));

    // Stop once the unpaused partition has delivered *and* two further polls
    // have added nothing: that is the evidence the assertion below needs, and
    // it usually arrives in three or four polls.
    //
    // The fixed `0..10` this replaces could not exit early, and every poll
    // past the point partition 0 drains costs a full `max_wait_ms` long-poll
    // for a broker with nothing to say. The ceiling stays, so a partition
    // that never delivers still fails rather than looping.
    let mut seen: HashSet<i32> = HashSet::new();
    let mut quiet = 0;
    for _ in 0..10 {
        let batch = consumer.poll().await.expect("poll");
        if batch.is_empty() {
            quiet += 1;
        } else {
            quiet = 0;
            for record in batch {
                seen.insert(record.partition);
            }
        }
        if seen.contains(&0) && quiet >= 2 {
            break;
        }
    }
    assert!(
        !seen.contains(&1),
        "a paused partition must not deliver records"
    );
    assert!(
        seen.contains(&0),
        "the unpaused partition must still deliver"
    );

    // Resume it and it starts from where it was, not from the beginning of
    // whatever the other partition has reached.
    consumer.resume(topic_a, 1);
    assert!(!consumer.is_paused(topic_a, 1));
    let mut resumed = false;
    for _ in 0..20 {
        if consumer
            .poll()
            .await
            .expect("poll")
            .iter()
            .any(|record| record.partition == 1)
        {
            resumed = true;
            break;
        }
    }
    assert!(resumed, "a resumed partition must deliver again");

    // Seek back to the start of partition 0 and the next records are its
    // earliest ones, not a continuation.
    consumer.seek(topic_a, 0, 0).expect("seek");
    assert_eq!(consumer.position(topic_a, 0), Some(0));

    let mut first_after_seek = None;
    for _ in 0..20 {
        if let Some(record) = consumer
            .poll()
            .await
            .expect("poll")
            .into_iter()
            .find(|record| record.partition == 0)
        {
            first_after_seek = Some(record.offset);
            break;
        }
    }
    assert_eq!(
        first_after_seek,
        Some(0),
        "a seek that still delivers the old position is not a seek"
    );

    // Seeking a partition that is not assigned is a caller error, not a
    // silent no-op.
    assert!(consumer.seek(topic_a, 99, 0).is_err());
}

/// Offsets for a consumer that is not a group member.
#[tokio::test]
#[ignore = "needs Docker"]
async fn a_non_member_can_commit_and_resume_from_its_commit() {
    const RECORDS: usize = 300;
    const GROUP: &str = "fetcher-non-member";

    let (cluster, topics) = setup("commit", &[2]).await;
    let topic_a = topics[0].as_str();
    produce(&cluster, topic_a, 2, RECORDS).await;

    let assignment = vec![(topic_a.to_owned(), 0), (topic_a.to_owned(), 1)];
    let config = ConsumerConfig::new()
        .visibility(Visibility::All)
        .max_wait_ms(100)
        .group_id(GROUP);

    let mut consumer = Consumer::new(cluster.clone(), config.clone());
    consumer
        .assign(assignment.clone(), Position::Earliest)
        .await
        .expect("assigned");

    let mut read = 0;
    while read < RECORDS / 2 {
        read += consumer.poll().await.expect("poll").len();
    }
    let positions: Vec<(String, i32, i64)> = assignment
        .iter()
        .map(|(topic, partition)| {
            (
                topic.clone(),
                *partition,
                consumer.position(topic, *partition).expect("position"),
            )
        })
        .collect();

    for (_, result) in consumer.commit().await.expect("commit") {
        result.expect("every partition commits");
    }

    // A second consumer, also not a member, resumes exactly where the first
    // stopped.
    let mut resumed = Consumer::new(cluster.clone(), config);
    resumed
        .assign(assignment, Position::Earliest)
        .await
        .expect("assigned");
    resumed
        .seek_to_committed()
        .await
        .expect("seek to committed");

    for (topic, partition, offset) in positions {
        assert_eq!(
            resumed.position(&topic, partition),
            Some(offset),
            "{topic}-{partition} did not resume from its commit"
        );
    }
}
