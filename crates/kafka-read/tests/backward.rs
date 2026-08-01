//! M10 acceptance: the backward scan.
//!
//! `cargo test -p kafka-read --test backward -- --ignored`
//!
//! In-container shell tools bootstrap `localhost:9093`, the BROKER
//! listener — see `testkit::INTERNAL_BOOTSTRAP`. Port 9092 is advertised
//! as the *host-mapped* port for the test process, so a client inside the
//! container follows metadata to a port nothing is listening on and dies
//! with a bare TimeoutException.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::time::Duration;

use kafka_admin::{Admin, ClusterConfig, NewTopic, OffsetSpec};
use kafka_read::{Cluster, TailSpec};
use testkit::{BrokerConfig, Cluster as _, KafkaCluster};

/// Produce with deliberately varied batch sizes, so batch boundaries do not
/// line up with any step the backward walk might choose. A partition whose
/// batches are uniform makes this test pass for the wrong reason.
async fn produce_randomised(fixture: &KafkaCluster, topic: &str, total: u32) {
    let command = format!(
        "for i in $(seq 1 40); do \
           BATCH=$(( (i * 7919) % 400 + 25 )); \
           LINGER=$(( (i * 13) % 30 + 1 )); \
           START=$(( (i - 1) * {per} + 1 )); \
           END=$(( i * {per} )); \
           seq $START $END | /opt/kafka/bin/kafka-console-producer.sh \
             --bootstrap-server localhost:9093 --topic {topic} \
             --producer-property batch.size=$((BATCH * 64)) \
             --producer-property linger.ms=$LINGER; \
         done",
        per = total / 40
    );
    fixture
        .exec(0, vec!["bash".to_owned(), "-c".to_owned(), command])
        .await
        .expect("produced");
}

async fn setup(topic: &str, partitions: i32, config: BrokerConfig) -> (KafkaCluster, Cluster) {
    let fixture = testkit::single_broker_with(config).await.expect("broker");
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .expect("admin");
    admin
        .create_topics([NewTopic::new(topic, partitions, 1)])
        .await
        .expect("topic");
    let cluster = admin.cluster().clone();
    (fixture, cluster)
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn the_last_500_of_100k_records_are_exactly_the_last_500() {
    let (fixture, cluster) = setup("tailed", 1, BrokerConfig::new()).await;
    produce_randomised(&fixture, "tailed", 100_000).await;

    let admin = Admin::new(cluster.clone());
    let latest = admin
        .list_offsets([("tailed".to_owned(), 0)], OffsetSpec::Latest)
        .await
        .unwrap();
    let end = latest[0].1.as_ref().unwrap().offset.unwrap();
    assert!(end >= 100_000, "only {end} records were produced");

    let before = partition_bytes_read(&cluster).await;

    let tails = kafka_read::tail(&cluster, &TailSpec::new("tailed", 500))
        .await
        .expect("tail");
    let after = partition_bytes_read(&cluster).await;

    assert_eq!(tails.len(), 1);
    let records = &tails[0].records;
    assert_eq!(records.len(), 500, "exactly the number asked for");

    // In order, and *the last* 500 — not any 500.
    assert!(
        records.windows(2).all(|w| w[0].offset < w[1].offset),
        "the tail came back out of order"
    );
    assert_eq!(
        records.last().map(|r| r.offset),
        Some(end - 1),
        "the tail must end at the high watermark"
    );
    assert_eq!(
        records.first().map(|r| r.offset),
        Some(end - 500),
        "the tail must start exactly 500 records back"
    );

    // The assertion the milestone exists for. A naive implementation reads the
    // whole partition; this one must read a sliver of it.
    let read = after.saturating_sub(before);
    let partition_size = admin
        .topic_sizes()
        .await
        .unwrap()
        .into_iter()
        .find(|(name, _)| name == "tailed")
        .and_then(|(_, size)| size.ok())
        .map(|size| size.logical_bytes)
        .unwrap_or_default();
    assert!(partition_size > 0, "the topic reported no size");

    let budget = partition_size / 20; // 5%
    assert!(
        i64::try_from(read).unwrap_or(i64::MAX) < budget,
        "read {read} bytes for the last 500 of a {partition_size}-byte partition \
         (budget {budget}); a whole-partition read would look exactly like this"
    );
    println!(
        "read {read} bytes of {partition_size} in {} fetches",
        tails[0].fetches
    );
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_compacted_topic_with_offset_gaps_terminates_and_counts_correctly() {
    // The second failure mode: offsets are no longer one apart, so offset
    // arithmetic over-estimates every window and a fixed step crawls. This
    // asserts both that it terminates and that the count is right.
    let (fixture, cluster) = setup(
        "compacted",
        1,
        BrokerConfig::new()
            .with_property("log.cleaner.enable", "true")
            .with_property("log.cleaner.backoff.ms", "1000")
            .with_property("log.cleaner.min.cleanable.ratio", "0.01")
            .with_property("log.segment.bytes", "16384")
            .with_property("log.roll.ms", "1000"),
    )
    .await;

    let admin = Admin::new(cluster.clone());
    admin
        .alter_configs([(
            kafka_admin::ConfigResource::topic("compacted"),
            vec![
                kafka_admin::ConfigChange::set("cleanup.policy", "compact"),
                kafka_admin::ConfigChange::set("min.cleanable.dirty.ratio", "0.01"),
                kafka_admin::ConfigChange::set("segment.bytes", "16384"),
                kafka_admin::ConfigChange::set("delete.retention.ms", "1000"),
                kafka_admin::ConfigChange::set("max.compaction.lag.ms", "1000"),
            ],
        )])
        .await
        .unwrap();

    // Write the same 200 keys over and over. Compaction collapses each key to
    // its newest value, leaving large offset gaps behind.
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "for round in $(seq 1 50); do \
                   for k in $(seq 1 200); do echo \"key$k:v$round\"; done \
                 done | /opt/kafka/bin/kafka-console-producer.sh \
                   --bootstrap-server localhost:9093 --topic compacted \
                   --property parse.key=true --property key.separator=:"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    // Give the cleaner time to actually create the gaps.
    tokio::time::sleep(Duration::from_secs(25)).await;

    let started = std::time::Instant::now();
    let tails = tokio::time::timeout(
        Duration::from_secs(60),
        kafka_read::tail(&cluster, &TailSpec::new("compacted", 100)),
    )
    .await
    .expect("the backward walk must terminate on a compacted topic")
    .expect("tail");

    assert_eq!(tails.len(), 1);
    let records = &tails[0].records;
    assert_eq!(records.len(), 100, "exact count despite the offset gaps");
    assert!(
        records.windows(2).all(|w| w[0].offset < w[1].offset),
        "out of order"
    );
    println!(
        "compacted tail took {:?} and {} fetches, offsets {}..{}",
        started.elapsed(),
        tails[0].fetches,
        records[0].offset,
        records[99].offset
    );
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_tail_longer_than_the_partition_returns_everything_without_looping() {
    let (fixture, cluster) = setup("short", 1, BrokerConfig::new()).await;
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 25 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9093 --topic short"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    let tails = tokio::time::timeout(
        Duration::from_secs(30),
        kafka_read::tail(&cluster, &TailSpec::new("short", 1000)),
    )
    .await
    .expect("must not loop looking for records that do not exist")
    .unwrap();
    assert_eq!(tails[0].records.len(), 25);
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn an_empty_partition_tails_to_nothing() {
    let (_fixture, cluster) = setup("empty", 1, BrokerConfig::new()).await;
    let tails = tokio::time::timeout(
        Duration::from_secs(30),
        kafka_read::tail(&cluster, &TailSpec::new("empty", 100)),
    )
    .await
    .expect("must not loop on an empty partition")
    .unwrap();
    assert!(tails[0].records.is_empty());
    assert_eq!(tails[0].fetches, 0, "an empty partition needs no fetch");
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_multi_partition_tail_spreads_the_limit() {
    let (fixture, cluster) = setup("spread", 4, BrokerConfig::new()).await;
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 4000 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9093 --topic spread"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    let tails = kafka_read::tail(&cluster, &TailSpec::new("spread", 400))
        .await
        .unwrap();
    assert_eq!(tails.len(), 4);
    let total: usize = tails.iter().map(|t| t.records.len()).sum();
    // 400 across 4 partitions is 100 each; a partition with fewer records than
    // its share contributes what it has.
    assert!(total <= 400, "{total} records for a limit of 400");
    assert!(total >= 300, "{total} records is too few");
    for tail in &tails {
        assert!(tail.records.len() <= 100, "{} records", tail.records.len());
    }
}

/// Bytes read from every connection in the pool.
async fn partition_bytes_read(cluster: &Cluster) -> u64 {
    let snapshot = cluster.snapshot();
    let mut total = 0;
    for broker in snapshot.brokers() {
        if let Ok(connection) = cluster.pool().get(broker.node_id).await {
            total += connection.stats_snapshot().bytes_received;
        }
    }
    total
}
