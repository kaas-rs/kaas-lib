//! M17 acceptance: KIP-848 consumer groups.
//!
//! `cargo test -p kafka-consumer --test group_848 -- --ignored`
//!
//! The assertions are **union and intersection**, not record counts. A
//! reconciliation that acknowledges a new assignment before revoking the old
//! one produces an *overlap*: two members holding the same partition, both
//! delivering its records, with no error anywhere. Counting records would pass
//! while that bug is live — only the intersection catches it.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_consumer::{ConsumerConfig, GroupConsumer};
use kafka_produce::{Producer, ProducerConfig, ProducerRecord};
use kafka_read::{Cluster, Visibility};
use testkit::{Cluster as _, KafkaCluster};

const TOPIC: &str = "group-848";
const PARTITIONS: i32 = 12;

async fn setup() -> (KafkaCluster, Cluster) {
    let fixture = testkit::cluster(3).await.expect("cluster");
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .expect("admin");
    admin
        .create_topics([NewTopic::new(TOPIC, PARTITIONS, 3)])
        .await
        .expect("topic");
    for _ in 0..50 {
        if let Ok(results) = admin.describe_topics([TOPIC.to_owned()]).await
            && results.iter().any(|(_, result)| result.is_ok())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let cluster = admin.cluster().clone();
    (fixture, cluster)
}

fn config() -> ConsumerConfig {
    ConsumerConfig::new()
        .visibility(Visibility::All)
        .max_wait_ms(200)
}

fn owned(members: &[&GroupConsumer]) -> Vec<BTreeSet<(String, i32)>> {
    members
        .iter()
        .map(|m| m.assignment().into_iter().collect())
        .collect()
}

/// The acceptance case: three members, full coverage, empty intersection.
#[tokio::test]
#[ignore = "needs Docker"]
async fn three_consumers_cover_every_partition_exactly_once() {
    let (_fixture, cluster) = setup().await;
    let group = "group-848-coverage";

    let mut a = GroupConsumer::subscribe(cluster.clone(), config(), group, [TOPIC])
        .await
        .expect("a");
    let mut b = GroupConsumer::subscribe(cluster.clone(), config(), group, [TOPIC])
        .await
        .expect("b");
    let mut c = GroupConsumer::subscribe(cluster.clone(), config(), group, [TOPIC])
        .await
        .expect("c");

    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        a.poll().await.expect("a");
        b.poll().await.expect("b");
        c.poll().await.expect("c");
        let total = a.assignment().len() + b.assignment().len() + c.assignment().len();
        if total == usize::try_from(PARTITIONS).unwrap()
            && !a.assignment().is_empty()
            && !b.assignment().is_empty()
            && !c.assignment().is_empty()
        {
            break;
        }
    }

    let sets = owned(&[&a, &b, &c]);
    let union: BTreeSet<_> = sets.iter().flatten().cloned().collect();
    assert_eq!(
        union.len(),
        usize::try_from(PARTITIONS).unwrap(),
        "the group must cover every partition; a gap is records nobody reads"
    );

    // Pairwise disjoint. An overlap delivers every record in it twice and
    // reports nothing at all.
    for (i, left) in sets.iter().enumerate() {
        for right in sets.iter().skip(i + 1) {
            let shared: Vec<_> = left.intersection(right).collect();
            assert!(
                shared.is_empty(),
                "two members own {shared:?} at the same time"
            );
        }
    }

    // Every member generated its own id, which KIP-848 requires and the
    // classic protocol does the opposite of.
    for id in [a.member_id(), b.member_id(), c.member_id()] {
        assert!(!id.is_empty(), "the client generates its own member id");
    }
    assert_ne!(a.member_id(), b.member_id());
}

/// A member leaving hands its partitions on rather than stranding them, and a
/// continuously-produced stream shows no gap and no double delivery across the
/// rebalance.
#[tokio::test]
#[ignore = "needs Docker"]
async fn a_departing_member_is_replaced_without_a_gap_or_a_duplicate() {
    const RECORDS: usize = 3_000;

    let (_fixture, cluster) = setup().await;
    let group = "group-848-rebalance";

    let producer = Producer::new(cluster.clone(), ProducerConfig::new());
    let mut pending = Vec::with_capacity(RECORDS);
    for i in 0..RECORDS {
        pending.push(
            producer
                .enqueue(ProducerRecord::new(TOPIC).value(format!("v{i}")))
                .await
                .expect("enqueued"),
        );
    }
    for delivery in pending {
        delivery.await.expect("delivered");
    }

    let mut a = GroupConsumer::subscribe(cluster.clone(), config(), group, [TOPIC])
        .await
        .expect("a");
    let mut b = GroupConsumer::subscribe(cluster.clone(), config(), group, [TOPIC])
        .await
        .expect("b");

    let mut seen: BTreeSet<(i32, i64)> = BTreeSet::new();
    let mut duplicates = 0;

    let deadline = Instant::now() + Duration::from_secs(90);
    let mut left = false;
    while Instant::now() < deadline && seen.len() < RECORDS {
        for record in a.poll().await.expect("a") {
            if !seen.insert((record.partition, record.offset)) {
                duplicates += 1;
            }
        }
        for record in b.poll().await.expect("b") {
            if !seen.insert((record.partition, record.offset)) {
                duplicates += 1;
            }
        }
        // Drop one member halfway through, so the rebalance happens with the
        // stream in flight rather than at rest.
        if !left && seen.len() > RECORDS / 3 {
            a.leave().await.expect("leave");
            left = true;
        }
    }

    assert!(left, "the test never reached the rebalance");
    assert_eq!(
        duplicates, 0,
        "a partition was delivered by two members at once"
    );
    assert_eq!(
        seen.len(),
        RECORDS,
        "records went missing across the rebalance"
    );

    assert_eq!(
        b.assignment().len(),
        usize::try_from(PARTITIONS).unwrap(),
        "the survivor must take over every partition the leaver held"
    );
}

/// A single member owns everything, and leaving is idempotent.
#[tokio::test]
#[ignore = "needs Docker"]
async fn one_member_owns_the_whole_topic_and_can_leave_twice() {
    let (_fixture, cluster) = setup().await;
    let group = "group-848-single";

    let mut only = GroupConsumer::subscribe(cluster.clone(), config(), group, [TOPIC])
        .await
        .expect("subscribe");

    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline
        && only.assignment().len() != usize::try_from(PARTITIONS).unwrap()
    {
        only.poll().await.expect("poll");
    }
    assert_eq!(
        only.assignment().len(),
        usize::try_from(PARTITIONS).unwrap()
    );

    only.leave().await.expect("leave");
    // Leaving a group we are not in must not be an error: a shutdown path that
    // can fail is a shutdown path that leaks members.
    only.leave().await.expect("leaving twice is not an error");
}
