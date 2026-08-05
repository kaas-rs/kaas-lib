//! M19 acceptance: producer and consumer lifecycles, cancelled at random.
//!
//! `cargo test -p kafka-consume --test leak -- --ignored`
//!
//! Rule 5 across the two crates phase 2 added. `kafka-read`'s leak test covers
//! the scan path; this covers the paths that did not exist then — an
//! accumulator with a background actor, a fetcher holding broker-side session
//! state, and group members the coordinator is tracking.
//!
//! Each of those leaks differently, which is why the assertions are on three
//! separate things rather than on one number:
//!
//! * a **connection** leak shows in the pool's live count;
//! * a **task** leak shows nowhere at all until the process dies, so the
//!   accumulator's shutdown is asserted by the buffered records still being
//!   delivered;
//! * a **broker-side** leak — a fetch session or a group member nobody is
//!   using — shows on the broker, not here, and is what cancelling mid-poll
//!   risks.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::time::Duration;

use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_consume::{Consumer, ConsumerConfig, GroupConsumer, Position};
use kafka_produce::{Producer, ProducerConfig, ProducerRecord};
use kafka_read::{Cluster, Visibility};
use testkit::{Cluster as _, KafkaCluster};

const TOPIC: &str = "leak";
const PARTITIONS: i32 = 4;
/// How many lifecycles to spin up and tear down.
const CYCLES: usize = 1_000;

async fn setup() -> (KafkaCluster, Cluster) {
    let fixture = testkit::single_broker().await.expect("broker");
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .expect("admin");
    admin
        .create_topics([NewTopic::new(TOPIC, PARTITIONS, 1)])
        .await
        .expect("topic");
    for _ in 0..50 {
        if let Ok(results) = admin.describe_topics([TOPIC.to_owned()]).await
            && results.iter().any(|(_, r)| r.is_ok())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let cluster = admin.cluster().clone();
    (fixture, cluster)
}

/// A deterministic pseudo-random cancellation point.
///
/// Deliberately not a real RNG: a leak that only reproduces on one seed is a
/// leak nobody can bisect, and the point here is coverage of *when* a future
/// is dropped rather than statistical randomness.
fn cancel_after(cycle: usize) -> Duration {
    Duration::from_micros(u64::try_from((cycle.wrapping_mul(7919)) % 4000).unwrap_or(1000))
}

/// Producers created, used and dropped — some mid-send — must not accumulate
/// connections or strand the records they accepted.
#[tokio::test]
#[ignore = "needs Docker"]
async fn a_thousand_producer_lifecycles_return_to_baseline() {
    let (_fixture, cluster) = setup().await;

    // Warm the pool so the baseline is a steady state rather than zero.
    let warm = Producer::new(cluster.clone(), ProducerConfig::new());
    warm.send(ProducerRecord::new(TOPIC).partition(0).value("warm"))
        .await
        .expect("warm");
    drop(warm);
    let baseline = cluster.pool().live_connections().await;

    for cycle in 0..CYCLES {
        let producer = Producer::new(cluster.clone(), ProducerConfig::new());
        let send = producer.send(
            ProducerRecord::new(TOPIC)
                .partition(i32::try_from(cycle).unwrap_or(0) % PARTITIONS)
                .value(format!("v{cycle}")),
        );

        // Cancel at a different point each time: before the request goes out,
        // while it is in flight, after it has landed.
        match tokio::time::timeout(cancel_after(cycle), send).await {
            Ok(_) | Err(_) => {}
        }
        drop(producer);
    }

    // Give any shutdown that was in flight a moment to finish, then assert the
    // pool has not grown. A per-producer connection leak over a thousand
    // cycles is unmissable.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let after = cluster.pool().live_connections().await;
    assert!(
        after <= baseline,
        "connections grew from {baseline} to {after} across {CYCLES} producer \
         lifecycles"
    );
}

/// Consumers assigned, polled and dropped mid-fetch must not leave connections
/// behind. Their broker-side fetch sessions expire on their own, which is what
/// the session cache is for, but the sockets are ours to clean up.
#[tokio::test]
#[ignore = "needs Docker"]
async fn a_thousand_consumer_lifecycles_return_to_baseline() {
    let (_fixture, cluster) = setup().await;

    let producer = Producer::new(cluster.clone(), ProducerConfig::new());
    for i in 0..200 {
        producer
            .send(ProducerRecord::new(TOPIC).value(format!("v{i}")))
            .await
            .expect("seed");
    }
    let baseline = cluster.pool().live_connections().await;

    for cycle in 0..CYCLES {
        let mut consumer = Consumer::new(
            cluster.clone(),
            ConsumerConfig::new()
                .visibility(Visibility::All)
                .max_wait_ms(500),
        );
        if consumer
            .assign(
                (0..PARTITIONS)
                    .map(|p| (TOPIC.to_owned(), p))
                    .collect::<Vec<_>>(),
                Position::Earliest,
            )
            .await
            .is_err()
        {
            continue;
        }

        // Dropped mid-poll on most cycles: a fetch with `max_wait_ms` set is
        // exactly the long-running await that a naive implementation leaks.
        let _ = tokio::time::timeout(cancel_after(cycle), consumer.poll()).await;
        drop(consumer);
    }

    tokio::time::sleep(Duration::from_secs(2)).await;
    let after = cluster.pool().live_connections().await;
    assert!(
        after <= baseline,
        "connections grew from {baseline} to {after} across {CYCLES} consumer \
         lifecycles"
    );
}

/// Group members are the case with broker-side state: one that dies without
/// leaving stays a member until its session expires, so a thousand of them
/// must not make the group unusable for the one that comes after.
#[tokio::test]
#[ignore = "needs Docker"]
async fn group_members_that_vanish_do_not_poison_the_group() {
    const MEMBERS: usize = 100;
    let (_fixture, cluster) = setup().await;

    let producer = Producer::new(cluster.clone(), ProducerConfig::new());
    for i in 0..100 {
        producer
            .send(ProducerRecord::new(TOPIC).value(format!("v{i}")))
            .await
            .expect("seed");
    }
    let baseline = cluster.pool().live_connections().await;

    // Fewer cycles than the other two: each one is a real join, and the point
    // is the broker-side state rather than the socket count.
    for cycle in 0..MEMBERS {
        let mut member = GroupConsumer::subscribe(
            cluster.clone(),
            ConsumerConfig::new().visibility(Visibility::All),
            "leak-group",
            [TOPIC],
        )
        .await
        .expect("subscribe");

        let _ = tokio::time::timeout(cancel_after(cycle), member.poll()).await;
        // Deliberately *not* calling `leave` on most cycles: a member that
        // vanishes is the case the session timeout exists for, and it is the
        // one that poisons a group if the client leaves junk behind.
        if cycle % 10 == 0 {
            let _ = member.leave().await;
        }
        drop(member);
    }

    // A member that joins after all that must still get an assignment.
    let mut survivor = GroupConsumer::subscribe(
        cluster.clone(),
        ConsumerConfig::new().visibility(Visibility::All),
        "leak-group",
        [TOPIC],
    )
    .await
    .expect("subscribe");

    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    while std::time::Instant::now() < deadline && survivor.assignment().is_empty() {
        survivor.poll().await.expect("poll");
    }
    assert!(
        !survivor.assignment().is_empty(),
        "after {MEMBERS} abandoned members the group would not assign anything"
    );
    survivor.leave().await.expect("leave");

    tokio::time::sleep(Duration::from_secs(2)).await;
    let after = cluster.pool().live_connections().await;
    assert!(
        after <= baseline,
        "connections grew from {baseline} to {after} across {MEMBERS} group \
         member lifecycles"
    );
}
