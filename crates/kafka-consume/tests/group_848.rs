//! M17 acceptance: KIP-848 consumer groups.
//!
//! `cargo test -p kafka-consume --test group_848 -- --ignored`
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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_consume::{ConsumerConfig, GroupConsumer, RevokedPartition};
use kafka_produce::{Producer, ProducerConfig, ProducerRecord};
use kafka_read::{Cluster, Visibility};
use testkit::{Cluster as _, KafkaCluster};

const TOPIC: &str = "group-848";
const PARTITIONS: i32 = 12;

/// One rebalance callback, as the listener saw it.
#[derive(Debug, Clone)]
enum Event {
    Revoke(Vec<RevokedPartition>),
    Assign(Vec<(String, i32)>),
}

/// The listener's log, shared with the test that asserts on it.
type Events = Arc<Mutex<Vec<Event>>>;

async fn setup() -> (KafkaCluster, Cluster) {
    let fixture = testkit::cluster(3).await.expect("cluster");
    fixture
        .wait_for_group_coordinator(Duration::from_secs(60))
        .await
        .expect("group coordinator");
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
                .enqueue(
                    // Explicitly round-robin. Keyless records go through the
                    // sticky partitioner, which fills one partition at a time
                    // by design — so the assertions below about *which*
                    // partitions carry a position were testing the
                    // partitioner's batching luck, not the rebalance.
                    ProducerRecord::new(TOPIC)
                        .partition(i32::try_from(i).expect("fits") % PARTITIONS)
                        .value(format!("v{i}")),
                )
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

    // Converge before asserting. The loop above stops when the last *record*
    // arrives, which says nothing about whether `b` has finished reconciling
    // the partitions `a` gave up — the takeover is a rebalance, and it
    // completes on the coordinator's schedule rather than the log's. The
    // assertion is unchanged and still fails if the takeover never happens;
    // it is only evaluated at a moment when it means something.
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline && b.assignment().len() != usize::try_from(PARTITIONS).unwrap()
    {
        b.poll().await.expect("b");
    }

    assert_eq!(
        b.assignment().len(),
        usize::try_from(PARTITIONS).unwrap(),
        "the survivor must take over every partition the leaver held"
    );
}

/// The rebalance hook fires **before** the partitions go, with the positions
/// they held, and the caller's flush lands ahead of the offset commit.
///
/// The ordering is the assertion. A hook that fires after the revocation is
/// worth nothing: by then another member owns the partition and may already be
/// writing, so a caller flushing its own state is racing somebody else.
#[tokio::test]
#[ignore = "needs Docker"]
async fn a_listener_is_told_what_it_is_losing_before_it_loses_it() {
    const RECORDS: usize = 500;

    let (_fixture, cluster) = setup().await;
    let group = "group-848-listener";

    let producer = Producer::new(cluster.clone(), ProducerConfig::new());
    let mut pending = Vec::with_capacity(RECORDS);
    for i in 0..RECORDS {
        pending.push(
            producer
                .enqueue(
                    // Explicitly round-robin. Keyless records go through the
                    // sticky partitioner, which fills one partition at a time
                    // by design — so the assertions below about *which*
                    // partitions carry a position were testing the
                    // partitioner's batching luck, not the rebalance.
                    ProducerRecord::new(TOPIC)
                        .partition(i32::try_from(i).expect("fits") % PARTITIONS)
                        .value(format!("v{i}")),
                )
                .await
                .expect("enqueued"),
        );
    }
    for delivery in pending {
        delivery.await.expect("delivered");
    }

    // What the listener saw, in the order it saw it.
    let seen: Events = Arc::new(Mutex::new(Vec::new()));

    struct Watcher {
        seen: Events,
    }

    impl kafka_consume::RebalanceListener for Watcher {
        fn on_revoke(
            &mut self,
            revoked: Vec<RevokedPartition>,
        ) -> futures::future::BoxFuture<'_, kafka_consume::Result<()>> {
            let seen = Arc::clone(&self.seen);
            Box::pin(async move {
                seen.lock().unwrap().push(Event::Revoke(revoked));
                Ok(())
            })
        }

        fn on_assign(
            &mut self,
            assigned: Vec<(String, i32)>,
        ) -> futures::future::BoxFuture<'_, kafka_consume::Result<()>> {
            let seen = Arc::clone(&self.seen);
            Box::pin(async move {
                seen.lock().unwrap().push(Event::Assign(assigned));
                Ok(())
            })
        }
    }

    let mut a = GroupConsumer::subscribe(cluster.clone(), config(), group, [TOPIC])
        .await
        .expect("a")
        .on_rebalance(Watcher {
            seen: Arc::clone(&seen),
        });

    // Read from *every* partition, so that whichever half `b` takes carries a
    // position. "Read something" was the old guard, and one record is one
    // partition out of twelve: the final assertion then depended on the
    // revoked half happening to include the partition that record came from.
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut read_from: BTreeSet<i32> = BTreeSet::new();
    while Instant::now() < deadline
        && (a.assignment().len() != usize::try_from(PARTITIONS).unwrap()
            || read_from.len() != usize::try_from(PARTITIONS).unwrap())
    {
        for record in a.poll().await.expect("a") {
            read_from.insert(record.partition);
        }
    }
    assert_eq!(
        read_from.len(),
        usize::try_from(PARTITIONS).unwrap(),
        "the member never read every partition, so a revoked one may carry no position"
    );

    let gained: Vec<(String, i32)> = seen
        .lock()
        .unwrap()
        .iter()
        .filter_map(|event| match event {
            Event::Assign(assigned) => Some(assigned.clone()),
            Event::Revoke(_) => None,
        })
        .flatten()
        .collect();
    assert_eq!(
        gained.len(),
        usize::try_from(PARTITIONS).unwrap(),
        "on_assign must report every partition the member gained"
    );

    // A second member forces a rebalance, which takes partitions away from the
    // first. That is the revocation the hook exists for.
    let mut b = GroupConsumer::subscribe(cluster.clone(), config(), group, [TOPIC])
        .await
        .expect("b");

    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline && a.assignment().len() == usize::try_from(PARTITIONS).unwrap()
    {
        a.poll().await.expect("a");
        b.poll().await.expect("b");
    }

    let revoked: Vec<RevokedPartition> = seen
        .lock()
        .unwrap()
        .iter()
        .filter_map(|event| match event {
            Event::Revoke(revoked) => Some(revoked.clone()),
            Event::Assign(_) => None,
        })
        .flatten()
        .collect();
    assert!(
        !revoked.is_empty(),
        "the hook never fired for a rebalance that plainly happened"
    );

    // The partitions it named are gone, and it was told while they were still
    // ours — this is the ordering assertion.
    let still_held: BTreeSet<(String, i32)> = a.assignment().into_iter().collect();
    for partition in &revoked {
        assert!(
            !still_held.contains(&(partition.topic.clone(), partition.partition)),
            "{}-{} was named as revoked but is still assigned",
            partition.topic,
            partition.partition
        );
        assert!(
            partition.position >= 0,
            "a revoked partition must carry the position it had reached"
        );
    }
    assert!(
        revoked.iter().any(|p| p.position > 0),
        "positions were all zero, so the hook fired after the state was reset"
    );

    // Let the handover finish before committing. The loop above stopped the
    // moment `a` *lost* partitions, which is the start of the rebalance and
    // not the end of it: `b` has been assigned nothing yet. Committing into a
    // group that is still moving is refused by the coordinator — correctly,
    // and with an error about the rebalance rather than anything to do with
    // callback ordering, which is what this assertion is actually for.
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline
        && (b.assignment().is_empty()
            || a.assignment().len() + b.assignment().len() != usize::try_from(PARTITIONS).unwrap())
    {
        a.poll().await.expect("a");
        b.poll().await.expect("b");
    }

    // And the offsets the group committed are consistent with what the hook was
    // told: auto-commit runs *after* the callback, never before it.
    let committed = b.commit().await.expect("commit");
    assert!(
        committed.iter().all(|(_, result)| result.is_ok()),
        "commit rejected after the group settled: {committed:?}"
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
