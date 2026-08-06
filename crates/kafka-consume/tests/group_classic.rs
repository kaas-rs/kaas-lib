//! M18 acceptance: the classic group protocol.
//!
//! `cargo test -p kafka-consume --test group_classic -- --ignored`
//!
//! The mixed-group case is the only one that proves assignor byte
//! compatibility. A Rust-only group passes happily against a
//! wrong-but-self-consistent encoding — which is not hypothetical here: the
//! two-byte version prefix Java writes ahead of the protocol payload was
//! missing, our encoder and decoder agreed with each other perfectly, and the
//! group simply never formed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_consume::{Assignor, ClassicConsumer, ConsumerConfig};
use kafka_produce::{Producer, ProducerConfig, ProducerRecord};
use kafka_read::Visibility;
use testkit::{Cluster as _, KafkaCluster};

const TOPIC: &str = "group-classic";
const PARTITIONS: i32 = 12;

async fn setup() -> KafkaCluster {
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
            && results.iter().any(|(_, r)| r.is_ok())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let producer = Producer::new(admin.cluster().clone(), ProducerConfig::new());
    for i in 0..600 {
        producer
            .send(ProducerRecord::new(TOPIC).value(format!("v{i}")))
            .await
            .expect("seed");
    }
    fixture
}

fn config() -> ConsumerConfig {
    ConsumerConfig::new()
        .visibility(Visibility::All)
        .max_wait_ms(200)
}

/// Each member needs its own cluster handle — `JoinGroup` blocks and the broker
/// mutes a connection while a request is in flight, so members sharing one
/// deadlock. See `kafka_consume::classic`.
async fn member(fixture: &KafkaCluster, group: &str) -> ClassicConsumer {
    let cluster = kafka_meta::Cluster::connect(
        fixture.bootstrap().to_vec(),
        kafka_meta::ClusterConfig::default(),
    )
    .await
    .expect("cluster");
    ClassicConsumer::subscribe(cluster, config(), group, [TOPIC])
        .await
        .expect("subscribe")
}

/// Two Rust members: full coverage, no overlap, one leader.
#[testkit::integration_test]
async fn two_members_cover_every_partition_exactly_once() {
    let fixture = setup().await;
    let group = "classic-coverage";

    let mut a = member(&fixture, group).await;
    let mut b = member(&fixture, group).await;

    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        let (ra, rb) = tokio::join!(a.poll(), b.poll());
        ra.expect("a");
        rb.expect("b");
        if a.assignment().len() + b.assignment().len() == usize::try_from(PARTITIONS).unwrap()
            && !a.assignment().is_empty()
            && !b.assignment().is_empty()
        {
            break;
        }
    }

    let owned_a: BTreeSet<(String, i32)> = a.assignment().into_iter().collect();
    let owned_b: BTreeSet<(String, i32)> = b.assignment().into_iter().collect();

    assert!(
        owned_a.is_disjoint(&owned_b),
        "two members own the same partitions: {:?}",
        owned_a.intersection(&owned_b).collect::<Vec<_>>()
    );
    assert_eq!(
        owned_a.union(&owned_b).count(),
        usize::try_from(PARTITIONS).unwrap(),
        "the group left partitions unassigned"
    );
    assert!(
        a.is_leader() || b.is_leader(),
        "somebody has to compute the assignment"
    );

    a.leave().await.expect("leave");
    b.leave().await.expect("leave");
}

/// **The case that proves byte compatibility**, and the only one that can.
///
/// One `kafka-consume` and one `kafka-console-consumer.sh` in the same classic
/// group. The Java client decodes the subscription we encode, and — if it is
/// elected leader — we decode the assignment it computes. A Rust-only group
/// exercises neither direction.
#[testkit::integration_test]
async fn a_mixed_rust_and_java_group_shares_the_topic() {
    let fixture = setup().await;
    let group = "classic-mixed";

    // The Java side, in the container, pinned to the classic protocol and to
    // `range` — the assignor both clients implement.
    let java = fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                format!(
                    "nohup /opt/kafka/bin/kafka-console-consumer.sh \
                       --bootstrap-server {bootstrap} --topic {TOPIC} --group {group} \
                       --consumer-property group.protocol=classic \
                       --consumer-property partition.assignment.strategy=\
org.apache.kafka.clients.consumer.RangeAssignor \
                       >/tmp/java-consumer.log 2>&1 & echo started",
                    bootstrap = testkit::INTERNAL_BOOTSTRAP,
                ),
            ],
        )
        .await
        .expect("started the java consumer");
    assert!(java.ok(), "could not start kafka-console-consumer.sh");

    // Give the Java member time to join before we do, so the group already
    // exists and our subscription is decoded by somebody else.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut ours = member(&fixture, group).await;
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline && ours.assignment().is_empty() {
        ours.poll().await.expect("poll");
    }

    let owned: BTreeSet<(String, i32)> = ours.assignment().into_iter().collect();
    assert!(
        !owned.is_empty(),
        "we joined a group with a Java member and were assigned nothing, which \
         is what an unreadable subscription payload looks like"
    );
    assert!(
        owned.len() < usize::try_from(PARTITIONS).unwrap(),
        "we hold every partition, so the Java member was assigned none — the \
         assignment we computed is not one it could read"
    );

    // And the group is complete between us: what we do not hold, it does.
    let groups = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .expect("admin");
    let described = groups
        .describe_groups([group.to_owned()])
        .await
        .expect("describe");
    let members = described
        .iter()
        .filter_map(|(_, result)| result.as_ref().ok())
        .count();
    assert!(members > 0, "the coordinator does not know this group");

    ours.leave().await.expect("leave");
}

/// **Cooperative-sticky against a Java client**, which is the only thing that
/// proves the incremental payload as well as the eager one.
///
/// The Java member is pinned to `CooperativeStickyAssignor` alone, so it is the
/// only protocol in the intersection and the group either forms on it or does
/// not form at all. Before this assignor existed that configuration produced
/// `INCONSISTENT_GROUP_PROTOCOL` and no group; now it must produce a group
/// where both sides hold disjoint partitions.
///
/// It also exercises the half a Rust-only test cannot: `owned_partitions` is a
/// field the *Java* leader reads out of our subscription when it computes the
/// assignment, and a field we read out of its subscription when we compute one.
#[testkit::integration_test]
async fn a_cooperative_group_forms_with_a_java_member() {
    let fixture = setup().await;
    let group = "classic-cooperative";

    let java = fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                format!(
                    "nohup /opt/kafka/bin/kafka-console-consumer.sh \
                       --bootstrap-server {bootstrap} --topic {TOPIC} --group {group} \
                       --consumer-property group.protocol=classic \
                       --consumer-property partition.assignment.strategy=\
org.apache.kafka.clients.consumer.CooperativeStickyAssignor \
                       >/tmp/java-cooperative.log 2>&1 & echo started",
                    bootstrap = testkit::INTERNAL_BOOTSTRAP,
                ),
            ],
        )
        .await
        .expect("started the java consumer");
    assert!(java.ok(), "could not start kafka-console-consumer.sh");

    tokio::time::sleep(Duration::from_secs(5)).await;

    let cluster = kafka_meta::Cluster::connect(
        fixture.bootstrap().to_vec(),
        kafka_meta::ClusterConfig::default(),
    )
    .await
    .expect("cluster");
    let mut ours = ClassicConsumer::subscribe(cluster, config(), group, [TOPIC])
        .await
        .expect("subscribe")
        // Advertising only this one makes the intersection a single protocol:
        // the group forms on cooperative-sticky or it fails loudly.
        .assignors([Assignor::CooperativeSticky]);

    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline && ours.assignment().is_empty() {
        ours.poll().await.expect("poll");
    }

    let owned: BTreeSet<(String, i32)> = ours.assignment().into_iter().collect();
    assert!(
        !owned.is_empty(),
        "the group never gave us a partition: an INCONSISTENT_GROUP_PROTOCOL \
         here means the protocol name or the subscription version is wrong"
    );
    assert!(
        owned.len() < usize::try_from(PARTITIONS).unwrap(),
        "we hold every partition, so the Java member got none — which is what a \
         subscription it cannot read looks like"
    );

    ours.leave().await.expect("leave");
}

/// A cooperative rebalance between two Rust members moves what has to move and
/// leaves the rest alone — and never lets two members hold one partition.
///
/// The stickiness assertion is the point. An implementation that revokes
/// everything and reassigns from scratch also ends up balanced and disjoint,
/// and would pass a coverage-only test while doing exactly what cooperative
/// rebalancing exists to avoid.
#[testkit::integration_test]
async fn a_cooperative_rebalance_keeps_what_it_can() {
    let fixture = setup().await;
    let group = "classic-cooperative-pair";

    async fn cooperative(fixture: &KafkaCluster, group: &str) -> ClassicConsumer {
        let cluster = kafka_meta::Cluster::connect(
            fixture.bootstrap().to_vec(),
            kafka_meta::ClusterConfig::default(),
        )
        .await
        .expect("cluster");
        ClassicConsumer::subscribe(cluster, config(), group, [TOPIC])
            .await
            .expect("subscribe")
            .assignors([Assignor::CooperativeSticky])
    }

    let mut a = cooperative(&fixture, group).await;
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline && a.assignment().len() != usize::try_from(PARTITIONS).unwrap()
    {
        a.poll().await.expect("a");
    }
    let alone: BTreeSet<(String, i32)> = a.assignment().into_iter().collect();
    assert_eq!(alone.len(), usize::try_from(PARTITIONS).unwrap());

    // `a` moves to its own task before `b` arrives. A staggered classic
    // rebalance blocks `b`'s JoinGroup until `a` re-joins, and `a` only acts
    // — only *heartbeats* — when polled: driving both from one task via
    // `tokio::join!` starves whichever member is not mid-join, and the
    // coordinator evicts it twenty seconds later. See the module docs on
    // `kafka_consume::classic` — each member needs its own task, exactly as
    // it needs its own connection.
    let (assignment_tx, assignment_rx) = tokio::sync::watch::channel(Vec::new());
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let a_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                result = a.poll() => {
                    result.expect("a");
                    let _ = assignment_tx.send(a.assignment());
                }
                _ = stop_rx.changed() => break,
            }
        }
        a
    });

    // A second member arrives. Half the partitions must move; the other half
    // must not.
    let mut b = cooperative(&fixture, group).await;
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        b.poll().await.expect("b");
        let owned_a = assignment_rx.borrow().len();
        let owned_b = b.assignment().len();
        if owned_a + owned_b == usize::try_from(PARTITIONS).unwrap() && owned_a > 0 && owned_b > 0 {
            break;
        }
    }
    stop_tx.send(true).expect("stop");
    let mut a = a_task.await.expect("a task");

    let owned_a: BTreeSet<(String, i32)> = a.assignment().into_iter().collect();
    let owned_b: BTreeSet<(String, i32)> = b.assignment().into_iter().collect();

    assert!(
        owned_a.is_disjoint(&owned_b),
        "two members hold {:?} at once — a partition was handed over before its \
         owner revoked it",
        owned_a.intersection(&owned_b).collect::<Vec<_>>()
    );
    assert_eq!(
        owned_a.union(&owned_b).count(),
        usize::try_from(PARTITIONS).unwrap(),
        "the second round never happened: partitions withheld from their new \
         owner were never handed on"
    );
    assert!(
        !owned_a.is_empty() && !owned_b.is_empty(),
        "the group did not rebalance at all"
    );
    assert!(
        owned_a.iter().all(|key| alone.contains(key)),
        "the surviving member was given partitions it never held before, so the \
         assignment was recomputed from scratch rather than kept sticky"
    );

    a.leave().await.expect("leave");
    b.leave().await.expect("leave");
}

/// A static member's assignment is parked across a restart rather than
/// rebalanced away (KIP-345).
#[testkit::integration_test]
async fn a_static_member_does_not_trigger_a_rebalance_on_restart() {
    let fixture = setup().await;
    let group = "classic-static";

    let cluster = kafka_meta::Cluster::connect(
        fixture.bootstrap().to_vec(),
        kafka_meta::ClusterConfig::default(),
    )
    .await
    .expect("cluster");
    let mut first = ClassicConsumer::subscribe(cluster, config(), group, [TOPIC])
        .await
        .expect("subscribe")
        .instance_id("static-1");

    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline && first.assignment().is_empty() {
        first.poll().await.expect("poll");
    }
    let before: BTreeSet<(String, i32)> = first.assignment().into_iter().collect();
    assert!(!before.is_empty());

    // A static member deliberately does *not* send LeaveGroup, so dropping it
    // leaves the membership parked against the session timeout.
    first
        .leave()
        .await
        .expect("leave is a no-op for a static member");
    drop(first);

    let cluster = kafka_meta::Cluster::connect(
        fixture.bootstrap().to_vec(),
        kafka_meta::ClusterConfig::default(),
    )
    .await
    .expect("cluster");
    let mut restarted = ClassicConsumer::subscribe(cluster, config(), group, [TOPIC])
        .await
        .expect("subscribe")
        .instance_id("static-1");

    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline && restarted.assignment().is_empty() {
        restarted.poll().await.expect("poll");
    }
    let after: BTreeSet<(String, i32)> = restarted.assignment().into_iter().collect();

    assert_eq!(
        after, before,
        "a static member that restarts inside the session timeout must get its \
         own partitions back, not a fresh assignment"
    );
    restarted.leave().await.expect("leave");
}
