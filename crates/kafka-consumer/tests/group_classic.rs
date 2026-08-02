//! M18 acceptance: the classic group protocol.
//!
//! `cargo test -p kafka-consumer --test group_classic -- --ignored`
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
use kafka_consumer::{ClassicConsumer, ConsumerConfig};
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
/// deadlock. See `kafka_consumer::classic`.
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
#[tokio::test]
#[ignore = "needs Docker"]
async fn two_members_cover_every_partition_exactly_once() {
    let fixture = setup().await;
    let group = "classic-coverage";

    let mut a = member(&fixture, group).await;
    let mut b = member(&fixture, group).await;

    let deadline = Instant::now() + Duration::from_secs(120);
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
/// One `kafka-consumer` and one `kafka-console-consumer.sh` in the same classic
/// group. The Java client decodes the subscription we encode, and — if it is
/// elected leader — we decode the assignment it computes. A Rust-only group
/// exercises neither direction.
#[tokio::test]
#[ignore = "needs Docker"]
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
    let deadline = Instant::now() + Duration::from_secs(120);
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

/// A static member's assignment is parked across a restart rather than
/// rebalanced away (KIP-345).
#[tokio::test]
#[ignore = "needs Docker"]
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
        .with_instance_id("static-1");

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
        .with_instance_id("static-1");

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
