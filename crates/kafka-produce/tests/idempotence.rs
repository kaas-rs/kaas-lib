//! M14 acceptance: idempotence.
//!
//! `cargo test -p kafka-produce --test idempotence -- --ignored`
//!
//! The property under test is not "records arrive" — M13 proved that. It is
//! that they arrive **exactly once, in order, across a leader election**. Those
//! are three separate failure modes and a test that only counts records catches
//! none of them:
//!
//! * a duplicate looks like a *successful* write, twice;
//! * a gap looks like a write that failed and was reported as failing;
//! * a reordering looks like nothing at all until someone reads the log.
//!
//! So the assertion is on the exact sequence, not the count.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::time::Duration;

use futures::StreamExt;
use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_produce::{Producer, ProducerConfig, ProducerRecord};
use kafka_read::{Cluster, ScanEvent, ScanSpec, StartPosition};
use testkit::{Cluster as _, KafkaCluster};

async fn setup(topic: &str, partitions: i32) -> (KafkaCluster, Cluster, Admin) {
    let fixture = testkit::cluster(3).await.expect("cluster");
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .expect("admin");
    admin
        .create_topics([NewTopic::new(topic, partitions, 3)])
        .await
        .expect("topic");
    await_topic(&admin, topic).await;
    let cluster = admin.cluster().clone();
    (fixture, cluster, admin)
}

async fn await_topic(admin: &Admin, topic: &str) {
    for _ in 0..50 {
        let described = admin.describe_topics([topic.to_owned()]).await;
        if let Ok(results) = described
            && results.iter().any(|(_, result)| result.is_ok())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("{topic} never became describable");
}

async fn read_partition(cluster: &Cluster, topic: &str, partition: i32) -> Vec<kafka_read::Record> {
    let spec = ScanSpec::new(topic)
        .partitions([partition])
        .from(StartPosition::Earliest);
    let mut stream = Box::pin(kafka_read::scan(cluster, spec).await.expect("scan"));

    let mut records = Vec::new();
    while let Some(event) = stream.next().await {
        match event.expect("scan event") {
            ScanEvent::Record(record) => records.push(record),
            ScanEvent::Malformed { offset, reason, .. } => {
                panic!("we wrote offset {offset} and could not read it: {reason}")
            }
            _ => {}
        }
    }
    records
}

/// Which container index hosts the leader of a partition.
///
/// The fixture's node order and the broker ids the cluster reports are not the
/// same numbering, so this maps one to the other rather than assuming.
async fn leader_index(cluster: &Cluster, fixture: &KafkaCluster, topic: &str) -> usize {
    let snapshot = cluster.refresh_topics(&[topic]).await.expect("metadata");
    let leader = snapshot
        .topic(topic)
        .and_then(|info| info.partition(0))
        .and_then(|info| info.leader)
        .expect("partition 0 has a leader");

    for index in 0..fixture.nodes() {
        let address = fixture.bootstrap_for(index).expect("bootstrap");
        let port = address.rsplit(':').next().expect("port");
        if let Some(broker) = snapshot.broker(leader)
            && broker.port.to_string() == port
        {
            return index;
        }
    }
    panic!("no fixture node matches broker {leader}");
}

/// The acceptance case: 20k records to one partition, through a leader that
/// dies and comes back.
#[testkit::integration_test]
async fn twenty_thousand_records_survive_a_leader_restart_exactly_once_and_in_order() {
    const TOPIC: &str = "idempotence-restart";
    const RECORDS: usize = 20_000;

    let (fixture, cluster, _admin) = setup(TOPIC, 1).await;
    let producer = Producer::new(cluster.clone(), ProducerConfig::new());

    // Send a first record so the producer has claimed its id and the pool has
    // a connection to the leader before anything is disturbed.
    producer
        .send(ProducerRecord::new(TOPIC).with_partition(0).with_value("0"))
        .await
        .expect("first record");

    let victim = leader_index(&cluster, &fixture, TOPIC).await;

    // Enqueue everything, then kill the leader while the requests are in
    // flight. The records already accepted must still be delivered — that is
    // what an ambiguous failure being retriable means.
    let mut pending = Vec::with_capacity(RECORDS);
    for i in 1..RECORDS {
        pending.push(
            producer
                .enqueue(
                    ProducerRecord::new(TOPIC)
                        .with_partition(0)
                        .with_value(format!("{i}")),
                )
                .await
                .expect("enqueued"),
        );

        // Mid-stream, not at the start: the point is to interrupt requests
        // that are already on the wire.
        if i == RECORDS / 2 {
            fixture.stop_node(victim).await.expect("stopped the leader");
        }
    }

    // Bring it back, so the test measures recovery rather than only failover.
    fixture.start_node(victim).await.expect("restarted");

    let mut delivered = 0;
    let mut failed = Vec::new();
    for delivery in pending {
        match delivery.await {
            Ok(_) => delivered += 1,
            Err(error) => failed.push(error.to_string()),
        }
    }

    assert!(
        failed.is_empty(),
        "an idempotent producer must ride out a leader election; {} records \
         failed, first: {}",
        failed.len(),
        failed.first().map_or("-", String::as_str)
    );
    assert_eq!(delivered, RECORDS - 1);

    // Poll: the restarted broker has to catch up before a scan sees the whole
    // log, and asserting immediately measures replication lag, not delivery.
    let mut records = Vec::new();
    for _ in 0..60 {
        records = read_partition(&cluster, TOPIC, 0).await;
        if records.len() >= RECORDS {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let observed: Vec<usize> = records
        .iter()
        .filter_map(|record| record.value.as_ref())
        .filter_map(|value| String::from_utf8_lossy(value).parse().ok())
        .collect();

    // All three failure modes at once. `assert_eq` on the whole sequence is
    // deliberate: a count catches gaps, a set catches duplicates, and only the
    // sequence catches reordering.
    let expected: Vec<usize> = (0..RECORDS).collect();
    assert_eq!(
        observed.len(),
        RECORDS,
        "expected {RECORDS} records, read {} — a gap or a duplicate",
        observed.len()
    );
    assert_eq!(
        observed, expected,
        "the log does not match what was produced: a duplicate, a gap or a \
         reordering survived the leader restart"
    );
}

/// The clamp PLAN.md names, asserted where a caller can see it.
#[testkit::integration_test]
async fn a_non_idempotent_producer_clamps_its_connections_to_one_in_flight() {
    const TOPIC: &str = "idempotence-in-flight";

    let fixture = testkit::cluster(3).await.expect("cluster");

    let lossy = Producer::connect(
        fixture.bootstrap().to_vec(),
        ClusterConfig::default(),
        ProducerConfig::new().idempotent(false),
    )
    .await
    .expect("producer");
    assert_eq!(
        lossy.max_in_flight(),
        1,
        "without sequence numbers a retried batch can overtake a later one, \
         so one is the only safe number"
    );

    let safe = Producer::connect(
        fixture.bootstrap().to_vec(),
        ClusterConfig::default(),
        ProducerConfig::new(),
    )
    .await
    .expect("producer");
    assert_eq!(
        safe.max_in_flight(),
        5,
        "the broker tracks five in-flight sequence windows per partition"
    );

    // And the clamped producer still works, so the clamp is not a way of
    // quietly disabling the thing it protects.
    let admin = Admin::new(safe.cluster().clone());
    admin
        .create_topics([NewTopic::new(TOPIC, 1, 3)])
        .await
        .expect("topic");
    await_topic(&admin, TOPIC).await;
    lossy
        .send(ProducerRecord::new(TOPIC).with_partition(0).with_value("v"))
        .await
        .expect("a clamped producer still produces");
}

/// A producer id is claimed once and reused, not re-claimed per batch.
///
/// Re-claiming would bump the epoch and fence the producer against itself,
/// which presents as every second batch failing with `INVALID_PRODUCER_EPOCH`.
#[testkit::integration_test]
async fn the_producer_id_is_claimed_once_for_many_batches() {
    const TOPIC: &str = "idempotence-one-id";
    const RECORDS: usize = 5_000;

    let (_fixture, cluster, _admin) = setup(TOPIC, 3).await;
    let producer = Producer::new(cluster.clone(), ProducerConfig::new());

    let mut pending = Vec::with_capacity(RECORDS);
    for i in 0..RECORDS {
        pending.push(
            producer
                .enqueue(ProducerRecord::new(TOPIC).with_value(format!("v{i}")))
                .await
                .expect("enqueued"),
        );
    }
    for delivery in pending {
        delivery.await.expect("every batch shares one producer id");
    }

    producer.flush().await.expect("flushed");
}
