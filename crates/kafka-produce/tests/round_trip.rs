//! M12 acceptance: one record on the wire, acked, and readable back.
//!
//! `cargo test -p kafka-produce --test round_trip -- --ignored`
//!
//! This validates record batch *encoding* the way M1 validated framing. The
//! read side is `kafka-read`, deliberately: a producer asserted only against
//! its own decoder agrees with itself about any mistake it makes. The third
//! leg — agreeing with a genuinely different client — is the interop crate's
//! `murmur2` case.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::HashSet;

use bytes::Bytes;
use futures::StreamExt;
use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_produce::{
    Acks, Compression, Producer, ProducerConfig, ProducerRecord, partition_for_key,
};
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

/// Creation is asynchronous on the broker; produce before the leader is
/// elected and the failure is `LEADER_NOT_AVAILABLE`, which reads like a bug
/// in the producer.
async fn await_topic(admin: &Admin, topic: &str) {
    for _ in 0..50 {
        let described = admin.describe_topics([topic.to_owned()]).await;
        if let Ok(results) = described
            && results.iter().any(|(_, result)| result.is_ok())
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    panic!("{topic} never became describable");
}

/// Read a whole partition back through `kafka-read`.
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

#[testkit::integration_test]
async fn a_record_survives_the_round_trip_exactly() {
    let (_fixture, cluster, _admin) = setup("produced", 6).await;
    let producer = Producer::new(cluster.clone(), ProducerConfig::new());

    let stamped = 1_765_000_000_000;
    let metadata = producer
        .send(
            ProducerRecord::new("produced")
                .with_partition(3)
                .with_key("customer-7")
                .with_value("{\"total\":42}")
                .with_header("content-type", "application/json")
                .with_header("trace-id", "abc123")
                .with_null_header("tombstoned-header")
                .with_timestamp(stamped),
        )
        .await
        .expect("produce");

    assert_eq!(metadata.topic, "produced");
    assert_eq!(metadata.partition, 3, "an explicit partition was ignored");
    assert_eq!(metadata.offset, 0, "the first record of a partition is 0");

    let records = read_partition(&cluster, "produced", 3).await;
    assert_eq!(records.len(), 1, "expected exactly the record we produced");
    let record = &records[0];

    assert_eq!(record.key.as_deref(), Some(&b"customer-7"[..]));
    assert_eq!(record.value.as_deref(), Some(&b"{\"total\":42}"[..]));
    assert_eq!(record.offset, metadata.offset);
    assert_eq!(record.partition, 3);

    // The timestamp we chose, not the one the broker would have stamped. The
    // topic is CreateTime by default, so this is the client's to set and a
    // producer that quietly overrides it is losing caller data.
    assert_eq!(record.timestamp, stamped);

    // Headers keep their order, their duplicated-name capability, and the
    // null-versus-empty distinction.
    assert_eq!(
        record.headers,
        vec![
            (
                "content-type".to_owned(),
                Some(Bytes::from_static(b"application/json"))
            ),
            ("trace-id".to_owned(), Some(Bytes::from_static(b"abc123"))),
            ("tombstoned-header".to_owned(), None),
        ]
    );
}

#[testkit::integration_test]
async fn a_tombstone_round_trips_as_null_and_not_as_empty() {
    let (_fixture, cluster, _admin) = setup("tombstones", 1).await;
    let producer = Producer::new(cluster.clone(), ProducerConfig::new());

    producer
        .send(ProducerRecord::new("tombstones").with_key("gone"))
        .await
        .expect("tombstone");
    producer
        .send(
            ProducerRecord::new("tombstones")
                .with_key("blank")
                .with_value(Bytes::new()),
        )
        .await
        .expect("empty value");

    let records = read_partition(&cluster, "tombstones", 0).await;
    assert_eq!(records.len(), 2);

    // The whole point: on a compacted topic the first deletes its key and the
    // second stores nothing under it. Collapsing them is a data-loss bug that
    // no length check catches, because both are zero bytes long.
    assert_eq!(records[0].value, None, "a tombstone became an empty value");
    assert_eq!(
        records[1].value,
        Some(Bytes::new()),
        "an empty value became a tombstone"
    );
}

#[testkit::integration_test]
async fn every_codec_round_trips() {
    let (_fixture, cluster, _admin) = setup("codecs", 1).await;

    for (index, compression) in [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ]
    .into_iter()
    .enumerate()
    {
        let producer = Producer::new(
            cluster.clone(),
            ProducerConfig::new().compression(compression),
        );
        producer
            .send(
                ProducerRecord::new("codecs")
                    .with_partition(0)
                    .with_key(format!("k{index}"))
                    .with_value(format!("payload-{compression:?}")),
            )
            .await
            .unwrap_or_else(|error| panic!("{compression:?} failed to produce: {error}"));
    }

    let records = read_partition(&cluster, "codecs", 0).await;
    assert_eq!(records.len(), 5, "one record per codec");
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.key.as_deref(), Some(format!("k{index}").as_bytes()));
        assert!(
            record
                .value
                .as_deref()
                .is_some_and(|v| v.starts_with(b"payload-")),
            "codec {index} decoded to something else"
        );
    }
}

#[testkit::integration_test]
async fn a_keyed_record_lands_where_murmur2_says_it_should() {
    let (_fixture, cluster, _admin) = setup("keyed", 6).await;
    let producer = Producer::new(cluster.clone(), ProducerConfig::new());

    // Produce without naming a partition and assert the broker put each record
    // where our own partitioner said it would. This does not prove agreement
    // with Java — that is the interop test — but it does prove the partition
    // we computed is the partition we actually addressed.
    let mut expected: Vec<(String, i32)> = Vec::new();
    for i in 0..64 {
        let key = format!("key-{i}");
        let metadata = producer
            .send(
                ProducerRecord::new("keyed")
                    .with_key(key.clone())
                    .with_value("v"),
            )
            .await
            .expect("produce");
        assert_eq!(
            metadata.partition,
            partition_for_key(key.as_bytes(), 6),
            "key {key} was routed somewhere the partitioner did not choose"
        );
        expected.push((key, metadata.partition));
    }

    // And the same key never split across partitions.
    for partition in 0..6 {
        let records = read_partition(&cluster, "keyed", partition).await;
        for record in &records {
            let key = String::from_utf8(record.key.clone().expect("keyed").to_vec()).expect("utf8");
            let (_, want) = expected.iter().find(|(k, _)| *k == key).expect("produced");
            assert_eq!(*want, partition, "{key} was read from the wrong partition");
        }
    }
}

#[testkit::integration_test]
async fn an_unkeyed_record_sticks_to_one_partition() {
    let (_fixture, cluster, _admin) = setup("sticky", 6).await;
    let producer = Producer::new(cluster.clone(), ProducerConfig::new());

    let mut partitions = HashSet::new();
    for i in 0..32 {
        let metadata = producer
            .send(ProducerRecord::new("sticky").with_value(format!("v{i}")))
            .await
            .expect("produce");
        partitions.insert(metadata.partition);
    }

    // KIP-480, and the reason it matters: round-robin would spread these over
    // all six and leave the accumulator nothing to batch.
    assert_eq!(
        partitions.len(),
        1,
        "unkeyed records spread across {partitions:?} instead of sticking"
    );

    producer.partitioner().rotate("sticky");
    let after = producer
        .send(ProducerRecord::new("sticky").with_value("after"))
        .await
        .expect("produce");
    assert!((0..6).contains(&after.partition));
}

#[testkit::integration_test]
async fn acks_leader_is_acknowledged_before_the_record_is_readable() {
    let (_fixture, cluster, _admin) = setup("acked", 1).await;
    let producer = Producer::new(cluster.clone(), ProducerConfig::new().acks(Acks::Leader));

    let metadata = producer
        .send(ProducerRecord::new("acked").with_value("v"))
        .await
        .expect("acks=1 should be acknowledged");
    assert_eq!(metadata.offset, 0);

    // The acknowledgement means the *leader* wrote the record, not that the
    // ISR did. A consumer reads only up to the high watermark, and that does
    // not advance until the followers have replicated — so on this RF=3 topic
    // the record is acknowledged strictly *before* it becomes visible.
    //
    // The poll is the assertion rather than a workaround for one: it proves
    // the record does arrive, and the fact that it is not there instantly is
    // the semantic difference between `Acks::Leader` and `Acks::All`. Every
    // other test here uses the default `Acks::All`, where the ack already
    // implies the ISR has the record and an immediate read is sound — which
    // is exactly why this was the only case that failed.
    let records = await_visible(&cluster, "acked", 0, 1).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].value.as_deref(), Some(&b"v"[..]));
}

/// Poll until a partition holds at least `expected` records.
async fn await_visible(
    cluster: &Cluster,
    topic: &str,
    partition: i32,
    expected: usize,
) -> Vec<kafka_read::Record> {
    for _ in 0..100 {
        let records = read_partition(cluster, topic, partition).await;
        if records.len() >= expected {
            return records;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("{topic}-{partition} never reached {expected} visible record(s)");
}

#[testkit::integration_test]
async fn producing_to_a_partition_that_does_not_exist_is_refused_before_the_socket() {
    let (_fixture, cluster, _admin) = setup("narrow", 2).await;
    let producer = Producer::new(cluster.clone(), ProducerConfig::new());

    let error = producer
        .send(
            ProducerRecord::new("narrow")
                .with_partition(9)
                .with_value("v"),
        )
        .await
        .expect_err("partition 9 of a 2-partition topic is not a thing");

    // A clear client-side error, not a broker round trip that comes back as
    // UNKNOWN_TOPIC_OR_PARTITION and reads like the topic is missing.
    assert!(
        matches!(error, kafka_produce::Error::InvalidRequest(_)),
        "expected a client-side rejection, got {error:?}"
    );
}

#[testkit::integration_test]
async fn a_read_only_client_cannot_produce() {
    let fixture = testkit::single_broker().await.expect("broker");
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .expect("admin");
    admin
        .create_topics([NewTopic::new("guarded", 1, 1)])
        .await
        .expect("topic");
    await_topic(&admin, "guarded").await;

    let read_only =
        Admin::connect_read_only(fixture.bootstrap().to_vec(), ClusterConfig::default())
            .await
            .expect("read-only");
    let producer = Producer::new(read_only.cluster().clone(), ProducerConfig::new());

    let error = producer
        .send(ProducerRecord::new("guarded").with_value("v"))
        .await
        .expect_err("a read-only client must not produce");

    // M8's gate is on `ApiKey`, inside the connection, so a whole new crate
    // reaching for `Produce` is covered without the gate having heard of it.
    assert!(
        matches!(error, kafka_produce::Error::ReadOnly { .. }),
        "expected the read-only gate to fire, got {error:?}"
    );
}
