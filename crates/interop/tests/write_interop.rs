//! M19 acceptance: the write and consume paths against a different client.
//!
//! `cargo xtask interop`
//!
//! M11 proved `rdkafka` → `kafka-read`. This is the other direction, and it is
//! the one that matters more: a decoder that misreads a field usually fails
//! loudly, whereas an **encoder** that writes a field wrongly produces bytes
//! our own decoder reads back perfectly and every other client in the
//! ecosystem misreads. `kafka-produce/tests/roundtrip.rs` cannot catch that by
//! construction — both halves share our reading of the spec. Only a genuinely
//! different implementation settles it.
//!
//! What is under test, specifically:
//!
//! * **murmur2 partitioning** — the same key must land in the same partition
//!   for both clients, or a keyed topic silently splits its keyspace in two.
//! * **snappy's xerial framing** — the newest code in the dependency, rewritten
//!   in `kafka-protocol` 0.17.0 and mutually incompatible with Java before it.
//! * **tombstones** — a null value must stay null, not become empty, or
//!   compaction stops deleting.
//! * **headers** — order and null values, which a map would destroy.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::HashMap;
use std::time::Duration;

use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_produce::{Compression, Producer, ProducerConfig, ProducerRecord};
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use rdkafka::message::Headers as _;
use testkit::Cluster as _;

async fn setup(topic: &str, partitions: i32) -> (testkit::KafkaCluster, kafka_produce::Cluster) {
    let fixture = testkit::single_broker().await.expect("broker");
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .expect("admin");
    admin
        .create_topics([NewTopic::new(topic, partitions, 1)])
        .await
        .expect("topic");
    for _ in 0..50 {
        if let Ok(results) = admin.describe_topics([topic.to_owned()]).await
            && results.iter().any(|(_, r)| r.is_ok())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let cluster = admin.cluster().clone();
    (fixture, cluster)
}

fn rd_consumer(bootstrap: &str, group: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("rdkafka consumer")
}

/// Read `expected` messages with rdkafka, or fail.
async fn drain(consumer: &StreamConsumer, expected: usize) -> Vec<rdkafka::message::OwnedMessage> {
    let mut out = Vec::with_capacity(expected);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while out.len() < expected && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await {
            Ok(Ok(message)) => out.push(message.detach()),
            Ok(Err(error)) => panic!("rdkafka consume failed: {error}"),
            Err(_) => break,
        }
    }
    out
}

/// Every codec we write must be readable by librdkafka — snappy above all.
#[tokio::test]
#[ignore = "needs Docker"]
async fn what_we_produce_rdkafka_can_consume_through_every_codec() {
    for (codec, topic) in [
        (Compression::None, "w-interop-none"),
        (Compression::Gzip, "w-interop-gzip"),
        (Compression::Snappy, "w-interop-snappy"),
        (Compression::Lz4, "w-interop-lz4"),
        (Compression::Zstd, "w-interop-zstd"),
    ] {
        const RECORDS: usize = 500;
        let (fixture, cluster) = setup(topic, 1).await;
        let bootstrap = fixture.bootstrap()[0].clone();

        let producer = Producer::new(cluster, ProducerConfig::new().compression(codec));
        let mut pending = Vec::with_capacity(RECORDS);
        for i in 0..RECORDS {
            pending.push(
                producer
                    .enqueue(
                        ProducerRecord::new(topic)
                            .with_partition(0)
                            .with_key(format!("k{i}"))
                            .with_value(format!("v{i}")),
                    )
                    .await
                    .expect("enqueued"),
            );
        }
        for delivery in pending {
            delivery.await.expect("delivered");
        }

        let consumer = rd_consumer(&bootstrap, &format!("g-{topic}"));
        consumer.subscribe(&[topic]).expect("subscribe");
        let messages = drain(&consumer, RECORDS).await;

        assert_eq!(
            messages.len(),
            RECORDS,
            "{codec:?}: rdkafka could not read back everything we wrote"
        );
        for (i, message) in messages.iter().enumerate() {
            assert_eq!(
                message.payload(),
                Some(format!("v{i}").as_bytes()),
                "{codec:?}: record {i} decoded differently by librdkafka"
            );
        }
    }
}

/// A tombstone must survive as a null value, or compaction stops deleting.
#[tokio::test]
#[ignore = "needs Docker"]
async fn a_tombstone_we_write_is_a_tombstone_to_rdkafka() {
    const TOPIC: &str = "w-interop-tombstone";
    let (fixture, cluster) = setup(TOPIC, 1).await;
    let bootstrap = fixture.bootstrap()[0].clone();

    let producer = Producer::new(cluster, ProducerConfig::new());
    producer
        .send(
            ProducerRecord::new(TOPIC)
                .with_partition(0)
                .with_key("gone"),
        )
        .await
        .expect("tombstone");
    producer
        .send(
            ProducerRecord::new(TOPIC)
                .with_partition(0)
                .with_key("blank")
                .with_value(bytes::Bytes::new()),
        )
        .await
        .expect("empty");

    let consumer = rd_consumer(&bootstrap, "g-tombstone");
    consumer.subscribe(&[TOPIC]).expect("subscribe");
    let messages = drain(&consumer, 2).await;
    assert_eq!(messages.len(), 2);

    assert_eq!(
        messages[0].payload(),
        None,
        "our tombstone arrived at librdkafka as a value"
    );
    assert_eq!(
        messages[1].payload(),
        Some(&b""[..]),
        "our empty value arrived at librdkafka as a tombstone"
    );
}

/// The partitioner both clients must agree on, driven from the write side.
///
/// M12's case asserts our `partition_for_key` matches where rdkafka *puts* a
/// record. This asserts the reverse: where *we* put it is where rdkafka
/// expects to find it.
#[tokio::test]
#[ignore = "needs Docker"]
async fn our_murmur2_placement_is_where_rdkafka_looks() {
    const TOPIC: &str = "w-interop-murmur2";
    const PARTITIONS: i32 = 8;
    const KEYS: usize = 1_000;

    let (fixture, cluster) = setup(TOPIC, PARTITIONS).await;
    let bootstrap = fixture.bootstrap()[0].clone();

    let producer = Producer::new(cluster, ProducerConfig::new());
    let mut expected: HashMap<String, i32> = HashMap::new();
    let mut pending = Vec::with_capacity(KEYS);
    for i in 0..KEYS {
        let key = format!("key-{i}");
        expected.insert(
            key.clone(),
            kafka_produce::partition_for_key(key.as_bytes(), PARTITIONS),
        );
        pending.push(
            producer
                .enqueue(ProducerRecord::new(TOPIC).with_key(key).with_value("v"))
                .await
                .expect("enqueued"),
        );
    }
    for delivery in pending {
        delivery.await.expect("delivered");
    }

    let consumer = rd_consumer(&bootstrap, "g-murmur2");
    consumer.subscribe(&[TOPIC]).expect("subscribe");
    let messages = drain(&consumer, KEYS).await;
    assert_eq!(messages.len(), KEYS);

    for message in messages {
        let key = String::from_utf8_lossy(message.key().expect("key")).to_string();
        let ours = *expected.get(&key).expect("a key we wrote");
        assert_eq!(
            message.partition(),
            ours,
            "{key}: we computed partition {ours}, the broker holds it in {}",
            message.partition()
        );
    }
}

/// Headers keep their order and their null values across the client boundary.
#[tokio::test]
#[ignore = "needs Docker"]
async fn headers_we_write_reach_rdkafka_intact() {
    const TOPIC: &str = "w-interop-headers";
    let (fixture, cluster) = setup(TOPIC, 1).await;
    let bootstrap = fixture.bootstrap()[0].clone();

    let producer = Producer::new(cluster, ProducerConfig::new());
    producer
        .send(
            ProducerRecord::new(TOPIC)
                .with_partition(0)
                .with_value("v")
                .with_header("content-type", "application/json")
                .with_null_header("tombstoned")
                .with_header("trace", "abc"),
        )
        .await
        .expect("sent");

    let consumer = rd_consumer(&bootstrap, "g-headers");
    consumer.subscribe(&[TOPIC]).expect("subscribe");
    let messages = drain(&consumer, 1).await;

    let headers = messages[0].headers().expect("headers survived");
    assert_eq!(headers.count(), 3, "header count changed across clients");

    let first = headers.get(0);
    assert_eq!(first.key, "content-type");
    assert_eq!(first.value, Some(&b"application/json"[..]));

    let second = headers.get(1);
    assert_eq!(second.key, "tombstoned");
    assert_eq!(
        second.value, None,
        "a null header value became empty for librdkafka"
    );

    assert_eq!(headers.get(2).key, "trace", "header order changed");
}
