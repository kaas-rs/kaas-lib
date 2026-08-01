//! M11 acceptance: cross-client interoperability.
//!
//! `cargo xtask interop`
//!
//! Unit tests cannot find this class of bug, because both sides of a unit test
//! are our own code. Producing with `rdkafka` — a genuinely different
//! implementation, wrapping the C library the rest of the ecosystem uses — and
//! reading the result with `kafka-read` is what catches silent wrongness:
//! header encoding, tombstones, murmur2 partitioning, and above all snappy's
//! xerial framing.
//!
//! The snappy case is asserted explicitly because it is the newest and least
//! settled code in the dependency: `kafka-protocol` 0.17.0 *rewrote* its snappy
//! path to emit Java/xerial framing — 0.16 and earlier were mutually
//! incompatible with the Java client — and decodes by autodetection. It is the
//! part of the codec we are least entitled to assume works.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::HashMap;
use std::time::Duration;

use futures::StreamExt;
use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_read::{ScanEvent, ScanSpec};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use testkit::Cluster as _;

async fn producer(bootstrap: &str, compression: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("compression.type", compression)
        .set("batch.size", "16384")
        .set("linger.ms", "50")
        .set("message.timeout.ms", "10000")
        .create()
        .expect("rdkafka producer")
}

/// Produce with `rdkafka`, read with ours.
async fn produce_with_rdkafka_read_with_ours(compression: &str, topic: &str) {
    let fixture = testkit::single_broker().await.unwrap();
    let bootstrap = fixture.bootstrap()[0].clone();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    admin
        .create_topics([NewTopic::new(topic, 3, 1)])
        .await
        .unwrap();

    let producer = producer(&bootstrap, compression).await;
    for i in 0..500u32 {
        let key = format!("key-{i}");
        let payload = format!("payload-{i}-{}", "x".repeat(i as usize % 97));
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "trace",
                value: Some(&format!("trace-{i}")),
            })
            .insert(Header {
                key: "empty",
                value: None::<&str>,
            });
        producer
            .send(
                FutureRecord::to(topic)
                    .key(&key)
                    .payload(&payload)
                    .headers(headers),
                Duration::from_secs(10),
            )
            .await
            .expect("rdkafka produced");
    }
    // A tombstone, which is a null value and not an empty one.
    producer
        .send(
            FutureRecord::<String, String>::to(topic).key(&"tombstone".to_owned()),
            Duration::from_secs(10),
        )
        .await
        .expect("tombstone produced");

    let cluster = admin.cluster().clone();
    let mut stream = Box::pin(kafka_read::scan(&cluster, ScanSpec::new(topic)).await.unwrap());

    let mut by_key: HashMap<String, (Option<Vec<u8>>, Vec<(String, Option<Vec<u8>>)>)> =
        HashMap::new();
    while let Some(event) = stream.next().await {
        match event.expect("no scan failure") {
            ScanEvent::Record(record) => {
                let key = String::from_utf8_lossy(record.key.as_deref().unwrap_or_default())
                    .into_owned();
                by_key.insert(
                    key,
                    (
                        record.value.map(|v| v.to_vec()),
                        record
                            .headers
                            .into_iter()
                            .map(|(name, value)| (name, value.map(|v| v.to_vec())))
                            .collect(),
                    ),
                );
            }
            ScanEvent::Malformed { offset, reason, .. } => {
                panic!("{compression}: malformed batch at {offset}: {reason}")
            }
            _ => {}
        }
    }

    assert_eq!(by_key.len(), 501, "{compression}: wrong record count");

    let (value, headers) = by_key.get("key-42").expect("key-42");
    assert_eq!(
        value.as_deref().map(String::from_utf8_lossy),
        Some(std::borrow::Cow::Borrowed(
            format!("payload-42-{}", "x".repeat(42)).as_str()
        )),
        "{compression}: value did not survive"
    );
    assert_eq!(headers.len(), 2, "{compression}: headers were lost");
    assert_eq!(headers[0].0, "trace");
    assert_eq!(
        headers[0].1.as_deref().map(String::from_utf8_lossy),
        Some(std::borrow::Cow::Borrowed("trace-42"))
    );
    // A null header value is distinct from an empty one, and rdkafka can write
    // both.
    assert_eq!(headers[1].0, "empty");
    assert!(headers[1].1.is_none(), "{compression}: null header became empty");

    let (value, _) = by_key.get("tombstone").expect("tombstone");
    assert!(value.is_none(), "{compression}: tombstone became an empty value");
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn rdkafka_produces_and_we_read_it_uncompressed() {
    produce_with_rdkafka_read_with_ours("none", "interop-none").await;
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn rdkafka_produces_and_we_read_it_gzip() {
    produce_with_rdkafka_read_with_ours("gzip", "interop-gzip").await;
}

/// The one we are least entitled to assume works.
#[tokio::test]
#[ignore = "needs Docker"]
async fn rdkafka_produces_and_we_read_it_snappy() {
    produce_with_rdkafka_read_with_ours("snappy", "interop-snappy").await;
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn rdkafka_produces_and_we_read_it_lz4() {
    produce_with_rdkafka_read_with_ours("lz4", "interop-lz4").await;
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn rdkafka_produces_and_we_read_it_zstd() {
    produce_with_rdkafka_read_with_ours("zstd", "interop-zstd").await;
}

/// The reverse direction: what our admin layer writes, a different client
/// reads. Offsets committed by us must be the offsets `rdkafka` resumes from.
#[tokio::test]
#[ignore = "needs Docker"]
async fn a_group_offset_we_commit_is_the_offset_rdkafka_resumes_from() {
    let fixture = testkit::single_broker().await.unwrap();
    let bootstrap = fixture.bootstrap()[0].clone();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    admin
        .create_topics([NewTopic::new("interop-offsets", 1, 1)])
        .await
        .unwrap();

    let producer = producer(&bootstrap, "none").await;
    for i in 0..100u32 {
        producer
            .send(
                FutureRecord::to("interop-offsets")
                    .key(&format!("k{i}"))
                    .payload(&format!("v{i}")),
                Duration::from_secs(10),
            )
            .await
            .expect("produced");
    }

    // Create the group by consuming a little, then leave.
    {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", "interop-group")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("consumer");
        consumer.subscribe(&["interop-offsets"]).expect("subscribe");
        let _ = tokio::time::timeout(Duration::from_secs(10), consumer.recv()).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Commit offset 75 with *our* client, as a non-member.
    for _ in 0..20 {
        let reset = admin
            .reset_offsets(
                "interop-group",
                [kafka_admin::OffsetReset::new("interop-offsets", 0, 75)],
            )
            .await;
        if reset.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Now let rdkafka resume, and assert it starts where we said.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "interop-group")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");
    consumer.subscribe(&["interop-offsets"]).expect("subscribe");

    let message = tokio::time::timeout(Duration::from_secs(20), consumer.recv())
        .await
        .expect("rdkafka received something")
        .expect("a message");
    assert_eq!(
        message.offset(),
        75,
        "rdkafka resumed from the wrong offset — our commit did not mean what \
         we thought it meant"
    );
}
