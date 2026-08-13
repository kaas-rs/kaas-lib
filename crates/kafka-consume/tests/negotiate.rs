//! Acceptance for issue #20: the protocol is negotiated, not assumed.
//!
//! `cargo test -p kafka-consume --test negotiate -- --ignored`
//!
//! The scenario that motivated this: a `GroupConsumer` pointed at a broker
//! that serves the classic group APIs but not KIP-848's key 68 raised
//! `UnsupportedApi` from every heartbeat, the caller's retry loop read that
//! as transient, and the consumer looped forever. The fixture reproduces such
//! a Kafka 3.7 broker, where `ConsumerGroupHeartbeat` was still marked
//! unstable and is therefore — the fact negotiation keys on — absent from
//! `ApiVersions`.
//!
//! Three nearby shapes deliberately not used, all measured by the 0.8.0
//! release gates rather than assumed: a 4.x broker with
//! `group.coordinator.rebalance.protocols=classic`, a stock 3.9, and a 3.9
//! with `group.coordinator.new.enable=false` **all still advertise key 68**
//! and refuse it at runtime instead. From 3.8 the advertisement tracks the
//! API's stability marking, not whether the protocol is usable — so the
//! honest reproduction of "broker offers None" (kaas, and pre-3.8 clusters)
//! is a genuinely pre-3.8 broker. The advertise-but-refuse shapes are #28.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::time::Duration;

use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_consume::{ConsumerConfig, GroupConsumer, GroupProtocol, NegotiatedConsumer};
use kafka_produce::{Producer, ProducerConfig, ProducerRecord};
use kafka_read::Visibility;
use testkit::{BrokerConfig, Cluster as _, KafkaCluster};

const TOPIC: &str = "negotiated";

async fn seeded(config: BrokerConfig) -> (KafkaCluster, Admin) {
    let fixture = testkit::single_broker_with(config).await.expect("broker");
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .expect("admin");
    admin
        .create_topics([NewTopic::new(TOPIC, 3, 1)])
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
    for i in 0..50 {
        producer
            .send(ProducerRecord::new(TOPIC).with_value(format!("v{i}")))
            .await
            .expect("seed");
    }
    (fixture, admin)
}

/// A broker whose `ApiVersions` does not offer key 68: Kafka 3.7, the last
/// minor where `ConsumerGroupHeartbeat` is marked unstable and therefore
/// unadvertised.
fn classic_only() -> BrokerConfig {
    BrokerConfig::new().with_image("apache/kafka", "3.7.2")
}

fn config() -> ConsumerConfig {
    ConsumerConfig::new()
        .visibility(Visibility::All)
        .max_wait_ms(200)
}

/// The fixture's load-bearing property, measured on a fresh bare connection:
/// what this broker actually advertises for key 68, and how wide its table
/// is. In the assertion messages so a fixture that turns out to advertise
/// the key fails saying so, not three inferences later.
async fn key_68_facts(fixture: &KafkaCluster) -> String {
    let conn = kafka_conn::Connection::connect(
        &fixture.bootstrap()[0],
        kafka_conn::ConnectionConfig::new(),
    )
    .await
    .expect("probe connection");
    let versions = conn.versions();
    format!(
        "broker advertises {} api keys; ConsumerGroupHeartbeat row: {:?}",
        versions.len(),
        versions.get(kafka_conn::ApiKey::ConsumerGroupHeartbeat)
    )
}

/// Poll until records arrive, bounded so a consumer that cannot make progress
/// fails as this assertion rather than as the fixture's two-minute deadline.
async fn drain_some(consumer: &mut NegotiatedConsumer) -> usize {
    let mut got = 0usize;
    for _ in 0..200 {
        let records = tokio::time::timeout(Duration::from_secs(30), consumer.poll())
            .await
            .expect("poll must not block indefinitely")
            .expect("poll");
        got += records.len();
        if got > 0 {
            break;
        }
    }
    got
}

#[testkit::integration_test]
async fn auto_downgrades_to_classic_on_a_broker_without_kip848() {
    let (fixture, admin) = seeded(classic_only()).await;
    let facts = key_68_facts(&fixture).await;

    let mut consumer = NegotiatedConsumer::subscribe(
        admin.cluster().clone(),
        config(),
        "negotiate-classic",
        [TOPIC],
    )
    .await
    .expect("Auto must fall back to the classic protocol, not error");

    assert_eq!(
        consumer.protocol(),
        GroupProtocol::Classic,
        "key 68 must not be advertised here, so negotiation must land on classic — {facts}"
    );
    let got = drain_some(&mut consumer).await;
    assert!(got > 0, "the downgraded consumer must actually consume");
    consumer.leave().await.expect("leave");
}

#[testkit::integration_test]
async fn auto_picks_kip848_when_the_broker_serves_it() {
    let (_fixture, admin) = seeded(BrokerConfig::new()).await;

    let mut consumer =
        NegotiatedConsumer::subscribe(admin.cluster().clone(), config(), "negotiate-848", [TOPIC])
            .await
            .expect("subscribe");

    assert_eq!(consumer.protocol(), GroupProtocol::Consumer);
    let got = drain_some(&mut consumer).await;
    assert!(got > 0, "the negotiated KIP-848 consumer must consume");
    consumer.leave().await.expect("leave");
}

#[testkit::integration_test]
async fn a_pinned_group_consumer_fails_at_subscribe_not_on_every_poll() {
    // The bug as filed: the error surfaced from every heartbeat inside poll,
    // where a retry loop treats it as transient and spins. Pinning the KIP-848
    // type against a broker that cannot serve it must now fail once, at the
    // point the caller made the choice.
    let (fixture, admin) = seeded(classic_only()).await;
    let facts = key_68_facts(&fixture).await;

    let result = GroupConsumer::subscribe(
        admin.cluster().clone(),
        config(),
        "negotiate-pinned",
        [TOPIC],
    )
    .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("a broker without key 68 can never admit this member — {facts}"),
    };

    assert!(
        matches!(error, kafka_consume::Error::UnsupportedApi { .. }),
        "the error must stay the structured UnsupportedApi a caller can \
         branch on, got: {error}"
    );
}

#[testkit::integration_test]
async fn a_pinned_classic_choice_is_honoured_on_a_modern_broker() {
    // Mixed groups exist: a caller whose other members are pinned to the
    // classic protocol must be able to insist, even where KIP-848 is served.
    let (_fixture, admin) = seeded(BrokerConfig::new()).await;

    let mut consumer = NegotiatedConsumer::subscribe(
        admin.cluster().clone(),
        config().with_group_protocol(GroupProtocol::Classic),
        "negotiate-pinned-classic",
        [TOPIC],
    )
    .await
    .expect("subscribe");

    assert_eq!(consumer.protocol(), GroupProtocol::Classic);
    let got = drain_some(&mut consumer).await;
    assert!(got > 0);
    consumer.leave().await.expect("leave");
}
