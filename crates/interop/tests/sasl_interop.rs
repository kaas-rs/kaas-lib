//! Cross-client authentication: rdkafka and we log in to the same listener.
//!
//! `cargo xtask interop`
//!
//! The gap this closes: every other suite proves our SASL against *our*
//! understanding of the RFC. A mechanism is a place where a subtle
//! disagreement — the SASLprep profile, the proof, which bytes go into the
//! auth message — is invisible right up until it is a production login
//! failure, because a client that is wrong the same way in both directions
//! still authenticates against nothing.
//!
//! librdkafka needs OpenSSL for SCRAM's HMAC, so this suite exists only
//! because `crates/interop/Cargo.toml` asks for the `ssl` feature. Without it
//! librdkafka is built `WITH_SASL_SCRAM 0` and every case here fails at client
//! construction rather than at authentication.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::time::Duration;

use futures::StreamExt;
use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_conn::{ConnectionConfig, SaslConfig, SaslMechanism};
use kafka_read::{ScanEvent, ScanSpec};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use testkit::{BrokerConfig, Cluster as _, Security};

const USER: &str = "interop";
const PASSWORD: &str = "interop-pw";

/// Our side of the credential.
fn our_connection(mechanism: SaslMechanism) -> ConnectionConfig {
    let sasl = SaslConfig::new(mechanism, USER, PASSWORD);
    // `PLAIN` over an unencrypted socket needs the explicit opt-in, and a
    // fixture is where asking for it is reasonable.
    let sasl = if matches!(mechanism, SaslMechanism::Plain) {
        sasl.allow_plaintext_password()
    } else {
        sasl
    };
    ConnectionConfig::new().with_sasl(sasl)
}

/// Their side of the same credential.
fn their_producer(bootstrap: &str, mechanism: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanism", mechanism)
        .set("sasl.username", USER)
        .set("sasl.password", PASSWORD)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("rdkafka producer — a build without WITH_SASL_SCRAM fails here")
}

/// Both clients authenticate with the same mechanism against the same broker,
/// and the records one writes are the records the other reads.
async fn round_trip(mechanism: SaslMechanism, topic: &str) {
    let testkit_mechanism = match mechanism {
        SaslMechanism::ScramSha256 => testkit::SaslMechanism::ScramSha256,
        SaslMechanism::ScramSha512 => testkit::SaslMechanism::ScramSha512,
        SaslMechanism::Plain => testkit::SaslMechanism::Plain,
        other => panic!("{other} is not a username-and-password mechanism"),
    };

    let fixture = testkit::single_broker_with(
        BrokerConfig::new()
            .with_security(Security::SaslPlaintext)
            .with_mechanism(testkit_mechanism)
            .with_user(USER, PASSWORD),
    )
    .await
    .unwrap();
    let bootstrap = fixture.bootstrap()[0].clone();

    // Ours, authenticated.
    let admin = Admin::connect(
        fixture.bootstrap().to_vec(),
        ClusterConfig {
            connection: our_connection(mechanism),
            ..ClusterConfig::default()
        },
    )
    .await
    .expect("our client authenticates");
    admin
        .create_topics([NewTopic::new(topic, 1, 1)])
        .await
        .unwrap();

    // Theirs, authenticated with the same credential.
    let producer = their_producer(&bootstrap, mechanism.as_str());
    producer
        .send(
            FutureRecord::to(topic).key("k").payload("written by rdkafka"),
            Duration::from_secs(10),
        )
        .await
        .expect("rdkafka authenticates and produces");

    // Reading is the assertion that the authenticated connection is a working
    // one: an unauthenticated client cannot get this far on this listener.
    let cluster = admin.cluster().clone();
    let mut stream = Box::pin(kafka_read::scan(&cluster, ScanSpec::new(topic)).await.unwrap());

    let mut payloads = Vec::new();
    while let Some(event) = stream.next().await {
        match event.expect("no scan failure") {
            ScanEvent::Record(record) => {
                payloads.push(
                    String::from_utf8_lossy(record.value.as_deref().unwrap_or_default())
                        .into_owned(),
                );
            }
            ScanEvent::Malformed { offset, reason, .. } => {
                panic!("malformed batch at {offset}: {reason}")
            }
            _ => {}
        }
    }
    assert_eq!(payloads, vec!["written by rdkafka".to_owned()]);
}

#[testkit::integration_test]
async fn scram_sha_256_agrees_with_librdkafka() {
    round_trip(SaslMechanism::ScramSha256, "interop-scram-256").await;
}

#[testkit::integration_test]
async fn scram_sha_512_agrees_with_librdkafka() {
    round_trip(SaslMechanism::ScramSha512, "interop-scram-512").await;
}

#[testkit::integration_test]
async fn plain_agrees_with_librdkafka() {
    round_trip(SaslMechanism::Plain, "interop-plain").await;
}
