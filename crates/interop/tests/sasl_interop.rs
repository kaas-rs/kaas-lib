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
//!
//! # OAUTHBEARER, and what its case does and does not prove (#26)
//!
//! The last mechanism to get a cross-check, because it needs a token *issuer*
//! both clients accept. Both sides use the unsecured JWS the broker's
//! `OAuthBearerUnsecuredValidatorCallbackHandler` takes: we build ours with
//! `testkit::unsecured_jws`, librdkafka builds its own from
//! `sasl.oauthbearer.config` once `enable.sasl.oauthbearer.unsecure.jwt` is
//! on. Two independent encoders of the same token format, which is the point.
//!
//! What this proves is wire compatibility of the client-first message and the
//! token framing, plus that both clients' tokens name the same principal. It
//! does *not* exercise JWKS validation — the unsecured validator is a
//! broker-side choice — and the real-issuer path stays the `internal`
//! listener on the live Strimzi cluster.
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

/// How long an interop token stays valid. Long enough that a slow container
/// boot cannot expire it mid-suite, short enough to be plainly a fixture.
const TOKEN_LIFETIME: Duration = Duration::from_secs(600);

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
            FutureRecord::to(topic)
                .key("k")
                .payload("written by rdkafka"),
            Duration::from_secs(10),
        )
        .await
        .expect("rdkafka authenticates and produces");

    // Reading is the assertion that the authenticated connection is a working
    // one: an unauthenticated client cannot get this far on this listener.
    let cluster = admin.cluster().clone();
    let mut stream = Box::pin(
        kafka_read::scan(&cluster, ScanSpec::new(topic))
            .await
            .unwrap(),
    );

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

/// OAUTHBEARER, both directions, with the principal asserted rather than
/// assumed.
///
/// The authorizer is what turns "both authenticated" into "both authenticated
/// **as the same principal**": the fixture makes `User:interop` — the `sub`
/// claim, which is what Kafka derives an OAUTHBEARER principal from — the only
/// non-broker principal allowed to do anything. A client whose token named
/// something else would authenticate perfectly well and then be denied, so a
/// green round trip is a statement about the principal and not merely about
/// the handshake.
#[testkit::integration_test]
async fn oauthbearer_agrees_with_librdkafka() {
    let topic = "interop-oauthbearer";
    let fixture = testkit::single_broker_with(
        BrokerConfig::new()
            .with_security(Security::SaslPlaintext)
            .with_mechanism(testkit::SaslMechanism::OauthBearer)
            .with_authorizer(true)
            .with_super_user(format!("User:{USER}")),
    )
    .await
    .unwrap();
    let bootstrap = fixture.bootstrap()[0].clone();

    // Ours: a token we encoded.
    let admin = Admin::connect(
        fixture.bootstrap().to_vec(),
        ClusterConfig {
            connection: ConnectionConfig::new().with_sasl(
                SaslConfig::oauth_bearer_token(testkit::unsecured_jws(USER, TOKEN_LIFETIME))
                    // The token is a reusable credential, and this listener is
                    // deliberately unencrypted — the same opt-in PLAIN needs.
                    .allow_plaintext_password(),
            ),
            ..ClusterConfig::default()
        },
    )
    .await
    .expect("our client authenticates with an unsecured JWS");
    admin
        .create_topics([NewTopic::new(topic, 1, 1)])
        .await
        .unwrap();

    // Theirs: a token librdkafka encoded, from the same claims.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanism", "OAUTHBEARER")
        // Without this librdkafka expects a refresh callback and never
        // produces a token at all; the builtin unsecured handler is opt-in.
        .set("enable.sasl.oauthbearer.unsecure.jwt", "true")
        .set("sasl.oauthbearer.config", format!("principal={USER}"))
        .set("message.timeout.ms", "10000")
        .create()
        .expect("rdkafka producer");
    producer
        .send(
            FutureRecord::to(topic)
                .key("k")
                .payload("written by rdkafka"),
            Duration::from_secs(10),
        )
        .await
        .expect("rdkafka authenticates as the same principal and produces");

    let cluster = admin.cluster().clone();
    let mut stream = Box::pin(
        kafka_read::scan(&cluster, ScanSpec::new(topic))
            .await
            .unwrap(),
    );
    let mut payloads = Vec::new();
    while let Some(event) = stream.next().await {
        match event.expect("no scan failure") {
            ScanEvent::Record(record) => payloads.push(
                String::from_utf8_lossy(record.value.as_deref().unwrap_or_default()).into_owned(),
            ),
            ScanEvent::Malformed { offset, reason, .. } => {
                panic!("malformed batch at {offset}: {reason}")
            }
            _ => {}
        }
    }
    assert_eq!(payloads, vec!["written by rdkafka".to_owned()]);

    // And the negative that makes the principal assertion mean something: a
    // token that authenticates but names someone else is refused by the
    // authorizer, not by the handshake.
    let stranger = Admin::connect(
        fixture.bootstrap().to_vec(),
        ClusterConfig {
            connection: ConnectionConfig::new().with_sasl(
                SaslConfig::oauth_bearer_token(testkit::unsecured_jws("stranger", TOKEN_LIFETIME))
                    .allow_plaintext_password(),
            ),
            ..ClusterConfig::default()
        },
    )
    .await;
    let denied = match stranger {
        // Connecting fetches metadata, which this principal may already be
        // refused for; either point is a legitimate place to be denied.
        Err(error) => error,
        Ok(admin) => admin
            .describe_cluster()
            .await
            .expect_err("a principal with no ACLs must not be able to describe the cluster"),
    };
    assert!(
        format!("{denied}").to_lowercase().contains("authoriz"),
        "the refusal must name authorization — the token itself is valid: {denied}"
    );
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
