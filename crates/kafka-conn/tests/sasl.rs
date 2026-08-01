//! M3 acceptance: TLS and SASL.
//!
//! `cargo test -p kafka-conn --test sasl -- --ignored`
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::time::{Duration, Instant};

use kafka_conn::protocol::messages::MetadataRequest;
use kafka_conn::{Connection, ConnectionConfig, Error, SaslConfig, SaslMechanism, TlsConfig};
use testkit::{BrokerConfig, Cluster, Security};

fn metadata_request() -> MetadataRequest {
    MetadataRequest::default()
        .with_topics(Some(vec![]))
        .with_allow_auto_topic_creation(false)
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn sasl_plaintext_plain_authenticates() {
    let broker = testkit::single_broker_with(
        BrokerConfig::new()
            .with_security(Security::SaslPlaintext)
            .with_mechanism(testkit::SaslMechanism::Plain)
            .with_user("alice", "alice-pw"),
    )
    .await
    .unwrap();

    let config = ConnectionConfig::new().with_sasl(
        // PLAIN over an unencrypted socket is refused unless asked for
        // explicitly; a fixture is exactly where asking is reasonable.
        SaslConfig::new(SaslMechanism::Plain, "alice", "alice-pw").allow_plaintext_password(),
    );
    let conn = Connection::connect(&broker.bootstrap()[0], config)
        .await
        .expect("PLAIN authenticates");
    conn.send(metadata_request()).await.unwrap();
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn plain_over_an_unencrypted_socket_is_refused_by_default() {
    let broker = testkit::single_broker_with(
        BrokerConfig::new()
            .with_security(Security::SaslPlaintext)
            .with_mechanism(testkit::SaslMechanism::Plain)
            .with_user("alice", "alice-pw"),
    )
    .await
    .unwrap();

    let config = ConnectionConfig::new().with_sasl(SaslConfig::new(
        SaslMechanism::Plain,
        "alice",
        "alice-pw",
    ));
    let err = Connection::connect(&broker.bootstrap()[0], config)
        .await
        .expect_err("the password must not go out in the clear by accident");
    assert!(matches!(err, Error::Authentication(_)), "{err:?}");
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn sasl_ssl_scram_sha_512_authenticates() {
    let broker = testkit::single_broker_with(
        BrokerConfig::new()
            .with_security(Security::SaslSsl)
            .with_mechanism(testkit::SaslMechanism::ScramSha512)
            .with_user("bob", "bob-pw"),
    )
    .await
    .unwrap();

    let ca = broker.ca_pem(0).await.expect("fixture CA");
    let config = ConnectionConfig::new()
        .with_tls(TlsConfig::with_ca_pem(ca).with_server_name("localhost"))
        .with_sasl(SaslConfig::new(SaslMechanism::ScramSha512, "bob", "bob-pw"));

    let conn = Connection::connect(&broker.bootstrap()[0], config)
        .await
        .expect("SCRAM-SHA-512 over TLS authenticates");
    conn.send(metadata_request()).await.unwrap();
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_wrong_password_is_an_authentication_error_not_a_timeout() {
    // The distinction matters to a UI: "check your credentials" and "the
    // cluster is unreachable" are different screens, and a handshake that
    // stalls until the connect timeout renders as the wrong one.
    let broker = testkit::single_broker_with(
        BrokerConfig::new()
            .with_security(Security::SaslPlaintext)
            .with_mechanism(testkit::SaslMechanism::ScramSha512)
            .with_user("bob", "bob-pw"),
    )
    .await
    .unwrap();

    let config = ConnectionConfig::new()
        .with_connect_timeout(Duration::from_secs(20))
        .with_sasl(SaslConfig::new(
            SaslMechanism::ScramSha512,
            "bob",
            "wrong-password",
        ));

    let started = Instant::now();
    let err = Connection::connect(&broker.bootstrap()[0], config)
        .await
        .expect_err("a wrong password cannot authenticate");
    assert!(matches!(err, Error::Authentication(_)), "{err:?}");
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "failed by timeout rather than by rejection"
    );
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn an_unknown_user_is_an_authentication_error() {
    let broker = testkit::single_broker_with(
        BrokerConfig::new()
            .with_security(Security::SaslPlaintext)
            .with_mechanism(testkit::SaslMechanism::ScramSha512)
            .with_user("bob", "bob-pw"),
    )
    .await
    .unwrap();

    let config = ConnectionConfig::new().with_sasl(SaslConfig::new(
        SaslMechanism::ScramSha512,
        "nobody",
        "bob-pw",
    ));
    let err = Connection::connect(&broker.bootstrap()[0], config)
        .await
        .expect_err("an unknown user cannot authenticate");
    assert!(matches!(err, Error::Authentication(_)), "{err:?}");
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_connection_survives_kip_368_reauthentication() {
    // Without KIP-368 this connection dies on a timer roughly ten seconds in,
    // and the symptom — a broker that "randomly" drops long-lived connections —
    // reads as a network fault rather than as an auth problem. The test runs
    // past two full windows so a single lucky round trip cannot pass it.
    const REAUTH_WINDOW: Duration = Duration::from_secs(10);

    let broker = testkit::single_broker_with(
        BrokerConfig::new()
            .with_security(Security::SaslPlaintext)
            .with_mechanism(testkit::SaslMechanism::ScramSha512)
            .with_user("carol", "carol-pw")
            .with_property(
                "listener.name.external.connections.max.reauth.ms",
                REAUTH_WINDOW.as_millis().to_string(),
            ),
    )
    .await
    .unwrap();

    let config = ConnectionConfig::new().with_sasl(SaslConfig::new(
        SaslMechanism::ScramSha512,
        "carol",
        "carol-pw",
    ));
    let conn = Connection::connect(&broker.bootstrap()[0], config)
        .await
        .unwrap();

    let started = Instant::now();
    let deadline = started + REAUTH_WINDOW * 2 + Duration::from_secs(5);
    let mut round_trips = 0u32;
    while Instant::now() < deadline {
        conn.send(metadata_request())
            .await
            .unwrap_or_else(|e| panic!("connection died after {round_trips} round trips: {e}"));
        round_trips += 1;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    assert!(!conn.is_closed(), "connection was closed at session expiry");

    // The property is that the connection outlived two reauth windows while
    // still serving, and the loop above has just spent that long proving it —
    // every iteration is a round trip that had to succeed. So assert on
    // *elapsed time*, which is what the claim is about.
    //
    // The round-trip count is a sanity floor, not the assertion. It used to be
    // `> 20` against roughly 25 one-second iterations, which quietly required
    // each round trip to finish inside 250ms and so turned a loaded runner into
    // a failure of the library. That is the same trap `connection.rs` documents
    // having already fallen into once.
    assert!(
        started.elapsed() >= REAUTH_WINDOW * 2,
        "the connection has not yet outlived two reauth windows"
    );
    assert!(
        round_trips > 5,
        "{round_trips} round trips cannot demonstrate anything"
    );
}
