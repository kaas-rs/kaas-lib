//! Acceptance for issue #27: mutual TLS against a real broker.
//!
//! `cargo test -p kafka-conn --test mtls -- --ignored`
//!
//! Until this landed, `TlsConfig::with_client_certificate` was the one part of
//! `TlsConfig` with no coverage against a broker at all — its only automated
//! test was unit-level classification of synthetic rustls alerts, which proves
//! how we *report* a refusal and nothing about whether a handshake works.
//!
//! The fixture reproduces the live topology deliberately: the broker's own
//! certificate chains to the cluster CA, the client's to a separate clients
//! CA, and the two are verified in opposite directions within one handshake.
//! Strimzi arranges it exactly this way, and mixing the two anchors up is the
//! single most likely way to lose an hour to mTLS — so a mix-up should fail
//! here, in CI, rather than against a live cluster.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use kafka_conn::protocol::messages::MetadataRequest;
use kafka_conn::{Connection, ConnectionConfig, Error, TlsConfig};
use testkit::{BrokerConfig, ClientAuth, Cluster, Security};

fn metadata_request() -> MetadataRequest {
    MetadataRequest::default()
        .with_topics(Some(vec![]))
        .with_allow_auto_topic_creation(false)
}

fn requires_client_certificates() -> BrokerConfig {
    BrokerConfig::new()
        .with_security(Security::Ssl)
        .with_client_auth(ClientAuth::Required)
}

#[testkit::integration_test]
async fn a_client_certificate_authenticates_against_a_broker_that_requires_one() {
    let broker = testkit::single_broker_with(requires_client_certificates())
        .await
        .unwrap();

    let ca = broker.ca_pem(0).await.expect("fixture CA");
    let (chain, key) = broker
        .client_certificate()
        .expect("a fixture with client auth generates a client certificate");

    let config = ConnectionConfig::new().with_tls(
        TlsConfig::with_ca_pem(ca)
            .with_server_name("localhost")
            .with_client_certificate(chain, key),
    );

    let conn = Connection::connect(&broker.bootstrap()[0], config)
        .await
        .expect("the client certificate must complete the handshake");
    conn.send(metadata_request())
        .await
        .expect("and the authenticated principal must be able to work");
}

/// The negative half, and the reason it is worth a test of its own: a refused
/// certificate must arrive as `Error::Authentication`, not as a transport
/// failure or a hang. TLS 1.3 delivers the alert *after* the client thinks the
/// handshake finished, so this classification is timing-sensitive in a way no
/// synthetic alert can prove — which is what the unit tests in `tls.rs` cover
/// and this one does not duplicate.
#[testkit::integration_test]
async fn a_missing_client_certificate_is_an_authentication_failure_not_a_transport_one() {
    let broker = testkit::single_broker_with(requires_client_certificates())
        .await
        .unwrap();

    let ca = broker.ca_pem(0).await.expect("fixture CA");
    let config =
        ConnectionConfig::new().with_tls(TlsConfig::with_ca_pem(ca).with_server_name("localhost"));

    // Approaching the same listener without a certificate: server
    // authentication succeeds, and the broker then refuses us.
    let error = match Connection::connect(&broker.bootstrap()[0], config).await {
        Err(error) => error,
        Ok(conn) => {
            // A connection is not proof of anything until it is used — under
            // TLS 1.3 the refusal can arrive on the first read.
            match conn.send(metadata_request()).await {
                Err(error) => error,
                Ok(_) => panic!(
                    "a broker configured ssl.client.auth=required admitted a client \
                     with no certificate; the fixture is not reproducing mTLS"
                ),
            }
        }
    };

    assert!(
        matches!(error, Error::Authentication(_)),
        "a rejected certificate is a credentials problem and must say so, got: {error:?}"
    );
}

/// The mix-up the two-CA topology exists to catch: a certificate the broker's
/// clients CA did not issue must be refused, not accepted.
///
/// The certificate has to be a *coherent* pair from an unrelated CA. The
/// first version of this test presented the cluster CA's certificate with the
/// client's key — the anchor an operator reaches for first — and rustls
/// rejected it locally as `KeyMismatch` before a byte reached the broker.
/// Correct behaviour, and a good error, but it proved nothing about what the
/// broker does with a well-formed certificate from the wrong issuer, which is
/// the actual mistake in the field.
#[testkit::integration_test]
async fn a_certificate_from_the_wrong_ca_is_refused() {
    let broker = testkit::single_broker_with(requires_client_certificates())
        .await
        .unwrap();

    let ca = broker.ca_pem(0).await.expect("fixture CA");
    let (chain, key) =
        testkit::untrusted_client_certificate().expect("generate an unrelated client identity");

    let config = ConnectionConfig::new().with_tls(
        TlsConfig::with_ca_pem(ca)
            .with_server_name("localhost")
            .with_client_certificate(chain, key),
    );

    let outcome = match Connection::connect(&broker.bootstrap()[0], config).await {
        Err(error) => Err(error),
        Ok(conn) => conn.send(metadata_request()).await.map(|_| ()),
    };
    let error = outcome.expect_err("a certificate from the wrong CA must not authenticate");
    assert!(
        matches!(error, Error::Authentication(_)),
        "a certificate the broker's clients CA did not issue is a credentials \
         problem and must say so, got: {error:?}"
    );
}
