//! SASL/OAUTHBEARER against a real OAuth-secured cluster.
//!
//! The container fixtures use Kafka's *unsecured* JWS validator, which is the
//! only way to get an OAUTHBEARER broker without booting an identity provider —
//! and it proves the framing, not the thing operators actually run. This example
//! is the other half: a real issuer, real signatures, a real JWKS lookup on the
//! broker side.
//!
//! Two modes, chosen by which variables are set.
//!
//! A token the caller already has:
//!
//! ```sh
//! export KAAS_TEST_BOOTSTRAP=kafka-cluster-kafka-internal-bootstrap.strimzi.svc.cluster.local:9094
//! export KAAS_TEST_CA_FILE=/tmp/ca.crt
//! export KAAS_TEST_OAUTH_TOKEN="$(cat token.jwt)"
//! cargo run -q -p kafka-conn --example oauth_live
//! ```
//!
//! Or the `client_credentials` flow, fetched and refreshed by the library
//! (needs `--features oidc`):
//!
//! ```sh
//! export KAAS_TEST_OAUTH_TOKEN_ENDPOINT=https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token
//! export KAAS_TEST_OAUTH_CLIENT_ID=<client-id>
//! export KAAS_TEST_OAUTH_CLIENT_SECRET=<client-secret>
//! export KAAS_TEST_OAUTH_SCOPE=<client-id>/.default
//! cargo run -q -p kafka-conn --features oidc --example oauth_live
//! ```
//!
//! Nothing is created and nothing is deleted: this reads metadata and exits.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use kafka_conn::protocol::messages::MetadataRequest;
use kafka_conn::{ApiKey, Connection, ConnectionConfig, SaslConfig, TlsConfig};

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kafka_conn=debug".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let bootstrap = var("KAAS_TEST_BOOTSTRAP")
        .expect("KAAS_TEST_BOOTSTRAP is not set; see .claude/skills/live-cluster/SKILL.md");
    let addr = bootstrap
        .split(',')
        .next()
        .expect("at least one bootstrap address")
        .trim()
        .to_owned();

    let mut config = ConnectionConfig::new()
        .with_client_id("kaas-lib-oauth-live")
        .with_connect_timeout(Duration::from_secs(20));

    // An OAUTHBEARER listener is a TLS listener in every deployment worth
    // testing against; the client refuses a bearer token on a plaintext socket
    // unless told otherwise, which is the point.
    if let Some(pem) = ca_pem() {
        let mut tls = TlsConfig::with_ca_pem(pem);
        if let Some(name) = var("KAAS_TEST_TLS_SERVER_NAME") {
            tls = tls.with_server_name(name);
        }
        config = config.with_tls(tls);
    } else {
        eprintln!("note: no CA configured, so this connection is unencrypted");
    }

    config = config.with_sasl(sasl());

    let conn = Connection::connect(&addr, config)
        .await
        .expect("OAUTHBEARER authenticates");
    println!("authenticated = true");
    println!("peer = {}", conn.peer());
    println!(
        "api.keys.negotiated = {}",
        conn.versions()
            .entries()
            .filter(|e| e.negotiated().is_some())
            .count()
    );
    println!(
        "metadata.version = {:?}",
        conn.negotiated_version(ApiKey::Metadata)
    );

    let metadata = conn
        .send(
            MetadataRequest::default()
                .with_topics(Some(vec![]))
                .with_allow_auto_topic_creation(false),
        )
        .await
        .expect("metadata after authenticating");
    println!("brokers = {}", metadata.brokers.len());
    for broker in &metadata.brokers {
        println!(
            "broker = {} {}:{}",
            broker.node_id.0,
            broker.host.as_str(),
            broker.port
        );
    }
    println!("cluster.id = {:?}", metadata.cluster_id.as_deref());
}

fn ca_pem() -> Option<Vec<u8>> {
    if let Some(pem) = var("KAAS_TEST_CA_PEM") {
        return Some(pem.into_bytes());
    }
    let path = var("KAAS_TEST_CA_FILE")?;
    Some(std::fs::read(&path).unwrap_or_else(|e| panic!("reading KAAS_TEST_CA_FILE {path}: {e}")))
}

fn sasl() -> SaslConfig {
    if let Some(token) = var("KAAS_TEST_OAUTH_TOKEN") {
        println!("token.source = KAAS_TEST_OAUTH_TOKEN");
        return SaslConfig::oauth_bearer_token(token);
    }
    oidc()
}

#[cfg(feature = "oidc")]
fn oidc() -> SaslConfig {
    use kafka_conn::{OidcConfig, OidcTokenProvider};

    let endpoint = var("KAAS_TEST_OAUTH_TOKEN_ENDPOINT").expect(
        "set KAAS_TEST_OAUTH_TOKEN for a pre-fetched token, or \
         KAAS_TEST_OAUTH_TOKEN_ENDPOINT + _CLIENT_ID + _CLIENT_SECRET to fetch one",
    );
    let mut oidc = OidcConfig::new(
        &endpoint,
        var("KAAS_TEST_OAUTH_CLIENT_ID").expect("KAAS_TEST_OAUTH_CLIENT_ID"),
        var("KAAS_TEST_OAUTH_CLIENT_SECRET").expect("KAAS_TEST_OAUTH_CLIENT_SECRET"),
    )
    .with_maybe_scope(var("KAAS_TEST_OAUTH_SCOPE"))
    .with_maybe_audience(var("KAAS_TEST_OAUTH_AUDIENCE"));
    if var("KAAS_TEST_OAUTH_CREDENTIALS_IN_BODY").is_some() {
        oidc = oidc.with_credentials_in_body();
    }
    println!("token.source = {endpoint}");
    SaslConfig::oauth_bearer(OidcTokenProvider::new(oidc).expect("OIDC token provider"))
}

#[cfg(not(feature = "oidc"))]
fn oidc() -> SaslConfig {
    panic!(
        "KAAS_TEST_OAUTH_TOKEN is not set, and fetching one needs the `oidc` feature: \
         cargo run -p kafka-conn --features oidc --example oauth_live"
    )
}
