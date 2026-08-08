# Connecting

Everything starts from a `Cluster`: the metadata cache, connection pool,
routing and retry policy behind one cheap-to-clone handle. Clone it freely —
every clone shares the same cache and the same connections.

```rust,no_run
use kafka_meta::{Cluster, ClusterConfig};

# async fn example() -> kafka_meta::Result<()> {
let cluster = Cluster::connect(["broker-1:9092", "broker-2:9092"], ClusterConfig::default()).await?;
# Ok(())
# }
```

`Admin::connect` and `Admin::connect_read_only` build one for you;
`admin.cluster()` hands it back for the read path.

## Give it more than one bootstrap address

Bootstrap addresses are re-resolved when **every** known broker goes
unreachable. A cluster that rolls all its brokers onto new addresses is a
normal Kubernetes event, and a pool that only remembers addresses from its
last successful metadata fetch never recovers from one.

A single bootstrap address is fine when it is a stable service DNS name that
load-balances; it is a liability when it is one pod IP.

## `ClusterConfig`

```rust,no_run
use std::time::Duration;
use kafka_meta::ClusterConfig;

# fn example() {
let config = ClusterConfig {
    refresh_interval: Duration::from_secs(30),
    max_staleness: Duration::from_secs(5),
    ..ClusterConfig::default()
};
# }
```

| Field | Default | Notes |
|---|---|---|
| `connection` | — | the per-connection settings below |
| `retry` | capped, jittered | applied to routed requests |
| `refresh_interval` | 30s | background metadata refresh |
| `max_staleness` | — | refresh before answering if the snapshot is older |

Kafka's own client default for metadata refresh is five minutes. A UI wants
fresher than that, and metadata for a large cluster is not cheap, so 30
seconds is the compromise — with on-demand invalidation doing the real work
whenever a `NOT_LEADER_OR_FOLLOWER` comes back.

## `ConnectionConfig`

```rust,no_run
use std::time::Duration;
use kafka_conn::ConnectionConfig;

# fn example() {
let connection = ConnectionConfig::new()
    .with_client_id("cluster-ui")
    .with_request_timeout(Duration::from_secs(30))
    .with_connect_timeout(Duration::from_secs(10))
    .with_max_in_flight(5);
# }
```

**Set `client_id` to something recognisable.** It appears in broker request
logs and in quota attribution, and "which client is hammering this cluster"
is a question someone will eventually ask about your service.

`max_in_flight` defaults to 5, matching Kafka. The broker processes one
connection's requests in order regardless, so this trades head-of-line
blocking for memory rather than buying parallelism. Zero is clamped to 1.

## TLS

```rust,no_run
use kafka_conn::{ConnectionConfig, TlsConfig};

# fn example() -> kafka_conn::Result<()> {
// System trust roots.
let tls = TlsConfig::system();

// Or a private CA.
let tls = TlsConfig::with_ca_pem(std::fs::read("ca.pem")?);

// Or mutual TLS.
let tls = TlsConfig::system()
    .with_client_certificate(std::fs::read("client.pem")?, std::fs::read("client.key")?);

let connection = ConnectionConfig::new().with_tls(tls);
# Ok(())
# }
```

**`with_server_name` is the one you will need unexpectedly.** Brokers
advertise the names in their own `advertised.listeners`, and those routinely
do not resolve from where the client is running — behind a Kubernetes
service, a load balancer, or a port-forward. Overriding the name sent in SNI
and verified against the certificate is what makes that work without
disabling verification.

## SASL

```rust,no_run
use kafka_conn::{ConnectionConfig, SaslConfig, SaslMechanism, TlsConfig};

# fn example() {
let sasl = SaslConfig::new(SaslMechanism::ScramSha512, "ui-service", "hunter2");

let connection = ConnectionConfig::new()
    .with_tls(TlsConfig::system())
    .with_sasl(sasl);
# }
```

`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` and `OAUTHBEARER`.

**`PLAIN` over a plaintext transport sends a recoverable password in the
clear**, and the library knows it — that combination requires
`allow_plaintext_password()` to be called explicitly rather than being
silently permitted. If you find yourself reaching for it outside a test
fixture, reach for TLS instead. `OAUTHBEARER` is gated the same way, for the
same reason: a bearer token read off the wire is usable until it expires.

Re-authentication is automatic. On any cluster with
`connections.max.reauth.ms` set, the connection re-issues `SaslAuthenticate`
before the session expires; without that the broker kills the connection and
the symptom looks like a network fault. See
[TLS, SASL and re-authentication](../architecture/security.md).

## OAUTHBEARER

A token source, not a token — because re-authentication asks again, on a timer
this library owns, long after the token you started with has expired:

```rust,no_run
use kafka_conn::{ConnectionConfig, Result, SaslConfig, TlsConfig};

# async fn fetch_from_your_own_token_service() -> Result<String> { Ok(String::new()) }
# fn example() {
let sasl = SaslConfig::oauth_bearer(|| async { fetch_from_your_own_token_service().await });

let connection = ConnectionConfig::new()
    .with_tls(TlsConfig::system())
    .with_sasl(sasl);
# }
```

Any `Fn() -> impl Future<Output = Result<String>>` will do. Managed clusters
that select a logical cluster or identity pool through RFC 7628 extensions get
them with `with_extension("logicalCluster", "lkc-42")`.

`SaslConfig::oauth_bearer_token("eyJ…")` takes one fixed token instead. That is
right for a CLI run and wrong for a service: the first re-authentication will
present the same expired token and the broker will close the connection.

### Fetching tokens for yourself

With the `oidc` feature, the library runs `client_credentials` against your
issuer and refreshes ahead of expiry:

```toml
kafka-conn = { version = "0.5", features = ["oidc"] }
```

```rust,no_run
# #[cfg(feature = "oidc")]
# fn example() -> kafka_conn::Result<()> {
use kafka_conn::{ConnectionConfig, OidcConfig, OidcTokenProvider, SaslConfig, TlsConfig};

let provider = OidcTokenProvider::new(
    OidcConfig::new(
        "https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token",
        "<client-id>",
        std::env::var("OAUTH_CLIENT_SECRET").unwrap_or_default(),
    )
    // Entra wants the scope; Keycloak usually wants nothing; Auth0 wants an
    // audience. All three are optional and none is guessed for you.
    .with_scope("<client-id>/.default"),
)?;

let connection = ConnectionConfig::new()
    .with_tls(TlsConfig::system())
    .with_sasl(SaslConfig::oauth_bearer(provider));
# Ok(())
# }
```

Share **one** provider across a cluster: it is what keeps every connection on
one token and one fetch. Handing the same `SaslConfig` to `ClusterConfig` does
that already.

Two failures worth recognising, because they belong to different systems:
`Error::TokenEndpoint` means your identity provider would not issue a token,
and carries its `error_description`; `Error::Authentication` means the broker
would not accept the one it issued, and carries the RFC 7628 `status`.

## Read-only clients

```rust,no_run
use kafka_conn::ConnectionConfig;
use kafka_meta::ClusterConfig;

# fn example() {
let config = ClusterConfig {
    connection: ConnectionConfig::new().read_only(),
    ..ClusterConfig::default()
};
# }
```

Every mutating api key now returns `Error::ReadOnly` **before a socket is
touched**. `Admin::connect_read_only` is the shorthand.

This is a client-side safety catch, not a replacement for broker ACLs — see
[The read-only gate](../architecture/read-only-gate.md) for what it does and
does not protect.

## Inspecting what was negotiated

Useful when a call fails with `UnsupportedApi` and you want to know which
side is the ceiling:

```rust,no_run
# use kafka_conn::{ApiKey, Connection};
# async fn example(conn: &Connection) {
for entry in conn.versions().entries() {
    println!("{} broker={:?} ours={:?}", entry.api_key, entry.broker, entry.ours);
}
# }
```

`ours: None` means the codec has no schema for that key at all. `broker_ahead()`
is true whenever the broker offers something newer than we can encode — which,
[given the upstream gap](../compat/upstream-gap.md), is the normal case.
