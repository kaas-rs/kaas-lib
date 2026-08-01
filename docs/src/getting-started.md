# Getting started

## Adding the crates

kaas-lib is not published to crates.io yet, so depend on it by git. Take only
the layers you need — each one re-exports the types from the layers below it,
so an admin-only consumer never names `kafka-conn` directly.

```toml
[dependencies]
kafka-admin = { git = "https://github.com/kaas-rs/kaas-lib" }
kafka-read  = { git = "https://github.com/kaas-rs/kaas-lib" }
tokio       = { version = "1", features = ["full"] }
futures     = "0.3"
```

The workspace targets Rust 1.97 and edition 2024.

## Connecting

Everything starts from a `Cluster` — the metadata cache, connection pool and
retry policy behind one cheap-to-clone handle. `Admin` wraps one; the read
path borrows one.

```rust,no_run
use kafka_admin::{Admin, ClusterConfig};

# async fn example() -> kafka_admin::Result<()> {
let admin = Admin::connect(["localhost:9092"], ClusterConfig::default()).await?;
# Ok(())
# }
```

Bootstrap addresses are a list because they are re-resolved when every known
broker goes unreachable — a rolling restart onto new addresses is a normal
Kubernetes event, and a pool that only remembers the addresses from its last
successful metadata fetch never recovers from one. See
[Metadata, routing and the pool](architecture/metadata-routing.md).

## Listing and describing topics

Note the shape of the return value: one result per topic, not one result for
the batch. This is the third invariant from the
[introduction](introduction.md), and it is the single most visible thing
about this API.

```rust,no_run
# use kafka_admin::Admin;
# async fn example(admin: &Admin) -> kafka_admin::Result<()> {
for (name, result) in admin.describe_topics(["orders", "shipments"]).await? {
    match result {
        Ok(topic) => println!("{name}: {} partitions", topic.partitions.len()),
        Err(error) => println!("{name}: {error}"),
    }
}
# Ok(())
# }
```

## Creating a topic and changing a config

```rust,no_run
use kafka_admin::{Admin, ConfigChange, ConfigResource, NewTopic};

# async fn example(admin: &Admin) -> kafka_admin::Result<()> {
for (name, result) in admin.create_topics([NewTopic::new("orders", 6, 3)]).await? {
    match result {
        Ok(created) => println!("{name}: {} partitions", created.partitions),
        Err(error) => println!("{name}: {error}"),
    }
}

admin
    .alter_configs([(
        ConfigResource::topic("orders"),
        vec![ConfigChange::set("retention.ms", "604800000")],
    )])
    .await?;
# Ok(())
# }
```

## Reading records

Two shapes, because a UI asks two different questions. `tail` answers "what
just happened" and is the most-used view in any Kafka UI; `scan` answers
"show me this topic from the beginning" and streams without ever
materialising a `Vec`.

```rust,no_run
use futures::StreamExt;
use kafka_read::{ScanEvent, ScanSpec, StartPosition, TailSpec};

# async fn example(cluster: &kafka_meta::Cluster) -> kafka_read::Result<()> {
// The last 500 records per partition.
let tails = kafka_read::tail(cluster, &TailSpec::new("orders", 500)).await?;

// Or browse forwards from the start.
let mut stream = Box::pin(
    kafka_read::scan(cluster, ScanSpec::new("orders").from(StartPosition::Earliest)).await?,
);
while let Some(event) = stream.next().await {
    match event? {
        ScanEvent::Record(record) => println!("{}: {:?}", record.offset, record.value),
        ScanEvent::Progress(progress) => println!("{:?}", progress.fraction()),
        ScanEvent::Malformed { offset, reason, .. } => {
            println!("offset {offset} did not decode: {reason}")
        }
        _ => {}
    }
}
# Ok(())
# }
```

`ScanEvent::Malformed` is not an error path you can ignore into a `_` arm and
forget — it is how a batch that will not decode reaches your UI instead of
ending the scan. [Tolerant decoding](architecture/tolerant-decoding.md)
explains why the granularity is a batch rather than a record.

Get a `&Cluster` from an `Admin` with `admin.cluster()`, or construct one
directly with `Cluster::connect`.

## Connecting safely to production

A client constructed read-only refuses every mutating API *before opening a
socket*, enforced on the api key rather than on the method surface:

```rust,no_run
use kafka_admin::{Admin, ClusterConfig};

# async fn example() -> kafka_admin::Result<()> {
let admin = Admin::connect_read_only(["localhost:9092"], ClusterConfig::default()).await?;
// admin.create_topics(..) now returns Error::ReadOnly without touching the network.
# Ok(())
# }
```

This is worth reaching for whenever a UI points at a cluster its operator
would rather nobody mutated. [The read-only gate](architecture/read-only-gate.md)
covers what it does and does not protect.

## TLS and SASL

```rust,no_run
use kafka_conn::{ConnectionConfig, SaslConfig, SaslMechanism, TlsConfig};
use kafka_meta::ClusterConfig;

# fn example() -> kafka_conn::Result<()> {
let connection = ConnectionConfig::new()
    .with_client_id("cluster-ui")
    .with_tls(TlsConfig::default())
    .with_sasl(SaslConfig::new(
        SaslMechanism::ScramSha512,
        "ui-service",
        "hunter2",
    ));

let config = ClusterConfig {
    connection,
    ..ClusterConfig::default()
};
# let _ = config;
# Ok(())
# }
```

Long-lived connections re-authenticate themselves; on any cluster that sets
`connections.max.reauth.ms` — Confluent Cloud does — this is the difference
between a session that survives the afternoon and periodic unexplained
disconnects. See [TLS, SASL and re-authentication](architecture/security.md).

## Running the tests

```sh
cargo xtask ci                     # fmt + clippy + unit tests, no Docker
cargo xtask integration            # acceptance tests, boots real brokers
cargo xtask docs --serve           # this book, with live reload
```

The integration tests are `#[ignore]`d by default so `cargo test` stays fast
without a Docker daemon. Each one boots `apache/kafka:4.3.1` through
[`testkit`](code-tour/testkit.md); nothing in this repository is verified
against a mock broker.
