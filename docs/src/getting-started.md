# Getting started

## Adding the crates

Take only the layers you need — each one re-exports the types from the layers
below it, so a producer never names `kafka-conn` directly — and pull them all
at the **same version**. The crates publish to crates.io in lockstep because
they are one library split along a layering boundary, so `kafka-admin` 0.4
against `kafka-conn` 0.3 is not a combination anyone tests.

```toml
[dependencies]
kafka-produce = "0.4"   # writing records
kafka-consume = "0.4"   # reading them back, with or without a group
kafka-admin   = "0.4"   # topics, configs, acls, groups
kafka-read    = "0.4"   # browse-shaped scans and tails, for a UI
tokio         = { version = "1", features = ["full"] }
futures       = "0.3"   # only for the scan stream
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

## Producing a record

`Producer::connect` takes bootstrap addresses of its own; `Producer::new`
wraps a `Cluster` you already have — from `admin.cluster()`, say. A producer
is cheap to clone, and every clone shares one accumulator, so two clones
writing to the same partition fill the same batch rather than two.

```rust,no_run
use kafka_produce::{ClusterConfig, Producer, ProducerConfig, ProducerRecord};

# async fn example() -> kafka_produce::Result<()> {
let producer = Producer::connect(
    ["localhost:9092"],
    ClusterConfig::default(),
    ProducerConfig::new(),
)
.await?;

let meta = producer
    .send(
        ProducerRecord::new("orders")
            .with_key("customer-7")
            .with_value(r#"{"total":42}"#)
            .with_header("content-type", "application/json"),
    )
    .await?;

println!("landed at {}-{} offset {}", meta.topic, meta.partition, meta.offset);
# Ok(())
# }
```

That is one record, acknowledged by the full ISR, with the partition chosen
by murmur2 over the key — the same hash a Java or C client uses, so a
co-partitioned join still lines up. Records without a key go to a sticky
partition instead of round-robin (KIP-480). Give
`ProducerRecord::with_partition` an index to choose yourself.

**Writing many records? Use `enqueue`, not a loop of `send`.** `send` waits
for the broker, so awaiting it in a loop keeps exactly one record in flight
and batches nothing. `enqueue` returns as soon as the record is buffered and
hands back a `Delivery` to await later:

```rust,no_run
# use kafka_produce::{Producer, ProducerRecord};
# async fn example(producer: &Producer) -> kafka_produce::Result<()> {
let mut pending = Vec::new();
for i in 0..10_000 {
    pending.push(
        producer
            .enqueue(ProducerRecord::new("orders").with_value(format!("{i}")))
            .await?,
    );
}

for delivery in pending {
    let meta = delivery.await?;   // where that record landed, or why it did not
}
# Ok(())
# }
```

Idempotence is on by default, which is what makes a re-send after a timeout
safe rather than a duplicate. There is no `acks=0`, deliberately. Both are in
[Producing records](guide/producing.md), along with compression,
transactions and the batching knobs.

## Consuming records

A `Consumer` reads an explicit set of partitions: nothing rebalances it,
nothing heartbeats, and no broker knows it exists as a member of anything.
That is the mode to pin a reader to a partition, and it is the engine the
group protocols sit on.

```rust,no_run
use kafka_consume::{Consumer, ConsumerConfig, Position};

# async fn example(cluster: kafka_consume::Cluster) -> kafka_consume::Result<()> {
let mut consumer = Consumer::new(cluster, ConsumerConfig::new().group_id("reporting"));

consumer
    .assign(
        [("orders".to_owned(), 0), ("orders".to_owned(), 1)],
        Position::Earliest,
    )
    .await?;

loop {
    // Empty is a normal answer: a consumer at the log end is caught up, not
    // broken.
    for record in consumer.poll().await? {
        println!(
            "{}-{} @{}: {:?}",
            record.topic, record.partition, record.offset, record.value
        );
    }

    for ((topic, partition), result) in consumer.commit().await? {
        if let Err(error) = result {
            eprintln!("{topic}-{partition}: commit failed: {error}");
        }
    }
}
# }
```

Setting `group_id` on a manually-assigned consumer borrows that group's
*offset storage* — `commit` and `committed` read and write under it using the
non-member sentinel. Borrowing the storage is not joining the group. Note the
commit result: one entry per partition, the same shape as every other
multi-resource call here.

### Joining a group

`GroupConsumer` joins a KIP-848 group and lets the **broker** compute the
assignment:

```rust,no_run
use kafka_consume::{ConsumerConfig, GroupConsumer};

# async fn example(cluster: kafka_consume::Cluster) -> kafka_consume::Result<()> {
let mut consumer =
    GroupConsumer::subscribe(cluster, ConsumerConfig::new(), "billing", ["orders"]).await?;

for _ in 0..100 {
    for record in consumer.poll().await? {
        println!("{}-{} @{}", record.topic, record.partition, record.offset);
    }
}

consumer.leave().await?;
# Ok(())
# }
```

**`poll` is what heartbeats.** It beats when one is due, reconciles any new
assignment and then reads, so a member that stops polling — because it is
doing slow work between batches — is a member the coordinator evicts. Nothing
is owned until the first heartbeat comes back with an assignment, so the
first `poll` or two returning empty is normal.

`ClassicConsumer` speaks the older `JoinGroup`/`SyncGroup`/`Heartbeat`
protocol for pre-4.0 brokers and mixed groups. Which to use, offsets, the
rebalance listener and the one hard constraint the classic path carries are
all in [Consuming records](guide/consuming.md).

## Browsing a topic

`kafka-read` answers a different question from a consumer: not "keep
delivering this topic to me" but "show me a page of it". Two shapes, because
a UI asks two things. `tail` answers "what just happened" and is the
most-used view in any Kafka UI; `scan` answers "show me this topic from the
beginning" and streams without ever materialising a `Vec`.

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

The integration tests wear `#[testkit::integration_test]`, which expands to
`#[tokio::test]` + `#[ignore = "needs Docker"]` — so `cargo test` stays fast
without a Docker daemon — and caps each test at two minutes of wall clock,
container boot included. Each one boots `apache/kafka:4.3.1` through
[`testkit`](code-tour/testkit.md); nothing in this repository is verified
against a mock broker.
