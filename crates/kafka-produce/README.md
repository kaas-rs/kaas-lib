# kafka-produce

The write path for [kaas-lib](https://github.com/kaas-rs/kaas-lib): record
batch encoding, murmur2 and KIP-480 sticky partitioning, a batching
accumulator, idempotence and transactions.

```rust,no_run
use kafka_produce::{ClusterConfig, Producer, ProducerConfig, ProducerRecord};

let producer =
    Producer::connect(["localhost:9092"], ClusterConfig::default(), ProducerConfig::new())
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
```

**Writing many records? Use `enqueue`, not a loop of `send`.** `send` waits
for the broker, so awaiting it in a loop keeps one record in flight and
batches nothing. `enqueue` returns as soon as the record is buffered and
hands back a `Delivery` to await later — that is where the throughput is, and
`linger` can stay at its default of zero because a partition holds one batch
on the wire at a time and records accumulate into the next one on their own.

Idempotence is on by default, which is what makes a re-send after a timeout
safe rather than a duplicate: a rejected batch is always retriable, an
*ambiguous* one only with a producer id. Transactions ride on the same
machinery — set `ProducerConfig::transactional_id`, then
`init_transactions`, `begin_transaction`, `commit_transaction`.

`acks=0` is deliberately not offered. It is a request the broker never
answers, and a correlation-based client reports every successful write as a
timeout; see the crate documentation for the full argument and for what
replaces it.

📖 **[Producing records](https://kaas-rs.github.io/kaas-lib/guide/producing.html)**
— the guide, including compression, the failure model and every config
default.

Part of a workspace whose crates release in lockstep; pull it at the same
version as `kafka-conn`, `kafka-meta`, `kafka-admin`, `kafka-read` and
`kafka-consume`.

Apache-2.0.
