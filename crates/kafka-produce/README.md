# kafka-produce

The write path for [kaas-lib](https://github.com/kaas-rs/kaas-lib): record
batch encoding, murmur2 and sticky partitioning, and acknowledged produce
against the partition leader.

```rust,no_run
use kafka_produce::{Producer, ProducerConfig, ProducerRecord};

let producer = Producer::new(cluster, ProducerConfig::new());
let meta = producer
    .send(ProducerRecord::new("orders").key("customer-7").value("hello"))
    .await?;
println!("landed at {}:{}", meta.partition, meta.offset);
```

`acks=0` is deliberately not offered — see the crate documentation for why,
and for what replaces it.

Part of a workspace whose crates release in lockstep; pull it at the same
version as `kafka-conn`, `kafka-meta`, `kafka-admin` and `kafka-read`.

Apache-2.0.
