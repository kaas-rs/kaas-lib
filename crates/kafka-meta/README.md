# kafka-meta

**Kafka 4.x cluster state and routing.** Part of [kaas-lib](https://kaas-rs.github.io/kaas-lib).

Metadata cache, RPC routing, connection pooling and retry. Everything above
sends through `Cluster`, which resolves the right broker, retries on the
errors that mean "your view is stale", and keeps an immutable snapshot readers
take without blocking.

Not every RPC goes to any broker. Six routing classes — any, controller, group
coordinator, transaction coordinator, a broker the caller names, and the
partition leader — live in one table, because getting it wrong produces a
`NOT_CONTROLLER` retry loop that looks like a flaky cluster rather than an
error.

## Documentation

- [Metadata, routing and the pool](https://kaas-rs.github.io/kaas-lib/architecture/metadata-routing.html)
- [The error taxonomy](https://kaas-rs.github.io/kaas-lib/architecture/errors.html)

Full book: <https://kaas-rs.github.io/kaas-lib/>

## Licence

Apache-2.0
