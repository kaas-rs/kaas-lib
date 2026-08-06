# Summary

[Introduction](introduction.md)

- [Getting started](getting-started.md)

# Part I — Architecture

- [System overview](architecture/overview.md)
- [The domain boundary](architecture/domain-boundary.md)
- [The connection actor](architecture/connection.md)
- [Version negotiation](architecture/version-negotiation.md)
- [TLS, SASL and re-authentication](architecture/security.md)
- [Metadata, routing and the pool](architecture/metadata-routing.md)
- [The error taxonomy](architecture/errors.md)
- [The read-only gate](architecture/read-only-gate.md)
- [The read path](architecture/read-path.md)
- [Tolerant decoding](architecture/tolerant-decoding.md)
- [Cancel safety](architecture/cancel-safety.md)

# Part II — Kafka Compatibility

- [API support matrix](compat/api-matrix.md)
- [The upstream schema gap](compat/upstream-gap.md)
- [The four group kinds](compat/group-kinds.md)
- [KIP index](compat/kip-index.md)
- [Non-goals](compat/non-goals.md)
- [Verification](compat/verification.md)

# Part III — Code Tour

- [Workspace layout](code-tour/workspace.md)
  - [kafka-conn](code-tour/kafka-conn.md)
  - [kafka-meta](code-tour/kafka-meta.md)
  - [kafka-admin](code-tour/kafka-admin.md)
  - [kafka-read](code-tour/kafka-read.md)
  - [kafka-produce](code-tour/kafka-produce.md)
  - [kafka-consume](code-tour/kafka-consume.md)
  - [testkit](code-tour/testkit.md)
  - [livetest](code-tour/livetest.md)
  - [xtask](code-tour/xtask.md)

# Part IV — Using the Library

- [Connecting](guide/connecting.md)
- [Admin operations](guide/admin.md)
- [Producing records](guide/producing.md)
- [Consuming records](guide/consuming.md)
- [Reading records](guide/reading.md)
- [Testing against a real cluster](guide/live-cluster.md)
- [Roadmap](guide/roadmap.md)
