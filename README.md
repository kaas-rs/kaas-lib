# kaas-lib

A Kafka 4.x client layer for Rust, built directly on the
[`kafka-protocol`](https://github.com/kafka-protocol-rs/kafka-protocol-rs)
codec. Admin and control-plane first, with a read path shaped for browsing a
topic rather than for joining a consumer group.

📖 **[Documentation](https://kaas-rs.github.io/kaas-lib/)**

## Crates

| Crate | What it does |
|---|---|
| [`kafka-conn`](crates/kafka-conn) | framing, correlation, per-key version negotiation, TLS, SASL |
| [`kafka-meta`](crates/kafka-meta) | metadata cache, RPC routing, connection pool, error taxonomy |
| [`kafka-admin`](crates/kafka-admin) | 31 admin RPCs, one result per resource |
| [`kafka-read`](crates/kafka-read) | streaming forward scans, backward tails, tolerant decoding |

```toml
[dependencies]
kafka-admin = { git = "https://github.com/kaas-rs/kaas-lib" }
kafka-read  = { git = "https://github.com/kaas-rs/kaas-lib" }
```

Not yet on crates.io — the manifests are release-ready and
`.github/workflows/release.yml` publishes on a `v*` tag, but nothing has
been uploaded. See [RELEASING.md](RELEASING.md).

## The goal

**Which Kafka version a cluster runs should not be your problem.** No version
in your config, no feature flags, no `match` on a version number — you ask
for topics, you get topics. Per-key version negotiation, `Unknown` variants
on the api-key and error-code enums, automatic API fallback and
version-dependent request shapes are all handled below the API you call.

Where a difference genuinely cannot be absorbed, it surfaces as something
legible — `Unsupported`, `Unrecognized`, or an `UnsupportedApi` carrying both
version ranges — rather than as a silently wrong answer.

## Three invariants and a constraint

Most of the design follows from four statements, covered in the
[introduction](https://kaas-rs.github.io/kaas-lib/introduction.html):

1. **No upstream type reaches a public signature.** `kafka-protocol`'s types
   are `#[non_exhaustive]` and regenerate every Kafka release; each crate
   owns its domain types and converts at the boundary.
2. **Nothing panics.** A malformed record on one topic must not take down a
   server hosting other clusters. `unwrap`, `expect` and `panic!` are denied
   at the workspace root.
3. **Partial failure is a result, not an error.** Describing 500 topics while
   3 are mid-deletion returns 497 descriptions and 3 errors.
4. **The codec is a Kafka release behind the broker.** `kafka-protocol` 0.17
   ships Kafka 4.0 schemas; the tests run against 4.3.1. Version negotiation
   and `Unknown(i16)` arms are structural, not defensive.

## Status

37 of the protocol's 87 api keys — see the
[API support matrix](https://kaas-rs.github.io/kaas-lib/compat/api-matrix.html).

**There is no producer and no consumer-group membership.** That is the
current scope boundary, and lifting it is the project's stretch goal — see
[Roadmap](https://kaas-rs.github.io/kaas-lib/guide/roadmap.html).
**Contributions are very welcome**, particularly on that work.

## Development

```sh
cargo xtask ci             # fmt + clippy + unit tests, no Docker
cargo xtask integration    # acceptance tests against apache/kafka:4.3.1
cargo xtask docs --serve   # the book, with live reload
```

Releases: [RELEASING.md](RELEASING.md).

Integration tests are `#[ignore]`d by default so `cargo test` stays fast
without a Docker daemon. There are no mocked brokers in this workspace —
every milestone is verified against a real broker in a container.

## Relationship to kaas

This is a **client**. [`kaas`](https://github.com/kaas-rs/kaas) is a
**broker**. They speak the same protocol from opposite ends and deliberately
share no code: kaas-lib is the natural conformance harness for kaas, and two
implementations sharing a codec would share its bugs — a mutual misreading of
the spec encodes and decodes consistently, passes green, and hides exactly
the class of wire bug the harness exists to catch.

## Licence

Apache-2.0
