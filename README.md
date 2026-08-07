# kaas-lib

A general-purpose Kafka 4.x client library for Rust, built directly on the
[`kafka-protocol`](https://github.com/kafka-protocol-rs/kafka-protocol-rs)
codec. Admin, produce and consume are all first-class, with a fluent API and
no librdkafka underneath.

It was written to support the [kaas initiative](#relationship-to-the-kaas-initiative)
— the kaas broker, kaas-ui, and the tooling around them — but nothing in it
assumes a kaas component on either end.

📖 **[Documentation](https://kaas-rs.github.io/kaas-lib/)**

## Crates

| Crate | What it does |
|---|---|
| [`kafka-conn`](crates/kafka-conn) | framing, correlation, per-key version negotiation, TLS, SASL |
| [`kafka-meta`](crates/kafka-meta) | metadata cache, RPC routing, connection pool, error taxonomy |
| [`kafka-admin`](crates/kafka-admin) | 31 admin RPCs, one result per resource |
| [`kafka-read`](crates/kafka-read) | streaming forward scans, backward tails, tolerant decoding |
| [`kafka-produce`](crates/kafka-produce) | record batch encoding, murmur2 and sticky partitioning, batching, idempotence, transactions |
| [`kafka-consume`](crates/kafka-consume) | incremental fetch sessions, a streaming fetcher, KIP-848 and classic group membership |

```toml
[dependencies]
kafka-produce = "0.4"
kafka-consume = "0.4"
kafka-admin   = "0.4"
kafka-read    = "0.4"
```

The crates publish to crates.io in lockstep at a single version — they are one
library split along a layering boundary, not several independently useful
things, so `kafka-admin` 0.2 against `kafka-conn` 0.1 is not a combination
anyone tests. Pull whichever layers you need at the same version.

Pre-1.0, so breaking changes land in the minor position. See
[RELEASING.md](RELEASING.md).

## The goal

**Which Kafka version a cluster runs should not be your problem.** No version
in your config, no feature flags, no `match` on a version number — you ask
for topics, you get topics. Per-key version negotiation, `Unknown` variants
on the api-key and error-code enums, automatic API fallback and
version-dependent request shapes are all handled below the API you call.

Where a difference genuinely cannot be absorbed, it surfaces as something
legible — `Unsupported`, `Unrecognized`, or an `UnsupportedApi` carrying both
version ranges — rather than as a silently wrong answer.

**Rust, not a binding.** There is no librdkafka here, and no cmake. One
honest exception: the `lz4` and `zstd` codecs reach C through
`kafka-protocol`'s `lz4-sys` and `zstd-sys`, so a build wants a C compiler
for those two. `gzip` and `snappy` are pure Rust. That is a much smaller ask
than a vendored broker client, but it is not zero and we would rather say so
than imply otherwise.

**Everything is built by chaining.** Optional settings are consuming
`with_*` builders on owned types, so configuration reads as one expression
and there is no `let mut` and no half-built struct to pass around.

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

The write and consume paths are complete, which is what "general-purpose"
was shorthand for. `kafka-produce` writes records with a batching
accumulator, Java-compatible murmur2 partitioning, KIP-480 sticky
partitioning for unkeyed records, every compression codec, idempotence and
transactions. `kafka-consume` reads them back over incremental fetch
sessions (KIP-227), as a manually-assigned consumer or as a member of a
KIP-848 or classic group. A consume-process-produce loop is exactly-once end
to end: `send_offsets_to_transaction` (KIP-447) commits the consumer's offsets
inside the producer's transaction, so an abort takes them with it.

Two deliberate omissions, both stated where you would hit them:

* **`acks=0` is not offered.** It is a request the broker never answers, and a
  correlation-based client reports every successful write as a timeout. See
  the [`kafka-produce` documentation](crates/kafka-produce).
* **Classic groups advertise `range`, `roundrobin` and `cooperative-sticky`,
  not eager `sticky`.** `StickyAssignor` carries its state in the
  subscription's `user_data` as a struct with no schema in `kafka-protocol`,
  and hand-rolling a wire format is exactly what this codebase does not do.
  Cooperative-sticky has no such problem — `owned_partitions` and
  `generation_id` are real fields of the real schema — so incremental
  rebalancing is available. A group whose other members are pinned to eager
  `sticky` alone fails at join time with `INCONSISTENT_GROUP_PROTOCOL`, which
  is loud rather than subtle.

See [Roadmap](https://kaas-rs.github.io/kaas-lib/guide/roadmap.html).
**Contributions are very welcome**, particularly on the remaining write and
consume work.

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

## Relationship to the kaas initiative

kaas-lib exists to support the kaas initiative — the
[`kaas`](https://github.com/kaas-rs/kaas) broker, kaas-ui, and the tooling
around them. That is where it came from, not what limits it: the crates are
published for any Rust program that speaks to a Kafka 4.x cluster, and no
public API assumes a kaas component on either end.

Within that, one boundary is deliberate. This is a **client**; `kaas` is a
**broker**. They speak the same protocol from opposite ends and share no
code: kaas-lib is the natural conformance harness for kaas, and two
implementations sharing a codec would share its bugs — a mutual misreading of
the spec encodes and decodes consistently, passes green, and hides exactly
the class of wire bug the harness exists to catch.

## Licence

Apache-2.0
