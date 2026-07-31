# CLAUDE.md

## What this is

A Kafka 4.x client layer built directly on the `kafka-protocol` crate, for the backend of a Kafka cluster UI (a kafbat-ui equivalent). Admin/control-plane first, plus a UI-shaped read path. Not a general-purpose client — see PLAN.md for scope.

## Stack

- Rust stable, edition 2021, tokio
- `kafka-protocol = "0.17"` — **codec only**, features: `snappy`, `gzip`, `lz4`, `zstd`
- `tokio-util` (LengthDelimitedCodec), `tokio-rustls`, `bytes`, `thiserror`, `tracing`
- Tests: `testcontainers` against `apache/kafka:4.2.0`

## Layout

```
crates/
  kafka-conn/     # framing, correlation, ApiVersions, TLS, SASL, connection pool
  kafka-meta/     # metadata cache, leader + coordinator routing, error taxonomy
  kafka-admin/    # admin RPCs
  kafka-read/     # scan API (forward + backward)
```

## Hard rules

1. **Never leak `kafka_protocol::*` types in a public API signature.** They are `#[non_exhaustive]` and regenerate on every Kafka release. Define owned domain types in each crate and convert at the boundary. This one is not negotiable — violating it means every upstream bump is a breaking change for us.
2. **No `unwrap`, `expect`, or `panic!` in library code.** A malformed record from one topic must not kill a server hosting other clusters. `#![deny(clippy::unwrap_used, clippy::expect_used)]` at each crate root. Tests may unwrap freely.
3. **No stubs, no `todo!()`, no mocked brokers.** If a task can't be completed, stop and say so rather than leaving a placeholder that looks finished. Every milestone is verified against a real broker in a container.
4. **Multi-resource admin calls return per-item results**, i.e. `Vec<(ResourceId, Result<T, KafkaError>)>`, never `Result<Vec<T>, _>`. Describing 500 topics where 3 are mid-deletion must return 497 successes.
5. **Every public async fn must be cancel-safe.** Dropping the future releases buffers and either completes-and-discards the in-flight request or closes the connection. Never leave a connection with a half-read response.
6. Conventional commits. One milestone per branch.

## Protocol traps — read before writing wire code

These are the things that are easy to get subtly wrong, and each produces a confusing off-by-a-few-bytes failure rather than a clear error:

- **The response header version is not the request's API version.** Resolve it via the helper methods on `ApiKey`; do not derive it from the api version yourself.
- **`ApiVersions` responses always use response header v0**, even on a flexible-versions connection. This is a special case in the protocol. Get this wrong and your very first round trip fails.
- **Never hardcode an API version.** Negotiate via `ApiVersions` on connect, store the `(min, max)` range per api key per connection, and pick `min(broker_max, our_max)`. A hardcoded version works on your laptop and fails on the customer's cluster.
- **Construct protocol structs with `Default::default()` plus `.with_*()` builders**, never struct literals — they are `#[non_exhaustive]` and literals won't compile.
- **Compression features must be enabled in Cargo.toml.** Without them, decoding a compressed batch fails at runtime, not compile time, and the error is unhelpful.
- **Do not parse `__consumer_offsets`.** Use `OffsetFetch`. The internal format is not a stable interface.
- **`ListOffsets` has more than earliest/latest.** `EARLIEST_LOCAL_TIMESTAMP` and `LATEST_TIERED_TIMESTAMP` matter for tiered storage and must be surfaced distinctly, or the UI reports wrong retention.
- **Kafka 4.x groups come in three kinds** — classic, consumer (KIP-848), share — described by *different RPCs* with *different response shapes*. Do not flatten them into one struct.

## Verification

Every milestone has a command in PLAN.md that must pass before it is considered done. Run it. Do not report a milestone complete on the basis of `cargo build` succeeding.

```sh
cargo clippy --all-targets -- -D warnings
cargo test -p <crate>                 # unit
cargo test -p <crate> -- --ignored    # integration, needs Docker
```

Integration tests are `#[ignore]`d by default so `cargo test` stays fast without Docker.

## Reference

- Protocol spec: https://kafka.apache.org/protocol.html
- Message schemas (source of truth for versions/fields): https://github.com/apache/kafka/tree/trunk/clients/src/main/resources/common/message
- `kafka-protocol` docs: build locally with `cargo doc --open` (docs.rs build for 0.17.0 is broken)
