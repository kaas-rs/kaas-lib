# CLAUDE.md

## What this is

A Kafka 4.x client layer built directly on the `kafka-protocol` crate, for the backend of a Kafka cluster UI (a kafbat-ui equivalent). Admin/control-plane first, plus a UI-shaped read path. Not a general-purpose client — see PLAN.md for scope.

## Relationship to kaas

**This repo is a client. `kaas` (kaas-rs/kaas) is a broker.** They speak the same protocol from opposite ends. If you are carrying context from the kaas repo, none of its architecture applies here.

Do not import `kaas-codec` or reuse its wire code, and do not treat "kaas already does this" as a reason to skip work. Concretely, it doesn't:

- It is server-shaped — `decode_request` + `encode_response`. `encode_request` is missing on exactly the APIs we reach for first (metadata, fetch, list_offsets, api_versions).
- 12 of the ~37 APIs we need have no module at all, and they are M7/M8's bulk: `DescribeCluster`, `DescribeTopicPartitions`, `ConsumerGroupDescribe`, `ShareGroupDescribe`, `List`/`AlterPartitionReassignments`, `ElectLeaders`, `List`/`DescribeTransactions`, `DescribeProducers`, `Describe`/`AlterUserScramCredentials`.
- It targets Kafka 3.7. `ListOffsets` caps at v7, so the tiered sentinels — the thing M6 exists to surface — are unreachable there by design, not by omission.
- It cannot decode a record. Byte-opacity is an *enforced invariant* in kaas (`tripwires.rs` counts every record decode and batch re-encode; the tests assert zero), and there is no compression code in that workspace at all. M9 and M10 have no ancestor to inherit.
- Its SCRAM is the server half — it consumes client-first/client-final against a credential store. We need the mirror, and there is no SASLprep there either.

There is a second, stronger reason to keep the codecs separate: **kaas-lib is the natural conformance harness for kaas.** Point `testkit` at a kaas broker instead of `apache/kafka:4.3.1` and the acceptance suite becomes a typed parity check with real diffs, which is more than the shell-tool suite in that repo can give. That only works if the two implementations are independent — share a codec and a mutual misreading of the spec encodes and decodes consistently, passes green, and hides precisely the class of wire bug the harness exists to catch. So keep `testkit`'s bootstrap addresses behind a trait rather than hardcoding the Apache image, and keep the codec dependency `kafka-protocol`.

## Stack

- Rust stable, edition 2024, tokio
- `tokio-util` (LengthDelimitedCodec), `tokio-rustls`, `bytes`, `thiserror`, `tracing`
- Tests: `testcontainers` against `apache/kafka:4.3.1`

```toml
kafka-protocol = { version = "0.17", default-features = false,
                   features = ["client", "messages_enums",
                               "gzip", "snappy", "lz4", "zstd"] }
```

**Codec only** — we build every layer above the wire format ourselves. Note the feature selection is deliberate:

- The four compression features are **already in `default`**; naming them only matters because we turn defaults off.
- `default-features = false` + `client` drops the `broker` half of the codegen — every response encoder and request decoder across 87 message types. We never act as a broker.
- `messages_enums` is **not** in `default`. We need it: `RequestKind` / `ResponseKind` are what the read-only gate and the generic dispatch layer match on.

## Layout

```
crates/
  testkit/        # testcontainers fixtures: brokers, clusters, SASL/authorizer configs
  kafka-conn/     # framing, correlation, ApiVersions, TLS, SASL, connection pool
  kafka-meta/     # metadata cache, leader + coordinator routing, error taxonomy, RPC routing
  kafka-admin/    # admin RPCs
  kafka-read/     # scan API (forward + backward)
```

## Upstream constraint: the crate is a Kafka release behind the broker

`kafka-protocol` 0.17.0 ships **Kafka 4.0** message schemas. (0.16.0 added 4.0 support and error codes through 4.1.0; 0.17.0 changed only snappy framing and `ApiKey::iter`.) We test against a 4.3.1 broker, so *schemas older than the broker is the normal case, not an edge case* — which is exactly why version negotiation is non-negotiable below.

Verified consequences, all of which have to be designed around rather than discovered:

1. **`StreamsGroupDescribe` / `StreamsGroupHeartbeat` have no schema in the crate.** A 4.1+ broker running Kafka Streams reports `groupType=streams` in `ListGroups` and we cannot describe those groups at all. Degrade, don't fail — see the group-kinds trap below.
2. **`ListOffsets` caps at v10 in the crate; 4.3.1 serves v11.** `EARLIEST_PENDING_UPLOAD_TIMESTAMP` (`-6`) requires v11 and is therefore unreachable until upstream bumps. Surface the other five.
3. **Error codes stop at 4.1.** A 4.3 broker can return codes the crate doesn't name. `ResponseError` already models this with `Unknown(i16)`; our owned enum must too.

When any of these blocks a milestone, that is an honest blocker (rule 3) — say so, don't work around it with a hand-rolled schema.

## Hard rules

1. **Never leak `kafka_protocol::*` types in a public API signature.** They are `#[non_exhaustive]` and regenerate on every Kafka release. Define owned domain types in each crate and convert at the boundary. This one is not negotiable — violating it means every upstream bump is a breaking change for us. Watch for the quiet violation: `StrBytes` is a kafka-protocol type, so domain types use `String` / `Bytes`. (`Bytes` itself is fine — that's the `bytes` crate, shared ecosystem vocabulary.)
2. **No `unwrap`, `expect`, or `panic!` in library code.** A malformed record from one topic must not kill a server hosting other clusters. `#![deny(clippy::unwrap_used, clippy::expect_used)]` at each crate root. Tests may unwrap freely.
3. **No stubs, no `todo!()`, no mocked brokers.** If a task can't be completed, stop and say so rather than leaving a placeholder that looks finished. Every milestone is verified against a real broker in a container.
4. **Multi-resource admin calls return per-item results**, i.e. `Vec<(ResourceId, Result<T, KafkaError>)>`, never `Result<Vec<T>, _>`. Describing 500 topics where 3 are mid-deletion must return 497 successes.
5. **Every public async fn must be cancel-safe.** Dropping the future releases buffers and either completes-and-discards the in-flight request or closes the connection. Never leave a connection with a half-read response.
6. Conventional commits. Work lands on `main` — no feature branches, single-developer repo.

## Protocol traps — read before writing wire code

These are the things that are easy to get subtly wrong, and each produces a confusing off-by-a-few-bytes failure rather than a clear error:

- **`MetadataRequest::default()` sets `allow_auto_topic_creation: true`.** The schema default is `true` and the crate honours it, so following the "Default + builders" rule literally produces a UI that *creates a topic every time someone typos a name into the search box*, on any cluster with `auto.create.topics.enable=true`. Call `.with_allow_auto_topic_creation(false)` unconditionally in the metadata layer. There is no legitimate case for `true` in this codebase.
- **The response header version is not the request's API version.** Resolve it via the helper methods on `ApiKey`; do not derive it from the api version yourself.
- **`ApiVersions` responses always use response header v0**, even on a flexible-versions connection. This is a special case in the protocol. Get this wrong and your very first round trip fails.
- **The `ApiVersions` request itself is a bootstrapping problem.** You don't know the broker's supported range until you've asked, so send at our max and treat error code 35 `UNSUPPORTED_VERSION` as "retry at v0" — the broker still returns its version table in that error response. Do not treat it as a fatal handshake failure.
- **Never hardcode an API version.** Negotiate via `ApiVersions` on connect, store the `(min, max)` range per api key per connection, and pick `min(broker_max, our_max)`. A hardcoded version works on your laptop and fails on the customer's cluster. Given the constraint section above, `our_max` is the binding side more often than not.
- **Construct protocol structs with `Default::default()` plus `.with_*()` builders**, never struct literals — they are `#[non_exhaustive]` and literals won't compile. Schema defaults are not always the safe value (see the auto-topic-creation trap).
- **Not every RPC goes to any broker.** Four routing classes, and sending to the wrong one yields `NOT_CONTROLLER` / `NOT_COORDINATOR` retry loops that look like a flaky cluster:
  - *controller only* — `CreateTopics`, `DeleteTopics`, `CreatePartitions`, `AlterPartitionReassignments`, `ElectLeaders`, `UpdateFeatures`
  - *group/txn coordinator* — resolved per group or transactional id via `FindCoordinator`
  - *one specific broker* — `DescribeLogDirs`, `DescribeProducers`
  - *any broker* — `DescribeConfigs`, `DescribeAcls`, `ListGroups`, metadata
  This table lives in `kafka-meta` next to the error table and is a first-class artifact, same as it is.
- **Long-lived connections need KIP-368 re-authentication.** `SaslAuthenticate`'s response carries a session lifetime; where `connections.max.reauth.ms` is set (Confluent Cloud sets it) the broker *kills the connection* at expiry unless we re-issue `SaslAuthenticate` on the live socket. A UI backend holds connections for hours — without this you get periodic unexplained disconnects that read as a network fault.
- **SASLprep must be a real stringprep implementation.** A password containing a non-ASCII space authenticates against a Java client and fails against ours if this is skipped.
- **Do not parse `__consumer_offsets`.** Use `OffsetFetch`. The internal format is not a stable interface.
- **`ListOffsets` has six sentinels, not two.** `-1` LATEST, `-2` EARLIEST, `-3` MAX_TIMESTAMP (KIP-734), `-4` EARLIEST_LOCAL_TIMESTAMP (KIP-405), `-5` LATEST_TIERED_TIMESTAMP (KIP-1005), `-6` EARLIEST_PENDING_UPLOAD_TIMESTAMP (KIP-1023, needs v11 — unreachable, see the constraint section). Surface them distinctly or the UI reports wrong retention on tiered clusters.
- **Kafka 4.x groups come in four kinds** — classic, consumer (KIP-848), share (KIP-932), and streams (KIP-1071) — described by *different RPCs* with *different response shapes*. Do not flatten them into one struct. We can describe the first three; streams groups exist on the wire but have no schema in the crate, so the domain enum needs an `Unrecognized { group_id, group_type }` variant. A UI that hard-fails on an undescribable group is a UI that hard-fails on most real clusters.
- **A truncated trailing batch is normal, not corruption.** Fetch responses cut off at `max_bytes` mid-batch by design. Discard silently — reporting it as malformed means every scan claims corruption at the end of every fetch, which makes the tolerant decoder worse than useless.
- **Control batches (attribute bit 5) are transaction markers, not user data.** Skip them. Separately, decide and document whether aborted-transaction records are visible; `read_uncommitted` shows them and the `AbortedTransactions` list in the fetch response is what filters them.

## Verification

Every milestone has a command in PLAN.md that must pass before it is considered done. Run it. Do not report a milestone complete on the basis of `cargo build` succeeding.

```sh
cargo xtask ci                        # fmt + clippy + unit tests, no Docker
cargo xtask integration               # the #[ignore]d acceptance tests, needs Docker
cargo xtask fmt-check

cargo clippy --all-targets -- -D warnings
cargo test -p <crate>                 # unit
cargo test -p <crate> -- --ignored    # integration, needs Docker
```

Integration tests are `#[ignore]`d by default so `cargo test` stays fast without Docker.

Lints live in `[workspace.lints]` at the root, so a new crate inherits rule 2 by adding `[lints] workspace = true` to its manifest rather than by repeating `#![deny(...)]` attributes and eventually forgetting one. `.cargo/config.toml` also sets `rustflags = ["-D", "warnings"]`, so warnings fail the build locally, not just in CI.

## Reference

- Protocol spec: https://kafka.apache.org/protocol.html
- Message schemas (source of truth for versions/fields), pinned to the broker we test against:
  https://github.com/apache/kafka/tree/4.3.1/clients/src/main/resources/common/message
- `kafka-protocol` source (the project moved orgs; older links redirect):
  https://github.com/kafka-protocol-rs/kafka-protocol-rs — check `CHANGELOG.md` for which Kafka release the schemas track
- `kafka-protocol` docs: build locally with `cargo doc --open` (docs.rs build for 0.17.0 is broken)
