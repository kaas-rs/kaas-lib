# PLAN.md — Kafka 4.x client layer

Scope: enough client to back a Kafka cluster UI. Admin coverage is the bulk; the read path is browse-shaped, not group-consumer-shaped. Producer is minimal. Estimated 3–5 weeks of agent-assisted work.

Each milestone has an **Acceptance** command that must pass. A milestone is not done until it does.

---

## M0 — Scaffolding + broker harness

**Goal:** a real Kafka 4.2 broker in a container, reachable from a test.

- Cargo workspace with the four crates from CLAUDE.md.
- `crates/testkit/`: a `testcontainers` helper exposing `single_broker()` and `cluster(n: usize)` returning bootstrap addresses. Use `apache/kafka:4.2.0` in KRaft mode.
- Clippy lints and `deny` attributes wired up per CLAUDE.md rule 2.
- CI: fmt, clippy, unit tests. Integration tests gated behind `--ignored`.

**Acceptance**
```sh
cargo test -p testkit -- --ignored
# a test that boots a broker and opens a bare TcpStream to it
```

**Note:** `cluster(3)` will be needed from M6 onward for log dirs and reassignment. Build it now; retrofitting multi-broker fixtures later is painful.

---

## M1 — One round trip

**Goal:** send `ApiVersions`, decode the response. This is the milestone that validates framing and header versions — the two things most likely to be subtly wrong. Do not proceed until it works.

- `LengthDelimitedCodec` with 4-byte big-endian prefix.
- Encode `RequestHeader` + body; decode `ResponseHeader` + body.
- Handle the `ApiVersions`-uses-header-v0 special case explicitly, with a comment explaining why.

**Acceptance**
```sh
cargo test -p kafka-conn --test api_versions -- --ignored --nocapture
# prints the broker's supported (api_key, min, max) triples; assert ApiKey::Metadata is present
```

---

## M2 — Connection actor

**Goal:** typed, concurrent request/response over one socket.

- Owning task per connection: read loop + write loop, `HashMap<i32, oneshot::Sender<_>>` for correlation.
- `send<R: Request>(&self, req: R) -> Result<R::Response>` picking the negotiated version automatically.
- Bounded in-flight (default 5). Deadline per request, propagated from caller — not a fixed timeout constant.
- On connection death, all pending oneshots resolve to `Error::ConnectionClosed`. No hangs.

**Acceptance**
```sh
cargo test -p kafka-conn -- --ignored
# includes: 100 concurrent Metadata requests on one connection all resolve correctly;
# killing the container mid-flight resolves all pending futures with an error within 5s
```

---

## M3 — TLS and SASL

- rustls: system roots, custom CA, client certs, SNI override.
- SASL/PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 via `SaslHandshake` + `SaslAuthenticate`.
- SASLprep must be a real stringprep implementation. A password containing a non-ASCII space will authenticate against a Java client and fail against ours if this is skipped.

**Acceptance**
```sh
cargo test -p kafka-conn --test sasl -- --ignored
# broker containers configured for SASL_PLAINTEXT/PLAIN and SASL_SSL/SCRAM-SHA-512;
# assert both authenticate and a wrong password yields Error::Authentication, not a timeout
```

---

## M4 — Metadata cache and routing

- Background refresh + on-demand invalidation. `ArcSwap` over an immutable snapshot; reads never block.
- `leader_for(topic, partition)` and `coordinator_for(group)` (via `FindCoordinator`).
- Snapshot carries its fetch timestamp so the UI can render staleness.
- Retry on `NOT_LEADER_OR_FOLLOWER` / `COORDINATOR_NOT_AVAILABLE` with metadata refresh, capped and jittered.

**Acceptance**
```sh
cargo test -p kafka-meta -- --ignored
# 3-broker cluster, topic with 6 partitions RF=3: assert every partition resolves to a
# leader that is actually in its replica set, and that leaders are spread across brokers
```

---

## M5 — Error taxonomy

- Enumerate all broker error codes. Classify along three independent axes: `retriable`, `needs_metadata_refresh`, `needs_coordinator_refresh`.
- Keep it as one table in one file. This is a first-class artifact, not a match arm scattered through call sites.
- Distinguish at the type level: transport / timeout / authentication / authorization / broker-error-with-code / decode-failure. The UI renders each differently.

**Acceptance**
```sh
cargo test -p kafka-meta --test errors
# table-driven test over every code; plus an integration test that describes a
# nonexistent topic and asserts UnknownTopicOrPartition classified non-retriable
```

---

## M6 — Admin: topics, configs, offsets

- `CreateTopics`, `DeleteTopics`, `CreatePartitions`, `DeleteRecords`
- `DescribeConfigs`, `IncrementalAlterConfigs`
- `DescribeCluster`, `DescribeLogDirs`
- `ListOffsets` including the tiered-storage timestamp variants
- Helper joining `DescribeLogDirs` × `Metadata` for per-topic size. Do not double-count replicas — assert this in the test.

All multi-resource calls return per-item results (CLAUDE.md rule 4).

**Acceptance**
```sh
cargo test -p kafka-admin --test topics -- --ignored
# create → describe → alter retention → verify → delete round trip on a 3-broker cluster;
# plus: describe 50 topics where 2 names don't exist, assert 48 Ok + 2 Err, not a global Err
```

---

## M7 — Admin: groups

The milestone most likely to be done wrong. Read CLAUDE.md's note on the three group kinds first.

- `ListGroups` returning protocol type per group
- `DescribeGroups` (classic), `ConsumerGroupDescribe` (KIP-848), share-group describe
- Unified `GroupDescription` enum that *preserves* the distinction
- `OffsetFetch`, `OffsetCommit` (offset reset for a non-member, `generation_id = -1`), `DeleteGroups`, `OffsetDelete`
- Reject offset reset with a clear error when the group is not `EMPTY`, rather than letting the broker accept a commit a live member immediately overwrites

**Acceptance**
```sh
cargo test -p kafka-admin --test groups -- --ignored
# fixture creates one classic group (group.protocol=classic), one KIP-848 group
# (group.protocol=consumer), and one share group in the same cluster.
# Assert all three list correctly with the right protocol type and non-empty member info.
```

Generating the fixtures needs real consumers. Use `rdkafka` as a dev-dependency to produce them — it is a test fixture, not a runtime dependency, and writing our own group consumer just to test this is out of scope.

---

## M8 — Admin: security and partitions

- `DescribeAcls`, `CreateAcls`, `DeleteAcls`
- `DescribeClientQuotas`, `AlterClientQuotas`
- `DescribeUserScramCredentials`, `AlterUserScramCredentials`
- `ListPartitionReassignments`, `AlterPartitionReassignments`, `ElectLeaders`
- `ListTransactions`, `DescribeTransactions`, `DescribeProducers` (describe only)

**Read-only mode:** a client constructed read-only returns `Error::ReadOnly` for every mutating RPC *before touching the network*. Enforce centrally in dispatch, not per call site — a missed call site here is a security bug.

**Acceptance**
```sh
cargo test -p kafka-admin -- --ignored
# ACL create/describe/delete round trip against an authorizer-enabled broker;
# reassignment triggered and observed reaching completion;
# a read-only client is asserted to reject every mutating method (use a compile-time
# exhaustive match or a macro so new methods can't silently skip the check)
```

---

## M9 — Read path: forward scan

- `ScanSpec { topic, partitions, from: Offset|Timestamp|Earliest|Latest, limit, filter }`
- Returns `Stream<Item = ScanEvent>`. Never materialise a `Vec`. Memory capped by config regardless of partition count.
- Multi-partition interleave by timestamp with a documented bounded reorder window.
- `ScanEvent` includes progress variants so the UI shows a progress bar, not a spinner.
- Decompression is size-bounded — a hostile producer must not be able to send a decompression bomb.

**Tolerant decoding** (this is the point of the whole design):
```rust
enum RecordOutcome {
    Ok(Record),
    Malformed { offset: i64, raw: Bytes, reason: DecodeError },
}
```
One corrupt record must not fail its batch; one corrupt batch must not fail the scan.

**Acceptance**
```sh
cargo test -p kafka-read --test forward -- --ignored
# produce 10k records across 6 partitions with mixed compression codecs (none/gzip/
# snappy/lz4/zstd), scan from earliest, assert exact count and per-partition ordering.
# Separately: hand-craft a batch with a corrupt record, assert Malformed is yielded
# and the scan continues.
```

---

## M10 — Read path: backward scan

"Last N messages" — the most-used view in any Kafka UI, and not a forward read.

- `ListOffsets(LATEST)` per partition, then walk backwards in bounded chunks.
- Batch boundaries do not align to the step size. Compacted topics have offset gaps. A naive implementation reads the whole partition — the test below is designed to catch that.

**Acceptance**
```sh
cargo test -p kafka-read --test backward -- --ignored
# partition with 100k records and randomised batch sizes: request last 500,
# assert exactly the last 500 in order, and assert bytes fetched < 5% of partition size.
# Second case: a compacted topic with offset gaps, assert no infinite loop and correct count.
```

---

## M11 — Hardening

- `cargo-fuzz` target over `RecordBatch` bytes. Pass condition: no panic. This is CLAUDE.md rule 2 made executable.
- Leak test: spawn 1000 scans, cancel at random points, assert connection count and RSS return to baseline.
- Cross-client interop: produce with `rdkafka`, read with `kafka-read`, and the reverse. Covers murmur2 partitioning, snappy xerial framing, header encoding, tombstones — the silent-wrongness class of bug that unit tests never catch.
- `tracing` spans on every RPC (broker id, api key, version, correlation id, duration); `metrics` histograms per api key.

**Acceptance**
```sh
cargo fuzz run record_batch -- -max_total_time=300
cargo test -p kafka-read --test leak -- --ignored
cargo test --test interop -- --ignored
```

---

## Stretch (post-UI-MVP)

Minimal producer for "send a test message" — single record, explicit partition, headers, tombstones. **Murmur2 must be byte-identical to the Java client**; verify by producing the same key from both and comparing assigned partition. No accumulator or linger, but shape the API so they can be added behind config without changing call sites.

---

## Working notes for the agent

- **One milestone per session.** `/clear` between them. These milestones are sized so context stays clean.
- **Plan mode first** on M4, M7, M9, and M10 — the ones with real design content. The rest are mechanical enough to implement directly.
- **Run the acceptance command before reporting done.** A green `cargo build` is not evidence.
- **When the protocol is ambiguous, read the message schema JSON**, not a blog post and not your recollection. Linked in CLAUDE.md.
- **If something can't be done cleanly, stop and say so.** A `todo!()` that looks like progress costs more than an honest blocker.
