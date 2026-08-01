# Roadmap

The library today is admin-first with a browse-shaped read path. **Phase 2
drops that qualifier**: a real producer and real consumer-group membership,
which together turn kaas-lib into a general-purpose Kafka client.

The full milestone breakdown with acceptance criteria lives in `PLAN.md`
(M12–M19). This page is the shape of it and the reasoning behind the
ordering.

## What is already general-purpose

Roughly 80% of the codebase has nothing UI-shaped about it and would not be
rebuilt:

| Layer | Reusable as-is? |
|---|---|
| [`kafka-conn`](../code-tour/kafka-conn.md) — framing, correlation, versions, TLS, SASL, error taxonomy | yes, entirely |
| [`kafka-meta`](../code-tour/kafka-meta.md) — metadata, routing, pool, retry | yes, entirely |
| [`kafka-admin`](../code-tour/kafka-admin.md) — 31 admin RPCs | yes — this *is* an AdminClient |
| record decoding + bounded decompression | yes |

The unglamorous parts — negotiated versions, the routing table, SASLprep,
KIP-368, the error table — are the ones pure-Rust client attempts usually
skimp on, and they are done.

## The producer half (M12–M15)

**M12 — one record round trip.** Validates record batch *encoding* the way
M1 validated framing. The codec side is free: `RecordBatchEncoder` is
available under the current feature selection, along with all four
compression codecs, so no manifest change is needed.

Two traps are called out up front because both are cheap to design for and
expensive to debug:

- **`Produce` v13 replaces the topic name with `topic_id: Uuid`**, the same
  transition `Fetch` made at v13.
- **`acks=0` is a request with no response.** The broker sends nothing back,
  and [the connection actor](../architecture/connection.md) correlates on a
  map of pending `oneshot` senders — so an `acks=0` produce would leave an
  entry that never resolves and *every successful write would report a
  timeout*. Either refuse `acks=0` at the config boundary or add an explicit
  fire-and-forget path. This has to be decided before the encoder is
  written.

**M13 — the accumulator.** Batching, linger, compression on write, per-record
delivery futures, bounded buffer memory. A batch exceeding
`max.message.bytes` fails its own records only — [rule 4](../introduction.md)
in the write direction.

**M14 — idempotence.** `InitProducerId`, per-partition sequence numbers,
recovery from `OUT_OF_ORDER_SEQUENCE_NUMBER` and `UNKNOWN_PRODUCER_ID`.

> ⚠️ **`max_in_flight` defaults to 5 and that is only safe once this lands.**
> It matches Kafka's own default and is harmless today because nothing
> retries a write. The moment M13 retries a batch, five requests in flight
> reorders records silently — no error, no log line, just a topic whose order
> is wrong. A non-idempotent producer must clamp to 1; an idempotent one may
> use 5 and no more, because the broker tracks exactly five in-flight
> sequence windows per partition.

**M15 — transactions.** And the point where
`Visibility::CommittedOnly` finally gets exercised end to end: the aborted-
transaction filter exists, but nothing in the workspace has ever *produced*
an aborted transaction to test it against.

## The consumer half (M16–M19)

**M16 — fetch sessions and the streaming fetcher.** The read-path reshape,
and a prerequisite for both group milestones.

`crates/kafka-read/src/fetch.rs` pins `session_id = 0, session_epoch = -1` —
Java's `FetchMetadata.LEGACY` sentinel. Correct for a one-shot UI scan,
wrong for a consumer, which wants KIP-227 incremental sessions. The current
`fetch()` also takes one topic per call; a consumer holding partitions across
several topics on one broker needs one request per *broker*.

This milestone also adds `OffsetCommit` for a non-member, which
[`kafka-admin`](../code-tour/kafka-admin.md) has as an admin operation but
the read path does not have at all.

**M17 — KIP-848 first.** Deliberately before the classic protocol. The
broker computes the assignment, so **there is no assignor payload to make
byte-compatible with Java's** — which is the single largest source of subtle
incompatibility in M18. On a 4.x cluster it is also the default.

**M18 — the classic protocol, only if needed.** `JoinGroup`/`SyncGroup`/
`Heartbeat`, and assignor payloads that must be byte-identical to Java's
because the group leader may be a Java client. Good news: `kafka-protocol`
ships `ConsumerProtocolSubscription` and `ConsumerProtocolAssignment` as real
schemas, so that encoding does not need hand-rolling.

Strictly more work than M17 for strictly older clusters. If you do not need
brokers older than 4.0, skip it and say so rather than half-building it.

**M19 — interop and hardening.** Produce with kafka-produce, consume with
`rdkafka`, and the reverse. Plus: the read-only gate now has four more
reachable mutating keys to hold the line on, and the `ApiKey::iter`-driven
test covers them automatically.

## Honest cost

This roughly doubles the codebase — the producer around 3–5k lines, the
consumer 4–6k, plus integration tests. The correctness bar is also higher
than anything in phase 1: these are the paths where a bug **loses or
duplicates data** rather than rendering a wrong number in a UI.

## Smaller things

Not milestones, but real:

- **KIP-699 batched `FindCoordinator`** — one round trip instead of one per
  group, which matters for a UI rendering hundreds of groups.
- **`DescribeQuorum`** — the one KRaft-adjacent API a cluster UI plausibly
  wants.
- **Delegation tokens**, `OffsetForLeaderEpoch`, `UpdateFeatures`,
  `ListConfigResources` — ordinary gaps, nothing structural blocking them.
- **Publishing to crates.io** — the crates are unpublished; internal path
  dependencies would need versions and every crate needs a `description`.

## Blocked upstream

Not roadmap items, because nothing in this repository can move them. See
[The upstream schema gap](../compat/upstream-gap.md):

- **Streams groups (KIP-1071)** — no schema in `kafka-protocol` 0.17.
- **`ListOffsets` `-6`** — needs v11; the codec caps at v10.
- **Error codes past Kafka 4.1** — surfaced as `Unknown(i16)` until upstream
  names them.
