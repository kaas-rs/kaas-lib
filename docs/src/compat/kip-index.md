# KIP index

The Kafka Improvement Proposals this library actually implements, degrades
on, or deliberately does not implement. Ordered by number.

**Legend:** ✅ implemented · ⚠️ partial or degraded · ❌ not implemented ·
🚫 blocked upstream

Each KIP is searchable by number on the
[Apache Kafka KIP index](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals).

| KIP | What it is | Status |
|---|---|---|
| KIP-98 | Exactly-once: idempotent producer + transactions | ✅ |
| KIP-227 | Incremental fetch sessions | ✅ in `kafka-consume` |
| KIP-345 | Static consumer membership | ✅ both protocols |
| KIP-368 | SASL re-authentication | ✅ |
| KIP-405 | Tiered storage | ⚠️ `-4` sentinel surfaced |
| KIP-447 | Exactly-once v2 | ⚠️ no `sendOffsetsToTransaction` ([#10]) |
| KIP-480 | Sticky partitioner | ✅ |
| KIP-482 | Flexible versions / tagged fields | ✅ via the codec |
| KIP-516 | Topic IDs | ✅ `Fetch` v13+ |
| KIP-554 | SCRAM admin API | ✅ describe + alter |
| KIP-699 | Batched `FindCoordinator` | ⚠️ single-key form |
| KIP-734 | `MAX_TIMESTAMP` sentinel | ✅ `-3` |
| KIP-848 | The next-generation consumer rebalance protocol | ✅ describe + membership |
| KIP-932 | Queues for Kafka (share groups) | ⚠️ describe only |
| KIP-1005 | `LATEST_TIERED_TIMESTAMP` sentinel | ✅ `-5` |
| KIP-1023 | `EARLIEST_PENDING_UPLOAD_TIMESTAMP` sentinel | 🚫 needs `ListOffsets` v11 |
| KIP-1071 | Streams groups | 🚫 no schema in the codec |

## The ones worth expanding on

### KIP-848 — the new consumer group protocol ✅

Kafka 4.x's default group protocol, and the reason
[group kinds](group-kinds.md) is its own chapter. Assignment moves
server-side: the broker computes it and the client acknowledges, replacing
the JoinGroup/SyncGroup dance entirely.

Both halves are implemented. `ConsumerGroupDescribe` renders these groups
completely — epochs, assignor, per-member assignment — and
`ConsumerGroupHeartbeat` joins one, with a client-generated member id and a
broker-computed assignment. This was adopted before the classic protocol on
purpose: server-side assignment removes the byte-compatibility problem that
makes classic hard, so it was the cheaper of the two to get right first.

### KIP-932 — share groups ⚠️

Queue semantics on top of Kafka: multiple consumers on the same partition,
per-record acknowledgement, no partition-exclusive ownership.

`ShareGroupDescribe` is implemented. Note that `librdkafka` has **no**
KIP-932 support at all, so `rdkafka` cannot even generate a share-group
fixture — the tests drive `kafka-console-share-consumer.sh` in the container
instead.

### KIP-405 and KIP-1005 — tiered storage ⚠️

Both sentinels this library can reach are surfaced distinctly rather than
collapsed, and that matters more on a tiered cluster than anywhere else:
`EARLIEST` and `EARLIEST_LOCAL_TIMESTAMP` differ by exactly the data that has
been offloaded to remote storage, which on a tiered cluster is most of it. A
UI that treats them as interchangeable reports wrong retention.

The third tiered sentinel, KIP-1023's `-6`, is
[blocked upstream](upstream-gap.md).

### KIP-227 — incremental fetch sessions ✅ in one crate, deliberately not the other

`kafka-consume` establishes and maintains sessions, which is what a
steady-state consumer needs: after the first full fetch, subsequent requests
carry only what changed.

`kafka-read` deliberately does not. `crates/kafka-read/src/fetch.rs` pins
`session_id = 0, session_epoch = -1` — Java's `FetchMetadata.LEGACY`
sentinel — because a browse-shaped scan is one-shot, and an incremental
session would make each scan depend on the last for no benefit.

The split is the point: the same KIP is right for one crate and wrong for the
other, which is why they are separate crates.

### KIP-98 — exactly-once ✅ · KIP-447 — exactly-once v2 ⚠️

Both halves of KIP-98 are here. On the read side,
`Visibility::CommittedOnly` sets `read_committed` and the client filters
aborted records using the `AbortedTransactions` list the broker returns — the
broker does not filter for you. On the write side, `kafka-produce` claims a
producer id, tracks per-partition sequences, and drives
`InitProducerId`/`AddPartitionsToTxn`/`EndTxn` behind `init_transactions`,
`begin_transaction`, `commit_transaction` and `abort_transaction` —
including the epoch bump KIP-890 hides inside `EndTxn`. `DescribeTransactions`,
`ListTransactions` and `DescribeProducers` inspect the resulting state.

**KIP-447 is the gap.** There is no `sendOffsetsToTransaction`: a consumer's
offsets cannot be committed *inside* a producer transaction, so the
consume-process-produce loop cannot be made exactly-once end to end. Both
`AddOffsetsToTxn` and `TxnOffsetCommit` are already typed and routed in
`kafka-conn`, so what is missing is the producer-side method, a
group-metadata type to carry `member_id`/generation across the crate
boundary, and an acceptance test — not wire support.

Tracked in [#10].

[#10]: https://github.com/kaas-rs/kaas-lib/issues/10

### KIP-699 — batched `FindCoordinator` ⚠️

The batched form resolves many coordinators in one round trip. `kafka-meta`
uses the single-key form, which is correct on every broker version and costs
an extra round trip per group on a cold cache. Worth revisiting for a UI
rendering hundreds of groups at once.
