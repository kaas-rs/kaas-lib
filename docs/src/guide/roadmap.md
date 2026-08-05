# Roadmap

Phase 2 has landed. The library was admin-first with a browse-shaped read
path; it now has a real producer and both consumer-group protocols, which is
what "general-purpose Kafka client" was shorthand for.

The milestone breakdown with acceptance criteria lives in `PLAN.md` (M0–M19).
This page is what shipped, what is deliberately still missing, and what
nothing in this repository can move.

## What phase 2 delivered (M12–M19)

**The producer** — [`kafka-produce`](../code-tour/kafka-produce.md).

| | |
|---|---|
| M12 | one record round trip, `Produce` v13's `topic_id: Uuid` |
| M13 | the accumulator: batching, linger, bounded buffer memory, per-record delivery futures |
| M14 | idempotence: `InitProducerId`, per-partition sequences, recovery from `OUT_OF_ORDER_SEQUENCE_NUMBER` and `UNKNOWN_PRODUCER_ID` |
| M15 | transactions, including the epoch bump KIP-890 hides inside `EndTxn` |

Both traps this page flagged before M12 was written got resolved rather than
discovered:

- **`acks=0` is not offered.** A request with no response would leave a
  pending `oneshot` in [the connection actor](../architecture/connection.md)
  forever, so every successful write would report a timeout. It is refused at
  the config boundary rather than given a fire-and-forget path.
- **The `max_in_flight` warning turned out to be the wrong worry.** At most
  one batch per partition is on the wire regardless, so ordering does not
  depend on the setting at all. The clamp — one without idempotence, five
  with — is defence for the connection layer, not the mechanism keeping the
  log in order.

**The consumer** — `kafka-consume`.

| | |
|---|---|
| M16 | KIP-227 incremental fetch sessions, a streaming fetcher batching partitions per *broker*, and `OffsetCommit` for a non-member |
| M17 | KIP-848 groups: client-generated member id, broker-computed assignment |
| M18 | the classic protocol: `JoinGroup`/`SyncGroup`/`Heartbeat`, with assignor payloads byte-identical to Java's |
| M19 | interop against `rdkafka` in both directions, plus leak tests for the new crates |

M18 was conditional on the classic protocol being needed, and it was: the
acceptance suite runs a mixed group with one Rust member and one
`kafka-console-consumer.sh`, which is the case that makes byte-compatible
assignor payloads non-optional.

Two gaps found after the fact and since closed: a caller had no way to flush
per-partition state before revocation, so `on_rebalance` now runs `on_revoke`
while the member still owns the partitions and before the auto-commit; and
the classic path advertised only eager assignors, so `cooperative-sticky`
joined them.

**Publishing.** The crates are on crates.io, releasing in lockstep at a single
version — see [RELEASING.md](https://github.com/kaas-rs/kaas-lib/blob/main/RELEASING.md).
`kafka-consume` joins the published set at 0.3.0.

## Next

Nothing here is structural. These are ordinary gaps with no blocker beyond
someone doing them.

- **Java's `StickyAssignor`.** The classic path ships three of the four
  assignors `PLAN.md` lists — `range`, `round-robin` and `cooperative-sticky`.
  Plain `sticky` is the eager one that keeps assignments stable across a
  rebalance without the two-round handover, and it is the remaining name a
  mixed group might vote for.
- **KIP-699 batched `FindCoordinator`.** The v4+ `coordinator_keys` shape is
  already used, but with one key per request. Batching is one round trip
  instead of one per group, which matters for a UI rendering hundreds.
- **`DescribeQuorum`** — the one KRaft-adjacent API a cluster UI plausibly
  wants. Present in the `ApiKey` enum and reachable through generic dispatch;
  there is no typed method.
- **Delegation token management.** Only the ACL *resource type* exists today;
  `Create`/`Renew`/`Expire`/`DescribeDelegationToken` do not.
- **`OffsetForLeaderEpoch`, `UpdateFeatures`, `ListConfigResources`** — routing
  entries only, no user-facing surface.
- **A code-tour page for `kafka-consume`**, which is the one published crate
  the book does not walk through.

## Blocked upstream

Not roadmap items, because nothing in this repository can move them. See
[The upstream schema gap](../compat/upstream-gap.md):

- **Streams groups (KIP-1071)** — no schema in `kafka-protocol` 0.17, so a
  4.1+ cluster running Kafka Streams reports `groupType=streams` in
  `ListGroups` and we surface it as `Unrecognized` rather than describing it.
- **`ListOffsets` `-6`** (`EARLIEST_PENDING_UPLOAD_TIMESTAMP`) — needs v11;
  the codec caps at v10. The other five sentinels are surfaced.
- **Error codes past Kafka 4.1** — surfaced as `Unknown(i16)` until upstream
  names them.
