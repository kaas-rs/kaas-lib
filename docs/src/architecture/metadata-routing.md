# Metadata, routing and the pool

`kafka-meta` is the layer that knows what a cluster looks like. Everything
above it sends through `Cluster`, which resolves the right broker, retries on
the errors that mean "your view is stale", and keeps an immutable snapshot
that readers can take without blocking.

## The metadata snapshot

An `ArcSwap` over an immutable `MetadataSnapshot`, refreshed in the
background and invalidated on demand. Reads never block and never wait on a
refresh.

The snapshot carries its own fetch timestamp (`snapshot.age()`), which exists
for the UI: rendering "as of 4 seconds ago" is honest, and rendering stale
data as though it were live is not.

### `allow_auto_topic_creation` is always false

> `MetadataRequest::default()` sets `allow_auto_topic_creation: true`.

The schema default really is `true`, and `kafka-protocol` honours it. So
following "construct with `Default` plus builders" literally — which is the
right rule everywhere else — produces a UI that **creates a topic every time
someone typos a name into the search box**, on any cluster with
`auto.create.topics.enable=true`.

The metadata layer calls `.with_allow_auto_topic_creation(false)`
unconditionally. There is no legitimate case for `true` in this codebase, and
a unit test asserts it, because it is a one-word regression with a
destructive blast radius.

The acceptance test goes further: it requests metadata for a nonexistent
topic against a broker with auto-creation enabled, then uses a second client
to assert the topic was not created.

## The routing table

**Not every RPC goes to any broker**, and getting it wrong does not produce
an error — it produces a `NOT_CONTROLLER` or `NOT_COORDINATOR` retry loop
that presents as a flaky cluster. So this is a first-class table in its own
file (`crates/kafka-meta/src/routing.rs`), next to the error table, rather
than a decision scattered across call sites.

| Class | Resolution | Examples |
|---|---|---|
| `Routing::Any` | any live broker | `Metadata`, `DescribeConfigs`, `DescribeAcls`, `ListGroups`, `ListTransactions`, SCRAM and quota describes |
| `Routing::Controller` | the active controller | `CreateTopics`, `DeleteTopics`, `CreatePartitions`, `AlterPartitionReassignments`, `ListPartitionReassignments`, `ElectLeaders`, `UpdateFeatures` |
| `Routing::Coordinator(Group)` | `FindCoordinator` by group id | `OffsetCommit`, `OffsetFetch`, `OffsetDelete`, `DescribeGroups`, `DeleteGroups`, `ConsumerGroupDescribe`, `ShareGroupDescribe`, the share-group offset APIs, `TxnOffsetCommit` |
| `Routing::Coordinator(Transaction)` | `FindCoordinator` by transactional id | `InitProducerId`, `AddPartitionsToTxn`, `AddOffsetsToTxn`, `EndTxn`, `DescribeTransactions` |
| `Routing::Specific(Caller)` | a broker id the caller names | `DescribeLogDirs`, `AlterReplicaLogDirs`, `DescribeProducers` |
| `Routing::Specific(PartitionLeader)` | the leader from the snapshot | `Produce`, `Fetch`, `ListOffsets`, `OffsetForLeaderEpoch` |

Two things this table encodes that the four-class summary glosses over:

**`Specific` splits in two.** A broker the *caller* names
(`DescribeLogDirs` against a particular node) and a broker the *snapshot*
names (a partition leader, for `Fetch`) are the same routing class but need
completely different resolution. `BrokerSelector` carries the distinction.

**Controller-only is stricter than KRaft requires.** A KRaft broker will
forward most of these. "Most" is doing a lot of work in that sentence, and
the forwarding path has failure modes of its own, so the table routes them
directly.

### The wildcard arm is `Any`

And unlike [the read-only gate](read-only-gate.md), that is the safe default
here. Mis-routing costs at worst a redirect and a retry; mis-classifying a
mutating API costs the security property. Two wildcard arms, opposite
defaults, for reasons specific to each.

## The connection pool

One connection per broker, opened lazily, reconnected with capped jittered
backoff.

**Bootstrap re-resolution matters more than it looks.** When every known
broker is unreachable, the pool falls back to the bootstrap addresses. A
cluster that rolls every broker onto new addresses is a normal Kubernetes
event, and a pool that only knows the addresses from its last successful
metadata fetch never recovers from one — it retries a set of dead endpoints
forever.

`Endpoint` is therefore an enum, not a string: `Node(i32)` for a broker known
by id whose address comes from metadata, `Bootstrap(String)` for the
addresses we were given.

**Connecting happens under a per-endpoint async mutex**, not a global one.
Two consequences, both deliberate: a slow handshake to a dead broker does not
stall connections to healthy ones, and twenty concurrent callers wanting the
same broker open one socket rather than twenty.

## Retry and the two refresh axes

`Cluster::send` retries on the codes that mean the caller's view is stale,
with capped jittered backoff. Which cache to invalidate is decided by the
[error taxonomy](errors.md)'s two ownership axes:

- `needs_metadata_refresh` → drop the snapshot, refetch, retry.
  `NOT_LEADER_OR_FOLLOWER`, `UNKNOWN_TOPIC_OR_PARTITION` mid-reassignment.
- `needs_coordinator_refresh` → drop the cached coordinator for that group or
  transactional id, re-run `FindCoordinator`, retry.
  `NOT_COORDINATOR`, `COORDINATOR_NOT_AVAILABLE`.

These are independent. A code can need both, either, or neither, which is why
they are two booleans rather than one enum.

## Verification

The acceptance test runs against a 3-broker cluster with a 6-partition, RF=3
topic and asserts every partition resolves to a leader that is genuinely in
its own replica set, and that leadership is spread across brokers rather than
all resolving to whichever broker answered first.
