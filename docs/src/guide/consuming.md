# Consuming records

`kafka-consume` is the long-running read path: incremental fetch sessions, a
streaming fetcher, and three ways of deciding which partitions you own.

```toml
[dependencies]
kafka-consume = "0.4"
```

## Which shape do you want?

| | Assignment comes from | Rebalances | Use it when |
|---|---|---|---|
| `Consumer` | you, explicitly | never | pinning a reader to a partition, a single-instance tail, or anything that must not move |
| `GroupConsumer` | the broker (KIP-848) | yes | the default on a Kafka 4.x cluster |
| `ClassicConsumer` | the group leader, client-side | yes | brokers older than 4.0, or a mixed group with Java clients pinned to `group.protocol=classic` |

`GroupConsumer` and `ClassicConsumer` both wrap a `Consumer` rather than
replacing it: the fetch path, the sessions and the decoding are identical,
and the only thing membership changes is *where the assignment comes from*.
That is why the manual mode is not a degraded group consumer — it is the same
engine with the assignment supplied by the caller.

If what you actually want is a page of a topic rather than a stream of it,
you want [`kafka-read`](reading.md) instead. A scan is bounded and reports
progress because a UI is drawing a page; a consumer runs until told to stop,
and its interesting operations — `seek`, `pause`, `resume` — are about
changing its mind mid-stream, which a bounded scan never does.

## A manually-assigned consumer

```rust,no_run
use kafka_consume::{Consumer, ConsumerConfig, Position};

# async fn example(cluster: kafka_consume::Cluster) -> kafka_consume::Result<()> {
let mut consumer = Consumer::new(cluster, ConsumerConfig::new());

consumer
    .assign(
        [("orders".to_owned(), 0), ("orders".to_owned(), 1)],
        Position::Earliest,
    )
    .await?;

loop {
    for record in consumer.poll().await? {
        println!(
            "{}-{} @{}: {:?}",
            record.topic, record.partition, record.offset, record.value
        );
    }
}
# }
```

`Consumer::connect` takes bootstrap addresses if you have no `Cluster` yet.

`assign` **replaces** the assignment rather than adding to it, and partitions
that leave it are forgotten in the next fetch — which is what stops the
broker holding session state for partitions nobody is reading.

| `Position` | Starts at |
|---|---|
| `Earliest` | the first offset still retained |
| `Latest` | the end of the log: only records written from now on |
| `Offset(i64)` | that offset in every partition named |

**An empty `poll` is a normal answer, not an error.** A consumer at the log
end is caught up; it returns after `max_wait_ms` with nothing. A poll loop
that treats empty as a failure is a poll loop that fails on every healthy
cluster.

Records are decoded with the same tolerant decoder the scan path uses. A
batch that will not decode does not stall the partition and does not end the
stream: the position steps past it and polling continues. Unlike
`ScanEvent::Malformed`, the consumer does not surface those bytes to you —
[Tolerant decoding](../architecture/tolerant-decoding.md) covers the
difference.

### Changing its mind mid-stream

```rust,no_run
# use kafka_consume::Consumer;
# fn example(consumer: &mut Consumer) -> kafka_consume::Result<()> {
consumer.seek("orders", 0, 4_200)?;    // takes effect on the next fetch
consumer.pause("orders", 1);           // stop fetching, keep the partition
consumer.resume("orders", 1);          // continue from where it stopped

let next = consumer.position("orders", 0);   // Option<i64>
let behind = consumer.lag("orders", 0);      // Option<i64>, None until a fetch reports
# Ok(())
# }
```

A seek discards anything already buffered for that partition — a seek that
still delivered the old records would not be a seek. A paused partition keeps
its position and its place in the assignment, so `resume` does not re-resolve
anything.

## Offsets

A manually-assigned consumer can borrow a group's **offset storage** without
joining the group:

```rust,no_run
use kafka_consume::{Consumer, ConsumerConfig, Position};

# fn handle(record: &kafka_consume::Record) -> kafka_consume::Result<()> { Ok(()) }
# async fn example(cluster: kafka_consume::Cluster) -> kafka_consume::Result<()> {
let mut consumer = Consumer::new(cluster, ConsumerConfig::new().group_id("reporting"));
consumer.assign([("orders".to_owned(), 0)], Position::Earliest).await?;

// Resume where the last run stopped, rather than where `assign` started.
consumer.seek_to_committed().await?;

for record in consumer.poll().await? {
    handle(&record)?;   // …and only then commit
}

for ((topic, partition), result) in consumer.commit().await? {
    if let Err(error) = result {
        eprintln!("{topic}-{partition}: commit failed: {error}");
    }
}

let stored = consumer.committed().await?;   // HashMap<(String, i32), CommittedOffset>
# Ok(())
# }
```

Three things about this that bite if you assume otherwise:

- **A committed offset is the offset of the *next* record to read**, not the
  last one handled. Storing the last record's offset re-delivers it forever.
  `commit` stores the consumer's current positions, which are already that.
- **Borrowing the storage is not joining the group.** The commit goes out
  anonymously, with an empty member id and the `-1` non-member sentinel. The
  coordinator honours that form **only while the group has no members** —
  precisely so a detached client cannot scribble over a live group's
  positions. Point a standalone consumer at a group that has live members and
  every partition comes back `UNKNOWN_MEMBER_ID`. Group members commit as
  themselves; `GroupConsumer::commit` and `ClassicConsumer::commit` do that
  for you.
- **The result is per partition.** `commit` returns
  `Vec<((String, i32), Result<()>)>` — the same invariant as everywhere else
  in this library. An auto-commit whose result nobody checks is refused
  silently, so check it when it matters.

Do not read `__consumer_offsets` yourself. `committed` uses `OffsetFetch`;
the internal topic format is not a stable interface.

## Group membership (KIP-848)

```rust,no_run
use kafka_consume::{ConsumerConfig, GroupConsumer};

# fn still_running() -> bool { true }
# async fn example(cluster: kafka_consume::Cluster) -> kafka_consume::Result<()> {
let mut consumer =
    GroupConsumer::subscribe(cluster, ConsumerConfig::new(), "billing", ["orders", "refunds"])
        .await?
        .auto_commit(true)                    // the default
        .instance_id("billing-pod-3");        // static membership, optional

while still_running() {
    for record in consumer.poll().await? {
        println!("{}-{} @{}", record.topic, record.partition, record.offset);
    }
}

consumer.leave().await?;
# Ok(())
# }
```

**`poll` is the heartbeat.** It beats when one is due, reconciles any new
assignment, and only then reads. A member that stops polling — because it is
doing slow work between batches, or because its task was descheduled — is a
member the coordinator eventually evicts, and the partitions go to somebody
else. Keep the loop tight and do slow work elsewhere.

Nothing is owned until the first heartbeat comes back with an assignment, so
the first `poll` or two returning empty is expected rather than a symptom.
`assignment()` says what is owned right now, and `member_id()` is the id this
client generated for itself — KIP-848 inverts the classic protocol here,
where the broker issues it.

`instance_id` makes the member **static** (KIP-345): a restart inside
`session.timeout.ms` parks the assignment rather than triggering a rebalance,
which is the difference between a rolling deploy that shuffles every
partition and one that does not.

`leave()` releases the assignment — or parks it, for a static member — after
running the rebalance listener and committing. Dropping the consumer without
calling it leaves the group waiting out the session timeout before it
notices.

### The rebalance listener

Auto-commit flushes the offsets *this crate* tracks. A caller that keeps its
own per-partition state — a windowed aggregate, a write-behind buffer, a file
handle per partition — has state the library knows nothing about, and by the
time `poll` returns the partition is gone and another member may already own
it.

So the callback fires **before** the revocation takes effect, while this
member still owns the partitions and the broker is still waiting for the
acknowledgement that gives them away:

```text
listener.on_revoke  →  auto-commit  →  drop the partitions  →  acknowledge
```

```rust,no_run
use futures::future::BoxFuture;
use kafka_consume::{ConsumerConfig, GroupConsumer, RebalanceListener, Result, RevokedPartition};

struct FlushOnRevoke;

impl RebalanceListener for FlushOnRevoke {
    fn on_revoke(&mut self, revoked: Vec<RevokedPartition>) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            for partition in revoked {
                // `position` is what auto-commit is about to store — write your
                // own checkpoint at the same offset, not at the last record's.
                println!(
                    "flushing {}-{} at {}",
                    partition.topic, partition.partition, partition.position
                );
            }
            Ok(())
        })
    }
}

# async fn example(cluster: kafka_consume::Cluster) -> Result<()> {
let consumer =
    GroupConsumer::subscribe(cluster, ConsumerConfig::new(), "billing", ["orders"])
        .await?
        .on_rebalance(FlushOnRevoke);
# Ok(())
# }
```

`on_assign` fires after the new partitions are owned and readable, and
defaults to doing nothing — gaining a partition needs no protection.

Two properties to design around:

- **`on_revoke` is at-least-once.** Dropping a `poll` future must be safe, so
  a rebalance that has been computed but not finished is held on the consumer
  and retried by the next `poll` — which means a `poll` cancelled *during*
  `on_revoke` runs it again with the same partitions. A listener that flushes
  twice writes the same bytes twice; a listener that never fires loses them.
  Make it idempotent.
- **An error from the listener does not stop the rebalance.** By the time it
  runs, the group has already moved on and the broker is waiting for the
  acknowledgement. Refusing to revoke would leave this member holding
  partitions the group has given away — the double-ownership the
  revoke-then-acknowledge ordering exists to prevent. An `Err` is logged at
  `warn` and the rebalance proceeds.

## Classic groups

Only for brokers older than 4.0, or a mixed group where a Java client is
pinned to `group.protocol=classic`. `GroupConsumer` is the default on 4.x and
is strictly less work.

```rust,no_run
use kafka_consume::{Assignor, ClassicConsumer, ConsumerConfig};

# async fn example(cluster: kafka_consume::Cluster) -> kafka_consume::Result<()> {
let mut consumer =
    ClassicConsumer::subscribe(cluster, ConsumerConfig::new(), "billing", ["orders"])
        .await?
        .assignors([Assignor::CooperativeSticky, Assignor::Range]);

for record in consumer.poll().await? {
    println!("{}-{} @{}", record.topic, record.partition, record.offset);
}
# Ok(())
# }
```

**Every member needs its own `Cluster`.** That is not a style preference but
a hard requirement of this protocol: `JoinGroup` blocks on the coordinator,
and a Kafka broker will not read a second request from a socket until it has
answered the first — so two members of one group sharing a connection
deadlock, and it presents as a plain timeout with nothing in any log to
explain it. `GroupConsumer` has no such constraint.

| `Assignor` | Rebalancing | Notes |
|---|---|---|
| `Range` | eager | Java's default first choice, and therefore ours |
| `RoundRobin` | eager | deals every partition in rotation |
| `CooperativeSticky` | incremental (KIP-429) | keeps what it can, moves the rest over two rounds |

The advertised order is a **vote, not a demand**: the coordinator intersects
every member's list, each member votes for the first of its own that
survived, and the most-voted protocol wins. Advertising exactly one assignor
forces the issue, at the cost of failing to join any group that does not
share it — `INCONSISTENT_GROUP_PROTOCOL`, at join time, loudly.

Eager `sticky` is deliberately absent: `StickyAssignor` carries its state in
the subscription's `user_data` as a struct with no schema in
`kafka-protocol`, and hand-rolling a wire format is what this codebase does
not do. `cooperative-sticky` has no such problem, so incremental rebalancing
is available.

One protocol difference worth knowing for the listener: the classic protocol
revokes **eagerly**, so every rebalance hands `on_revoke` the *whole*
assignment rather than only the partitions that end up moving. That is what
range-style rebalancing does; it is not this client rounding up.

## Configuration

| Setting | Default | What it governs |
|---|---|---|
| `max_wait_ms` | 500 | how long a fetch may wait before answering empty |
| `max_bytes` | 50 MiB | ceiling on one fetch response |
| `partition_max_bytes` | 1 MiB | ceiling on one partition's share of it |
| `visibility` | `CommittedOnly` | whether aborted-transaction records are visible |
| `max_decompressed_bytes` | 64 MiB | ceiling on a single batch's decompressed size |
| `group_id` | none | which group's offset storage `commit` and `committed` use |

```rust,no_run
use kafka_consume::{ConsumerConfig, Visibility};

# fn example() {
let config = ConsumerConfig::new()
    .group_id("reporting")
    .visibility(Visibility::All)
    .max_wait_ms(200);

// The rest are public fields on an owned type:
let tuned = ConsumerConfig {
    partition_max_bytes: 4 * 1024 * 1024,
    ..ConsumerConfig::new()
};
# }
```

Note the default: a consumer is `CommittedOnly`, while a `ScanSpec` is
`Visibility::All`. A consumer is usually a pipeline stage that should not see
records a transaction abandoned; a scan is usually a human looking at what is
actually in the log. `read_committed` does not mean the broker filters for
you — it sends the records plus an `AbortedTransactions` list and the client
does the work.

`ConsumerConfig` and `ProducerConfig` still use bare setter names
(`.group_id(…)`, not `.with_group_id(…)`). The rest of the workspace uses the
`with_` prefix, and these predate it; renaming them breaks callers, so it is
a deliberate future pass rather than a trickle. See
[STYLE.md](https://github.com/kaas-rs/kaas-lib/blob/main/STYLE.md).

## Fetch sessions, for free

Every consumer keeps a KIP-227 incremental fetch session per broker. The
first request establishes the assignment and every request after it sends
only what changed, which in steady state is nothing at all — so a consumer
holding twelve partitions across three brokers sends **three** fetches per
round, not twenty-four, and each carries almost no request body.

A broker that drops the session (a restart, or eviction under cache pressure)
answers `FETCH_SESSION_ID_NOT_FOUND` or `INVALID_FETCH_SESSION_EPOCH`. Both
are recovered by opening a new session with the full assignment, and neither
is ever surfaced to you: a broker restart must not kill a consumer.

`kafka-read`'s `scan` and `tail` deliberately keep the legacy sentinel and
open no session, because they are one-shot and would otherwise leave broker
state behind for a client that is not coming back.

## Cancel safety

Dropping a `poll` future may discard a fetch that was in flight. It never
advances a position for records you did not receive, so the worst case is
re-fetching the same records. For a group member, a rebalance computed but
not carried out is held on the consumer, so a dropped `poll` cannot skip the
callback — the next one picks it up, still ahead of the acknowledging
heartbeat.

See [Cancel safety](../architecture/cancel-safety.md).
