# The read path

Two shapes, because a UI asks two different questions. Neither is a consumer:
there is no rebalance, no commit, no membership.

| API | Question | Wire strategy |
|---|---|---|
| `scan` | "show me this topic from *here*" | forward `Fetch`, streamed |
| `tail` | "what just happened" | `ListOffsets(LATEST)`, then walk backwards in chunks |

## Forward scan

`scan` returns a `Stream`, never a `Vec`.

A UI browsing a partition with a hundred million records must not decide how
much memory to use based on how much data the user happened to ask for, and a
scan that materialises its results has lost that argument before the first
record is decoded.

```rust,no_run
use futures::StreamExt;
use kafka_read::{ScanEvent, ScanSpec, StartPosition};

# async fn example(cluster: &kafka_meta::Cluster) -> kafka_read::Result<()> {
let spec = ScanSpec::new("orders")
    .from(StartPosition::Earliest)
    .partitions([0, 1, 2])
    .limit(10_000);

let mut stream = Box::pin(kafka_read::scan(cluster, spec).await?);
while let Some(event) = stream.next().await {
    match event? {
        ScanEvent::Record(record) => { /* … */ }
        ScanEvent::Progress(progress) => { /* fraction() drives a progress bar */ }
        ScanEvent::Malformed { offset, .. } => { /* render, do not abort */ }
        _ => {}
    }
}
# Ok(())
# }
```

`StartPosition` covers `Earliest`, `Latest`, an explicit `Offset(i64)`, and a
wall-clock `Timestamp` resolved through `ListOffsets`.

### Memory is bounded regardless of partition count

`max_buffered_records` caps the buffer **across the whole scan**, not per
partition.

The distinction is the entire point. Interleaving records across partitions
needs lookahead, and the naive implementation keeps one fetch's worth per
partition — which on a thousand-partition topic is a thousand times the
budget anyone intended, and it is discovered in production rather than in
testing because nobody scans a thousand-partition topic on a laptop.

### Ordering, stated honestly

Within a partition: exact log order, always.

Across partitions: timestamp order whenever the buffer holds at least one
record from every partition still being read, which is the usual case. When
the cap forces an emit before every partition is represented, ordering
degrades gracefully — the record emitted is the earliest among those
buffered, so **the reorder is bounded by the span of the buffer rather than
by the length of the topic**.

`ScanProgress::reorder_window` reports the magnitude when that happens —
"records may be up to N apart", where N is the buffer budget spread over the
merge's width — so a UI can say "approximately ordered, within N" rather
than quietly lying. It is `0` whenever cross-partition order held, and
always `0` on a single-partition scan: within a partition the order is exact
whatever the buffer did, and a caveat about a guarantee that still holds
would undersell a promise the library keeps. Degradation you can observe is
a different thing from degradation you cannot.

## Backward scan

"Last N messages" is the most-used view in any Kafka UI, and it is **not** a
forward read with a different starting point.

> Reading forward from `latest - N` is wrong on any topic where records are
> not one offset apart — which is every compacted topic and every topic that
> has had `DeleteRecords` run against it.

`ListOffsets(LATEST)` per partition, then walk backwards in bounded chunks:
read `[end - step, end)`, keep what came out, and if it was not enough, move
`end` back and go again. Each chunk is an ordinary forward fetch — Kafka has
no backward read, so the walk lives in the *planning*, not in the protocol.

```rust,no_run
use kafka_read::TailSpec;

# async fn example(cluster: &kafka_meta::Cluster) -> kafka_read::Result<()> {
let tails = kafka_read::tail(cluster, &TailSpec::new("orders", 500)).await?;
# Ok(())
# }
```

### Three ways to get it wrong

**Batch boundaries do not align to the step.** A fetch from `end - step`
begins at whatever batch contains that offset, so a chunk routinely returns
records from before the window. They are filtered out, not trusted.

**Compacted topics have offset gaps.** Ask for the last 500 records of a
partition whose offsets run 0, 7, 91, 4001 and offset arithmetic
over-estimates every time. A step that assumes one record per offset walks
back a handful of records per round trip and ends up re-reading the whole
partition — precisely the naive behaviour this design exists to avoid. So the
step **grows** when a chunk yields fewer records than its offset span
suggested.

**The loop must terminate.** It stops at the partition's log start, and
because the step only ever grows, a partition with a thousand-fold offset gap
converges rather than crawling.

The acceptance test is built to catch exactly this: a partition with 100k
records and randomised batch sizes, request the last 500, and assert both
that the right 500 come back *and* that fewer than 5% of the partition's
bytes were fetched — measured with
[the connection byte counters](connection.md).

## Fetch is deliberately session-less

`crates/kafka-read/src/fetch.rs` pins `session_id = 0` and
`session_epoch = -1` — Java's `FetchMetadata.LEGACY` sentinel, meaning no
incremental fetch session at all.

That is correct for a UI. A scan is one-shot, and an incremental session
would make each scan depend on the last, which is a coupling with no benefit
when the next request may be for a different topic entirely.

It is also exactly wrong for a consumer, and reshaping it is a
[roadmap](../guide/roadmap.md) prerequisite for group membership.

## Filtering and visibility

`RecordFilter` runs client-side — Kafka has no server-side filtering — and
`Visibility` chooses the isolation level:

| `Visibility` | Isolation | Shows aborted transaction records? |
|---|---|---|
| `All` | `read_uncommitted` (0) | yes |
| `CommittedOnly` | `read_committed` (1) | no |

Note that `read_committed` does **not** mean the broker filters for you — it
sends the records and an `AbortedTransactions` list, and the client does the
filtering. See [Tolerant decoding](tolerant-decoding.md).
