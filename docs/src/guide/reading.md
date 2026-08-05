# Reading records

Two APIs answering two different questions. Neither is a consumer — see
[The read path](../architecture/read-path.md).

Both take a `&Cluster`. Get one from `Cluster::connect`, or from
`admin.cluster()`.

## The tail — "what just happened"

The most-used view in any Kafka UI, and it is a backward walk rather than a
forward read.

```rust,no_run
use kafka_read::TailSpec;

# async fn example(cluster: &kafka_meta::Cluster) -> kafka_read::Result<()> {
// Last 500 records per partition, all partitions.
let tails = kafka_read::tail(cluster, &TailSpec::new("orders", 500)).await?;

for tail in &tails {
    println!(
        "partition {}: {} records, {} malformed batches, {} fetches",
        tail.partition,
        tail.records.len(),
        tail.malformed,
        tail.fetches,
    );
}

// Or narrow it.
let spec = TailSpec::new("orders", 100).partitions([0, 3]);
let tails = kafka_read::tail(cluster, &spec).await?;
# Ok(())
# }
```

This returns a `Vec` rather than a stream, deliberately: you asked for a
bounded number of records, and the implementation reads roughly that many
bytes rather than the whole partition. On a compacted topic with large offset
gaps it still converges — the step grows when a chunk yields fewer records
than its offset span suggested.

## The scan — "show me this topic"

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
        ScanEvent::Record(record) => {
            println!("{}:{} {:?}", record.partition, record.offset, record.value);
        }
        ScanEvent::Progress(progress) => {
            if let Some(fraction) = progress.fraction() {
                println!("{:.0}%", fraction * 100.0);
            }
        }
        ScanEvent::Malformed { offset, last_offset, reason, .. } => {
            eprintln!("offsets {offset}..={last_offset:?} did not decode: {reason}");
        }
        _ => {}
    }
}
# Ok(())
# }
```

`Box::pin` because the returned stream is not `Unpin`.

**Start positions:**

| `StartPosition` | Meaning |
|---|---|
| `Earliest` | the first offset still retained |
| `Latest` | the end of the log — only new records |
| `Offset(i64)` | the same explicit offset in every partition |
| `Timestamp(i64)` | the first record at or after a wall-clock time, epoch millis |

## Handle `Malformed`, do not ignore it

This is the point of the whole design. A batch that will not decode becomes
an event carrying the offsets it covered and the raw bytes, and the scan
continues.

```rust,no_run
# use kafka_read::ScanEvent;
# fn example(event: ScanEvent) {
match event {
    ScanEvent::Malformed { offset, last_offset, raw, reason } => {
        // Render "offsets 4102–4530 would not decode", offer the hex.
        // Do NOT abort the scan, and do NOT treat this as a transport error.
    }
    _ => {}
}
# }
```

Granularity is a **batch**, not a record — a corrupt record takes its batch
with it, bounded by `max.message.bytes`. That was a deliberate choice, not a
limitation discovered late; see
[Tolerant decoding](../architecture/tolerant-decoding.md).

What you will **not** see as `Malformed`: a batch truncated by `max_bytes`
(normal on every fetch), control batches (transaction markers), or aborted
records under `CommittedOnly`. Those are filtered silently, because reporting
them means crying wolf on every fetch of every healthy cluster.

## Transactions and visibility

```rust,no_run
use kafka_read::{ScanSpec, Visibility};

# fn example() {
// Default: read_uncommitted. Aborted records are visible.
let all = ScanSpec::new("orders").visibility(Visibility::All);

// read_committed. Aborted records filtered client-side.
let committed = ScanSpec::new("orders").visibility(Visibility::CommittedOnly);
# }
```

`read_committed` does **not** mean the broker filters for you — it sends the
records plus an `AbortedTransactions` list, and the client does the work.
That is Kafka's design, not a shortcut here.

## Filtering

```rust,no_run
# use kafka_read::{RecordFilter, ScanSpec};
# fn example(filter: RecordFilter) {
let spec = ScanSpec::new("orders").filter(filter);
# }
```

`RecordFilter` runs **client-side**, after decoding — Kafka has no
server-side filtering, so a filter reduces what you iterate, not what crosses
the network. Use `partitions` and `limit` to reduce bytes; use the
filter to reduce noise.

## Cancelling

Drop the stream. That is the whole protocol.

Dropping mid-scan releases the buffer, drops the in-flight fetch futures, and
leaves every connection consistent — no half-read responses, nothing to
unwind. See [Cancel safety](../architecture/cancel-safety.md).

```rust,no_run
# use futures::StreamExt;
# async fn example(cluster: &kafka_meta::Cluster) -> kafka_read::Result<()> {
# let spec = kafka_read::ScanSpec::new("orders");
let mut stream = Box::pin(kafka_read::scan(cluster, spec).await?);
while let Some(event) = stream.next().await {
    // …stop whenever you like; just drop it
    break;
}
# Ok(())
# }
```

## Memory

Bounded by `ScanSpec::max_buffered_records` (default **10,000**) across the
**whole scan**, not per partition. Scanning a thousand-partition topic uses
the same budget as scanning one — which is the difference between a UI
backend that survives a large cluster and one that does not.

Lowering it tightens memory at the cost of cross-partition ordering: a
smaller buffer forces more emits before every partition is represented, which
widens the bounded reorder window that `ScanEvent::Progress` reports.
