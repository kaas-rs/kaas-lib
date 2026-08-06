# Producing records

`kafka-produce` writes records: it encodes a v2 record batch, routes it to
the partition leader, and reports where it landed. Everything else on this
page is about the two things a producer has to be honest about — *where* a
record went, and *whether it really got there*.

```toml
[dependencies]
kafka-produce = "0.4"
```

## Constructing one

Two constructors, and the difference is not cosmetic.

```rust,no_run
use kafka_produce::{ClusterConfig, Producer, ProducerConfig};

# async fn example() -> kafka_produce::Result<()> {
// Its own connections, clamped to the in-flight count its guarantees allow.
let producer = Producer::connect(
    ["localhost:9092"],
    ClusterConfig::default(),
    ProducerConfig::new(),
)
.await?;

// Or share a cluster handle you already have — from `admin.cluster()`, say.
# let cluster = producer.cluster().clone();
let second = Producer::new(cluster, ProducerConfig::new());
# Ok(())
# }
```

`connect` clamps the connections it opens to `max_in_flight` — one without
idempotence, five with it, because the broker tracks exactly five in-flight
sequence windows per partition. `new` cannot: the cluster it is handed is
shared with whatever else is using it, and throttling a pool the admin and
read paths also use would slow them down to protect a guarantee they do not
need.

That is safe because **ordering does not rest on that number**. The
accumulator holds at most one batch per partition on the wire, so a re-sent
batch can never overtake a later one regardless. The clamp is defence for the
connection layer, not the mechanism.

A `Producer` is cheap to clone, and every clone shares the metadata cache,
the connection pool, the sticky partitioner's state and the accumulator.
Sharing the accumulator is what makes batching work across clones: two clones
producing to one partition fill the same batch, not two.

## One record

```rust,no_run
# use kafka_produce::{Producer, ProducerRecord};
# async fn example(producer: &Producer) -> kafka_produce::Result<()> {
let meta = producer
    .send(
        ProducerRecord::new("orders")
            .with_key("customer-7")
            .with_value(r#"{"total":42}"#)
            .with_header("content-type", "application/json"),
    )
    .await?;

println!("{}-{} @{}", meta.topic, meta.partition, meta.offset);
# Ok(())
# }
```

`RecordMetadata::timestamp` is an `Option` and is usually `None`: on a
`CreateTime` topic the timestamp stored is the one you supplied, so there is
nothing for the broker to report back and guessing would be a fabrication. A
topic configured `message.timestamp.type=LogAppendTime` fills it in.

### The record

| Builder | Effect |
|---|---|
| `with_key(k)` | the partition key, hashed with murmur2 |
| `with_value(v)` | the payload |
| `with_partition(i)` | choose the partition yourself, bypassing the partitioner |
| `with_maybe_partition(opt)` | the same, for a partition you are relaying rather than deciding |
| `with_header(name, value)` | one header; call it again for more, order preserved |
| `with_null_header(name)` | a header with a null value, which is not an empty one |
| `with_timestamp(ms)` | epoch millis; `None` means now |

**A record with no value is a tombstone.** `value: None` and
`value: Some(Bytes::new())` are different records, and on a compacted topic
the first deletes the key while the second stores nothing under it.
`kafka-read` preserves the same distinction coming back, and the round-trip
test asserts it — so never normalise one into the other.

One upstream limitation, stated where you would hit it: **a duplicate header
name cannot be written.** `ProducerRecord` keeps duplicates and `kafka-read`
returns them faithfully, but `kafka_protocol`'s record type holds headers in
an `IndexMap`, so a repeated name collapses to its last value on the way to
the wire with no error. Reading records a Java producer wrote is unaffected;
only writing them is impossible. Routing around it means hand-rolling the
record format, which is the one thing this codebase does not do.

## Many records: `enqueue`, not a loop of `send`

`send` accepts one record and waits for it, so a loop of `send().await` keeps
exactly one record in flight and batches nothing. To get the throughput,
enqueue and await the handles together:

```rust,no_run
# use kafka_produce::{Producer, ProducerRecord};
# async fn example(producer: &Producer) -> kafka_produce::Result<()> {
let mut pending = Vec::new();
for i in 0..10_000 {
    pending.push(
        producer
            .enqueue(ProducerRecord::new("orders").with_value(format!("{i}")))
            .await?,
    );
}

for delivery in pending {
    match delivery.await {
        Ok(meta) => println!("{}-{} @{}", meta.topic, meta.partition, meta.offset),
        Err(error) => eprintln!("not delivered: {error}"),
    }
}
# Ok(())
# }
```

`enqueue` returns as soon as the record is buffered; the `Delivery` resolves
when the broker has answered for the batch the record travelled in. Dropping
a `Delivery` is allowed and does **not** cancel the write — the record has
been accepted, and only the result is discarded.

`enqueue` waits when the buffer is full, and that wait is the backpressure.
Without the `buffer_memory` bound, a producer whose broker has stopped
acknowledging accepts records until the process dies.

**`linger` defaults to zero, and that is not a reason to raise it.** A
partition holds one batch on the wire at a time, so records arriving during a
round trip accumulate into the next batch on their own. Batching scales with
load rather than with the setting: an idle producer pays no latency, and a
busy one batches anyway. Raise it only for a producer whose records arrive in
bursts smaller than one round trip.

`flush` waits for everything buffered to be acknowledged. Per-record errors
still belong to their own `Delivery`; `flush` reports only that the flush
itself could not be carried out.

## Where a record lands

| The record has | The partitioner does |
|---|---|
| an explicit partition | uses it, or fails with `InvalidRequest` if the topic has no such partition |
| a key | murmur2 over the key bytes, modulo the partition count |
| neither | a **sticky** partition (KIP-480), reused until the batch is sent |

The murmur2 implementation is the Java one, checked against `rdkafka` in the
interop crate rather than against ourselves: a partitioner that is *nearly*
Java's returns a partition for every key and passes any test written against
our own reader — it just puts keys where a Java or C client would not look
for them, which breaks co-partitioned joins and compacted-topic semantics
silently and much later. `partition_for_key` is public if you need to
compute the same answer outside a producer.

## Durability

```rust,no_run
use kafka_produce::{Acks, ProducerConfig};

# fn example() {
let config = ProducerConfig::new().acks(Acks::Leader);
# }
```

| `Acks` | Means |
|---|---|
| `All` (default) | every in-sync replica has written the record |
| `Leader` | the leader has written it to its own log |

`Acks::Leader` is lossy exactly once — if the leader fails before a follower
replicates the record, the record is gone and the caller was told it arrived.
It also **acknowledges before the record is readable**: a consumer reads only
up to the high watermark, which does not advance until the ISR has the
record, so there is a window where `send` has returned an offset that a scan
of that partition will not yet show. Code that reads its own writes back
wants `Acks::All`.

### `acks=0` is not offered

There is no `None` variant on `Acks`, so the mode cannot be selected and then
fail at runtime. `acks=0` is a request the broker sends **no response to at
all**, and the connection actor correlates every in-flight request on a
`HashMap<i32, oneshot::Sender<_>>` — an `acks=0` produce would register a
waiter nothing ever resolves, and every *successful* write would surface to
the caller as a timeout. What the mode actually buys, not waiting on the
leader, is what `enqueue` provides safely. The full argument is in
[the crate's code tour](../code-tour/kafka-produce.md).

## What happens when it fails

Two kinds of failure look similar and permit completely different things:

| | What happened | May we re-send? |
|---|---|---|
| **Rejected** | a response arrived carrying an error code — `NOT_LEADER_OR_FOLLOWER` after a leader moved | **yes**, the records were definitively not appended |
| **Ambiguous** | a timeout, or the connection died in flight | **only with idempotence**, because they may already be in the log |

Collapsing the two is a bug in either direction. Retry everything without
sequence numbers and you duplicate a record on every timeout, with no error
anywhere; retry nothing and an ordinary leader election becomes a delivery
failure.

Rejections are retried under `ProducerConfig::retry` after refreshing the
metadata that made us ask the wrong broker. The delay is the point rather
than the count — three *immediate* retries all re-read the same stale answer
and fail identically.

### Idempotence

On by default. The producer claims a producer id and numbers every record, so
the broker recognises a re-sent batch and answers with the original offsets
instead of appending it twice. That is what makes an ambiguous failure
retriable, and it is why an ordinary leader election is something the
producer rides out rather than something the caller sees.

`ProducerConfig::idempotent(false)` is for brokers that cannot issue a
producer id. It does not make the producer faster; it makes it lossier.

## Compression

```rust,no_run
use kafka_produce::{Compression, ProducerConfig};

# fn example() {
let config = ProducerConfig::new().compression(Compression::Zstd);
# }
```

`None` (default), `Gzip`, `Snappy`, `Lz4`, `Zstd`. Codec choice is per
producer and applies to the whole batch; the consumer side needs no
configuration, because the codec travels in the batch header.

Note the build implication rather than a runtime one: `Lz4` and `Zstd` reach
C through `kafka-protocol`'s `lz4-sys` and `zstd-sys`, so a downstream build
wants a C compiler. `Gzip` and `Snappy` are pure Rust. This is the one place
"no C in the dependency tree" is not literally true, and it is stated rather
than papered over.

## Transactions

A transactional producer is always idempotent, and setting a transactional id
changes what the producer *is*: it claims a fenced producer id, and claiming
it **fences any earlier producer holding the same id**. That is the point of
the id rather than a side effect — it is how a restarted application takes
over cleanly from the instance it replaced.

```rust,no_run
use kafka_produce::{Producer, ProducerConfig, ProducerRecord};

# async fn example(cluster: kafka_produce::Cluster) -> kafka_produce::Result<()> {
let producer = Producer::new(
    cluster,
    ProducerConfig::new().transactional_id("billing-writer-1"),
);

// Once, before any transaction: claims the id and fences the previous holder.
producer.init_transactions().await?;

producer.begin_transaction()?;
producer.send(ProducerRecord::new("orders").with_value("a")).await?;
producer.send(ProducerRecord::new("invoices").with_value("b")).await?;

match producer.commit_transaction().await {
    Ok(()) => {}
    Err(error) => {
        eprintln!("committing failed: {error}");
        producer.abort_transaction().await?;
    }
}
# Ok(())
# }
```

Things worth knowing before you rely on it:

- **`begin_transaction` is local.** The protocol has no "begin" request; the
  coordinator first learns of the transaction when the producer enrols a
  partition in it, which happens automatically on the first write to each.
- **Commit and abort flush first.** A record still sitting in the accumulator
  when the marker is written is not in the transaction, and would appear
  afterwards as an ordinary uncommitted write.
- **Aborting deletes nothing.** It writes a marker, so a `read_committed`
  reader never sees the records and a `read_uncommitted` one does. That
  asymmetry is the protocol's. Read them back with
  `Visibility::CommittedOnly` to see what a committed reader sees.
- **`PRODUCER_FENCED` is terminal.** Another producer with the same
  transactional id has bumped the epoch, and every later request of this one
  will fail identically. Retrying is an infinite loop; the correct response is
  to stop.

## Configuration reference

Every setter is a consuming builder, so a config is one expression.

| Setting | Default | What it governs |
|---|---|---|
| `acks` | `Acks::All` | how many replicas must have the record |
| `compression` | `None` | the batch codec |
| `idempotent` | `true` | producer id and per-record sequences |
| `transactional_id` | none | transactions; implies `idempotent` |
| `linger` | `0` | how long an open batch waits for company |
| `batch_size` | 16 KiB | when an open batch is closed and sent |
| `max_request_size` | 1 MiB | ceiling on one partition's batch |
| `buffer_memory` | 32 MiB | unsent bytes before `enqueue` waits |
| `delivery_timeout` | 30 s | how long the *broker* may collect acknowledgements |
| `retry` | `RetryPolicy::default()` | how a rejected batch is re-sent |

The batching defaults are Java's, deliberately: they are the numbers every
operator's intuition is calibrated against.

A record accounted larger than `max_request_size` is refused at `enqueue`
with `MESSAGE_TOO_LARGE` before it is buffered, so it fails alone rather than
taking a batch with it. A record larger than `batch_size` is still sent, in a
batch of its own — which is the only way it can be sent at all.

Note that `delivery_timeout` is a field in the request, honoured by the
leader while it waits on its followers. It is not the connection's own
request timeout, which bounds how long we wait for the socket.

## Cancel safety

Dropping a `send` future does **not** cancel the write. Once the record has
been accepted into the accumulator it will be sent, and dropping only
discards the result. Dropping while still waiting for buffer space does
cancel it, and in that case the record was never accepted.

That is the same rule the whole workspace follows — see
[Cancel safety](../architecture/cancel-safety.md).

## Reading it back

[Consuming records](consuming.md) for a consumer, or
[Reading records](reading.md) for the browse-shaped scan and tail. A record
written with `Acks::All` is readable as soon as `send` returns; one written
with `Acks::Leader` may not be.
