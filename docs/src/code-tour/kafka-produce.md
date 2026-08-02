# kafka-produce

The write path: encode a record batch, route it to the partition leader, and
report where it landed. The first half of lifting the library past its
admin-first scope.

**Module map**

| File | Lines | What |
|---|---|---|
| `producer.rs` | 462 | `Producer::send`, leader routing, **the retry classification** |
| `partition.rs` | 293 | murmur2 and the KIP-480 sticky partitioner |
| `encode.rs` | 196 | v2 record batches, and the batch-splitting trap |
| `config.rs` | 172 | `ProducerConfig`, `Acks`, `Compression` |
| `record.rs` | 135 | `ProducerRecord`, `RecordMetadata` |

## Three decisions worth knowing before reading the code

### `acks=0` is refused at the type level

[`Acks`] has no `None` variant, so the mode cannot be selected and then fail
at runtime. It is not squeamishness: `acks=0` is a request the broker sends
**no response to at all**, and `Connection` correlates every in-flight
request on a `HashMap<i32, oneshot::Sender<_>>`. An `acks=0` produce would
register a waiter nothing ever resolves, and every *successful* write would
surface to the caller as a timeout.

The alternative was a fire-and-forget path that drops the correlation entry
at send time. That was rejected because it punches a hole in the connection
actor's invariant that every in-flight request has a waiter, because a mode
whose whole character is discarding results sits badly with a library that
treats partial failure as a result, and because idempotence needs the
response to advance its sequence numbers. What `acks=0` actually buys — not
waiting on the leader — is what a batching accumulator provides safely.

### A rejection is not an ambiguous failure

This is the crate's central safety property, and it is a type rather than a
boolean so that a new failure path has to declare which kind it is:

| | what happened | may we re-send? |
|---|---|---|
| `Attempt::Rejected` | a response arrived carrying an error code | **yes** — the record was definitively not appended |
| `Attempt::Ambiguous` | a timeout, or the connection died in flight | **no** — it may have been written and the ack lost |

Collapsing the two is a bug in either direction. Retry everything and you
duplicate a record on every timeout, with no error anywhere. Retry nothing
and an ordinary leader election becomes a delivery failure.

The second is not hypothetical — it is what the library did until a live run
against a second broker implementation caught it, on a freshly created topic
whose leader had not settled. Note also that the backoff matters as much as
the count: three *immediate* retries all re-read the same stale metadata and
fail identically, so the crate reuses [`RetryPolicy`] rather than counting
attempts itself.

### The encoder splits batches where you do not expect

`RecordBatchEncoder` decides where one batch ends and the next begins by
walking records while `offset - sequence` stays constant. Offsets necessarily
increase, so the obvious thing — a constant `NO_SEQUENCE` on every record —
makes that difference increase too, and **every record is emitted as its own
batch**, each with its own 61-byte header and its own CRC.

The records all arrive, in order, and read back correctly. It is a throughput
bug wearing a correctness result, and the only thing that catches it is an
assertion on `lastOffsetDelta` in the encoded bytes. `encode.rs` counts the
sequence up from `NO_SEQUENCE`, which is what the wire format implies anyway:
the batch header stores a base sequence plus a per-record offset delta, and
the decoder reconstructs the sequence as their sum.

## murmur2 is checked against a different implementation, not against itself

A partitioner that is *nearly* Java's returns a partition for every key,
round trips through our own reader, and passes any test written against
ourselves. It just puts keys where a Java or C client would not look for
them, which breaks co-partitioned joins and compacted-topic semantics
silently and much later.

So `partition.rs`'s own tests assert properties — determinism, range, tail
handling for every length residue, spread — and the byte-exactness assertion
lives in the interop crate, where `rdkafka` produces 1000 keys with
`partitioner=murmur2_random` and every one must land where we say it does.
That setting is explicit for a reason: librdkafka's *default* partitioner is
not the Java-compatible one, so leaving it unset would compare our murmur2
against a different hash entirely.

## What is not here yet

No accumulator, so a produce is one round trip per record. No idempotence, no
transactions, no `InitProducerId`. See [Roadmap](../guide/roadmap.md).

[`Acks`]: https://docs.rs/kafka-produce
[`RetryPolicy`]: https://docs.rs/kafka-meta
