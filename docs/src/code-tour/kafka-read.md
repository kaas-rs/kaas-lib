# kafka-read

The read path: browse a topic forwards, or read its tail. Shaped for a UI
rather than for a consumer group — there is no rebalance, no commit, no
membership.

**Module map**

| File | Lines | What |
|---|---|---|
| `scan.rs` | 789 | the forward scan, `ScanSpec`, `ScanEvent`, interleaving |
| `batch.rs` | 609 | **the tolerant decoder** — the module everything is built around |
| `backward.rs` | 440 | the backward walk, `TailSpec` |
| `decompress.rs` | 214 | size-bounded decompression |
| `record.rs` | 204 | `Record`, `RecordOutcome`, `DecodeError`, `TimestampType` |
| `fetch.rs` | 156 | the `Fetch` request itself |
| `offsets.rs` | 78 | `ListOffsets` for start positions and tails |

## `batch.rs` is the subtle one

Read its module docs before touching anything here. Three things in a fetch
response look like corruption and are not — a truncated trailing batch, a
control batch, and aborted-transaction records — and a decoder that reports
them is **worse than no decoder at all**, because it cries wolf on every
fetch of every healthy cluster.

The `header` submodule reads fixed byte offsets out of the v2 batch header
directly (`BASE_OFFSET`, `BATCH_LENGTH`, `MAGIC`, `ATTRIBUTES`,
`LAST_OFFSET_DELTA`, `PRODUCER_ID`). That is not schema duplication: it is
the minimum needed to decide whether a batch is *complete* before handing it
to a decoder that would otherwise report truncation as corruption.

See [Tolerant decoding](../architecture/tolerant-decoding.md).

## `scan.rs` — a `Stream`, never a `Vec`

Memory is bounded by `max_buffered_records` **across the whole scan**, not
per partition. The naive implementation keeps one fetch's worth per
partition, which on a thousand-partition topic is a thousand times the
intended budget — and is discovered in production, because nobody scans a
thousand-partition topic on a laptop.

Cross-partition ordering degrades gracefully rather than silently: when the
buffer cap forces an emit before every partition is represented, the reorder
is bounded by the buffer span rather than by the topic length, and
`ScanEvent::Progress` reports that it happened.

## `backward.rs` — not a forward read with a different start

Reading forward from `latest - N` is wrong on any topic where records are not
one offset apart, which is every compacted topic and every topic that has had
`DeleteRecords` run against it.

The step **grows** when a chunk yields fewer records than its offset span
suggested. That is what stops a compacted partition with thousand-fold offset
gaps from crawling backwards a handful of records per round trip and
re-reading the whole log.

## `decompress.rs` — three codecs stream, one does not

Gzip, LZ4 and zstd decompress through a `Read` wrapped in `take()`, so the
limit applies *during* decompression and the oversized allocation never
happens. Snappy is delegated to `kafka-protocol` and bounded on its
*compressed* input instead — Kafka's snappy is xerial-framed, the crate
rewrote that code in 0.17 to match the Java client, and maintaining a second
divergent copy of the newest code in the dependency is a worse trade than the
bound it would buy.

## `fetch.rs` — deliberately session-less

`session_id = 0`, `session_epoch = -1`: Java's `FetchMetadata.LEGACY`
sentinel, no incremental fetch session. Correct for one-shot UI scans, wrong
for a steady-state consumer, and
[a roadmap prerequisite](../guide/roadmap.md) for group membership.

`min_bytes` is 1 rather than 0. Zero would also work, but Kafka treats
`min_bytes = 0` as "return immediately even with nothing", which turns a scan
into a spin when a partition is briefly empty.

Topic identification switches on the negotiated version: `Fetch` v13+ uses a
`Uuid`, below that a name. Both paths exist here.

## Where the boundary sits

This is the only place in the workspace that parses bytes from an untrusted
producer. Everything it returns is owned — `Record` holds `Bytes` and
`String`, never `StrBytes`.

**Start reading at** `batch.rs`'s module docs, then `record.rs` for
`RecordOutcome`, then `scan.rs`.

Related chapters: [The read path](../architecture/read-path.md),
[Tolerant decoding](../architecture/tolerant-decoding.md).
