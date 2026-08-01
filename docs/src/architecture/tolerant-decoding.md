# Tolerant decoding

The module the whole read path is built around. One batch that will not
decode does not fail the scan — it becomes a value the UI can render.

```rust,no_run
# use bytes::Bytes;
# struct Record;
# struct DecodeError;
enum RecordOutcome {
    Ok(Record),
    Malformed {
        offset: i64,              // from the batch header, readable even when the records are not
        last_offset: Option<i64>, // when the header was intact enough to say
        raw: Bytes,               // so it can be dumped, hexed or reported
        reason: DecodeError,
    },
}
```

A UI that says "offsets 4,102–4,530 would not decode, here are the raw
bytes" is useful. A UI that says "this partition failed" is not, and a
library that returns the second when it could return the first has discarded
information the operator needs.

## Three things that look like corruption and are not

A decoder that reports these is **worse than no decoder at all**, because it
cries wolf on every single fetch.

**1. A truncated trailing batch.** `max_bytes` cuts a fetch mid-batch by
design. Every fetch ends this way whenever there is more data than the
budget — which is to say, most fetches. Flagging it means claiming corruption
at the end of every fetch on every healthy cluster.

This is why `crates/kafka-read/src/batch.rs` reads a handful of fixed offsets
out of the v2 batch header directly. It is not schema duplication: it is the
minimum needed to decide whether a batch is *complete* before handing it to a
decoder that would otherwise report a truncation as corruption.

**2. Control batches.** Attribute bit 5 marks a transaction marker — a commit
or abort record the broker itself writes into the log. It is not user data
and has no key or value worth showing. Skip it.

**3. Aborted transaction records.** Under `read_committed` the broker sends
them anyway and hands over an `AbortedTransactions` list for the *client* to
filter with. Not filtering means showing records that were explicitly rolled
back, which is a correctness bug that looks like working software.

Everything else that fails to decode becomes `Malformed` and the scan
continues.

## Granularity is a batch, decided deliberately

`RecordBatchDecoder` decodes a whole batch into a `Vec<Record>` and errors at
*batch* granularity. Per-record tolerance is not something the crate's API
offers.

The two options were to vendor the record loop or to accept batch-level
`Malformed`. The workspace accepts batch-level, and the cost is stated
plainly: **a corrupt record takes its batch with it**, bounded by
`max.message.bytes`.

What matters is that this was settled up front rather than discovered while
writing the scan. The design's actual claim — one bad batch does not fail the
scan, and the failure carries enough information to be actionable — holds
either way.

## Decompression is size-bounded

Gzip's maximum expansion ratio is about **1032:1**. A producer that can write
a 1 MiB batch — Kafka's default `max.message.bytes` — can make a client
allocate a gigabyte.

For a UI backend serving many clusters, that is a denial of service against
every other cluster in the process, and it costs the attacker almost nothing.
So decompression is bounded, using
`RecordBatchDecoder::decode_with_custom_compression` as the hook.

| Codec | Bound | How |
|---|---|---|
| gzip, LZ4, zstd | on **output** | decompressed through a `Read` wrapped in `take()`, so the limit applies *during* decompression and the allocation never happens |
| snappy, unframed | on **declared output** | the block header states its decompressed size, checked before anything is allocated |
| snappy, xerial-framed | on **input** | delegated to `kafka-protocol`, which walks the blocks itself |

The framed case keeps an input cap rather than an output one because
`kafka-protocol` allocates per block from each block's own declared length,
with no hook in between. That cap is a real bound rather than a hopeful one:
snappy's expansion is limited by its format — a copy operation emits at most
64 bytes, and xerial chunks decompress to at most 32 KiB each.

## Kafka's snappy is two formats, and the crate cannot tell them apart

Snappy on the wire is not one thing. The Java client frames it with
snappy-java's *xerial* header; `librdkafka` — and with it most of the
non-Java ecosystem — writes **raw, unframed** snappy. A reader has to accept
both, which is why `kafka-protocol` autodetects.

Its autodetection is broken in 0.17.0. It reads the 16-byte magic header with
`try_get_bytes(16)`, and that call *advances* the buffer. When the header
does not match — the raw case — the fallback then runs on a buffer whose
first sixteen bytes are already gone, and fails with `failed to decompress
raw snappy bytes`. Upstream's own fallback test passes only because its
fixture is fifteen bytes long, one short of the header, so the read returns
`Err` and consumes nothing.

The consequence for a UI is not subtle: **no snappy topic written by a
non-Java producer can be read at all.**

So `crates/kafka-read/src/decompress.rs` decides the framing itself, while
the buffer is still whole, and delegates only the xerial case. This is not
the reimplementation the module otherwise refuses to attempt — the raw branch
is a single `snap` call with no framing logic in it, and it is the branch that
gets the *better* bound of the two.

This is the one place the workspace knowingly diverges from the codec crate.
Revisit it when `kafka-protocol` fixes the detection upstream.

## A record count is not a promise

`RecordBatchDecoder` reserves the whole `Vec<Record>` from the batch header's
`recordsCount` *before parsing a single record*, and the only check it applies
to that number is that it is not negative. The count is attacker-controlled
bytes: a 99-byte batch declaring 285 million records asks for a multi-gigabyte
allocation.

Decompression bounds do not help, because the reservation happens on the
header, not on the payload. So the count is checked against what the batch
could physically hold — its own payload when uncompressed, the decompression
ceiling when compressed — and an impossible count becomes `Malformed` like any
other unreadable batch. The allocation is then proportional to bytes already
accepted rather than to a number the sender chose freely.

The divisor is six bytes per record, deliberately one below the true
seven-byte floor for a v2 record, so no batch a real producer writes is ever
rejected.

## Verified by fuzzing

Rule 2 says a malformed record must not kill the process. The executable form
of that claim is a `cargo-fuzz` target over `RecordBatch` bytes whose pass
condition is simply *no panic*:

```sh
cargo xtask fuzz
```

It needs a nightly toolchain, so it has its own CI job rather than pinning
the whole workspace to nightly.

The unbounded record count above is what it found on its first genuinely
green run, and the shape of that finding is worth keeping in mind: libFuzzer
reported it as an **out-of-memory**, not a panic. "No panic" is the pass
condition, but a decoder can violate rule 2 without ever panicking — killing
a process by allocation rather than by abort. Both count.

The unit tests hand-craft a batch with a corrupt record and assert
`Malformed` is yielded and the scan continues; separately, they fetch with
`max_bytes` small enough to truncate a batch and assert **zero** `Malformed`
events — the truncation must be invisible.
