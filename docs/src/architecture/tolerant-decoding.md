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

**Three codecs stream, one does not:**

| Codec | Bound | How |
|---|---|---|
| gzip, LZ4, zstd | on **output** | decompressed through a `Read` wrapped in `take()`, so the limit applies *during* decompression and the allocation never happens |
| snappy | on **input** | delegated to `kafka-protocol` |

Snappy is the exception for a specific reason. Kafka's snappy is
xerial-framed, not the standard snappy frame format, and `kafka-protocol`
0.17 rewrote that code to match the Java client, decoding by autodetecting
between the two framings. Reimplementing it here to get a streaming limit
would mean maintaining a second, divergent copy of the newest and least
settled code in the dependency — a worse trade than the bound it would buy.

The input cap is a real bound rather than a hopeful one, because snappy's
expansion is limited by its format: a copy operation emits at most 64 bytes,
and xerial chunks decompress to at most 32 KiB each.

## Verified by fuzzing

Rule 2 says a malformed record must not kill the process. The executable form
of that claim is a `cargo-fuzz` target over `RecordBatch` bytes whose pass
condition is simply *no panic*:

```sh
cargo xtask fuzz
```

It needs a nightly toolchain, so it has its own CI job rather than pinning
the whole workspace to nightly.

The unit tests hand-craft a batch with a corrupt record and assert
`Malformed` is yielded and the scan continues; separately, they fetch with
`max_bytes` small enough to truncate a batch and assert **zero** `Malformed`
events — the truncation must be invisible.
