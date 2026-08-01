# kafka-read

**Kafka 4.x read path.** Part of [kaas-lib](https://kaas-rs.github.io/kaas-lib).

Forward scans and backward tails, with tolerant decoding.

`scan` returns a `Stream`, never a `Vec`, and memory is bounded across the
whole scan rather than per partition. `tail` is a real backward walk, because
reading forward from `latest - N` is wrong on any compacted topic.

One batch that will not decode does not fail the scan — it becomes a
`Malformed` event carrying the raw bytes and the offsets it covered.
Truncated trailing batches, control batches and aborted-transaction records
are *not* reported as corruption: they are normal, and a decoder that flags
them cries wolf on every fetch.

## Documentation

- [The read path](https://kaas-rs.github.io/kaas-lib/architecture/read-path.html)
- [Tolerant decoding](https://kaas-rs.github.io/kaas-lib/architecture/tolerant-decoding.html)
- [Reading records](https://kaas-rs.github.io/kaas-lib/guide/reading.html)

Full book: <https://kaas-rs.github.io/kaas-lib/>

## Licence

Apache-2.0
