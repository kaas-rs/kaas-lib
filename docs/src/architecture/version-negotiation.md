# Version negotiation

> **Never hardcode an API version.** A hardcoded version works on your laptop
> and fails on the customer's cluster.

Every Kafka API is versioned independently, and brokers advertise a
`(min, max)` range per api key in their `ApiVersions` response. The rule is
to intersect that with what this build can encode and take the highest
version in the overlap.

```
negotiated = min(broker.max, ours.max)   — provided the ranges overlap at all
```

## Which side binds

In most Kafka clients the broker's ceiling is the interesting one: the client
is current and some clusters are old. **Here it is the other way round, and
that inversion drives a lot of the design.**

`kafka-protocol` 0.17 ships Kafka **4.0** schemas. The acceptance suite runs
against a **4.3.1** broker. So our max is the binding side more often than
not, and *schemas older than the broker is the normal case, not an edge
case*.

Three consequences, each of which had to be designed for rather than
discovered:

1. A broker will advertise **versions we cannot encode**. Clamp to ours.
2. A broker will advertise **api keys we cannot name at all** —
   `StreamsGroupDescribe` on any 4.1+ broker running Kafka Streams. The
   version table keeps those rows.
3. A broker will return **error codes the crate does not name**. Both our
   `ApiKey` and `ErrorCode` enums carry an `Unknown(i16)` arm.

[The upstream schema gap](../compat/upstream-gap.md) lists exactly what is
unreachable today because of this.

## The table

`ApiVersions` is keyed by wire code rather than by `ApiKey`, precisely so
that a key we have no name for still survives into the table:

```rust,no_run
# use kafka_conn::{ApiKey, Connection};
# async fn example(conn: &Connection) {
for entry in conn.versions().entries() {
    println!(
        "{} broker={:?} ours={:?} negotiated={:?}",
        entry.api_key,
        entry.broker,
        entry.ours,        // None when this build has no schema for the key
        entry.negotiated(),
    );
}
assert!(conn.versions().supports(ApiKey::Metadata));
# }
```

`ours` is `Option<VersionRange>`, and `None` means `kafka-protocol` has no
schema for that key. Returning `None` rather than guessing is the whole point
— the gap stays visible instead of being papered over.

`broker_ahead()` reports the normal case against a newer broker, and is what
the acceptance test asserts on to prove the clamp is happening on our side.

## Why `negotiate_with` exists

There are two ways to ask "what version can we send", and the difference
between them is a real bug that was found live.

`ApiVersions::negotiate` uses `our_range`, which reads
`ApiKey::valid_versions()`. That is derived **per api key**, and where a
request and its response have different schema ranges it reports the wider of
the two.

`OffsetFetch` is the live example: **the response reaches v10, the request
stops at v9.** Negotiating from the api key alone picks v10, and the encoder
then refuses to encode a v10 request that does not exist.

So `Connection::send` calls `negotiate_with`, passing the request and
response types' own `VERSIONS` constants instead of the api key's:

| Function | Range used | Correct for |
|---|---|---|
| `negotiate(api_key)` | `ApiKey::valid_versions()` | a **report** — the version table a UI renders |
| `negotiate_with(api_key, ours)` | the specific request/response types | **encoding an actual request** |

## Failure is typed, never a guess

When there is no overlap — or the broker never advertised the key at all —
the result is `Error::UnsupportedApi`, carrying both ranges:

```text
no usable version of StreamsGroupDescribe: broker offers Some((0, 1)), we speak None
```

Both halves matter for diagnosis. `ours: None` means *we* have no schema
(bump the codec); a narrower `ours` than `broker` means the broker is ahead
(also bump the codec); a narrower `broker` than `ours` means the cluster is
old (nothing to do but degrade).

Falling back to "send it at v0 and hope" would turn a clear, actionable error
into a decode failure several layers away.

## Version-dependent request shapes

Negotiating the number is only half the job. Where a request's *shape*
changes with the version, the code has to build a different request — and the
codec rejects a field set outside its own version range rather than ignoring
it, so "set both the old field and the new one" is an encode failure, not a
compatibility trick.

The live examples in this workspace:

- **`Fetch` v13+** identifies topics by `Uuid` instead of by name. Below v13
  it is the name. Both paths exist in `crates/kafka-read/src/fetch.rs`.
- **`DescribeTopicPartitions`** exists at all only on newer brokers, so
  `kafka-admin` checks `supports()` and falls back to `Metadata`. That
  fallback has to handle *two* causes — "the broker is too old" and "our
  schemas are too old" — arriving at the same place.

## Per connection, not per cluster

The table is negotiated on each connection during the handshake and stored on
it. Brokers in one cluster can be mid-rolling-upgrade and genuinely disagree
about what they support, so a cluster-wide table would be wrong during
exactly the window when being right matters.
