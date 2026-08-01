# The domain boundary

> **No `kafka_protocol::*` type may appear in a public API signature.**

This is the first invariant from the [introduction](../introduction.md), and
the one with the least visible payoff and the highest cost of violating. It
is worth being precise about what it buys.

## Why

`kafka-protocol`'s message types are generated from Apache Kafka's schema
JSON, and they are all `#[non_exhaustive]`. Two consequences:

1. **They regenerate on every Kafka release.** Fields are added; enums gain
   variants. If `TopicInfo` were `kafka_protocol::messages::MetadataResponseTopic`,
   then every upstream bump would be a semver-breaking change for every
   consumer of kaas-lib, whether or not the field they use moved.
2. **Consumers cannot construct or exhaustively match them.** `#[non_exhaustive]`
   means downstream code cannot build one with a struct literal or write a
   `match` without a wildcard arm. Handing those types out makes callers
   inherit a constraint that exists for the codec's benefit, not theirs.

So each crate defines owned types and converts at its boundary. `TopicInfo`,
`GroupDescription`, `Record`, `ErrorCode`, `ApiKey` — all ours.

## The quiet violation

The obvious cases are easy to spot. The one that slips through is
**`StrBytes`**, which reads like a `bytes`-crate type and is not: it is
`kafka_protocol::protocol::StrBytes`. A domain struct holding one has
violated the rule as thoroughly as one holding a `MetadataResponseTopic`,
and it looks entirely reasonable in review.

The rule of thumb the workspace follows:

| Type | Verdict |
|---|---|
| `String` | fine — domain types hold this |
| `bytes::Bytes` | fine — shared ecosystem vocabulary, not a protocol type |
| `kafka_protocol::protocol::StrBytes` | **violation**, however innocent it looks |
| `uuid::Uuid` | fine — but note `kafka-protocol` uses it for topic ids, so `kafka-meta` wraps it as `TopicId` anyway |

## The one exception

`Connection::send` is generic over `kafka_protocol::protocol::Request`:

```rust,no_run
# use kafka_conn::{Connection, Result};
# async fn example(conn: &Connection) -> Result<()> {
# use kafka_conn::protocol::Request;
// conn.send(request) — generic over the codec's own Request trait.
# Ok(())
# }
```

`kafka-conn` *is* the wire boundary. Defining a parallel `Rpc` trait here and
requiring every request type to implement it would mean converting protocol
types into protocol types for no gain — the crate's whole job at that point
is to encode a `kafka-protocol` struct.

The exception stops there. `kafka-meta`, `kafka-admin` and `kafka-read` are
held to the rule without exception, which is why `kafka-admin` has a
524-line `types.rs` doing nothing but owning the vocabulary.

`kafka-conn` also re-exports the codec as `kafka_conn::protocol`, so crates
above it pin the dependency in exactly one manifest. Re-exporting is not
licence to expose: those types may be *used* above, never *returned*.

## What this costs, honestly

A lot of conversion code that does nothing clever. `kafka-admin` is 4,636
lines and a large fraction of it is field-by-field translation from a
response struct into an owned one.

The alternative is worse in a specific way: a UI backend hosting many
clusters cannot afford a client whose types change shape when the library
tracks a new Kafka release. Absorbing the churn at one boundary inside this
workspace is exactly the point.

## Where the boundary is enforced

By review, not by a lint — there is no `clippy` rule for "this type came from
that crate". The practical guards are:

- Owned types live in obvious places: `crates/kafka-admin/src/types.rs`,
  `crates/kafka-meta/src/snapshot.rs`, `crates/kafka-read/src/record.rs`.
- The conversion is always at the *edge* of a public function, never
  half-done: a public function returns owned types or it does not return.

Note what the rule does **not** say. Every crate above `kafka-conn` still
depends on `kafka-protocol` directly and uses its types freely in private
code — `crates/kafka-read/src/batch.rs` is built around
`RecordBatchDecoder`, and every admin module constructs request structs. The
constraint is on *signatures*, not on imports. A `pub(crate)` helper passing
a `MetadataResponseTopic` around is fine; a `pub fn` returning one is not.

