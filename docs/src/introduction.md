# Introduction

kaas-lib is a general-purpose Kafka 4.x client library for Rust, built
directly on the
[`kafka-protocol`](https://github.com/kafka-protocol-rs/kafka-protocol-rs)
crate. Admin, produce and consume are all first-class, configuration is
built by chaining `with_*` builders, and there is no librdkafka underneath.

It was written to support the **kaas initiative** — the
[kaas](https://github.com/kaas-rs/kaas) broker, kaas-ui, and the tooling
around them. That is its origin rather than its boundary: the crates are
published for any Rust program that talks to a Kafka 4.x cluster, and no
public API assumes a kaas component on either end. The library began
admin-first, with a read path shaped for browsing a topic; phase 2 added a
real producer and both consumer-group protocols, which is what
"general-purpose" was shorthand for. See the [roadmap](guide/roadmap.md).

If you know Apache Kafka, you know what every RPC in here does. What this
book is actually about is the layer *around* those RPCs: which broker each
one has to reach, what its errors mean, which of them are allowed to run at
all, and what happens when a broker returns something no released version of
the schema describes. That last question is not hypothetical here — it is
the normal case, and [an entire chapter](architecture/version-negotiation.md).

## This is a client. `kaas` is a broker.

The two projects speak the same protocol from opposite ends and share no
code. [kaas](https://github.com/kaas-rs/kaas) decodes requests and encodes
responses; kaas-lib does the mirror. Nothing in kaas's architecture carries
over, and its codec is deliberately not a dependency here.

That separation is load-bearing rather than incidental. kaas-lib is the
natural conformance harness for kaas — point [`testkit`](code-tour/testkit.md)
at a kaas broker instead of `apache/kafka:4.3.1` and the acceptance suite
becomes a typed parity check with real diffs. A shared codec would defeat
that: two ends agreeing on a mutual misreading of the spec encode and decode
consistently, pass green, and hide precisely the class of wire bug the
harness exists to catch.

## Three invariants and a constraint

Almost every design decision in this book follows from four statements. They
are worth reading once here, because the rest of the book keeps referring
back to them.

**1. No upstream type reaches a public signature.** `kafka-protocol`'s
generated types are `#[non_exhaustive]` and regenerate on every Kafka
release. If one appears in our public API, every upstream bump becomes a
breaking change for everyone downstream. Each crate defines owned domain
types and converts at the boundary — including the quiet case, `StrBytes`,
which is why domain types hold `String` and `Bytes`. See
[The domain boundary](architecture/domain-boundary.md).

**2. Nothing panics.** A single malformed record on one topic must not take
down a server hosting other clusters for other users. There is no `unwrap`,
`expect` or `panic!` in library code, denied at the workspace root rather
than repeated per crate, and the tolerant decoder turns a batch that will not
parse into [a value you can render](architecture/tolerant-decoding.md) rather
than an error that ends a scan.

**3. Partial failure is a result, not an error.** Any call naming several
resources returns one answer per resource — `Vec<(ResourceId, Result<T, _>)>`,
never `Result<Vec<T>, _>`. Describing five hundred topics while three are
mid-deletion returns 497 descriptions and three errors, because the
alternative makes a UI unusable on exactly the clusters that need one.

**And the constraint: the codec is a Kafka release behind the broker.**
`kafka-protocol` 0.17 ships Kafka 4.0 schemas; the acceptance suite runs
against a 4.3.1 broker. So *our* ceiling binds more often than the broker's,
brokers routinely advertise APIs and versions we cannot encode, and they
return error codes the crate cannot name. This is the normal operating
condition, not an edge case — which is why version negotiation, an
`Unknown(i16)` arm on both the api-key and error-code enums, and honest
[gap documentation](compat/upstream-gap.md) are structural rather than
defensive.

## The goal: which Kafka version a cluster runs is not your problem

That constraint has a payoff, and it is the clearest statement of what this
library is *for*.

**A caller should never have to ask what version a cluster runs.** Not in a
config file, not in a feature flag, not in a `match` on a version number. You
ask for topics; you get topics. The library works out what the cluster can
actually do and does the most it can with it.

Concretely, that means a caller never writes any of this:

```rust,no_run
# struct Client; struct Config { kafka_version: String }
// None of this exists in kaas-lib's API, deliberately.
let client = Client::connect(cfg, KafkaVersion::V3_7)?;   // no
if cluster.version() >= (4, 0) { /* use the new API */ }   // no
config.set("api.version.request", "false");                // no
```

Five mechanisms carry that promise, and most of Part I is one of them
seen up close:

| Mechanism | What it absorbs |
|---|---|
| [Per-key version negotiation](architecture/version-negotiation.md) | brokers that are newer *or* older than this build |
| `ErrorCode::Unknown(i16)` | codes from Kafka releases the codec has never heard of |
| `GroupDescription::Unrecognized` | [group kinds](compat/group-kinds.md) that cannot be described at all |
| Automatic API fallback | `DescribeTopicPartitions` where offered, `Metadata` where not — one method, either way |
| [The domain boundary](architecture/domain-boundary.md) | schema churn, so a Kafka release does not reshape *your* types |

The library also switches request *shapes* on the negotiated version without
telling you: `Fetch` v13+ identifies topics by UUID and older versions by
name; `OffsetFetch` moved its group field at v8. Both paths exist, and which
one runs is not a decision a caller makes.

### Where the abstraction stops, it says so

"As much as possible" is doing real work in that sentence, and pretending
otherwise would be the more damaging choice. Some differences between Kafka
versions are not absorbable, and for those the library's job is to be
*legible* rather than silent:

- A sentinel that needs a schema this build cannot encode is a documented
  `Unsupported`, not a silently wrong answer — see
  [`ListOffsets` `-6`](compat/upstream-gap.md).
- A group kind with no schema renders as `Unrecognized` carrying its type,
  not as an error and not as a fabricated description.
- An api the cluster genuinely lacks is `Error::UnsupportedApi`, carrying
  *both* version ranges so the reader can tell whether the cluster is old or
  this build is.

A version difference you can see and act on is a feature. One that is papered
over into a wrong number is the bug this design exists to prevent.

## What is here, and what is not

Six library crates, layered strictly:
[`kafka-conn`](code-tour/kafka-conn.md) (the wire),
[`kafka-meta`](code-tour/kafka-meta.md) (routing and cluster state),
[`kafka-admin`](code-tour/kafka-admin.md) (31 admin RPCs),
[`kafka-read`](code-tour/kafka-read.md) (forward and backward scans),
[`kafka-produce`](code-tour/kafka-produce.md) (the write path) and
`kafka-consume` (fetch sessions and group membership), plus
[`testkit`](code-tour/testkit.md) for container fixtures.

They publish in lockstep at one version: this is one library split along a
layering boundary, not six independently useful things.

What is deliberately *not* here is on [Non-goals](compat/non-goals.md), and
it is a shorter list than it used to be.

## Rust, not a binding

There is no librdkafka in this dependency tree, and no cmake. That is the
main reason to reach for this library over
[`rdkafka`](https://crates.io/crates/rdkafka), which wraps a mature C client
and is the right answer when maturity matters more than toolchain.

One honest exception, because it would be easy to imply otherwise: two
compression codecs reach C. `kafka-protocol`'s `lz4` and `zstd` features pull
`lz4-sys` and `zstd-sys`, which `cc` builds from source, so a downstream
build wants a C compiler for those. `gzip` (miniz_oxide) and `snappy` (snap)
are pure Rust. Closing the gap would mean dropping codecs that real clusters
use — zstd especially — or hand-rolling compression, and hand-rolling wire
formats is exactly what this codebase does not do.

## Everything is built by chaining

Optional settings are consuming `with_*` builders on owned types, so a
configuration is one expression rather than a `let mut` and a sequence of
assignments, and a half-built value never has to be passed anywhere. It is a
single convention across every crate: if a type has an optional setting, it
has a `with_` method for it.

### Contributions are very welcome

Especially on the above. The project is
[Apache-2.0 on GitHub](https://github.com/kaas-rs/kaas-lib), and a few things
make it a friendlier codebase to contribute to than the size suggests:

- **Every milestone has an acceptance command** that must pass, so "is this
  done?" has an answer that is not a matter of opinion.
- **No mocked brokers.** Everything runs against `apache/kafka:4.3.1` in a
  container via [`testkit`](code-tour/testkit.md), so a green test means it
  works against Kafka.
- **The traps are written down.** Part I explains *why* each decision is what
  it is, usually by naming the specific way of getting it wrong — which is
  the part that is hard to reconstruct from the source alone.

If you are picking something up, M12 (a single produced record, round-tripped)
and M16 (fetch sessions) are the two that unblock the most downstream work.
Open an issue first for anything milestone-sized, so the design conversation
happens before the code does.

## How to read this book

- **Evaluating it?** This page, then [Non-goals](compat/non-goals.md) for the
  fastest honest answer to "can I use this", then the
  [API support matrix](compat/api-matrix.md).
- **Using it?** [Getting started](getting-started.md), then Part IV —
  [connecting](guide/connecting.md), [admin](guide/admin.md),
  [reading](guide/reading.md).
- **Working on it?** Part I in order from the
  [system overview](architecture/overview.md); Part III is the
  [crate-by-crate tour](code-tour/workspace.md) of where it all lives.
