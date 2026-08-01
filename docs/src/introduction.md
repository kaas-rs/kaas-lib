# Introduction

kaas-lib is a Kafka 4.x client layer for Rust, built directly on the
[`kafka-protocol`](https://github.com/kafka-protocol-rs/kafka-protocol-rs)
crate. It exists to back a Kafka cluster UI — a kafbat-ui equivalent — so
its shape is admin-and-control-plane first, with a read path designed for
browsing a topic rather than for joining a consumer group.

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

## What is here, and what is not

Five library crates, layered strictly:
[`kafka-conn`](code-tour/kafka-conn.md) (the wire),
[`kafka-meta`](code-tour/kafka-meta.md) (routing and cluster state),
[`kafka-admin`](code-tour/kafka-admin.md) (31 admin RPCs), and
[`kafka-read`](code-tour/kafka-read.md) (forward and backward scans), plus
[`testkit`](code-tour/testkit.md) for container fixtures.

There is **no producer and no consumer-group membership**. That is a scope
decision, not an oversight — see [Non-goals](compat/non-goals.md) for what
that rules out today.

## The stretch goal: a full-fledged client

The long-term aim is to drop the "admin-first" qualifier entirely and make
this a **general-purpose Kafka client** — a real producer and real
consumer-group membership alongside the admin surface it already has.

That is a bigger claim than it sounds, so here is the honest version of where
it stands.

**Most of the foundation is already general-purpose.** Roughly 80% of the
codebase has nothing UI-shaped about it and would not be rebuilt: version
negotiation, the routing table, the connection pool, the error taxonomy,
SASLprep and KIP-368 re-authentication, the tolerant record decoder and
bounded decompression. `kafka-admin` *is* an AdminClient. These are the
unglamorous parts that pure-Rust client attempts usually skimp on, and they
are done and tested against a real broker.

**What is missing is sharply defined**, which is what makes it tractable:

- a **producer** — batching, a Java-compatible murmur2 partitioner,
  idempotence, transactions
- **consumer-group membership** — KIP-848 first, since server-side assignment
  removes the byte-compatibility problem that makes the classic protocol hard
- **incremental fetch sessions**, and a streaming fetcher to go with them

[Roadmap](guide/roadmap.md) has the shape of it and the reasoning behind the
ordering; `PLAN.md` in the repository carries the full milestone breakdown
(M12–M19) with acceptance criteria for each.

It roughly doubles the codebase, and the correctness bar is higher than
anything in the current scope — these are the paths where a bug **loses or
duplicates data** rather than rendering a wrong number in a UI.

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
