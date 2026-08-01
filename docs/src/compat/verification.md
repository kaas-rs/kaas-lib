# Verification

> **A green `cargo build` is not evidence.** Every milestone has an
> acceptance command, and it must pass before the milestone is done.

Four layers, in increasing order of cost and of what they prove.

| Layer | Command | Needs | Proves |
|---|---|---|---|
| unit | `cargo xtask ci` | nothing | fmt, clippy, actionlint, table-driven logic |
| acceptance | `cargo xtask integration` | Docker | it works against Apache Kafka 4.3.1 |
| fuzz | `cargo xtask fuzz` | nightly | the decoder cannot panic *or exhaust memory* |
| interop | `cargo xtask interop` | Docker, cmake, libclang, libcurl | we agree with a genuinely different client |

## Unit — the fast gate

`cargo xtask ci` is fmt, clippy across all targets with `-D warnings`, and
the unit tests. **No Docker**, so it stays fast enough to run on every save,
and it is what CI runs on every push.

`.cargo/config.toml` sets `rustflags = ["-D", "warnings"]`, so warnings fail
the build locally rather than only in CI. The lints from
[rule 2](../introduction.md) live in `[workspace.lints]` at the root, which
means a new crate inherits them by adding `[lints] workspace = true` to its
manifest rather than by repeating `#![deny(...)]` attributes and eventually
forgetting one.

The unit tests that matter here are table-driven over things that are easy to
get quietly wrong: every error code including one no Kafka release defines,
the version-negotiation clamp in both directions, the routing class of every
api key, and `allow_auto_topic_creation` being false.

## Acceptance — real brokers, no mocks

`#[ignore]`d by default so `cargo test` stays useful without a Docker daemon.
Each test boots `apache/kafka:4.3.1` in KRaft mode through
[`testkit`](../code-tour/testkit.md).

The acceptance tests are written to fail on the specific wrong implementation
rather than to confirm the right one. A few examples of what that means in
practice:

- **Backward scan** asserts not just that the last 500 records of a
  100k-record partition come back correctly, but that **fewer than 5% of the
  partition's bytes were fetched** — measured with the connection byte
  counters. A naive implementation that reads the whole partition returns the
  right answer and fails this test.
- **Auto topic creation** requests metadata for a nonexistent topic against a
  broker with `auto.create.topics.enable=true`, then uses a *second client*
  to assert the topic was not created. Asserting on our own response would
  prove nothing.
- **Truncated batches** fetch with `max_bytes` small enough to cut a batch
  mid-stream and assert **zero** `Malformed` events. The truncation must be
  invisible, and a decoder that reports it passes a naive test and fails this
  one.
- **KIP-368 re-authentication** runs a broker with
  `connections.max.reauth.ms` around 10 seconds and asserts a connection
  survives past twice that window while still serving requests. The only way
  to prove re-auth works is to let a session expire.
- **The read-only gate** drives its assertion from `ApiKey::iter` rather than
  a hand-written list, so new protocol keys are covered automatically.
- **Per-item results** describe 50 topics where 2 do not exist and assert
  48 `Ok` plus 2 `Err`, not a global `Err`.

Multi-broker fixtures (`cluster(3)`) exist because leader spread, log dirs
and reassignment are not observable on one broker.

The suite runs `--no-fail-fast`. Cargo otherwise stops at the first test
*binary* that fails, and each one here is a whole milestone — one broken
assertion in `kafka-read`'s forward scan is enough to leave the leak suite
unrun and M11's status simply unknown. Finding out *which* milestones are red
is the entire point of the command, so it pays for all of them; the exit
status still carries any failure.

## Fuzz — rule 2, made executable

```sh
cargo xtask fuzz
```

A `cargo-fuzz` target over `RecordBatch` bytes whose pass condition is simply
**no panic**. That is rule 2 stated as a program rather than as an intention:
a malformed record from one topic must not kill a server hosting other
clusters.

It needs a nightly toolchain, so it gets its own CI job rather than pinning
the whole workspace to nightly for one target.

Its first genuinely green run found a real bug, and found it as an
**out-of-memory** rather than a panic: a 99-byte batch declaring 285 million
records, against a decoder that sized its `Vec` from that number before
parsing anything. Worth internalising — rule 2 can be violated without any
`unwrap` in sight, by allocation rather than by abort, and libFuzzer counts
that as a finding even though "no panic" is the stated pass condition. See
[Tolerant decoding](../architecture/tolerant-decoding.md#a-record-count-is-not-a-promise).

## Interop — the silent-wrongness class

```sh
cargo xtask interop
```

Produce with `rdkafka`, read with `kafka-read`, and the reverse. This is the
only layer that catches bugs where both ends of *our* code agree with each
other and disagree with the rest of the world:

- murmur2 partitioning
- snappy xerial framing
- header encoding
- tombstones (a null value must round-trip as null, not as empty bytes)

**The snappy path is asserted explicitly, and it is the assertion that paid.**
`kafka-protocol` 0.17.0 *rewrote* snappy to emit Java/xerial framing — 0.16
and earlier were mutually incompatible with the Java client — and it decodes
by autodetecting between that and raw snappy. It is the newest code in the
dependency and the one we were least entitled to assume was right.

It was not. `rdkafka` writes raw unframed snappy, upstream's autodetection
consumes the bytes it is sniffing, and the interop case failed the first time
it ran — a bug no unit test in this workspace could have found, because both
ends of every unit test are our own code and our own encoder emits xerial.
That is precisely the silent-wrongness class this layer exists for. See
[The upstream schema gap](upstream-gap.md#5-raw-snappy-does-not-decode).

The `interop` crate is deliberately **outside the workspace**: `rdkafka`
builds librdkafka from C source and wants cmake, which is a fine thing to
require of the cross-client job and a terrible thing to require of
`cargo xtask ci`.

## Live clusters

Beyond the container fixtures, the workspace has
[`livetest`](../code-tour/livetest.md) — a binary that runs the library
against real Kafka clusters rather than ephemeral containers. It emits
partial reports on failure and ranks topics by record count, so a run against
a production-shaped cluster produces something readable rather than a
stack trace. See [Testing against a real cluster](../guide/live-cluster.md).

This is also where the conformance-harness idea from
[Non-goals](non-goals.md) becomes concrete: pointing the same suite at a
`kaas` broker instead of `apache/kafka:4.3.1` turns it into a typed parity
check with real diffs.

## What CI runs when

| Job | Trigger |
|---|---|
| `rust` (fmt, clippy, unit) | every push and PR |
| `docs` (mdbook + linkcheck + path scan) | every push and PR |
| `integration` | manual (`workflow_dispatch`) |
| `fuzz` | manual |
| `interop` | manual |

The slow three are manual because they are minutes rather than seconds. That
is a deliberate trade and it has a cost: the acceptance tests are the ones
that actually decide whether a milestone is done, so they have to be run —
by a person, before saying so.
