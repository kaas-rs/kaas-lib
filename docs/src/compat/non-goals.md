# Non-goals

The fastest honest answer to "can I use this". Everything here is a decision,
not a gap waiting to be closed by accident — though
[Roadmap](../guide/roadmap.md) covers which ones are on the list to lift.

## This is not a general-purpose Kafka client

**There is no producer.** `Produce` is classified, routed and gated, and
nothing sends one. You cannot write a record with this library.

**There is no consumer-group membership.** The library *describes* groups
thoroughly — all three describable kinds, members, assignments, committed
offsets — but it never joins one. No `JoinGroup`, no
`ConsumerGroupHeartbeat`, no rebalance, no auto-commit, no poll loop.

If you want to produce or to consume as a group member today, use
[`rdkafka`](https://crates.io/crates/rdkafka). It wraps librdkafka, needs
cmake and a C toolchain, and is the mature option.

## The read path is browse-shaped, not consumer-shaped

`scan` and `tail` answer a UI's questions: show me this topic from here, show
me what just happened. They are bounded, one-shot, and they never commit an
offset.

Consequences worth being explicit about:

- **No incremental fetch sessions.** Every fetch is a full fetch. Correct for
  one-shot scans, wrong for a steady-state consumer — see
  [KIP-227](kip-index.md).
- **No offset commits from the read path.** `kafka-admin` can commit offsets
  as an *admin* operation (resetting a group's position); the read path never
  does it as a side effect of reading.
- **No partition assignment or ownership.** Two `scan` calls on the same
  partition both read it. Nothing coordinates them, because nothing is
  supposed to.

## We do not parse `__consumer_offsets`

> Use `OffsetFetch`. The internal format is not a stable interface.

It is tempting — reading the internal topic directly gives you every group's
offsets in one scan instead of one `OffsetFetch` per group. It is also a
format Kafka changes between releases without notice, because it is
internal, and a client that parses it is a client that breaks on upgrade in a
way nobody can debug from the outside.

This is why `FindCoordinator` is classified read-only by
[the gate](../architecture/read-only-gate.md) despite being able to trigger
`__consumer_offsets` creation on some clusters: the alternative is worse.

## We do not act as a broker

`kafka-protocol` is depended on with `default-features = false` and the
`client` feature, which **drops the broker half of the codegen** — every
response encoder and request decoder across 87 message types.

That is not a size optimisation, it is a statement of scope. The
[KRaft and broker-internal APIs](api-matrix.md) are not "not yet
implemented"; they are not ours to send.

## We do not share code with the kaas broker

Stated as a non-goal because it looks like obvious reuse and is not.

[kaas](https://github.com/kaas-rs/kaas) is a Kafka-compatible broker by the
same author, and it has a codec. Using it here would be a mistake: kaas-lib
is the natural conformance harness for kaas, and **two implementations
sharing a codec share its bugs**. A mutual misreading of the spec encodes and
decodes consistently, passes green, and hides precisely the class of wire bug
the harness exists to catch.

So `testkit`'s bootstrap addresses sit behind a trait rather than hardcoding
the Apache image, and the codec dependency stays `kafka-protocol`.

## We do not hide upstream gaps

Where `kafka-protocol` cannot express something, the library says so rather
than working around it. `ErrorCode::Unknown(i16)`,
`GroupDescription::Unrecognized`, `ours: None` in the version table, the
documented-but-unreachable `ListOffsets` `-6` sentinel — all of these are
visible degradation by choice.

A hand-rolled schema to fill a gap would be a private fork of a generated
artifact, and it would destroy the compile-error signal that makes
[the error taxonomy](../architecture/errors.md) trustworthy. See
[The upstream schema gap](upstream-gap.md).

## We do not mock brokers

Every milestone is verified against a real broker in a container. There are
no mocked responses, no recorded fixtures replayed as if they were a cluster,
and no `todo!()` standing in for an unfinished path.

The cost is that the meaningful tests need Docker and take minutes. The
benefit is that "the tests pass" and "it works against Kafka" are the same
statement. See [Verification](verification.md).
