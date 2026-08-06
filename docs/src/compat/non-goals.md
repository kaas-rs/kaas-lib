# Non-goals

The fastest honest answer to "can I use this". Everything here is a decision,
not a gap waiting to be closed by accident — though
[Roadmap](../guide/roadmap.md) covers which ones are on the list to lift.

## What is no longer a non-goal

This page used to open by saying kaas-lib was not a general-purpose client:
a small producer with no accumulator, and no group membership at all. Both
have since shipped, and the page is kept honest here rather than quietly
edited, because "can I use this" deserves a straight answer.

- **The producer is a real one.**
  [`kafka-produce`](../code-tour/kafka-produce.md) has a batching accumulator
  with linger and bounded buffer memory, Java-compatible murmur2
  partitioning, KIP-480 sticky partitioning, every compression codec,
  idempotence and transactions.
- **Group membership exists**, in both protocols — KIP-848 and the classic
  `JoinGroup`/`SyncGroup`/`Heartbeat` path, with assignor payloads
  byte-identical to Java's.
- **Incremental fetch sessions exist** (KIP-227), behind a streaming fetcher
  that batches partitions per broker.

Two genuine omissions remain inside that surface, and both are decisions:

- **`acks=0` is not offered.** It is a request the broker never answers, so a
  correlation-based client would leave a pending `oneshot` forever and report
  every successful write as a timeout. Refused at the config boundary rather
  than given a fire-and-forget path.
- **Classic groups advertise `range`, `roundrobin` and `cooperative-sticky`,
  not eager `sticky`.** `StickyAssignor` carries its state in the
  subscription's `user_data` as a struct with no schema in `kafka-protocol`,
  and hand-rolling a wire format is what this codebase does not do.
  Cooperative-sticky has no such problem, so incremental rebalancing is
  available; a group whose other members are pinned to eager `sticky` alone
  fails at join time with `INCONSISTENT_GROUP_PROTOCOL`, which is loud rather
  than subtle.

[`rdkafka`](https://crates.io/crates/rdkafka) remains the more mature option,
and the honest tradeoff is maturity against toolchain: it wraps librdkafka
and wants cmake and a C toolchain, where this is Rust apart from the `lz4`
and `zstd` codecs, which reach C through `lz4-sys` and `zstd-sys`.

## `kafka-read` is browse-shaped, and stays that way

This is a scoping decision between two crates rather than a missing feature.
If you want a consumer, use `kafka-consume`. `kafka-read`'s `scan` and `tail`
answer a UI's questions — show me this topic from here, show me what just
happened — and they are bounded, one-shot, and never commit an offset.

Consequences worth being explicit about, all of them about `kafka-read`
specifically:

- **No incremental fetch sessions.** Every fetch is a full fetch. Correct for
  one-shot scans, wrong for a steady-state consumer — which is why
  `kafka-consume` has them and this does not. See [KIP-227](kip-index.md).
- **No offset commits from the read path.** `kafka-admin` can commit offsets
  as an *admin* operation (resetting a group's position) and `kafka-consume`
  commits as a group member; `kafka-read` never does it as a side effect of
  reading.
- **No partition assignment or ownership.** Two `scan` calls on the same
  partition both read it. Nothing coordinates them, because nothing is
  supposed to — coordination is what group membership is for.

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
