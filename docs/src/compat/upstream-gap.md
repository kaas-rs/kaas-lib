# The upstream schema gap

> `kafka-protocol` 0.17.0 ships **Kafka 4.0** message schemas. The acceptance
> suite runs against a **4.3.1** broker.

That gap is permanent in shape even though its contents change: a codec crate
tracks a Kafka release, and brokers keep shipping. So *schemas older than the
broker is the normal operating condition*, not an edge case, and this page
lists exactly what it costs today.

Version history for context: 0.16.0 added Kafka 4.0 support and error codes
through 4.1.0; 0.17.0 changed only snappy framing and `ApiKey::iter`. Check
the crate's `CHANGELOG.md` for which release the schemas track before
assuming anything here is still current.

## 1. Streams groups cannot be described at all

`StreamsGroupDescribe` and `StreamsGroupHeartbeat` (KIP-1071) **have no
schema in the crate**. They occupy wire codes a 4.1+ broker advertises and
`kafka-protocol` 0.17 leaves unassigned.

The consequence is concrete and it will hit any real cluster: a 4.1+ broker
running Kafka Streams reports `groupType=streams` in `ListGroups`, and we
cannot describe those groups.

**Degrade, do not fail.** The domain enum carries an
`Unrecognized { group_id, group_type }` variant, so a streams group renders
as a known-but-undescribable group rather than taking down the group list.
A UI that hard-fails on an undescribable group is a UI that hard-fails on
most real clusters. See [The four group kinds](group-kinds.md).

There is a unit test asserting that wire code 89 — unassigned in the crate,
where a 4.1+ broker advertises `StreamsGroupDescribe` — survives in the
version table as `ApiKey::Unknown(89)` with `ours: None`.

## 2. `ListOffsets` sentinel `-6` is unreachable

`ListOffsets` has **six** sentinels, not two:

| Sentinel | Meaning | KIP | Reachable? |
|---|---|---|---|
| `-1` | `LATEST` | — | yes |
| `-2` | `EARLIEST` | — | yes |
| `-3` | `MAX_TIMESTAMP` | KIP-734 | yes |
| `-4` | `EARLIEST_LOCAL_TIMESTAMP` | KIP-405 | yes |
| `-5` | `LATEST_TIERED_TIMESTAMP` | KIP-1005 | yes |
| `-6` | `EARLIEST_PENDING_UPLOAD_TIMESTAMP` | KIP-1023 | **no** |

`-6` requires `ListOffsets` **v11**. The crate caps at **v10**; a 4.3.1
broker serves v11. So it is unreachable until upstream bumps — not by
omission, but because there is no v11 request to encode.

The domain enum documents the gap rather than silently omitting the variant,
because "this client does not support tiered-storage upload watermarks" and
"this sentinel does not exist" are different answers.

The other five are surfaced distinctly. Collapsing them is how a UI reports
wrong retention on a tiered cluster: `EARLIEST` and
`EARLIEST_LOCAL_TIMESTAMP` differ by exactly the data that has been offloaded
to remote storage, which on a tiered cluster is most of it.

## 3. Error codes stop at 4.1

`kafka-protocol` knows codes through Kafka 4.1. A 4.3 broker can return codes
it cannot name.

`ResponseError` already models this with its own unknown handling, and our
owned enum carries `ErrorCode::Unknown(i16)` for the same reason. An
unrecognised code round-trips and renders; it never panics and never
collapses into a generic failure that discards the number — the number is the
only thing anyone can search for. See
[The error taxonomy](../architecture/errors.md).

## 4. `OffsetFetch` request and response ranges disagree

Not a broker gap but a codec one, and it caused a real bug.

`ApiKey::valid_versions()` is derived per api key, and where a request and
its response have different schema ranges it reports the **wider** one. For
`OffsetFetch` the response reaches v10 while the request stops at v9, so
negotiating from the api key alone picks v10 and the encoder then refuses.

The fix is `ApiVersions::negotiate_with`, which takes the specific request
and response types' own `VERSIONS` rather than the api key's. See
[Version negotiation](../architecture/version-negotiation.md).

## 5. Raw snappy does not decode

Also a codec bug rather than a broker gap, and the most consequential one on
this page — it is the only entry that makes real data unreadable rather than
merely unreachable.

Kafka's snappy is two formats. The Java client frames it with snappy-java's
xerial header; `librdkafka`, and so most of the non-Java ecosystem, writes
raw unframed snappy. `kafka-protocol` autodetects between them and gets it
wrong: it reads the 16-byte magic header with `try_get_bytes(16)`, which
*advances* the buffer, so the raw fallback runs on a buffer already missing
its first sixteen bytes. Upstream's own fallback test passes only because its
fixture is fifteen bytes, one short of the header.

Note the version history above — 0.17.0's one substantive change was to the
snappy framing. This is the newest code in the dependency, and it is the code
we were least entitled to assume.

Untreated, this means **no snappy topic written by a non-Java producer can be
read**. So this is the single place the workspace takes option 3 below in its
strongest form: `kafka-read` decides the framing itself and delegates only
the xerial case, which is a knowing divergence from the codec crate rather
than a gap it merely documents. See
[Tolerant decoding](../architecture/tolerant-decoding.md).

Revisit when upstream fixes the detection. The workaround is small and
deliberately shaped to be deleted.

## What to do when this blocks you

**Say so.** An honest blocker is a legitimate outcome; a hand-rolled schema
is not.

Adding a message definition by hand to work around a missing one means
maintaining a private fork of a generated artifact, diverging from upstream
silently, and losing the compile-error signal that makes
[the error taxonomy](../architecture/errors.md) trustworthy. The correct
responses, in order of preference:

1. Wait for the upstream bump and note the gap here.
2. Contribute the schema upstream to `kafka-protocol`.
3. Degrade visibly — an `Unrecognized` variant, an `Unknown(i16)`, a
   documented `None` — so the gap is observable rather than invisible.

Every gap on this page took option 3 while waiting on option 1.
