# The four group kinds

> Kafka 4.x groups come in four kinds, described by **different RPCs** with
> **different response shapes**. Do not flatten them into one struct.

This is the single most likely thing to get wrong in a Kafka 4.x client,
because every earlier version of Kafka had exactly one kind and every
tutorial still assumes it.

| Kind | KIP | Described by | Supported here |
|---|---|---|---|
| classic | — | `DescribeGroups` | yes |
| consumer | KIP-848 | `ConsumerGroupDescribe` | yes |
| share | KIP-932 | `ShareGroupDescribe` | yes |
| streams | KIP-1071 | `StreamsGroupDescribe` | **no schema in the codec** |

`ListGroups` reports the type per group, so the flow is: list, then dispatch
to the right describe RPC per group type.

## The distinction is preserved, not flattened

`GroupDescription` is an enum whose variants carry the fields their protocol
actually has — not a struct with everything optional:

```rust,no_run
# struct ClassicGroupMember; struct ConsumerGroupMember; struct ShareGroupMember;
# #[derive(Debug)] enum GroupState { Empty }
enum GroupDescription {
    Classic {
        group_id: String,
        state: GroupState,
        protocol_type: String,      // "consumer", "connect", …
        protocol: String,           // the assignor the group agreed on
        members: Vec<ClassicGroupMember>,
    },
    Consumer {
        group_id: String,
        state: GroupState,
        group_epoch: i32,           // KIP-848 has epochs; classic has generations
        assignment_epoch: i32,
        assignor: String,           // server-side, not client-negotiated
        members: Vec<ConsumerGroupMember>,
    },
    Share { /* … */ },
    Unrecognized { group_id: String, group_type: String },
}
```

Flattening these into one struct forces every field to be `Option`, and then
every consumer has to know which combinations are possible for which kind —
which is the same knowledge, moved somewhere it cannot be checked.

The differences are real, not cosmetic. A classic group negotiates its
assignor between members and has a *generation*; a KIP-848 consumer group has
the broker choose the assignor and has a *group epoch* and a separate
*assignment epoch*. There is no honest single field for "the version number
of this group's membership".

## `Unrecognized` is what makes this work on real clusters

Streams groups exist on the wire, list on any 4.1+ broker running Kafka
Streams, and have **no schema in `kafka-protocol` 0.17**. See
[The upstream schema gap](upstream-gap.md).

So the enum needs a variant meaning *"this group exists, I know its id and
its type, and I cannot tell you more"*:

```rust,no_run
# enum GroupDescription {
Unrecognized { group_id: String, group_type: String },
# }
```

A UI that hard-fails on an undescribable group is a UI that hard-fails on
most real clusters. Rendering the group id with "streams group — not
describable by this client" is both honest and useful; returning an error for
the whole group list because one entry was a streams group is neither.

The acceptance test asserts a fourth, undescribable group type surfaces as
`Unrecognized` rather than `Err`.

## `GroupState::Other(String)`

The same reasoning one level down. The named states are `Empty`,
`PreparingRebalance`, `CompletingRebalance`, `Stable` and `Dead`, and
`Other(String)` carries anything else the broker says.

Group states are transmitted as strings, and Kafka adds them — KIP-848
introduced `Assigning` and `Reconciling` for consumer groups. Collapsing an
unknown state into `Dead` or into an error would be worse than reporting the
string the broker actually sent.

## Offset reset differs by protocol

A trap worth naming because it fails on exactly one of the two group types,
which is easy to miss if your fixture only covers one:

| Group kind | Non-member offset reset uses |
|---|---|
| classic | `generation_id = -1` |
| KIP-848 consumer | `member_epoch = -1` |

Get it wrong and the broker returns `ILLEGAL_GENERATION` — against one kind
only, while the other keeps working.

`kafka-admin` also refuses an offset reset when the group is not `EMPTY`,
with a clear error, rather than letting the broker accept a commit that a
live member immediately overwrites. That is a silent no-op from the
operator's point of view, and a silent no-op in an admin tool is worse than a
refusal.

## Fixtures come from the container

Worth recording, because it is a trap in the *test* rather than the code.

`librdkafka` has no KIP-932 share-group support, so `rdkafka` cannot generate
the share-group fixture the acceptance test requires — and it drags cmake and
a C toolchain into CI besides.

The `apache/kafka` image already ships `kafka-console-consumer.sh` and
`kafka-console-share-consumer.sh`. Driving those through
[`testkit`](../code-tour/testkit.md)'s `exec` helper reaches every group kind
with zero build dependencies. `rdkafka` earns its place in the
[interop suite](verification.md), where being a genuinely different client
implementation is the entire point.
