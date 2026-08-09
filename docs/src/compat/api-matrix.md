# API support matrix

**52 of the 87 api keys this build names**, listed in wire-code order.
Every row is negotiated per connection — the version column is deliberately
absent, because there is no fixed version to report. See
[Version negotiation](../architecture/version-negotiation.md).

| API | Code | Routing | Mutating | Sent by |
|---|---|---|---|---|
| `Produce` | 0 | partition leader | **yes** | produce |
| `Fetch` | 1 | partition leader | no | read, consume |
| `ListOffsets` | 2 | partition leader | no | admin, read |
| `Metadata` | 3 | any | no | conn, meta |
| `OffsetCommit` | 8 | group coordinator | **yes** | admin, consume |
| `OffsetFetch` | 9 | group coordinator | no | admin, consume |
| `FindCoordinator` | 10 | any | no | meta |
| `JoinGroup` | 11 | group coordinator | **yes** | consume |
| `Heartbeat` | 12 | group coordinator | **yes** | consume |
| `LeaveGroup` | 13 | group coordinator | **yes** | consume |
| `SyncGroup` | 14 | group coordinator | **yes** | consume |
| `DescribeGroups` | 15 | group coordinator | no | admin |
| `ListGroups` | 16 | any | no | admin |
| `SaslHandshake` | 17 | any | no | conn |
| `ApiVersions` | 18 | any | no | conn |
| `CreateTopics` | 19 | controller | **yes** | admin |
| `DeleteTopics` | 20 | controller | **yes** | admin |
| `DeleteRecords` | 21 | any | **yes** | admin |
| `InitProducerId` | 22 | txn coordinator | **yes** | produce |
| `AddPartitionsToTxn` | 24 | txn coordinator | **yes** | produce |
| `AddOffsetsToTxn` | 25 | txn coordinator | **yes** | produce |
| `EndTxn` | 26 | txn coordinator | **yes** | produce |
| `TxnOffsetCommit` | 28 | group coordinator | **yes** | produce |
| `DescribeAcls` | 29 | any | no | admin |
| `CreateAcls` | 30 | any | **yes** | admin |
| `DeleteAcls` | 31 | any | **yes** | admin |
| `DescribeConfigs` | 32 | any | no | admin |
| `DescribeLogDirs` | 35 | specific broker | no | admin |
| `SaslAuthenticate` | 36 | any | no | conn |
| `CreatePartitions` | 37 | controller | **yes** | admin |
| `CreateDelegationToken` | 38 | any | **yes** | admin |
| `RenewDelegationToken` | 39 | any | **yes** | admin |
| `ExpireDelegationToken` | 40 | any | **yes** | admin |
| `DescribeDelegationToken` | 41 | any | no | admin |
| `DeleteGroups` | 42 | group coordinator | **yes** | admin |
| `ElectLeaders` | 43 | controller | **yes** | admin |
| `IncrementalAlterConfigs` | 44 | any | **yes** | admin |
| `AlterPartitionReassignments` | 45 | controller | **yes** | admin |
| `ListPartitionReassignments` | 46 | controller | no | admin |
| `OffsetDelete` | 47 | group coordinator | **yes** | admin |
| `DescribeClientQuotas` | 48 | any | no | admin |
| `AlterClientQuotas` | 49 | any | **yes** | admin |
| `DescribeUserScramCredentials` | 50 | any | no | admin |
| `AlterUserScramCredentials` | 51 | any | **yes** | admin |
| `DescribeCluster` | 60 | any | no | admin |
| `DescribeProducers` | 61 | specific broker | no | admin |
| `DescribeTransactions` | 65 | txn coordinator | no | admin |
| `ListTransactions` | 66 | any | no | admin |
| `ConsumerGroupHeartbeat` | 68 | group coordinator | **yes** | consume |
| `ConsumerGroupDescribe` | 69 | group coordinator | no | admin |
| `DescribeTopicPartitions` | 75 | any | no | admin |
| `ShareGroupDescribe` | 77 | group coordinator | no | admin |

## Reading the table

**Routing** is the class from `crates/kafka-meta/src/routing.rs`. Sending to
the wrong class does not produce an error — it produces a `NOT_CONTROLLER` or
`NOT_COORDINATOR` retry loop that presents as a flaky cluster. See
[Metadata, routing and the pool](../architecture/metadata-routing.md).

**Mutating** is `ApiKey::is_mutating`, which is what a read-only client
refuses before opening a socket. Note the entries that look surprising in
both directions: `OffsetCommit` and `OffsetDelete` are mutating because they
write group state, while `SaslHandshake`, `SaslAuthenticate` and
`FindCoordinator` are *not*, for reasons the
[read-only gate](../architecture/read-only-gate.md) chapter explains.

`DeleteRecords`, `CreateAcls`, `IncrementalAlterConfigs` and the quota and
SCRAM alters route to **any** broker rather than to the controller, which is
correct — these are not controller-only APIs even though they mutate.

## The other 35

Not sent, and they fall into four groups.

**Broker-internal and KRaft APIs**, which a client has no business sending:
`WriteTxnMarkers` (27), `Vote` (52), `BeginQuorumEpoch` (53),
`EndQuorumEpoch` (54), `AlterPartition` (56), `Envelope` (58),
`FetchSnapshot` (59), `BrokerRegistration` (62), `BrokerHeartbeat` (63),
`UnregisterBroker` (64), `AllocateProducerIds` (67),
`ControllerRegistration` (70), `AssignReplicasToDirs` (73), the Raft voter
APIs (80–82), and the share-group *state* APIs (83–87) that live between
broker and coordinator. `DescribeQuorum` (55) is the one in this group a UI
might plausibly want.

**Share consumption, which is a scope decision.** Share groups are
*observed*, not joined: `ShareGroupDescribe` (77) is in the table, while
`ShareGroupHeartbeat` (76), `ShareFetch` (78), `ShareAcknowledge` (79) and
the share-group offset admin APIs (90–92) are not. KIP-932 consumption is a
second consumption model rather than a variation on the first, and
`kafka-consume` implements the classic and KIP-848 protocols.

**Superseded.** `AlterConfigs` (33) is deliberately absent —
`IncrementalAlterConfigs` (44) replaces it, and using the older API silently
resets every config you did not mention. There is no reason to offer it.

**Not yet needed.** `OffsetForLeaderEpoch` (23), `AlterReplicaLogDirs` (34),
`UpdateFeatures` (57), the telemetry APIs (71–72) and `ListConfigResources`
(74). These are ordinary gaps — nothing structural stops them being added.

**And two that cannot be named at all.** Api keys 88 and 89 are KIP-1071's
`StreamsGroupHeartbeat` and `StreamsGroupDescribe`, which have no schema in
`kafka-protocol` 0.17. They are not in the 87 this build names, they arrive
in a version table as `Unknown(88)` and `Unknown(89)`, and a Kafka Streams
application's group is reported as
[`GroupDescription::Unrecognized`](group-kinds.md) rather than failing the
call. See [The upstream schema gap](upstream-gap.md).

## Keeping this page honest

The table is generated from two sources of truth and must be regenerated when
either changes:

- `crates/kafka-conn/src/api_key.rs` — the wire codes and `is_mutating`
- `crates/kafka-meta/src/routing.rs` — the routing class

The `docs` CI job asserts that every `crates/…` path cited anywhere in this
book exists, so a refactor that moves a file fails the build rather than
leaving a citation that is confidently wrong. It does **not** yet verify the
rows of this table against the source.

**It has now gone stale once, and worth recording how.** Every code on this
page from 8 upwards was low by exactly four, in the table as well as in the
prose — `OffsetCommit` was listed as 4 when it is 8, delegation tokens as
34–37 when they are 38–41. Four is the number of api keys Kafka 4.0 *removed*
(`LeaderAndIsr`, `StopReplica`, `UpdateMetadata`, `ControlledShutdown`, codes
4–7), so the page had been built by enumerating what remained and numbering it
sequentially rather than by reading `ApiKey::code`. It looked entirely
plausible, which is why it survived: every number was wrong and every number
was consistent with its neighbours. That is the argument for generating the
rows in `xtask` and asserting them, rather than for being more careful.