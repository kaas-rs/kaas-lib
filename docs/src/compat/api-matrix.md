# API support matrix

**37 of the protocol's 87 api keys**, listed in wire-code order. Every row
is negotiated per connection — the version column is deliberately absent,
because there is no fixed version to report. See
[Version negotiation](../architecture/version-negotiation.md).

| API | Code | Routing | Mutating | Sent by |
|---|---|---|---|---|
| `Fetch` | 1 | partition leader | no | read |
| `ListOffsets` | 2 | partition leader | no | admin, read |
| `Metadata` | 3 | any | no | conn, meta |
| `OffsetCommit` | 4 | group coordinator | **yes** | admin |
| `OffsetFetch` | 5 | group coordinator | no | admin |
| `FindCoordinator` | 6 | any | no | meta |
| `DescribeGroups` | 11 | group coordinator | no | admin |
| `ListGroups` | 12 | any | no | admin |
| `SaslHandshake` | 13 | any | no | conn |
| `ApiVersions` | 14 | any | no | conn |
| `CreateTopics` | 15 | controller | **yes** | admin |
| `DeleteTopics` | 16 | controller | **yes** | admin |
| `DeleteRecords` | 17 | any | **yes** | admin |
| `DescribeAcls` | 25 | any | no | admin |
| `CreateAcls` | 26 | any | **yes** | admin |
| `DeleteAcls` | 27 | any | **yes** | admin |
| `DescribeConfigs` | 28 | any | no | admin |
| `DescribeLogDirs` | 31 | specific broker | no | admin |
| `SaslAuthenticate` | 32 | any | no | conn |
| `CreatePartitions` | 33 | controller | **yes** | admin |
| `DeleteGroups` | 38 | group coordinator | **yes** | admin |
| `ElectLeaders` | 39 | controller | **yes** | admin |
| `IncrementalAlterConfigs` | 40 | any | **yes** | admin |
| `AlterPartitionReassignments` | 41 | controller | **yes** | admin |
| `ListPartitionReassignments` | 42 | controller | no | admin |
| `OffsetDelete` | 43 | group coordinator | **yes** | admin |
| `DescribeClientQuotas` | 44 | any | no | admin |
| `AlterClientQuotas` | 45 | any | **yes** | admin |
| `DescribeUserScramCredentials` | 46 | any | no | admin |
| `AlterUserScramCredentials` | 47 | any | **yes** | admin |
| `DescribeCluster` | 56 | any | no | admin |
| `DescribeProducers` | 57 | specific broker | no | admin |
| `DescribeTransactions` | 61 | txn coordinator | no | admin |
| `ListTransactions` | 62 | any | no | admin |
| `ConsumerGroupDescribe` | 65 | group coordinator | no | admin |
| `DescribeTopicPartitions` | 71 | any | no | admin |
| `ShareGroupDescribe` | 73 | group coordinator | no | admin |

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

## The other 50

Not implemented, and they fall into four groups.

**Producer and consumer-group membership — the real gap.** `Produce` (0),
`InitProducerId` (18), the transaction write path
(`AddPartitionsToTxn` 20, `AddOffsetsToTxn` 21, `EndTxn` 22,
`TxnOffsetCommit` 24), classic membership (`JoinGroup` 7, `Heartbeat` 8,
`LeaveGroup` 9, `SyncGroup` 10), KIP-848 membership
(`ConsumerGroupHeartbeat` 64), and share consumption (`ShareGroupHeartbeat`
72, `ShareFetch` 74, `ShareAcknowledge` 75).

This is the scope boundary, not an oversight — the library observes groups
and transactions without joining or starting one. See
[Non-goals](non-goals.md) and [Roadmap](../guide/roadmap.md).

**Broker-internal and KRaft APIs**, which a client has no business sending:
`Vote` (48), `BeginQuorumEpoch` (49), `EndQuorumEpoch` (50), `AlterPartition`
(52), `Envelope` (54), `FetchSnapshot` (55), `BrokerRegistration` (58),
`BrokerHeartbeat` (59), `UnregisterBroker` (60), `AllocateProducerIds` (63),
`ControllerRegistration` (66), `AssignReplicasToDirs` (69), the Raft voter
APIs (76–78), and the share-group *state* APIs (79–83) that live between
broker and coordinator. `DescribeQuorum` (51) is the one in this group a UI
might plausibly want.

**Superseded.** `AlterConfigs` (29) is deliberately absent —
`IncrementalAlterConfigs` (40) replaces it, and using the older API silently
resets every config you did not mention. There is no reason to offer it.

**Not yet needed.** Delegation tokens (34–37), `OffsetForLeaderEpoch` (19),
`AlterReplicaLogDirs` (30), `UpdateFeatures` (53), the telemetry APIs
(67–68), `ListConfigResources` (70), and the share-group offset admin APIs
(84–86). These are ordinary gaps — nothing structural stops them being
added.

## Keeping this page honest

The table is generated from two sources of truth and must be regenerated when
either changes:

- `crates/kafka-conn/src/api_key.rs` — the wire codes and `is_mutating`
- `crates/kafka-meta/src/routing.rs` — the routing class

The `docs` CI job asserts that every `crates/…` path cited anywhere in this
book exists, so a refactor that moves a file fails the build rather than
leaving a citation that is confidently wrong. It does **not** yet verify the
rows of this table against the source; that check is worth adding the first
time this page goes stale.