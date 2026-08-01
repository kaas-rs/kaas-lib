//! Our own `ApiKey`.
//!
//! Rule 1 forbids `kafka_protocol` types in public signatures, and `ApiKey` is
//! the one that would otherwise leak everywhere: it appears in the version
//! table, the routing table, the read-only gate and every span. Owning it also
//! buys two things the upstream enum cannot give us:
//!
//! * `Unknown(i16)` — a 4.3 broker advertises API keys that `kafka-protocol`
//!   0.17 (Kafka 4.0 schemas) has no name for, `StreamsGroupDescribe` among
//!   them. Those have to survive a round trip through the version table and
//!   render in a UI, not vanish at the parse boundary.
//! * A stable public surface. New keys arrive with every Kafka release; adding
//!   a variant here is our decision to make rather than a breaking change
//!   handed to us by a dependency bump.
//!
//! Generated from the `ApiKey` enum in `kafka-protocol` 0.17.0 (Kafka 4.0
//! schemas). Regenerate by hand on an upstream bump — the list is short and
//! append-only.

use std::fmt;

/// A Kafka protocol API key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApiKey {
    /// `Produce`, api key 0.
    Produce,
    /// `Fetch`, api key 1.
    Fetch,
    /// `ListOffsets`, api key 2.
    ListOffsets,
    /// `Metadata`, api key 3.
    Metadata,
    /// `OffsetCommit`, api key 8.
    OffsetCommit,
    /// `OffsetFetch`, api key 9.
    OffsetFetch,
    /// `FindCoordinator`, api key 10.
    FindCoordinator,
    /// `JoinGroup`, api key 11.
    JoinGroup,
    /// `Heartbeat`, api key 12.
    Heartbeat,
    /// `LeaveGroup`, api key 13.
    LeaveGroup,
    /// `SyncGroup`, api key 14.
    SyncGroup,
    /// `DescribeGroups`, api key 15.
    DescribeGroups,
    /// `ListGroups`, api key 16.
    ListGroups,
    /// `SaslHandshake`, api key 17.
    SaslHandshake,
    /// `ApiVersions`, api key 18.
    ApiVersions,
    /// `CreateTopics`, api key 19.
    CreateTopics,
    /// `DeleteTopics`, api key 20.
    DeleteTopics,
    /// `DeleteRecords`, api key 21.
    DeleteRecords,
    /// `InitProducerId`, api key 22.
    InitProducerId,
    /// `OffsetForLeaderEpoch`, api key 23.
    OffsetForLeaderEpoch,
    /// `AddPartitionsToTxn`, api key 24.
    AddPartitionsToTxn,
    /// `AddOffsetsToTxn`, api key 25.
    AddOffsetsToTxn,
    /// `EndTxn`, api key 26.
    EndTxn,
    /// `WriteTxnMarkers`, api key 27.
    WriteTxnMarkers,
    /// `TxnOffsetCommit`, api key 28.
    TxnOffsetCommit,
    /// `DescribeAcls`, api key 29.
    DescribeAcls,
    /// `CreateAcls`, api key 30.
    CreateAcls,
    /// `DeleteAcls`, api key 31.
    DeleteAcls,
    /// `DescribeConfigs`, api key 32.
    DescribeConfigs,
    /// `AlterConfigs`, api key 33.
    AlterConfigs,
    /// `AlterReplicaLogDirs`, api key 34.
    AlterReplicaLogDirs,
    /// `DescribeLogDirs`, api key 35.
    DescribeLogDirs,
    /// `SaslAuthenticate`, api key 36.
    SaslAuthenticate,
    /// `CreatePartitions`, api key 37.
    CreatePartitions,
    /// `CreateDelegationToken`, api key 38.
    CreateDelegationToken,
    /// `RenewDelegationToken`, api key 39.
    RenewDelegationToken,
    /// `ExpireDelegationToken`, api key 40.
    ExpireDelegationToken,
    /// `DescribeDelegationToken`, api key 41.
    DescribeDelegationToken,
    /// `DeleteGroups`, api key 42.
    DeleteGroups,
    /// `ElectLeaders`, api key 43.
    ElectLeaders,
    /// `IncrementalAlterConfigs`, api key 44.
    IncrementalAlterConfigs,
    /// `AlterPartitionReassignments`, api key 45.
    AlterPartitionReassignments,
    /// `ListPartitionReassignments`, api key 46.
    ListPartitionReassignments,
    /// `OffsetDelete`, api key 47.
    OffsetDelete,
    /// `DescribeClientQuotas`, api key 48.
    DescribeClientQuotas,
    /// `AlterClientQuotas`, api key 49.
    AlterClientQuotas,
    /// `DescribeUserScramCredentials`, api key 50.
    DescribeUserScramCredentials,
    /// `AlterUserScramCredentials`, api key 51.
    AlterUserScramCredentials,
    /// `Vote`, api key 52.
    Vote,
    /// `BeginQuorumEpoch`, api key 53.
    BeginQuorumEpoch,
    /// `EndQuorumEpoch`, api key 54.
    EndQuorumEpoch,
    /// `DescribeQuorum`, api key 55.
    DescribeQuorum,
    /// `AlterPartition`, api key 56.
    AlterPartition,
    /// `UpdateFeatures`, api key 57.
    UpdateFeatures,
    /// `Envelope`, api key 58.
    Envelope,
    /// `FetchSnapshot`, api key 59.
    FetchSnapshot,
    /// `DescribeCluster`, api key 60.
    DescribeCluster,
    /// `DescribeProducers`, api key 61.
    DescribeProducers,
    /// `BrokerRegistration`, api key 62.
    BrokerRegistration,
    /// `BrokerHeartbeat`, api key 63.
    BrokerHeartbeat,
    /// `UnregisterBroker`, api key 64.
    UnregisterBroker,
    /// `DescribeTransactions`, api key 65.
    DescribeTransactions,
    /// `ListTransactions`, api key 66.
    ListTransactions,
    /// `AllocateProducerIds`, api key 67.
    AllocateProducerIds,
    /// `ConsumerGroupHeartbeat`, api key 68.
    ConsumerGroupHeartbeat,
    /// `ConsumerGroupDescribe`, api key 69.
    ConsumerGroupDescribe,
    /// `ControllerRegistration`, api key 70.
    ControllerRegistration,
    /// `GetTelemetrySubscriptions`, api key 71.
    GetTelemetrySubscriptions,
    /// `PushTelemetry`, api key 72.
    PushTelemetry,
    /// `AssignReplicasToDirs`, api key 73.
    AssignReplicasToDirs,
    /// `ListConfigResources`, api key 74.
    ListConfigResources,
    /// `DescribeTopicPartitions`, api key 75.
    DescribeTopicPartitions,
    /// `ShareGroupHeartbeat`, api key 76.
    ShareGroupHeartbeat,
    /// `ShareGroupDescribe`, api key 77.
    ShareGroupDescribe,
    /// `ShareFetch`, api key 78.
    ShareFetch,
    /// `ShareAcknowledge`, api key 79.
    ShareAcknowledge,
    /// `AddRaftVoter`, api key 80.
    AddRaftVoter,
    /// `RemoveRaftVoter`, api key 81.
    RemoveRaftVoter,
    /// `UpdateRaftVoter`, api key 82.
    UpdateRaftVoter,
    /// `InitializeShareGroupState`, api key 83.
    InitializeShareGroupState,
    /// `ReadShareGroupState`, api key 84.
    ReadShareGroupState,
    /// `WriteShareGroupState`, api key 85.
    WriteShareGroupState,
    /// `DeleteShareGroupState`, api key 86.
    DeleteShareGroupState,
    /// `ReadShareGroupStateSummary`, api key 87.
    ReadShareGroupStateSummary,
    /// `DescribeShareGroupOffsets`, api key 90.
    DescribeShareGroupOffsets,
    /// `AlterShareGroupOffsets`, api key 91.
    AlterShareGroupOffsets,
    /// `DeleteShareGroupOffsets`, api key 92.
    DeleteShareGroupOffsets,
    /// An api key this build has no name for.
    ///
    /// Constructed only by [`ApiKey::from_code`] for codes outside the known
    /// set; building `Unknown` with a code that *is* known produces a value
    /// that compares unequal to its named twin.
    Unknown(i16),
}

impl ApiKey {
    /// The wire code for this key.
    pub const fn code(self) -> i16 {
        match self {
            ApiKey::Produce => 0,
            ApiKey::Fetch => 1,
            ApiKey::ListOffsets => 2,
            ApiKey::Metadata => 3,
            ApiKey::OffsetCommit => 8,
            ApiKey::OffsetFetch => 9,
            ApiKey::FindCoordinator => 10,
            ApiKey::JoinGroup => 11,
            ApiKey::Heartbeat => 12,
            ApiKey::LeaveGroup => 13,
            ApiKey::SyncGroup => 14,
            ApiKey::DescribeGroups => 15,
            ApiKey::ListGroups => 16,
            ApiKey::SaslHandshake => 17,
            ApiKey::ApiVersions => 18,
            ApiKey::CreateTopics => 19,
            ApiKey::DeleteTopics => 20,
            ApiKey::DeleteRecords => 21,
            ApiKey::InitProducerId => 22,
            ApiKey::OffsetForLeaderEpoch => 23,
            ApiKey::AddPartitionsToTxn => 24,
            ApiKey::AddOffsetsToTxn => 25,
            ApiKey::EndTxn => 26,
            ApiKey::WriteTxnMarkers => 27,
            ApiKey::TxnOffsetCommit => 28,
            ApiKey::DescribeAcls => 29,
            ApiKey::CreateAcls => 30,
            ApiKey::DeleteAcls => 31,
            ApiKey::DescribeConfigs => 32,
            ApiKey::AlterConfigs => 33,
            ApiKey::AlterReplicaLogDirs => 34,
            ApiKey::DescribeLogDirs => 35,
            ApiKey::SaslAuthenticate => 36,
            ApiKey::CreatePartitions => 37,
            ApiKey::CreateDelegationToken => 38,
            ApiKey::RenewDelegationToken => 39,
            ApiKey::ExpireDelegationToken => 40,
            ApiKey::DescribeDelegationToken => 41,
            ApiKey::DeleteGroups => 42,
            ApiKey::ElectLeaders => 43,
            ApiKey::IncrementalAlterConfigs => 44,
            ApiKey::AlterPartitionReassignments => 45,
            ApiKey::ListPartitionReassignments => 46,
            ApiKey::OffsetDelete => 47,
            ApiKey::DescribeClientQuotas => 48,
            ApiKey::AlterClientQuotas => 49,
            ApiKey::DescribeUserScramCredentials => 50,
            ApiKey::AlterUserScramCredentials => 51,
            ApiKey::Vote => 52,
            ApiKey::BeginQuorumEpoch => 53,
            ApiKey::EndQuorumEpoch => 54,
            ApiKey::DescribeQuorum => 55,
            ApiKey::AlterPartition => 56,
            ApiKey::UpdateFeatures => 57,
            ApiKey::Envelope => 58,
            ApiKey::FetchSnapshot => 59,
            ApiKey::DescribeCluster => 60,
            ApiKey::DescribeProducers => 61,
            ApiKey::BrokerRegistration => 62,
            ApiKey::BrokerHeartbeat => 63,
            ApiKey::UnregisterBroker => 64,
            ApiKey::DescribeTransactions => 65,
            ApiKey::ListTransactions => 66,
            ApiKey::AllocateProducerIds => 67,
            ApiKey::ConsumerGroupHeartbeat => 68,
            ApiKey::ConsumerGroupDescribe => 69,
            ApiKey::ControllerRegistration => 70,
            ApiKey::GetTelemetrySubscriptions => 71,
            ApiKey::PushTelemetry => 72,
            ApiKey::AssignReplicasToDirs => 73,
            ApiKey::ListConfigResources => 74,
            ApiKey::DescribeTopicPartitions => 75,
            ApiKey::ShareGroupHeartbeat => 76,
            ApiKey::ShareGroupDescribe => 77,
            ApiKey::ShareFetch => 78,
            ApiKey::ShareAcknowledge => 79,
            ApiKey::AddRaftVoter => 80,
            ApiKey::RemoveRaftVoter => 81,
            ApiKey::UpdateRaftVoter => 82,
            ApiKey::InitializeShareGroupState => 83,
            ApiKey::ReadShareGroupState => 84,
            ApiKey::WriteShareGroupState => 85,
            ApiKey::DeleteShareGroupState => 86,
            ApiKey::ReadShareGroupStateSummary => 87,
            ApiKey::DescribeShareGroupOffsets => 90,
            ApiKey::AlterShareGroupOffsets => 91,
            ApiKey::DeleteShareGroupOffsets => 92,
            ApiKey::Unknown(code) => code,
        }
    }

    /// The key for a wire code, or [`ApiKey::Unknown`].
    pub const fn from_code(code: i16) -> Self {
        match code {
            0 => ApiKey::Produce,
            1 => ApiKey::Fetch,
            2 => ApiKey::ListOffsets,
            3 => ApiKey::Metadata,
            8 => ApiKey::OffsetCommit,
            9 => ApiKey::OffsetFetch,
            10 => ApiKey::FindCoordinator,
            11 => ApiKey::JoinGroup,
            12 => ApiKey::Heartbeat,
            13 => ApiKey::LeaveGroup,
            14 => ApiKey::SyncGroup,
            15 => ApiKey::DescribeGroups,
            16 => ApiKey::ListGroups,
            17 => ApiKey::SaslHandshake,
            18 => ApiKey::ApiVersions,
            19 => ApiKey::CreateTopics,
            20 => ApiKey::DeleteTopics,
            21 => ApiKey::DeleteRecords,
            22 => ApiKey::InitProducerId,
            23 => ApiKey::OffsetForLeaderEpoch,
            24 => ApiKey::AddPartitionsToTxn,
            25 => ApiKey::AddOffsetsToTxn,
            26 => ApiKey::EndTxn,
            27 => ApiKey::WriteTxnMarkers,
            28 => ApiKey::TxnOffsetCommit,
            29 => ApiKey::DescribeAcls,
            30 => ApiKey::CreateAcls,
            31 => ApiKey::DeleteAcls,
            32 => ApiKey::DescribeConfigs,
            33 => ApiKey::AlterConfigs,
            34 => ApiKey::AlterReplicaLogDirs,
            35 => ApiKey::DescribeLogDirs,
            36 => ApiKey::SaslAuthenticate,
            37 => ApiKey::CreatePartitions,
            38 => ApiKey::CreateDelegationToken,
            39 => ApiKey::RenewDelegationToken,
            40 => ApiKey::ExpireDelegationToken,
            41 => ApiKey::DescribeDelegationToken,
            42 => ApiKey::DeleteGroups,
            43 => ApiKey::ElectLeaders,
            44 => ApiKey::IncrementalAlterConfigs,
            45 => ApiKey::AlterPartitionReassignments,
            46 => ApiKey::ListPartitionReassignments,
            47 => ApiKey::OffsetDelete,
            48 => ApiKey::DescribeClientQuotas,
            49 => ApiKey::AlterClientQuotas,
            50 => ApiKey::DescribeUserScramCredentials,
            51 => ApiKey::AlterUserScramCredentials,
            52 => ApiKey::Vote,
            53 => ApiKey::BeginQuorumEpoch,
            54 => ApiKey::EndQuorumEpoch,
            55 => ApiKey::DescribeQuorum,
            56 => ApiKey::AlterPartition,
            57 => ApiKey::UpdateFeatures,
            58 => ApiKey::Envelope,
            59 => ApiKey::FetchSnapshot,
            60 => ApiKey::DescribeCluster,
            61 => ApiKey::DescribeProducers,
            62 => ApiKey::BrokerRegistration,
            63 => ApiKey::BrokerHeartbeat,
            64 => ApiKey::UnregisterBroker,
            65 => ApiKey::DescribeTransactions,
            66 => ApiKey::ListTransactions,
            67 => ApiKey::AllocateProducerIds,
            68 => ApiKey::ConsumerGroupHeartbeat,
            69 => ApiKey::ConsumerGroupDescribe,
            70 => ApiKey::ControllerRegistration,
            71 => ApiKey::GetTelemetrySubscriptions,
            72 => ApiKey::PushTelemetry,
            73 => ApiKey::AssignReplicasToDirs,
            74 => ApiKey::ListConfigResources,
            75 => ApiKey::DescribeTopicPartitions,
            76 => ApiKey::ShareGroupHeartbeat,
            77 => ApiKey::ShareGroupDescribe,
            78 => ApiKey::ShareFetch,
            79 => ApiKey::ShareAcknowledge,
            80 => ApiKey::AddRaftVoter,
            81 => ApiKey::RemoveRaftVoter,
            82 => ApiKey::UpdateRaftVoter,
            83 => ApiKey::InitializeShareGroupState,
            84 => ApiKey::ReadShareGroupState,
            85 => ApiKey::WriteShareGroupState,
            86 => ApiKey::DeleteShareGroupState,
            87 => ApiKey::ReadShareGroupStateSummary,
            90 => ApiKey::DescribeShareGroupOffsets,
            91 => ApiKey::AlterShareGroupOffsets,
            92 => ApiKey::DeleteShareGroupOffsets,
            other => ApiKey::Unknown(other),
        }
    }

    /// The key's name, for logs and UI.
    pub const fn name(self) -> &'static str {
        match self {
            ApiKey::Produce => "Produce",
            ApiKey::Fetch => "Fetch",
            ApiKey::ListOffsets => "ListOffsets",
            ApiKey::Metadata => "Metadata",
            ApiKey::OffsetCommit => "OffsetCommit",
            ApiKey::OffsetFetch => "OffsetFetch",
            ApiKey::FindCoordinator => "FindCoordinator",
            ApiKey::JoinGroup => "JoinGroup",
            ApiKey::Heartbeat => "Heartbeat",
            ApiKey::LeaveGroup => "LeaveGroup",
            ApiKey::SyncGroup => "SyncGroup",
            ApiKey::DescribeGroups => "DescribeGroups",
            ApiKey::ListGroups => "ListGroups",
            ApiKey::SaslHandshake => "SaslHandshake",
            ApiKey::ApiVersions => "ApiVersions",
            ApiKey::CreateTopics => "CreateTopics",
            ApiKey::DeleteTopics => "DeleteTopics",
            ApiKey::DeleteRecords => "DeleteRecords",
            ApiKey::InitProducerId => "InitProducerId",
            ApiKey::OffsetForLeaderEpoch => "OffsetForLeaderEpoch",
            ApiKey::AddPartitionsToTxn => "AddPartitionsToTxn",
            ApiKey::AddOffsetsToTxn => "AddOffsetsToTxn",
            ApiKey::EndTxn => "EndTxn",
            ApiKey::WriteTxnMarkers => "WriteTxnMarkers",
            ApiKey::TxnOffsetCommit => "TxnOffsetCommit",
            ApiKey::DescribeAcls => "DescribeAcls",
            ApiKey::CreateAcls => "CreateAcls",
            ApiKey::DeleteAcls => "DeleteAcls",
            ApiKey::DescribeConfigs => "DescribeConfigs",
            ApiKey::AlterConfigs => "AlterConfigs",
            ApiKey::AlterReplicaLogDirs => "AlterReplicaLogDirs",
            ApiKey::DescribeLogDirs => "DescribeLogDirs",
            ApiKey::SaslAuthenticate => "SaslAuthenticate",
            ApiKey::CreatePartitions => "CreatePartitions",
            ApiKey::CreateDelegationToken => "CreateDelegationToken",
            ApiKey::RenewDelegationToken => "RenewDelegationToken",
            ApiKey::ExpireDelegationToken => "ExpireDelegationToken",
            ApiKey::DescribeDelegationToken => "DescribeDelegationToken",
            ApiKey::DeleteGroups => "DeleteGroups",
            ApiKey::ElectLeaders => "ElectLeaders",
            ApiKey::IncrementalAlterConfigs => "IncrementalAlterConfigs",
            ApiKey::AlterPartitionReassignments => "AlterPartitionReassignments",
            ApiKey::ListPartitionReassignments => "ListPartitionReassignments",
            ApiKey::OffsetDelete => "OffsetDelete",
            ApiKey::DescribeClientQuotas => "DescribeClientQuotas",
            ApiKey::AlterClientQuotas => "AlterClientQuotas",
            ApiKey::DescribeUserScramCredentials => "DescribeUserScramCredentials",
            ApiKey::AlterUserScramCredentials => "AlterUserScramCredentials",
            ApiKey::Vote => "Vote",
            ApiKey::BeginQuorumEpoch => "BeginQuorumEpoch",
            ApiKey::EndQuorumEpoch => "EndQuorumEpoch",
            ApiKey::DescribeQuorum => "DescribeQuorum",
            ApiKey::AlterPartition => "AlterPartition",
            ApiKey::UpdateFeatures => "UpdateFeatures",
            ApiKey::Envelope => "Envelope",
            ApiKey::FetchSnapshot => "FetchSnapshot",
            ApiKey::DescribeCluster => "DescribeCluster",
            ApiKey::DescribeProducers => "DescribeProducers",
            ApiKey::BrokerRegistration => "BrokerRegistration",
            ApiKey::BrokerHeartbeat => "BrokerHeartbeat",
            ApiKey::UnregisterBroker => "UnregisterBroker",
            ApiKey::DescribeTransactions => "DescribeTransactions",
            ApiKey::ListTransactions => "ListTransactions",
            ApiKey::AllocateProducerIds => "AllocateProducerIds",
            ApiKey::ConsumerGroupHeartbeat => "ConsumerGroupHeartbeat",
            ApiKey::ConsumerGroupDescribe => "ConsumerGroupDescribe",
            ApiKey::ControllerRegistration => "ControllerRegistration",
            ApiKey::GetTelemetrySubscriptions => "GetTelemetrySubscriptions",
            ApiKey::PushTelemetry => "PushTelemetry",
            ApiKey::AssignReplicasToDirs => "AssignReplicasToDirs",
            ApiKey::ListConfigResources => "ListConfigResources",
            ApiKey::DescribeTopicPartitions => "DescribeTopicPartitions",
            ApiKey::ShareGroupHeartbeat => "ShareGroupHeartbeat",
            ApiKey::ShareGroupDescribe => "ShareGroupDescribe",
            ApiKey::ShareFetch => "ShareFetch",
            ApiKey::ShareAcknowledge => "ShareAcknowledge",
            ApiKey::AddRaftVoter => "AddRaftVoter",
            ApiKey::RemoveRaftVoter => "RemoveRaftVoter",
            ApiKey::UpdateRaftVoter => "UpdateRaftVoter",
            ApiKey::InitializeShareGroupState => "InitializeShareGroupState",
            ApiKey::ReadShareGroupState => "ReadShareGroupState",
            ApiKey::WriteShareGroupState => "WriteShareGroupState",
            ApiKey::DeleteShareGroupState => "DeleteShareGroupState",
            ApiKey::ReadShareGroupStateSummary => "ReadShareGroupStateSummary",
            ApiKey::DescribeShareGroupOffsets => "DescribeShareGroupOffsets",
            ApiKey::AlterShareGroupOffsets => "AlterShareGroupOffsets",
            ApiKey::DeleteShareGroupOffsets => "DeleteShareGroupOffsets",
            ApiKey::Unknown(_) => "Unknown",
        }
    }

    /// Every key this build knows, in wire-code order.
    pub fn known() -> impl Iterator<Item = ApiKey> {
        KNOWN.iter().copied()
    }

    /// Whether sending this key can change cluster state.
    ///
    /// This is the whole of the read-only gate (M8). It is expressed as an
    /// allowlist of *read-only* keys with a `_ => true` fallback, and the
    /// direction matters: deny-by-default means an API added by a future Kafka
    /// release — or an [`ApiKey::Unknown`] we have never heard of — is refused
    /// until someone deliberately classifies it. A `_ => false` arm would
    /// silently un-gate every one of them.
    ///
    /// Two entries that look surprising:
    ///
    /// * `FindCoordinator` is read-only here even though on some clusters it
    ///   can trigger creation of the internal `__consumer_offsets` topic. A
    ///   read-only client cannot fetch a committed offset without it, and the
    ///   alternative — parsing `__consumer_offsets` ourselves — is forbidden.
    /// * `SaslHandshake` and `SaslAuthenticate` mutate connection state rather
    ///   than cluster state, and gating them would make a read-only client
    ///   unable to authenticate at all.
    pub const fn is_mutating(self) -> bool {
        match self {
            ApiKey::Fetch
            | ApiKey::ListOffsets
            | ApiKey::Metadata
            | ApiKey::OffsetFetch
            | ApiKey::FindCoordinator
            | ApiKey::DescribeGroups
            | ApiKey::ListGroups
            | ApiKey::SaslHandshake
            | ApiKey::ApiVersions
            | ApiKey::OffsetForLeaderEpoch
            | ApiKey::DescribeAcls
            | ApiKey::DescribeConfigs
            | ApiKey::DescribeLogDirs
            | ApiKey::SaslAuthenticate
            | ApiKey::DescribeDelegationToken
            | ApiKey::ListPartitionReassignments
            | ApiKey::DescribeClientQuotas
            | ApiKey::DescribeUserScramCredentials
            | ApiKey::DescribeQuorum
            | ApiKey::DescribeCluster
            | ApiKey::DescribeProducers
            | ApiKey::DescribeTransactions
            | ApiKey::ListTransactions
            | ApiKey::ConsumerGroupDescribe
            | ApiKey::ListConfigResources
            | ApiKey::DescribeTopicPartitions
            | ApiKey::ShareGroupDescribe
            | ApiKey::DescribeShareGroupOffsets => false,
            // Deny by default. Do not replace this with `_ => false`.
            _ => true,
        }
    }
}

impl fmt::Display for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiKey::Unknown(code) => write!(f, "Unknown({code})"),
            named => f.write_str(named.name()),
        }
    }
}

/// Every named key, in wire-code order.
const KNOWN: [ApiKey; 87] = [
    ApiKey::Produce,
    ApiKey::Fetch,
    ApiKey::ListOffsets,
    ApiKey::Metadata,
    ApiKey::OffsetCommit,
    ApiKey::OffsetFetch,
    ApiKey::FindCoordinator,
    ApiKey::JoinGroup,
    ApiKey::Heartbeat,
    ApiKey::LeaveGroup,
    ApiKey::SyncGroup,
    ApiKey::DescribeGroups,
    ApiKey::ListGroups,
    ApiKey::SaslHandshake,
    ApiKey::ApiVersions,
    ApiKey::CreateTopics,
    ApiKey::DeleteTopics,
    ApiKey::DeleteRecords,
    ApiKey::InitProducerId,
    ApiKey::OffsetForLeaderEpoch,
    ApiKey::AddPartitionsToTxn,
    ApiKey::AddOffsetsToTxn,
    ApiKey::EndTxn,
    ApiKey::WriteTxnMarkers,
    ApiKey::TxnOffsetCommit,
    ApiKey::DescribeAcls,
    ApiKey::CreateAcls,
    ApiKey::DeleteAcls,
    ApiKey::DescribeConfigs,
    ApiKey::AlterConfigs,
    ApiKey::AlterReplicaLogDirs,
    ApiKey::DescribeLogDirs,
    ApiKey::SaslAuthenticate,
    ApiKey::CreatePartitions,
    ApiKey::CreateDelegationToken,
    ApiKey::RenewDelegationToken,
    ApiKey::ExpireDelegationToken,
    ApiKey::DescribeDelegationToken,
    ApiKey::DeleteGroups,
    ApiKey::ElectLeaders,
    ApiKey::IncrementalAlterConfigs,
    ApiKey::AlterPartitionReassignments,
    ApiKey::ListPartitionReassignments,
    ApiKey::OffsetDelete,
    ApiKey::DescribeClientQuotas,
    ApiKey::AlterClientQuotas,
    ApiKey::DescribeUserScramCredentials,
    ApiKey::AlterUserScramCredentials,
    ApiKey::Vote,
    ApiKey::BeginQuorumEpoch,
    ApiKey::EndQuorumEpoch,
    ApiKey::DescribeQuorum,
    ApiKey::AlterPartition,
    ApiKey::UpdateFeatures,
    ApiKey::Envelope,
    ApiKey::FetchSnapshot,
    ApiKey::DescribeCluster,
    ApiKey::DescribeProducers,
    ApiKey::BrokerRegistration,
    ApiKey::BrokerHeartbeat,
    ApiKey::UnregisterBroker,
    ApiKey::DescribeTransactions,
    ApiKey::ListTransactions,
    ApiKey::AllocateProducerIds,
    ApiKey::ConsumerGroupHeartbeat,
    ApiKey::ConsumerGroupDescribe,
    ApiKey::ControllerRegistration,
    ApiKey::GetTelemetrySubscriptions,
    ApiKey::PushTelemetry,
    ApiKey::AssignReplicasToDirs,
    ApiKey::ListConfigResources,
    ApiKey::DescribeTopicPartitions,
    ApiKey::ShareGroupHeartbeat,
    ApiKey::ShareGroupDescribe,
    ApiKey::ShareFetch,
    ApiKey::ShareAcknowledge,
    ApiKey::AddRaftVoter,
    ApiKey::RemoveRaftVoter,
    ApiKey::UpdateRaftVoter,
    ApiKey::InitializeShareGroupState,
    ApiKey::ReadShareGroupState,
    ApiKey::WriteShareGroupState,
    ApiKey::DeleteShareGroupState,
    ApiKey::ReadShareGroupStateSummary,
    ApiKey::DescribeShareGroupOffsets,
    ApiKey::AlterShareGroupOffsets,
    ApiKey::DeleteShareGroupOffsets,
];
