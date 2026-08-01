//! The client half of a Kafka RPC.
//!
//! `kafka_protocol::protocol::Request` looks like the natural bound for
//! [`crate::Connection::send`], and PLAN.md's M2 sketch names it. It is not
//! usable here: upstream gates that impl on `feature = "broker"` as well as
//! `feature = "client"`, because `Request: Decodable` — a *broker* decodes
//! requests. CLAUDE.md builds with the broker half of the codegen off, which
//! drops 87 response encoders and request decoders we would never call, so in
//! this build the trait has no implementors at all.
//!
//! [`Rpc`] is the client-shaped equivalent: an encodable request paired with a
//! decodable response. It is also a better fit for rule 1 than the upstream
//! trait would have been, since the bound callers name is ours and carries our
//! own [`ApiKey`].
//!
//! Generated from the `impl Request for …` blocks in `kafka-protocol` 0.17.0.

use kafka_protocol::messages;
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Message};

use crate::api_key::ApiKey;

/// A request type paired with the response the broker will send back.
pub trait Rpc: Encodable + Message + HeaderVersion + Sized {
    /// The api key this request is sent under.
    const API_KEY: ApiKey;
    /// The response type.
    type Response: Decodable + Message + HeaderVersion;
}

impl Rpc for messages::ProduceRequest {
    const API_KEY: ApiKey = ApiKey::Produce;
    type Response = messages::ProduceResponse;
}

impl Rpc for messages::FetchRequest {
    const API_KEY: ApiKey = ApiKey::Fetch;
    type Response = messages::FetchResponse;
}

impl Rpc for messages::ListOffsetsRequest {
    const API_KEY: ApiKey = ApiKey::ListOffsets;
    type Response = messages::ListOffsetsResponse;
}

impl Rpc for messages::MetadataRequest {
    const API_KEY: ApiKey = ApiKey::Metadata;
    type Response = messages::MetadataResponse;
}

impl Rpc for messages::OffsetCommitRequest {
    const API_KEY: ApiKey = ApiKey::OffsetCommit;
    type Response = messages::OffsetCommitResponse;
}

impl Rpc for messages::OffsetFetchRequest {
    const API_KEY: ApiKey = ApiKey::OffsetFetch;
    type Response = messages::OffsetFetchResponse;
}

impl Rpc for messages::FindCoordinatorRequest {
    const API_KEY: ApiKey = ApiKey::FindCoordinator;
    type Response = messages::FindCoordinatorResponse;
}

impl Rpc for messages::JoinGroupRequest {
    const API_KEY: ApiKey = ApiKey::JoinGroup;
    type Response = messages::JoinGroupResponse;
}

impl Rpc for messages::HeartbeatRequest {
    const API_KEY: ApiKey = ApiKey::Heartbeat;
    type Response = messages::HeartbeatResponse;
}

impl Rpc for messages::LeaveGroupRequest {
    const API_KEY: ApiKey = ApiKey::LeaveGroup;
    type Response = messages::LeaveGroupResponse;
}

impl Rpc for messages::SyncGroupRequest {
    const API_KEY: ApiKey = ApiKey::SyncGroup;
    type Response = messages::SyncGroupResponse;
}

impl Rpc for messages::DescribeGroupsRequest {
    const API_KEY: ApiKey = ApiKey::DescribeGroups;
    type Response = messages::DescribeGroupsResponse;
}

impl Rpc for messages::ListGroupsRequest {
    const API_KEY: ApiKey = ApiKey::ListGroups;
    type Response = messages::ListGroupsResponse;
}

impl Rpc for messages::SaslHandshakeRequest {
    const API_KEY: ApiKey = ApiKey::SaslHandshake;
    type Response = messages::SaslHandshakeResponse;
}

impl Rpc for messages::ApiVersionsRequest {
    const API_KEY: ApiKey = ApiKey::ApiVersions;
    type Response = messages::ApiVersionsResponse;
}

impl Rpc for messages::CreateTopicsRequest {
    const API_KEY: ApiKey = ApiKey::CreateTopics;
    type Response = messages::CreateTopicsResponse;
}

impl Rpc for messages::DeleteTopicsRequest {
    const API_KEY: ApiKey = ApiKey::DeleteTopics;
    type Response = messages::DeleteTopicsResponse;
}

impl Rpc for messages::DeleteRecordsRequest {
    const API_KEY: ApiKey = ApiKey::DeleteRecords;
    type Response = messages::DeleteRecordsResponse;
}

impl Rpc for messages::InitProducerIdRequest {
    const API_KEY: ApiKey = ApiKey::InitProducerId;
    type Response = messages::InitProducerIdResponse;
}

impl Rpc for messages::OffsetForLeaderEpochRequest {
    const API_KEY: ApiKey = ApiKey::OffsetForLeaderEpoch;
    type Response = messages::OffsetForLeaderEpochResponse;
}

impl Rpc for messages::AddPartitionsToTxnRequest {
    const API_KEY: ApiKey = ApiKey::AddPartitionsToTxn;
    type Response = messages::AddPartitionsToTxnResponse;
}

impl Rpc for messages::AddOffsetsToTxnRequest {
    const API_KEY: ApiKey = ApiKey::AddOffsetsToTxn;
    type Response = messages::AddOffsetsToTxnResponse;
}

impl Rpc for messages::EndTxnRequest {
    const API_KEY: ApiKey = ApiKey::EndTxn;
    type Response = messages::EndTxnResponse;
}

impl Rpc for messages::WriteTxnMarkersRequest {
    const API_KEY: ApiKey = ApiKey::WriteTxnMarkers;
    type Response = messages::WriteTxnMarkersResponse;
}

impl Rpc for messages::TxnOffsetCommitRequest {
    const API_KEY: ApiKey = ApiKey::TxnOffsetCommit;
    type Response = messages::TxnOffsetCommitResponse;
}

impl Rpc for messages::DescribeAclsRequest {
    const API_KEY: ApiKey = ApiKey::DescribeAcls;
    type Response = messages::DescribeAclsResponse;
}

impl Rpc for messages::CreateAclsRequest {
    const API_KEY: ApiKey = ApiKey::CreateAcls;
    type Response = messages::CreateAclsResponse;
}

impl Rpc for messages::DeleteAclsRequest {
    const API_KEY: ApiKey = ApiKey::DeleteAcls;
    type Response = messages::DeleteAclsResponse;
}

impl Rpc for messages::DescribeConfigsRequest {
    const API_KEY: ApiKey = ApiKey::DescribeConfigs;
    type Response = messages::DescribeConfigsResponse;
}

impl Rpc for messages::AlterConfigsRequest {
    const API_KEY: ApiKey = ApiKey::AlterConfigs;
    type Response = messages::AlterConfigsResponse;
}

impl Rpc for messages::AlterReplicaLogDirsRequest {
    const API_KEY: ApiKey = ApiKey::AlterReplicaLogDirs;
    type Response = messages::AlterReplicaLogDirsResponse;
}

impl Rpc for messages::DescribeLogDirsRequest {
    const API_KEY: ApiKey = ApiKey::DescribeLogDirs;
    type Response = messages::DescribeLogDirsResponse;
}

impl Rpc for messages::SaslAuthenticateRequest {
    const API_KEY: ApiKey = ApiKey::SaslAuthenticate;
    type Response = messages::SaslAuthenticateResponse;
}

impl Rpc for messages::CreatePartitionsRequest {
    const API_KEY: ApiKey = ApiKey::CreatePartitions;
    type Response = messages::CreatePartitionsResponse;
}

impl Rpc for messages::CreateDelegationTokenRequest {
    const API_KEY: ApiKey = ApiKey::CreateDelegationToken;
    type Response = messages::CreateDelegationTokenResponse;
}

impl Rpc for messages::RenewDelegationTokenRequest {
    const API_KEY: ApiKey = ApiKey::RenewDelegationToken;
    type Response = messages::RenewDelegationTokenResponse;
}

impl Rpc for messages::ExpireDelegationTokenRequest {
    const API_KEY: ApiKey = ApiKey::ExpireDelegationToken;
    type Response = messages::ExpireDelegationTokenResponse;
}

impl Rpc for messages::DescribeDelegationTokenRequest {
    const API_KEY: ApiKey = ApiKey::DescribeDelegationToken;
    type Response = messages::DescribeDelegationTokenResponse;
}

impl Rpc for messages::DeleteGroupsRequest {
    const API_KEY: ApiKey = ApiKey::DeleteGroups;
    type Response = messages::DeleteGroupsResponse;
}

impl Rpc for messages::ElectLeadersRequest {
    const API_KEY: ApiKey = ApiKey::ElectLeaders;
    type Response = messages::ElectLeadersResponse;
}

impl Rpc for messages::IncrementalAlterConfigsRequest {
    const API_KEY: ApiKey = ApiKey::IncrementalAlterConfigs;
    type Response = messages::IncrementalAlterConfigsResponse;
}

impl Rpc for messages::AlterPartitionReassignmentsRequest {
    const API_KEY: ApiKey = ApiKey::AlterPartitionReassignments;
    type Response = messages::AlterPartitionReassignmentsResponse;
}

impl Rpc for messages::ListPartitionReassignmentsRequest {
    const API_KEY: ApiKey = ApiKey::ListPartitionReassignments;
    type Response = messages::ListPartitionReassignmentsResponse;
}

impl Rpc for messages::OffsetDeleteRequest {
    const API_KEY: ApiKey = ApiKey::OffsetDelete;
    type Response = messages::OffsetDeleteResponse;
}

impl Rpc for messages::DescribeClientQuotasRequest {
    const API_KEY: ApiKey = ApiKey::DescribeClientQuotas;
    type Response = messages::DescribeClientQuotasResponse;
}

impl Rpc for messages::AlterClientQuotasRequest {
    const API_KEY: ApiKey = ApiKey::AlterClientQuotas;
    type Response = messages::AlterClientQuotasResponse;
}

impl Rpc for messages::DescribeUserScramCredentialsRequest {
    const API_KEY: ApiKey = ApiKey::DescribeUserScramCredentials;
    type Response = messages::DescribeUserScramCredentialsResponse;
}

impl Rpc for messages::AlterUserScramCredentialsRequest {
    const API_KEY: ApiKey = ApiKey::AlterUserScramCredentials;
    type Response = messages::AlterUserScramCredentialsResponse;
}

impl Rpc for messages::VoteRequest {
    const API_KEY: ApiKey = ApiKey::Vote;
    type Response = messages::VoteResponse;
}

impl Rpc for messages::BeginQuorumEpochRequest {
    const API_KEY: ApiKey = ApiKey::BeginQuorumEpoch;
    type Response = messages::BeginQuorumEpochResponse;
}

impl Rpc for messages::EndQuorumEpochRequest {
    const API_KEY: ApiKey = ApiKey::EndQuorumEpoch;
    type Response = messages::EndQuorumEpochResponse;
}

impl Rpc for messages::DescribeQuorumRequest {
    const API_KEY: ApiKey = ApiKey::DescribeQuorum;
    type Response = messages::DescribeQuorumResponse;
}

impl Rpc for messages::AlterPartitionRequest {
    const API_KEY: ApiKey = ApiKey::AlterPartition;
    type Response = messages::AlterPartitionResponse;
}

impl Rpc for messages::UpdateFeaturesRequest {
    const API_KEY: ApiKey = ApiKey::UpdateFeatures;
    type Response = messages::UpdateFeaturesResponse;
}

impl Rpc for messages::EnvelopeRequest {
    const API_KEY: ApiKey = ApiKey::Envelope;
    type Response = messages::EnvelopeResponse;
}

impl Rpc for messages::FetchSnapshotRequest {
    const API_KEY: ApiKey = ApiKey::FetchSnapshot;
    type Response = messages::FetchSnapshotResponse;
}

impl Rpc for messages::DescribeClusterRequest {
    const API_KEY: ApiKey = ApiKey::DescribeCluster;
    type Response = messages::DescribeClusterResponse;
}

impl Rpc for messages::DescribeProducersRequest {
    const API_KEY: ApiKey = ApiKey::DescribeProducers;
    type Response = messages::DescribeProducersResponse;
}

impl Rpc for messages::BrokerRegistrationRequest {
    const API_KEY: ApiKey = ApiKey::BrokerRegistration;
    type Response = messages::BrokerRegistrationResponse;
}

impl Rpc for messages::BrokerHeartbeatRequest {
    const API_KEY: ApiKey = ApiKey::BrokerHeartbeat;
    type Response = messages::BrokerHeartbeatResponse;
}

impl Rpc for messages::UnregisterBrokerRequest {
    const API_KEY: ApiKey = ApiKey::UnregisterBroker;
    type Response = messages::UnregisterBrokerResponse;
}

impl Rpc for messages::DescribeTransactionsRequest {
    const API_KEY: ApiKey = ApiKey::DescribeTransactions;
    type Response = messages::DescribeTransactionsResponse;
}

impl Rpc for messages::ListTransactionsRequest {
    const API_KEY: ApiKey = ApiKey::ListTransactions;
    type Response = messages::ListTransactionsResponse;
}

impl Rpc for messages::AllocateProducerIdsRequest {
    const API_KEY: ApiKey = ApiKey::AllocateProducerIds;
    type Response = messages::AllocateProducerIdsResponse;
}

impl Rpc for messages::ConsumerGroupHeartbeatRequest {
    const API_KEY: ApiKey = ApiKey::ConsumerGroupHeartbeat;
    type Response = messages::ConsumerGroupHeartbeatResponse;
}

impl Rpc for messages::ConsumerGroupDescribeRequest {
    const API_KEY: ApiKey = ApiKey::ConsumerGroupDescribe;
    type Response = messages::ConsumerGroupDescribeResponse;
}

impl Rpc for messages::ControllerRegistrationRequest {
    const API_KEY: ApiKey = ApiKey::ControllerRegistration;
    type Response = messages::ControllerRegistrationResponse;
}

impl Rpc for messages::GetTelemetrySubscriptionsRequest {
    const API_KEY: ApiKey = ApiKey::GetTelemetrySubscriptions;
    type Response = messages::GetTelemetrySubscriptionsResponse;
}

impl Rpc for messages::PushTelemetryRequest {
    const API_KEY: ApiKey = ApiKey::PushTelemetry;
    type Response = messages::PushTelemetryResponse;
}

impl Rpc for messages::AssignReplicasToDirsRequest {
    const API_KEY: ApiKey = ApiKey::AssignReplicasToDirs;
    type Response = messages::AssignReplicasToDirsResponse;
}

impl Rpc for messages::ListConfigResourcesRequest {
    const API_KEY: ApiKey = ApiKey::ListConfigResources;
    type Response = messages::ListConfigResourcesResponse;
}

impl Rpc for messages::DescribeTopicPartitionsRequest {
    const API_KEY: ApiKey = ApiKey::DescribeTopicPartitions;
    type Response = messages::DescribeTopicPartitionsResponse;
}

impl Rpc for messages::ShareGroupHeartbeatRequest {
    const API_KEY: ApiKey = ApiKey::ShareGroupHeartbeat;
    type Response = messages::ShareGroupHeartbeatResponse;
}

impl Rpc for messages::ShareGroupDescribeRequest {
    const API_KEY: ApiKey = ApiKey::ShareGroupDescribe;
    type Response = messages::ShareGroupDescribeResponse;
}

impl Rpc for messages::ShareFetchRequest {
    const API_KEY: ApiKey = ApiKey::ShareFetch;
    type Response = messages::ShareFetchResponse;
}

impl Rpc for messages::ShareAcknowledgeRequest {
    const API_KEY: ApiKey = ApiKey::ShareAcknowledge;
    type Response = messages::ShareAcknowledgeResponse;
}

impl Rpc for messages::AddRaftVoterRequest {
    const API_KEY: ApiKey = ApiKey::AddRaftVoter;
    type Response = messages::AddRaftVoterResponse;
}

impl Rpc for messages::RemoveRaftVoterRequest {
    const API_KEY: ApiKey = ApiKey::RemoveRaftVoter;
    type Response = messages::RemoveRaftVoterResponse;
}

impl Rpc for messages::UpdateRaftVoterRequest {
    const API_KEY: ApiKey = ApiKey::UpdateRaftVoter;
    type Response = messages::UpdateRaftVoterResponse;
}

impl Rpc for messages::InitializeShareGroupStateRequest {
    const API_KEY: ApiKey = ApiKey::InitializeShareGroupState;
    type Response = messages::InitializeShareGroupStateResponse;
}

impl Rpc for messages::ReadShareGroupStateRequest {
    const API_KEY: ApiKey = ApiKey::ReadShareGroupState;
    type Response = messages::ReadShareGroupStateResponse;
}

impl Rpc for messages::WriteShareGroupStateRequest {
    const API_KEY: ApiKey = ApiKey::WriteShareGroupState;
    type Response = messages::WriteShareGroupStateResponse;
}

impl Rpc for messages::DeleteShareGroupStateRequest {
    const API_KEY: ApiKey = ApiKey::DeleteShareGroupState;
    type Response = messages::DeleteShareGroupStateResponse;
}

impl Rpc for messages::ReadShareGroupStateSummaryRequest {
    const API_KEY: ApiKey = ApiKey::ReadShareGroupStateSummary;
    type Response = messages::ReadShareGroupStateSummaryResponse;
}

impl Rpc for messages::DescribeShareGroupOffsetsRequest {
    const API_KEY: ApiKey = ApiKey::DescribeShareGroupOffsets;
    type Response = messages::DescribeShareGroupOffsetsResponse;
}

impl Rpc for messages::AlterShareGroupOffsetsRequest {
    const API_KEY: ApiKey = ApiKey::AlterShareGroupOffsets;
    type Response = messages::AlterShareGroupOffsetsResponse;
}

impl Rpc for messages::DeleteShareGroupOffsetsRequest {
    const API_KEY: ApiKey = ApiKey::DeleteShareGroupOffsets;
    type Response = messages::DeleteShareGroupOffsetsResponse;
}
