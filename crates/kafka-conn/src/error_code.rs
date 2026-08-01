//! The broker error-code table.
//!
//! One table, one file — the M5 artifact. It is *derived* from
//! `kafka_protocol::ResponseError` rather than transcribed from it, and that
//! is the load-bearing part:
//!
//! * `retriable()` delegates to the crate's `is_retriable()`, which encodes
//!   what the protocol says rather than what we remember it saying.
//! * [`ErrorCode::from_response_error`] matches `ResponseError` exhaustively.
//!   `ResponseError` is a plain enum, so when an upstream bump adds a code that
//!   match stops compiling — a new error code becomes a build failure to
//!   triage rather than a silent hole in the classification.
//!
//! The two axes the crate does not model — whether a code should invalidate
//! the metadata snapshot, and whether it should invalidate a cached group or
//! transaction coordinator — are ours, and are exhaustive matches over our own
//! enum for the same reason.
//!
//! [`ErrorCode::Unknown`] is not optional. `kafka-protocol` 0.17 knows codes
//! through Kafka 4.1; the acceptance suite runs against 4.3.1, so codes with no
//! name here are the expected case and must round-trip and render rather than
//! collapsing into a generic failure.

use kafka_protocol::ResponseError;

/// A Kafka broker error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// The server experienced an unexpected error when processing the request.
    UnknownServerError,
    /// The requested offset is not within the range of offsets maintained by the server.
    OffsetOutOfRange,
    /// This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.
    CorruptMessage,
    /// This server does not host this topic-partition.
    UnknownTopicOrPartition,
    /// The requested fetch size is invalid.
    InvalidFetchSize,
    /// There is no leader for this topic-partition as we are in the middle of a leadership election.
    LeaderNotAvailable,
    /// For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.
    NotLeaderOrFollower,
    /// The request timed out.
    RequestTimedOut,
    /// The broker is not available.
    BrokerNotAvailable,
    /// The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.
    ReplicaNotAvailable,
    /// The request included a message larger than the max message size the server will accept.
    MessageTooLarge,
    /// The controller moved to another broker.
    StaleControllerEpoch,
    /// The metadata field of the offset request was too large.
    OffsetMetadataTooLarge,
    /// The server disconnected before a response was received.
    NetworkException,
    /// The coordinator is loading and hence can't process requests.
    CoordinatorLoadInProgress,
    /// The coordinator is not available.
    CoordinatorNotAvailable,
    /// This is not the correct coordinator.
    NotCoordinator,
    /// The request attempted to perform an operation on an invalid topic.
    InvalidTopicException,
    /// The request included message batch larger than the configured segment size on the server.
    RecordListTooLarge,
    /// Messages are rejected since there are fewer in-sync replicas than required.
    NotEnoughReplicas,
    /// Messages are written to the log, but to fewer in-sync replicas than required.
    NotEnoughReplicasAfterAppend,
    /// Produce request specified an invalid value for required acks.
    InvalidRequiredAcks,
    /// Specified group generation id is not valid.
    IllegalGeneration,
    /// The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.
    InconsistentGroupProtocol,
    /// The configured groupId is invalid.
    InvalidGroupId,
    /// The coordinator is not aware of this member.
    UnknownMemberId,
    /// The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).
    InvalidSessionTimeout,
    /// The group is rebalancing, so a rejoin is needed.
    RebalanceInProgress,
    /// The committing offset data size is not valid.
    InvalidCommitOffsetSize,
    /// Topic authorization failed.
    TopicAuthorizationFailed,
    /// Group authorization failed.
    GroupAuthorizationFailed,
    /// Cluster authorization failed.
    ClusterAuthorizationFailed,
    /// The timestamp of the message is out of acceptable range.
    InvalidTimestamp,
    /// The broker does not support the requested SASL mechanism.
    UnsupportedSaslMechanism,
    /// Request is not valid given the current SASL state.
    IllegalSaslState,
    /// The version of API is not supported.
    UnsupportedVersion,
    /// Topic with this name already exists.
    TopicAlreadyExists,
    /// Number of partitions is below 1.
    InvalidPartitions,
    /// Replication factor is below 1 or larger than the number of available brokers.
    InvalidReplicationFactor,
    /// Replica assignment is invalid.
    InvalidReplicaAssignment,
    /// Configuration is invalid.
    InvalidConfig,
    /// This is not the correct controller for this cluster.
    NotController,
    /// This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.
    InvalidRequest,
    /// The message format version on the broker does not support the request.
    UnsupportedForMessageFormat,
    /// Request parameters do not satisfy the configured policy.
    PolicyViolation,
    /// The broker received an out of order sequence number.
    OutOfOrderSequenceNumber,
    /// The broker received a duplicate sequence number.
    DuplicateSequenceNumber,
    /// Producer attempted to produce with an old epoch.
    InvalidProducerEpoch,
    /// The producer attempted a transactional operation in an invalid state.
    InvalidTxnState,
    /// The producer attempted to use a producer id which is not currently assigned to its transactional id.
    InvalidProducerIdMapping,
    /// The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).
    InvalidTransactionTimeout,
    /// The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.
    ConcurrentTransactions,
    /// Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.
    TransactionCoordinatorFenced,
    /// Transactional Id authorization failed.
    TransactionalIdAuthorizationFailed,
    /// Security features are disabled.
    SecurityDisabled,
    /// The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.
    OperationNotAttempted,
    /// Disk error when trying to access log file on the disk.
    KafkaStorageError,
    /// The user-specified log directory is not found in the broker config.
    LogDirNotFound,
    /// SASL Authentication failed.
    SaslAuthenticationFailed,
    /// This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.
    UnknownProducerId,
    /// A partition reassignment is in progress.
    ReassignmentInProgress,
    /// Delegation Token feature is not enabled.
    DelegationTokenAuthDisabled,
    /// Delegation Token is not found on server.
    DelegationTokenNotFound,
    /// Specified Principal is not valid Owner/Renewer.
    DelegationTokenOwnerMismatch,
    /// Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.
    DelegationTokenRequestNotAllowed,
    /// Delegation Token authorization failed.
    DelegationTokenAuthorizationFailed,
    /// Delegation Token is expired.
    DelegationTokenExpired,
    /// Supplied principalType is not supported.
    InvalidPrincipalType,
    /// The group is not empty.
    NonEmptyGroup,
    /// The group id does not exist.
    GroupIdNotFound,
    /// The fetch session ID was not found.
    FetchSessionIdNotFound,
    /// The fetch session epoch is invalid.
    InvalidFetchSessionEpoch,
    /// There is no listener on the leader broker that matches the listener on which metadata request was processed.
    ListenerNotFound,
    /// Topic deletion is disabled.
    TopicDeletionDisabled,
    /// The leader epoch in the request is older than the epoch on the broker.
    FencedLeaderEpoch,
    /// The leader epoch in the request is newer than the epoch on the broker.
    UnknownLeaderEpoch,
    /// The requesting client does not support the compression type of given partition.
    UnsupportedCompressionType,
    /// Broker epoch has changed.
    StaleBrokerEpoch,
    /// The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.
    OffsetNotAvailable,
    /// The group member needs to have a valid member id before actually entering a consumer group.
    MemberIdRequired,
    /// The preferred leader was not available.
    PreferredLeaderNotAvailable,
    /// The consumer group has reached its max size.
    GroupMaxSizeReached,
    /// The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.
    FencedInstanceId,
    /// Eligible topic partition leaders are not available.
    EligibleLeadersNotAvailable,
    /// Leader election not needed for topic partition.
    ElectionNotNeeded,
    /// No partition reassignment is in progress.
    NoReassignmentInProgress,
    /// Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.
    GroupSubscribedToTopic,
    /// This record has failed the validation on broker and hence will be rejected.
    InvalidRecord,
    /// There are unstable offsets that need to be cleared.
    UnstableOffsetCommit,
    /// The throttling quota has been exceeded.
    ThrottlingQuotaExceeded,
    /// There is a newer producer with the same transactionalId which fences the current one.
    ProducerFenced,
    /// A request illegally referred to a resource that does not exist.
    ResourceNotFound,
    /// A request illegally referred to the same resource twice.
    DuplicateResource,
    /// Requested credential would not meet criteria for acceptability.
    UnacceptableCredential,
    /// Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters
    InconsistentVoterSet,
    /// The given update version was invalid.
    InvalidUpdateVersion,
    /// Unable to update finalized features due to an unexpected server error.
    FeatureUpdateFailed,
    /// Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup.
    PrincipalDeserializationFailure,
    /// Requested snapshot was not found
    SnapshotNotFound,
    /// Requested position is not greater than or equal to zero, and less than the size of the snapshot.
    PositionOutOfRange,
    /// This server does not host this topic ID.
    UnknownTopicId,
    /// This broker ID is already in use.
    DuplicateBrokerRegistration,
    /// The given broker ID was not registered.
    BrokerIdNotRegistered,
    /// The log's topic ID did not match the topic ID in the request
    InconsistentTopicId,
    /// The clusterId in the request does not match that found on the server
    InconsistentClusterId,
    /// The transactionalId could not be found
    TransactionalIdNotFound,
    /// The fetch session encountered inconsistent topic ID usage
    FetchSessionTopicIdError,
    /// The new ISR contains at least one ineligible replica.
    IneligibleReplica,
    /// The AlterPartition request successfully updated the partition state but the leader has changed.
    NewLeaderElected,
    /// The requested offset is moved to tiered storage.
    OffsetMovedToTieredStorage,
    /// The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.
    FencedMemberEpoch,
    /// The instance ID is still used by another member in the consumer group. That member must leave first.
    UnreleasedInstanceId,
    /// The assignor or its version range is not supported by the consumer group.
    UnsupportedAssignor,
    /// The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.
    StaleMemberEpoch,
    /// The request was sent to an endpoint of the wrong type.
    MismatchedEndpointType,
    /// This endpoint type is not supported yet.
    UnsupportedEndpointType,
    /// This controller ID is not known.
    UnknownControllerId,
    /// Client sent a push telemetry request with an invalid or outdated subscription ID.
    UnknownSubscriptionId,
    /// Client sent a push telemetry request larger than the maximum size the broker will accept.
    TelemetryTooLarge,
    /// The controller has considered the broker registration to be invalid.
    InvalidRegistration,
    /// The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID.
    TransactionAbortable,
    /// The record state is invalid. The acknowledgement of delivery could not be completed.
    InvalidRecordState,
    /// The share session was not found.
    ShareSessionNotFound,
    /// The share session epoch is invalid.
    InvalidShareSessionEpoch,
    /// The share coordinator rejected the request because the share-group state epoch did not match.
    FencedStateEpoch,
    /// The voter key doesn't match the receiving replica's key.
    InvalidVoterKey,
    /// The voter is already part of the set of voters.
    DuplicateVoter,
    /// The voter is not part of the set of voters.
    VoterNotFound,
    /// The regular expression is not valid.
    InvalidRegularExpression,
    /// Client metadata is stale, client should rebootstrap to obtain new metadata.
    RebootstrapRequired,
    /// The supplied topology is invalid.
    StreamsInvalidTopology,
    /// The supplied topology epoch is invalid.
    StreamsInvalidTopologyEpoch,
    /// The supplied topology epoch is outdated.
    StreamsTopologyFenced,
    /// The limit of share sessions has been reached.
    ShareSessionLimitReached,
    /// A code this build has no name for.
    ///
    /// Carries the wire value so a UI can still show it and a bug report can
    /// still identify it.
    Unknown(i16),
}

impl ErrorCode {
    /// The wire code.
    pub const fn code(self) -> i16 {
        match self {
            ErrorCode::UnknownServerError => -1,
            ErrorCode::OffsetOutOfRange => 1,
            ErrorCode::CorruptMessage => 2,
            ErrorCode::UnknownTopicOrPartition => 3,
            ErrorCode::InvalidFetchSize => 4,
            ErrorCode::LeaderNotAvailable => 5,
            ErrorCode::NotLeaderOrFollower => 6,
            ErrorCode::RequestTimedOut => 7,
            ErrorCode::BrokerNotAvailable => 8,
            ErrorCode::ReplicaNotAvailable => 9,
            ErrorCode::MessageTooLarge => 10,
            ErrorCode::StaleControllerEpoch => 11,
            ErrorCode::OffsetMetadataTooLarge => 12,
            ErrorCode::NetworkException => 13,
            ErrorCode::CoordinatorLoadInProgress => 14,
            ErrorCode::CoordinatorNotAvailable => 15,
            ErrorCode::NotCoordinator => 16,
            ErrorCode::InvalidTopicException => 17,
            ErrorCode::RecordListTooLarge => 18,
            ErrorCode::NotEnoughReplicas => 19,
            ErrorCode::NotEnoughReplicasAfterAppend => 20,
            ErrorCode::InvalidRequiredAcks => 21,
            ErrorCode::IllegalGeneration => 22,
            ErrorCode::InconsistentGroupProtocol => 23,
            ErrorCode::InvalidGroupId => 24,
            ErrorCode::UnknownMemberId => 25,
            ErrorCode::InvalidSessionTimeout => 26,
            ErrorCode::RebalanceInProgress => 27,
            ErrorCode::InvalidCommitOffsetSize => 28,
            ErrorCode::TopicAuthorizationFailed => 29,
            ErrorCode::GroupAuthorizationFailed => 30,
            ErrorCode::ClusterAuthorizationFailed => 31,
            ErrorCode::InvalidTimestamp => 32,
            ErrorCode::UnsupportedSaslMechanism => 33,
            ErrorCode::IllegalSaslState => 34,
            ErrorCode::UnsupportedVersion => 35,
            ErrorCode::TopicAlreadyExists => 36,
            ErrorCode::InvalidPartitions => 37,
            ErrorCode::InvalidReplicationFactor => 38,
            ErrorCode::InvalidReplicaAssignment => 39,
            ErrorCode::InvalidConfig => 40,
            ErrorCode::NotController => 41,
            ErrorCode::InvalidRequest => 42,
            ErrorCode::UnsupportedForMessageFormat => 43,
            ErrorCode::PolicyViolation => 44,
            ErrorCode::OutOfOrderSequenceNumber => 45,
            ErrorCode::DuplicateSequenceNumber => 46,
            ErrorCode::InvalidProducerEpoch => 47,
            ErrorCode::InvalidTxnState => 48,
            ErrorCode::InvalidProducerIdMapping => 49,
            ErrorCode::InvalidTransactionTimeout => 50,
            ErrorCode::ConcurrentTransactions => 51,
            ErrorCode::TransactionCoordinatorFenced => 52,
            ErrorCode::TransactionalIdAuthorizationFailed => 53,
            ErrorCode::SecurityDisabled => 54,
            ErrorCode::OperationNotAttempted => 55,
            ErrorCode::KafkaStorageError => 56,
            ErrorCode::LogDirNotFound => 57,
            ErrorCode::SaslAuthenticationFailed => 58,
            ErrorCode::UnknownProducerId => 59,
            ErrorCode::ReassignmentInProgress => 60,
            ErrorCode::DelegationTokenAuthDisabled => 61,
            ErrorCode::DelegationTokenNotFound => 62,
            ErrorCode::DelegationTokenOwnerMismatch => 63,
            ErrorCode::DelegationTokenRequestNotAllowed => 64,
            ErrorCode::DelegationTokenAuthorizationFailed => 65,
            ErrorCode::DelegationTokenExpired => 66,
            ErrorCode::InvalidPrincipalType => 67,
            ErrorCode::NonEmptyGroup => 68,
            ErrorCode::GroupIdNotFound => 69,
            ErrorCode::FetchSessionIdNotFound => 70,
            ErrorCode::InvalidFetchSessionEpoch => 71,
            ErrorCode::ListenerNotFound => 72,
            ErrorCode::TopicDeletionDisabled => 73,
            ErrorCode::FencedLeaderEpoch => 74,
            ErrorCode::UnknownLeaderEpoch => 75,
            ErrorCode::UnsupportedCompressionType => 76,
            ErrorCode::StaleBrokerEpoch => 77,
            ErrorCode::OffsetNotAvailable => 78,
            ErrorCode::MemberIdRequired => 79,
            ErrorCode::PreferredLeaderNotAvailable => 80,
            ErrorCode::GroupMaxSizeReached => 81,
            ErrorCode::FencedInstanceId => 82,
            ErrorCode::EligibleLeadersNotAvailable => 83,
            ErrorCode::ElectionNotNeeded => 84,
            ErrorCode::NoReassignmentInProgress => 85,
            ErrorCode::GroupSubscribedToTopic => 86,
            ErrorCode::InvalidRecord => 87,
            ErrorCode::UnstableOffsetCommit => 88,
            ErrorCode::ThrottlingQuotaExceeded => 89,
            ErrorCode::ProducerFenced => 90,
            ErrorCode::ResourceNotFound => 91,
            ErrorCode::DuplicateResource => 92,
            ErrorCode::UnacceptableCredential => 93,
            ErrorCode::InconsistentVoterSet => 94,
            ErrorCode::InvalidUpdateVersion => 95,
            ErrorCode::FeatureUpdateFailed => 96,
            ErrorCode::PrincipalDeserializationFailure => 97,
            ErrorCode::SnapshotNotFound => 98,
            ErrorCode::PositionOutOfRange => 99,
            ErrorCode::UnknownTopicId => 100,
            ErrorCode::DuplicateBrokerRegistration => 101,
            ErrorCode::BrokerIdNotRegistered => 102,
            ErrorCode::InconsistentTopicId => 103,
            ErrorCode::InconsistentClusterId => 104,
            ErrorCode::TransactionalIdNotFound => 105,
            ErrorCode::FetchSessionTopicIdError => 106,
            ErrorCode::IneligibleReplica => 107,
            ErrorCode::NewLeaderElected => 108,
            ErrorCode::OffsetMovedToTieredStorage => 109,
            ErrorCode::FencedMemberEpoch => 110,
            ErrorCode::UnreleasedInstanceId => 111,
            ErrorCode::UnsupportedAssignor => 112,
            ErrorCode::StaleMemberEpoch => 113,
            ErrorCode::MismatchedEndpointType => 114,
            ErrorCode::UnsupportedEndpointType => 115,
            ErrorCode::UnknownControllerId => 116,
            ErrorCode::UnknownSubscriptionId => 117,
            ErrorCode::TelemetryTooLarge => 118,
            ErrorCode::InvalidRegistration => 119,
            ErrorCode::TransactionAbortable => 120,
            ErrorCode::InvalidRecordState => 121,
            ErrorCode::ShareSessionNotFound => 122,
            ErrorCode::InvalidShareSessionEpoch => 123,
            ErrorCode::FencedStateEpoch => 124,
            ErrorCode::InvalidVoterKey => 125,
            ErrorCode::DuplicateVoter => 126,
            ErrorCode::VoterNotFound => 127,
            ErrorCode::InvalidRegularExpression => 128,
            ErrorCode::RebootstrapRequired => 129,
            ErrorCode::StreamsInvalidTopology => 130,
            ErrorCode::StreamsInvalidTopologyEpoch => 131,
            ErrorCode::StreamsTopologyFenced => 132,
            ErrorCode::ShareSessionLimitReached => 133,
            ErrorCode::Unknown(code) => code,
        }
    }

    /// The code's protocol name, or `None` for an unrecognised code.
    pub const fn name(self) -> Option<&'static str> {
        match self {
            ErrorCode::UnknownServerError => Some("UNKNOWN_SERVER_ERROR"),
            ErrorCode::OffsetOutOfRange => Some("OFFSET_OUT_OF_RANGE"),
            ErrorCode::CorruptMessage => Some("CORRUPT_MESSAGE"),
            ErrorCode::UnknownTopicOrPartition => Some("UNKNOWN_TOPIC_OR_PARTITION"),
            ErrorCode::InvalidFetchSize => Some("INVALID_FETCH_SIZE"),
            ErrorCode::LeaderNotAvailable => Some("LEADER_NOT_AVAILABLE"),
            ErrorCode::NotLeaderOrFollower => Some("NOT_LEADER_OR_FOLLOWER"),
            ErrorCode::RequestTimedOut => Some("REQUEST_TIMED_OUT"),
            ErrorCode::BrokerNotAvailable => Some("BROKER_NOT_AVAILABLE"),
            ErrorCode::ReplicaNotAvailable => Some("REPLICA_NOT_AVAILABLE"),
            ErrorCode::MessageTooLarge => Some("MESSAGE_TOO_LARGE"),
            ErrorCode::StaleControllerEpoch => Some("STALE_CONTROLLER_EPOCH"),
            ErrorCode::OffsetMetadataTooLarge => Some("OFFSET_METADATA_TOO_LARGE"),
            ErrorCode::NetworkException => Some("NETWORK_EXCEPTION"),
            ErrorCode::CoordinatorLoadInProgress => Some("COORDINATOR_LOAD_IN_PROGRESS"),
            ErrorCode::CoordinatorNotAvailable => Some("COORDINATOR_NOT_AVAILABLE"),
            ErrorCode::NotCoordinator => Some("NOT_COORDINATOR"),
            ErrorCode::InvalidTopicException => Some("INVALID_TOPIC_EXCEPTION"),
            ErrorCode::RecordListTooLarge => Some("RECORD_LIST_TOO_LARGE"),
            ErrorCode::NotEnoughReplicas => Some("NOT_ENOUGH_REPLICAS"),
            ErrorCode::NotEnoughReplicasAfterAppend => Some("NOT_ENOUGH_REPLICAS_AFTER_APPEND"),
            ErrorCode::InvalidRequiredAcks => Some("INVALID_REQUIRED_ACKS"),
            ErrorCode::IllegalGeneration => Some("ILLEGAL_GENERATION"),
            ErrorCode::InconsistentGroupProtocol => Some("INCONSISTENT_GROUP_PROTOCOL"),
            ErrorCode::InvalidGroupId => Some("INVALID_GROUP_ID"),
            ErrorCode::UnknownMemberId => Some("UNKNOWN_MEMBER_ID"),
            ErrorCode::InvalidSessionTimeout => Some("INVALID_SESSION_TIMEOUT"),
            ErrorCode::RebalanceInProgress => Some("REBALANCE_IN_PROGRESS"),
            ErrorCode::InvalidCommitOffsetSize => Some("INVALID_COMMIT_OFFSET_SIZE"),
            ErrorCode::TopicAuthorizationFailed => Some("TOPIC_AUTHORIZATION_FAILED"),
            ErrorCode::GroupAuthorizationFailed => Some("GROUP_AUTHORIZATION_FAILED"),
            ErrorCode::ClusterAuthorizationFailed => Some("CLUSTER_AUTHORIZATION_FAILED"),
            ErrorCode::InvalidTimestamp => Some("INVALID_TIMESTAMP"),
            ErrorCode::UnsupportedSaslMechanism => Some("UNSUPPORTED_SASL_MECHANISM"),
            ErrorCode::IllegalSaslState => Some("ILLEGAL_SASL_STATE"),
            ErrorCode::UnsupportedVersion => Some("UNSUPPORTED_VERSION"),
            ErrorCode::TopicAlreadyExists => Some("TOPIC_ALREADY_EXISTS"),
            ErrorCode::InvalidPartitions => Some("INVALID_PARTITIONS"),
            ErrorCode::InvalidReplicationFactor => Some("INVALID_REPLICATION_FACTOR"),
            ErrorCode::InvalidReplicaAssignment => Some("INVALID_REPLICA_ASSIGNMENT"),
            ErrorCode::InvalidConfig => Some("INVALID_CONFIG"),
            ErrorCode::NotController => Some("NOT_CONTROLLER"),
            ErrorCode::InvalidRequest => Some("INVALID_REQUEST"),
            ErrorCode::UnsupportedForMessageFormat => Some("UNSUPPORTED_FOR_MESSAGE_FORMAT"),
            ErrorCode::PolicyViolation => Some("POLICY_VIOLATION"),
            ErrorCode::OutOfOrderSequenceNumber => Some("OUT_OF_ORDER_SEQUENCE_NUMBER"),
            ErrorCode::DuplicateSequenceNumber => Some("DUPLICATE_SEQUENCE_NUMBER"),
            ErrorCode::InvalidProducerEpoch => Some("INVALID_PRODUCER_EPOCH"),
            ErrorCode::InvalidTxnState => Some("INVALID_TXN_STATE"),
            ErrorCode::InvalidProducerIdMapping => Some("INVALID_PRODUCER_ID_MAPPING"),
            ErrorCode::InvalidTransactionTimeout => Some("INVALID_TRANSACTION_TIMEOUT"),
            ErrorCode::ConcurrentTransactions => Some("CONCURRENT_TRANSACTIONS"),
            ErrorCode::TransactionCoordinatorFenced => Some("TRANSACTION_COORDINATOR_FENCED"),
            ErrorCode::TransactionalIdAuthorizationFailed => {
                Some("TRANSACTIONAL_ID_AUTHORIZATION_FAILED")
            }
            ErrorCode::SecurityDisabled => Some("SECURITY_DISABLED"),
            ErrorCode::OperationNotAttempted => Some("OPERATION_NOT_ATTEMPTED"),
            ErrorCode::KafkaStorageError => Some("KAFKA_STORAGE_ERROR"),
            ErrorCode::LogDirNotFound => Some("LOG_DIR_NOT_FOUND"),
            ErrorCode::SaslAuthenticationFailed => Some("SASL_AUTHENTICATION_FAILED"),
            ErrorCode::UnknownProducerId => Some("UNKNOWN_PRODUCER_ID"),
            ErrorCode::ReassignmentInProgress => Some("REASSIGNMENT_IN_PROGRESS"),
            ErrorCode::DelegationTokenAuthDisabled => Some("DELEGATION_TOKEN_AUTH_DISABLED"),
            ErrorCode::DelegationTokenNotFound => Some("DELEGATION_TOKEN_NOT_FOUND"),
            ErrorCode::DelegationTokenOwnerMismatch => Some("DELEGATION_TOKEN_OWNER_MISMATCH"),
            ErrorCode::DelegationTokenRequestNotAllowed => {
                Some("DELEGATION_TOKEN_REQUEST_NOT_ALLOWED")
            }
            ErrorCode::DelegationTokenAuthorizationFailed => {
                Some("DELEGATION_TOKEN_AUTHORIZATION_FAILED")
            }
            ErrorCode::DelegationTokenExpired => Some("DELEGATION_TOKEN_EXPIRED"),
            ErrorCode::InvalidPrincipalType => Some("INVALID_PRINCIPAL_TYPE"),
            ErrorCode::NonEmptyGroup => Some("NON_EMPTY_GROUP"),
            ErrorCode::GroupIdNotFound => Some("GROUP_ID_NOT_FOUND"),
            ErrorCode::FetchSessionIdNotFound => Some("FETCH_SESSION_ID_NOT_FOUND"),
            ErrorCode::InvalidFetchSessionEpoch => Some("INVALID_FETCH_SESSION_EPOCH"),
            ErrorCode::ListenerNotFound => Some("LISTENER_NOT_FOUND"),
            ErrorCode::TopicDeletionDisabled => Some("TOPIC_DELETION_DISABLED"),
            ErrorCode::FencedLeaderEpoch => Some("FENCED_LEADER_EPOCH"),
            ErrorCode::UnknownLeaderEpoch => Some("UNKNOWN_LEADER_EPOCH"),
            ErrorCode::UnsupportedCompressionType => Some("UNSUPPORTED_COMPRESSION_TYPE"),
            ErrorCode::StaleBrokerEpoch => Some("STALE_BROKER_EPOCH"),
            ErrorCode::OffsetNotAvailable => Some("OFFSET_NOT_AVAILABLE"),
            ErrorCode::MemberIdRequired => Some("MEMBER_ID_REQUIRED"),
            ErrorCode::PreferredLeaderNotAvailable => Some("PREFERRED_LEADER_NOT_AVAILABLE"),
            ErrorCode::GroupMaxSizeReached => Some("GROUP_MAX_SIZE_REACHED"),
            ErrorCode::FencedInstanceId => Some("FENCED_INSTANCE_ID"),
            ErrorCode::EligibleLeadersNotAvailable => Some("ELIGIBLE_LEADERS_NOT_AVAILABLE"),
            ErrorCode::ElectionNotNeeded => Some("ELECTION_NOT_NEEDED"),
            ErrorCode::NoReassignmentInProgress => Some("NO_REASSIGNMENT_IN_PROGRESS"),
            ErrorCode::GroupSubscribedToTopic => Some("GROUP_SUBSCRIBED_TO_TOPIC"),
            ErrorCode::InvalidRecord => Some("INVALID_RECORD"),
            ErrorCode::UnstableOffsetCommit => Some("UNSTABLE_OFFSET_COMMIT"),
            ErrorCode::ThrottlingQuotaExceeded => Some("THROTTLING_QUOTA_EXCEEDED"),
            ErrorCode::ProducerFenced => Some("PRODUCER_FENCED"),
            ErrorCode::ResourceNotFound => Some("RESOURCE_NOT_FOUND"),
            ErrorCode::DuplicateResource => Some("DUPLICATE_RESOURCE"),
            ErrorCode::UnacceptableCredential => Some("UNACCEPTABLE_CREDENTIAL"),
            ErrorCode::InconsistentVoterSet => Some("INCONSISTENT_VOTER_SET"),
            ErrorCode::InvalidUpdateVersion => Some("INVALID_UPDATE_VERSION"),
            ErrorCode::FeatureUpdateFailed => Some("FEATURE_UPDATE_FAILED"),
            ErrorCode::PrincipalDeserializationFailure => Some("PRINCIPAL_DESERIALIZATION_FAILURE"),
            ErrorCode::SnapshotNotFound => Some("SNAPSHOT_NOT_FOUND"),
            ErrorCode::PositionOutOfRange => Some("POSITION_OUT_OF_RANGE"),
            ErrorCode::UnknownTopicId => Some("UNKNOWN_TOPIC_ID"),
            ErrorCode::DuplicateBrokerRegistration => Some("DUPLICATE_BROKER_REGISTRATION"),
            ErrorCode::BrokerIdNotRegistered => Some("BROKER_ID_NOT_REGISTERED"),
            ErrorCode::InconsistentTopicId => Some("INCONSISTENT_TOPIC_ID"),
            ErrorCode::InconsistentClusterId => Some("INCONSISTENT_CLUSTER_ID"),
            ErrorCode::TransactionalIdNotFound => Some("TRANSACTIONAL_ID_NOT_FOUND"),
            ErrorCode::FetchSessionTopicIdError => Some("FETCH_SESSION_TOPIC_ID_ERROR"),
            ErrorCode::IneligibleReplica => Some("INELIGIBLE_REPLICA"),
            ErrorCode::NewLeaderElected => Some("NEW_LEADER_ELECTED"),
            ErrorCode::OffsetMovedToTieredStorage => Some("OFFSET_MOVED_TO_TIERED_STORAGE"),
            ErrorCode::FencedMemberEpoch => Some("FENCED_MEMBER_EPOCH"),
            ErrorCode::UnreleasedInstanceId => Some("UNRELEASED_INSTANCE_ID"),
            ErrorCode::UnsupportedAssignor => Some("UNSUPPORTED_ASSIGNOR"),
            ErrorCode::StaleMemberEpoch => Some("STALE_MEMBER_EPOCH"),
            ErrorCode::MismatchedEndpointType => Some("MISMATCHED_ENDPOINT_TYPE"),
            ErrorCode::UnsupportedEndpointType => Some("UNSUPPORTED_ENDPOINT_TYPE"),
            ErrorCode::UnknownControllerId => Some("UNKNOWN_CONTROLLER_ID"),
            ErrorCode::UnknownSubscriptionId => Some("UNKNOWN_SUBSCRIPTION_ID"),
            ErrorCode::TelemetryTooLarge => Some("TELEMETRY_TOO_LARGE"),
            ErrorCode::InvalidRegistration => Some("INVALID_REGISTRATION"),
            ErrorCode::TransactionAbortable => Some("TRANSACTION_ABORTABLE"),
            ErrorCode::InvalidRecordState => Some("INVALID_RECORD_STATE"),
            ErrorCode::ShareSessionNotFound => Some("SHARE_SESSION_NOT_FOUND"),
            ErrorCode::InvalidShareSessionEpoch => Some("INVALID_SHARE_SESSION_EPOCH"),
            ErrorCode::FencedStateEpoch => Some("FENCED_STATE_EPOCH"),
            ErrorCode::InvalidVoterKey => Some("INVALID_VOTER_KEY"),
            ErrorCode::DuplicateVoter => Some("DUPLICATE_VOTER"),
            ErrorCode::VoterNotFound => Some("VOTER_NOT_FOUND"),
            ErrorCode::InvalidRegularExpression => Some("INVALID_REGULAR_EXPRESSION"),
            ErrorCode::RebootstrapRequired => Some("REBOOTSTRAP_REQUIRED"),
            ErrorCode::StreamsInvalidTopology => Some("STREAMS_INVALID_TOPOLOGY"),
            ErrorCode::StreamsInvalidTopologyEpoch => Some("STREAMS_INVALID_TOPOLOGY_EPOCH"),
            ErrorCode::StreamsTopologyFenced => Some("STREAMS_TOPOLOGY_FENCED"),
            ErrorCode::ShareSessionLimitReached => Some("SHARE_SESSION_LIMIT_REACHED"),
            ErrorCode::Unknown(_) => None,
        }
    }

    /// The protocol's own description of the code.
    pub const fn description(self) -> Option<&'static str> {
        match self {
            ErrorCode::UnknownServerError => {
                Some("The server experienced an unexpected error when processing the request.")
            }
            ErrorCode::OffsetOutOfRange => Some(
                "The requested offset is not within the range of offsets maintained by the server.",
            ),
            ErrorCode::CorruptMessage => Some(
                "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.",
            ),
            ErrorCode::UnknownTopicOrPartition => {
                Some("This server does not host this topic-partition.")
            }
            ErrorCode::InvalidFetchSize => Some("The requested fetch size is invalid."),
            ErrorCode::LeaderNotAvailable => Some(
                "There is no leader for this topic-partition as we are in the middle of a leadership election.",
            ),
            ErrorCode::NotLeaderOrFollower => Some(
                "For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.",
            ),
            ErrorCode::RequestTimedOut => Some("The request timed out."),
            ErrorCode::BrokerNotAvailable => Some("The broker is not available."),
            ErrorCode::ReplicaNotAvailable => Some(
                "The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.",
            ),
            ErrorCode::MessageTooLarge => Some(
                "The request included a message larger than the max message size the server will accept.",
            ),
            ErrorCode::StaleControllerEpoch => Some("The controller moved to another broker."),
            ErrorCode::OffsetMetadataTooLarge => {
                Some("The metadata field of the offset request was too large.")
            }
            ErrorCode::NetworkException => {
                Some("The server disconnected before a response was received.")
            }
            ErrorCode::CoordinatorLoadInProgress => {
                Some("The coordinator is loading and hence can't process requests.")
            }
            ErrorCode::CoordinatorNotAvailable => Some("The coordinator is not available."),
            ErrorCode::NotCoordinator => Some("This is not the correct coordinator."),
            ErrorCode::InvalidTopicException => {
                Some("The request attempted to perform an operation on an invalid topic.")
            }
            ErrorCode::RecordListTooLarge => Some(
                "The request included message batch larger than the configured segment size on the server.",
            ),
            ErrorCode::NotEnoughReplicas => {
                Some("Messages are rejected since there are fewer in-sync replicas than required.")
            }
            ErrorCode::NotEnoughReplicasAfterAppend => Some(
                "Messages are written to the log, but to fewer in-sync replicas than required.",
            ),
            ErrorCode::InvalidRequiredAcks => {
                Some("Produce request specified an invalid value for required acks.")
            }
            ErrorCode::IllegalGeneration => Some("Specified group generation id is not valid."),
            ErrorCode::InconsistentGroupProtocol => Some(
                "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.",
            ),
            ErrorCode::InvalidGroupId => Some("The configured groupId is invalid."),
            ErrorCode::UnknownMemberId => Some("The coordinator is not aware of this member."),
            ErrorCode::InvalidSessionTimeout => Some(
                "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).",
            ),
            ErrorCode::RebalanceInProgress => {
                Some("The group is rebalancing, so a rejoin is needed.")
            }
            ErrorCode::InvalidCommitOffsetSize => {
                Some("The committing offset data size is not valid.")
            }
            ErrorCode::TopicAuthorizationFailed => Some("Topic authorization failed."),
            ErrorCode::GroupAuthorizationFailed => Some("Group authorization failed."),
            ErrorCode::ClusterAuthorizationFailed => Some("Cluster authorization failed."),
            ErrorCode::InvalidTimestamp => {
                Some("The timestamp of the message is out of acceptable range.")
            }
            ErrorCode::UnsupportedSaslMechanism => {
                Some("The broker does not support the requested SASL mechanism.")
            }
            ErrorCode::IllegalSaslState => {
                Some("Request is not valid given the current SASL state.")
            }
            ErrorCode::UnsupportedVersion => Some("The version of API is not supported."),
            ErrorCode::TopicAlreadyExists => Some("Topic with this name already exists."),
            ErrorCode::InvalidPartitions => Some("Number of partitions is below 1."),
            ErrorCode::InvalidReplicationFactor => Some(
                "Replication factor is below 1 or larger than the number of available brokers.",
            ),
            ErrorCode::InvalidReplicaAssignment => Some("Replica assignment is invalid."),
            ErrorCode::InvalidConfig => Some("Configuration is invalid."),
            ErrorCode::NotController => {
                Some("This is not the correct controller for this cluster.")
            }
            ErrorCode::InvalidRequest => Some(
                "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.",
            ),
            ErrorCode::UnsupportedForMessageFormat => {
                Some("The message format version on the broker does not support the request.")
            }
            ErrorCode::PolicyViolation => {
                Some("Request parameters do not satisfy the configured policy.")
            }
            ErrorCode::OutOfOrderSequenceNumber => {
                Some("The broker received an out of order sequence number.")
            }
            ErrorCode::DuplicateSequenceNumber => {
                Some("The broker received a duplicate sequence number.")
            }
            ErrorCode::InvalidProducerEpoch => {
                Some("Producer attempted to produce with an old epoch.")
            }
            ErrorCode::InvalidTxnState => {
                Some("The producer attempted a transactional operation in an invalid state.")
            }
            ErrorCode::InvalidProducerIdMapping => Some(
                "The producer attempted to use a producer id which is not currently assigned to its transactional id.",
            ),
            ErrorCode::InvalidTransactionTimeout => Some(
                "The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).",
            ),
            ErrorCode::ConcurrentTransactions => Some(
                "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.",
            ),
            ErrorCode::TransactionCoordinatorFenced => Some(
                "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.",
            ),
            ErrorCode::TransactionalIdAuthorizationFailed => {
                Some("Transactional Id authorization failed.")
            }
            ErrorCode::SecurityDisabled => Some("Security features are disabled."),
            ErrorCode::OperationNotAttempted => Some(
                "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.",
            ),
            ErrorCode::KafkaStorageError => {
                Some("Disk error when trying to access log file on the disk.")
            }
            ErrorCode::LogDirNotFound => {
                Some("The user-specified log directory is not found in the broker config.")
            }
            ErrorCode::SaslAuthenticationFailed => Some("SASL Authentication failed."),
            ErrorCode::UnknownProducerId => Some(
                "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.",
            ),
            ErrorCode::ReassignmentInProgress => Some("A partition reassignment is in progress."),
            ErrorCode::DelegationTokenAuthDisabled => {
                Some("Delegation Token feature is not enabled.")
            }
            ErrorCode::DelegationTokenNotFound => Some("Delegation Token is not found on server."),
            ErrorCode::DelegationTokenOwnerMismatch => {
                Some("Specified Principal is not valid Owner/Renewer.")
            }
            ErrorCode::DelegationTokenRequestNotAllowed => Some(
                "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.",
            ),
            ErrorCode::DelegationTokenAuthorizationFailed => {
                Some("Delegation Token authorization failed.")
            }
            ErrorCode::DelegationTokenExpired => Some("Delegation Token is expired."),
            ErrorCode::InvalidPrincipalType => Some("Supplied principalType is not supported."),
            ErrorCode::NonEmptyGroup => Some("The group is not empty."),
            ErrorCode::GroupIdNotFound => Some("The group id does not exist."),
            ErrorCode::FetchSessionIdNotFound => Some("The fetch session ID was not found."),
            ErrorCode::InvalidFetchSessionEpoch => Some("The fetch session epoch is invalid."),
            ErrorCode::ListenerNotFound => Some(
                "There is no listener on the leader broker that matches the listener on which metadata request was processed.",
            ),
            ErrorCode::TopicDeletionDisabled => Some("Topic deletion is disabled."),
            ErrorCode::FencedLeaderEpoch => {
                Some("The leader epoch in the request is older than the epoch on the broker.")
            }
            ErrorCode::UnknownLeaderEpoch => {
                Some("The leader epoch in the request is newer than the epoch on the broker.")
            }
            ErrorCode::UnsupportedCompressionType => Some(
                "The requesting client does not support the compression type of given partition.",
            ),
            ErrorCode::StaleBrokerEpoch => Some("Broker epoch has changed."),
            ErrorCode::OffsetNotAvailable => Some(
                "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.",
            ),
            ErrorCode::MemberIdRequired => Some(
                "The group member needs to have a valid member id before actually entering a consumer group.",
            ),
            ErrorCode::PreferredLeaderNotAvailable => {
                Some("The preferred leader was not available.")
            }
            ErrorCode::GroupMaxSizeReached => Some("The consumer group has reached its max size."),
            ErrorCode::FencedInstanceId => Some(
                "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.",
            ),
            ErrorCode::EligibleLeadersNotAvailable => {
                Some("Eligible topic partition leaders are not available.")
            }
            ErrorCode::ElectionNotNeeded => Some("Leader election not needed for topic partition."),
            ErrorCode::NoReassignmentInProgress => {
                Some("No partition reassignment is in progress.")
            }
            ErrorCode::GroupSubscribedToTopic => Some(
                "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.",
            ),
            ErrorCode::InvalidRecord => {
                Some("This record has failed the validation on broker and hence will be rejected.")
            }
            ErrorCode::UnstableOffsetCommit => {
                Some("There are unstable offsets that need to be cleared.")
            }
            ErrorCode::ThrottlingQuotaExceeded => Some("The throttling quota has been exceeded."),
            ErrorCode::ProducerFenced => Some(
                "There is a newer producer with the same transactionalId which fences the current one.",
            ),
            ErrorCode::ResourceNotFound => {
                Some("A request illegally referred to a resource that does not exist.")
            }
            ErrorCode::DuplicateResource => {
                Some("A request illegally referred to the same resource twice.")
            }
            ErrorCode::UnacceptableCredential => {
                Some("Requested credential would not meet criteria for acceptability.")
            }
            ErrorCode::InconsistentVoterSet => Some(
                "Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters",
            ),
            ErrorCode::InvalidUpdateVersion => Some("The given update version was invalid."),
            ErrorCode::FeatureUpdateFailed => {
                Some("Unable to update finalized features due to an unexpected server error.")
            }
            ErrorCode::PrincipalDeserializationFailure => Some(
                "Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup.",
            ),
            ErrorCode::SnapshotNotFound => Some("Requested snapshot was not found"),
            ErrorCode::PositionOutOfRange => Some(
                "Requested position is not greater than or equal to zero, and less than the size of the snapshot.",
            ),
            ErrorCode::UnknownTopicId => Some("This server does not host this topic ID."),
            ErrorCode::DuplicateBrokerRegistration => Some("This broker ID is already in use."),
            ErrorCode::BrokerIdNotRegistered => Some("The given broker ID was not registered."),
            ErrorCode::InconsistentTopicId => {
                Some("The log's topic ID did not match the topic ID in the request")
            }
            ErrorCode::InconsistentClusterId => {
                Some("The clusterId in the request does not match that found on the server")
            }
            ErrorCode::TransactionalIdNotFound => Some("The transactionalId could not be found"),
            ErrorCode::FetchSessionTopicIdError => {
                Some("The fetch session encountered inconsistent topic ID usage")
            }
            ErrorCode::IneligibleReplica => {
                Some("The new ISR contains at least one ineligible replica.")
            }
            ErrorCode::NewLeaderElected => Some(
                "The AlterPartition request successfully updated the partition state but the leader has changed.",
            ),
            ErrorCode::OffsetMovedToTieredStorage => {
                Some("The requested offset is moved to tiered storage.")
            }
            ErrorCode::FencedMemberEpoch => Some(
                "The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.",
            ),
            ErrorCode::UnreleasedInstanceId => Some(
                "The instance ID is still used by another member in the consumer group. That member must leave first.",
            ),
            ErrorCode::UnsupportedAssignor => {
                Some("The assignor or its version range is not supported by the consumer group.")
            }
            ErrorCode::StaleMemberEpoch => Some(
                "The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.",
            ),
            ErrorCode::MismatchedEndpointType => {
                Some("The request was sent to an endpoint of the wrong type.")
            }
            ErrorCode::UnsupportedEndpointType => Some("This endpoint type is not supported yet."),
            ErrorCode::UnknownControllerId => Some("This controller ID is not known."),
            ErrorCode::UnknownSubscriptionId => Some(
                "Client sent a push telemetry request with an invalid or outdated subscription ID.",
            ),
            ErrorCode::TelemetryTooLarge => Some(
                "Client sent a push telemetry request larger than the maximum size the broker will accept.",
            ),
            ErrorCode::InvalidRegistration => {
                Some("The controller has considered the broker registration to be invalid.")
            }
            ErrorCode::TransactionAbortable => Some(
                "The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID.",
            ),
            ErrorCode::InvalidRecordState => Some(
                "The record state is invalid. The acknowledgement of delivery could not be completed.",
            ),
            ErrorCode::ShareSessionNotFound => Some("The share session was not found."),
            ErrorCode::InvalidShareSessionEpoch => Some("The share session epoch is invalid."),
            ErrorCode::FencedStateEpoch => Some(
                "The share coordinator rejected the request because the share-group state epoch did not match.",
            ),
            ErrorCode::InvalidVoterKey => {
                Some("The voter key doesn't match the receiving replica's key.")
            }
            ErrorCode::DuplicateVoter => Some("The voter is already part of the set of voters."),
            ErrorCode::VoterNotFound => Some("The voter is not part of the set of voters."),
            ErrorCode::InvalidRegularExpression => Some("The regular expression is not valid."),
            ErrorCode::RebootstrapRequired => {
                Some("Client metadata is stale, client should rebootstrap to obtain new metadata.")
            }
            ErrorCode::StreamsInvalidTopology => Some("The supplied topology is invalid."),
            ErrorCode::StreamsInvalidTopologyEpoch => {
                Some("The supplied topology epoch is invalid.")
            }
            ErrorCode::StreamsTopologyFenced => Some("The supplied topology epoch is outdated."),
            ErrorCode::ShareSessionLimitReached => {
                Some("The limit of share sessions has been reached.")
            }
            ErrorCode::Unknown(_) => None,
        }
    }

    /// Classify a wire code.
    ///
    /// `0` means success and has no `ErrorCode`; callers get `None` and should
    /// treat the response as good.
    pub fn from_code(code: i16) -> Option<Self> {
        ResponseError::try_from_code(code).map(Self::from_response_error)
    }

    /// Convert an upstream `ResponseError`.
    ///
    /// Exhaustive on purpose: see the module docs.
    fn from_response_error(err: ResponseError) -> Self {
        match err {
            ResponseError::UnknownServerError => ErrorCode::UnknownServerError,
            ResponseError::OffsetOutOfRange => ErrorCode::OffsetOutOfRange,
            ResponseError::CorruptMessage => ErrorCode::CorruptMessage,
            ResponseError::UnknownTopicOrPartition => ErrorCode::UnknownTopicOrPartition,
            ResponseError::InvalidFetchSize => ErrorCode::InvalidFetchSize,
            ResponseError::LeaderNotAvailable => ErrorCode::LeaderNotAvailable,
            ResponseError::NotLeaderOrFollower => ErrorCode::NotLeaderOrFollower,
            ResponseError::RequestTimedOut => ErrorCode::RequestTimedOut,
            ResponseError::BrokerNotAvailable => ErrorCode::BrokerNotAvailable,
            ResponseError::ReplicaNotAvailable => ErrorCode::ReplicaNotAvailable,
            ResponseError::MessageTooLarge => ErrorCode::MessageTooLarge,
            ResponseError::StaleControllerEpoch => ErrorCode::StaleControllerEpoch,
            ResponseError::OffsetMetadataTooLarge => ErrorCode::OffsetMetadataTooLarge,
            ResponseError::NetworkException => ErrorCode::NetworkException,
            ResponseError::CoordinatorLoadInProgress => ErrorCode::CoordinatorLoadInProgress,
            ResponseError::CoordinatorNotAvailable => ErrorCode::CoordinatorNotAvailable,
            ResponseError::NotCoordinator => ErrorCode::NotCoordinator,
            ResponseError::InvalidTopicException => ErrorCode::InvalidTopicException,
            ResponseError::RecordListTooLarge => ErrorCode::RecordListTooLarge,
            ResponseError::NotEnoughReplicas => ErrorCode::NotEnoughReplicas,
            ResponseError::NotEnoughReplicasAfterAppend => ErrorCode::NotEnoughReplicasAfterAppend,
            ResponseError::InvalidRequiredAcks => ErrorCode::InvalidRequiredAcks,
            ResponseError::IllegalGeneration => ErrorCode::IllegalGeneration,
            ResponseError::InconsistentGroupProtocol => ErrorCode::InconsistentGroupProtocol,
            ResponseError::InvalidGroupId => ErrorCode::InvalidGroupId,
            ResponseError::UnknownMemberId => ErrorCode::UnknownMemberId,
            ResponseError::InvalidSessionTimeout => ErrorCode::InvalidSessionTimeout,
            ResponseError::RebalanceInProgress => ErrorCode::RebalanceInProgress,
            ResponseError::InvalidCommitOffsetSize => ErrorCode::InvalidCommitOffsetSize,
            ResponseError::TopicAuthorizationFailed => ErrorCode::TopicAuthorizationFailed,
            ResponseError::GroupAuthorizationFailed => ErrorCode::GroupAuthorizationFailed,
            ResponseError::ClusterAuthorizationFailed => ErrorCode::ClusterAuthorizationFailed,
            ResponseError::InvalidTimestamp => ErrorCode::InvalidTimestamp,
            ResponseError::UnsupportedSaslMechanism => ErrorCode::UnsupportedSaslMechanism,
            ResponseError::IllegalSaslState => ErrorCode::IllegalSaslState,
            ResponseError::UnsupportedVersion => ErrorCode::UnsupportedVersion,
            ResponseError::TopicAlreadyExists => ErrorCode::TopicAlreadyExists,
            ResponseError::InvalidPartitions => ErrorCode::InvalidPartitions,
            ResponseError::InvalidReplicationFactor => ErrorCode::InvalidReplicationFactor,
            ResponseError::InvalidReplicaAssignment => ErrorCode::InvalidReplicaAssignment,
            ResponseError::InvalidConfig => ErrorCode::InvalidConfig,
            ResponseError::NotController => ErrorCode::NotController,
            ResponseError::InvalidRequest => ErrorCode::InvalidRequest,
            ResponseError::UnsupportedForMessageFormat => ErrorCode::UnsupportedForMessageFormat,
            ResponseError::PolicyViolation => ErrorCode::PolicyViolation,
            ResponseError::OutOfOrderSequenceNumber => ErrorCode::OutOfOrderSequenceNumber,
            ResponseError::DuplicateSequenceNumber => ErrorCode::DuplicateSequenceNumber,
            ResponseError::InvalidProducerEpoch => ErrorCode::InvalidProducerEpoch,
            ResponseError::InvalidTxnState => ErrorCode::InvalidTxnState,
            ResponseError::InvalidProducerIdMapping => ErrorCode::InvalidProducerIdMapping,
            ResponseError::InvalidTransactionTimeout => ErrorCode::InvalidTransactionTimeout,
            ResponseError::ConcurrentTransactions => ErrorCode::ConcurrentTransactions,
            ResponseError::TransactionCoordinatorFenced => ErrorCode::TransactionCoordinatorFenced,
            ResponseError::TransactionalIdAuthorizationFailed => {
                ErrorCode::TransactionalIdAuthorizationFailed
            }
            ResponseError::SecurityDisabled => ErrorCode::SecurityDisabled,
            ResponseError::OperationNotAttempted => ErrorCode::OperationNotAttempted,
            ResponseError::KafkaStorageError => ErrorCode::KafkaStorageError,
            ResponseError::LogDirNotFound => ErrorCode::LogDirNotFound,
            ResponseError::SaslAuthenticationFailed => ErrorCode::SaslAuthenticationFailed,
            ResponseError::UnknownProducerId => ErrorCode::UnknownProducerId,
            ResponseError::ReassignmentInProgress => ErrorCode::ReassignmentInProgress,
            ResponseError::DelegationTokenAuthDisabled => ErrorCode::DelegationTokenAuthDisabled,
            ResponseError::DelegationTokenNotFound => ErrorCode::DelegationTokenNotFound,
            ResponseError::DelegationTokenOwnerMismatch => ErrorCode::DelegationTokenOwnerMismatch,
            ResponseError::DelegationTokenRequestNotAllowed => {
                ErrorCode::DelegationTokenRequestNotAllowed
            }
            ResponseError::DelegationTokenAuthorizationFailed => {
                ErrorCode::DelegationTokenAuthorizationFailed
            }
            ResponseError::DelegationTokenExpired => ErrorCode::DelegationTokenExpired,
            ResponseError::InvalidPrincipalType => ErrorCode::InvalidPrincipalType,
            ResponseError::NonEmptyGroup => ErrorCode::NonEmptyGroup,
            ResponseError::GroupIdNotFound => ErrorCode::GroupIdNotFound,
            ResponseError::FetchSessionIdNotFound => ErrorCode::FetchSessionIdNotFound,
            ResponseError::InvalidFetchSessionEpoch => ErrorCode::InvalidFetchSessionEpoch,
            ResponseError::ListenerNotFound => ErrorCode::ListenerNotFound,
            ResponseError::TopicDeletionDisabled => ErrorCode::TopicDeletionDisabled,
            ResponseError::FencedLeaderEpoch => ErrorCode::FencedLeaderEpoch,
            ResponseError::UnknownLeaderEpoch => ErrorCode::UnknownLeaderEpoch,
            ResponseError::UnsupportedCompressionType => ErrorCode::UnsupportedCompressionType,
            ResponseError::StaleBrokerEpoch => ErrorCode::StaleBrokerEpoch,
            ResponseError::OffsetNotAvailable => ErrorCode::OffsetNotAvailable,
            ResponseError::MemberIdRequired => ErrorCode::MemberIdRequired,
            ResponseError::PreferredLeaderNotAvailable => ErrorCode::PreferredLeaderNotAvailable,
            ResponseError::GroupMaxSizeReached => ErrorCode::GroupMaxSizeReached,
            ResponseError::FencedInstanceId => ErrorCode::FencedInstanceId,
            ResponseError::EligibleLeadersNotAvailable => ErrorCode::EligibleLeadersNotAvailable,
            ResponseError::ElectionNotNeeded => ErrorCode::ElectionNotNeeded,
            ResponseError::NoReassignmentInProgress => ErrorCode::NoReassignmentInProgress,
            ResponseError::GroupSubscribedToTopic => ErrorCode::GroupSubscribedToTopic,
            ResponseError::InvalidRecord => ErrorCode::InvalidRecord,
            ResponseError::UnstableOffsetCommit => ErrorCode::UnstableOffsetCommit,
            ResponseError::ThrottlingQuotaExceeded => ErrorCode::ThrottlingQuotaExceeded,
            ResponseError::ProducerFenced => ErrorCode::ProducerFenced,
            ResponseError::ResourceNotFound => ErrorCode::ResourceNotFound,
            ResponseError::DuplicateResource => ErrorCode::DuplicateResource,
            ResponseError::UnacceptableCredential => ErrorCode::UnacceptableCredential,
            ResponseError::InconsistentVoterSet => ErrorCode::InconsistentVoterSet,
            ResponseError::InvalidUpdateVersion => ErrorCode::InvalidUpdateVersion,
            ResponseError::FeatureUpdateFailed => ErrorCode::FeatureUpdateFailed,
            ResponseError::PrincipalDeserializationFailure => {
                ErrorCode::PrincipalDeserializationFailure
            }
            ResponseError::SnapshotNotFound => ErrorCode::SnapshotNotFound,
            ResponseError::PositionOutOfRange => ErrorCode::PositionOutOfRange,
            ResponseError::UnknownTopicId => ErrorCode::UnknownTopicId,
            ResponseError::DuplicateBrokerRegistration => ErrorCode::DuplicateBrokerRegistration,
            ResponseError::BrokerIdNotRegistered => ErrorCode::BrokerIdNotRegistered,
            ResponseError::InconsistentTopicId => ErrorCode::InconsistentTopicId,
            ResponseError::InconsistentClusterId => ErrorCode::InconsistentClusterId,
            ResponseError::TransactionalIdNotFound => ErrorCode::TransactionalIdNotFound,
            ResponseError::FetchSessionTopicIdError => ErrorCode::FetchSessionTopicIdError,
            ResponseError::IneligibleReplica => ErrorCode::IneligibleReplica,
            ResponseError::NewLeaderElected => ErrorCode::NewLeaderElected,
            ResponseError::OffsetMovedToTieredStorage => ErrorCode::OffsetMovedToTieredStorage,
            ResponseError::FencedMemberEpoch => ErrorCode::FencedMemberEpoch,
            ResponseError::UnreleasedInstanceId => ErrorCode::UnreleasedInstanceId,
            ResponseError::UnsupportedAssignor => ErrorCode::UnsupportedAssignor,
            ResponseError::StaleMemberEpoch => ErrorCode::StaleMemberEpoch,
            ResponseError::MismatchedEndpointType => ErrorCode::MismatchedEndpointType,
            ResponseError::UnsupportedEndpointType => ErrorCode::UnsupportedEndpointType,
            ResponseError::UnknownControllerId => ErrorCode::UnknownControllerId,
            ResponseError::UnknownSubscriptionId => ErrorCode::UnknownSubscriptionId,
            ResponseError::TelemetryTooLarge => ErrorCode::TelemetryTooLarge,
            ResponseError::InvalidRegistration => ErrorCode::InvalidRegistration,
            ResponseError::TransactionAbortable => ErrorCode::TransactionAbortable,
            ResponseError::InvalidRecordState => ErrorCode::InvalidRecordState,
            ResponseError::ShareSessionNotFound => ErrorCode::ShareSessionNotFound,
            ResponseError::InvalidShareSessionEpoch => ErrorCode::InvalidShareSessionEpoch,
            ResponseError::FencedStateEpoch => ErrorCode::FencedStateEpoch,
            ResponseError::InvalidVoterKey => ErrorCode::InvalidVoterKey,
            ResponseError::DuplicateVoter => ErrorCode::DuplicateVoter,
            ResponseError::VoterNotFound => ErrorCode::VoterNotFound,
            ResponseError::InvalidRegularExpression => ErrorCode::InvalidRegularExpression,
            ResponseError::RebootstrapRequired => ErrorCode::RebootstrapRequired,
            ResponseError::StreamsInvalidTopology => ErrorCode::StreamsInvalidTopology,
            ResponseError::StreamsInvalidTopologyEpoch => ErrorCode::StreamsInvalidTopologyEpoch,
            ResponseError::StreamsTopologyFenced => ErrorCode::StreamsTopologyFenced,
            ResponseError::ShareSessionLimitReached => ErrorCode::ShareSessionLimitReached,
            ResponseError::Unknown(code) => ErrorCode::Unknown(code),
        }
    }

    /// Whether the protocol considers this code worth retrying.
    ///
    /// Delegated to the crate rather than re-stated here. `Unknown` is not
    /// retriable, matching what every other Kafka client does with a code it
    /// cannot interpret.
    pub fn retriable(self) -> bool {
        match self {
            ErrorCode::Unknown(_) => false,
            named => ResponseError::try_from_code(named.code())
                .map(|e| e.is_retriable())
                .unwrap_or(false),
        }
    }

    /// Whether retrying is worthwhile when the request named a specific
    /// resource that the broker says does not exist.
    ///
    /// This exists because PLAN.md's M5 acceptance and the protocol disagree,
    /// and both are right about different things. Kafka calls
    /// `UNKNOWN_TOPIC_OR_PARTITION` *retriable*, and for a topic that is
    /// mid-creation or mid-propagation it genuinely is. For a describe of a
    /// topic a user typed into a search box it is not: the answer will be the
    /// same five times over, and retrying turns a typo into a spinner.
    ///
    /// So it is a separate axis rather than a correction to [`Self::retriable`].
    /// The protocol's answer stays the protocol's answer — derived, not
    /// overridden — and callers that named a resource ask this one instead.
    pub fn retriable_for_named_resource(self) -> bool {
        !matches!(
            self,
            ErrorCode::UnknownTopicOrPartition
                | ErrorCode::UnknownTopicId
                | ErrorCode::GroupIdNotFound
                | ErrorCode::TransactionalIdNotFound
                | ErrorCode::UnknownMemberId
                | ErrorCode::ResourceNotFound
                | ErrorCode::LogDirNotFound
        ) && self.retriable()
    }

    /// Whether seeing this code should invalidate the metadata snapshot.
    ///
    /// Retrying a `NOT_LEADER_OR_FOLLOWER` against the same stale leader is an
    /// infinite loop that presents as a flaky cluster, so this axis exists
    /// separately from `retriable`.
    pub const fn needs_metadata_refresh(self) -> bool {
        matches!(
            self,
            ErrorCode::UnknownTopicOrPartition
                | ErrorCode::LeaderNotAvailable
                | ErrorCode::NotLeaderOrFollower
                | ErrorCode::BrokerNotAvailable
                | ErrorCode::ReplicaNotAvailable
                | ErrorCode::NetworkException
                | ErrorCode::NotController
                | ErrorCode::KafkaStorageError
                | ErrorCode::ListenerNotFound
                | ErrorCode::FencedLeaderEpoch
                | ErrorCode::UnknownLeaderEpoch
                | ErrorCode::OffsetNotAvailable
                | ErrorCode::PreferredLeaderNotAvailable
                | ErrorCode::UnknownTopicId
                | ErrorCode::InconsistentTopicId
                | ErrorCode::FetchSessionTopicIdError
                | ErrorCode::NewLeaderElected
                | ErrorCode::RebootstrapRequired
        )
    }

    /// Whether seeing this code should invalidate a cached coordinator.
    ///
    /// Independent of the metadata axis: a group coordinator moving says
    /// nothing about partition leadership, and refreshing the wrong cache
    /// leaves the retry pointed at the same wrong broker.
    pub const fn needs_coordinator_refresh(self) -> bool {
        matches!(
            self,
            ErrorCode::CoordinatorLoadInProgress
                | ErrorCode::CoordinatorNotAvailable
                | ErrorCode::NotCoordinator
        )
    }

    /// Whether this code means the credentials were rejected.
    pub const fn is_authentication(self) -> bool {
        matches!(
            self,
            ErrorCode::UnsupportedSaslMechanism
                | ErrorCode::IllegalSaslState
                | ErrorCode::SaslAuthenticationFailed
        )
    }

    /// Whether this code means the principal lacked permission.
    pub const fn is_authorization(self) -> bool {
        matches!(
            self,
            ErrorCode::TopicAuthorizationFailed
                | ErrorCode::GroupAuthorizationFailed
                | ErrorCode::ClusterAuthorizationFailed
                | ErrorCode::TransactionalIdAuthorizationFailed
                | ErrorCode::DelegationTokenAuthorizationFailed
        )
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.name() {
            Some(name) => write!(f, "{name}({})", self.code()),
            None => write!(f, "UNKNOWN({})", self.code()),
        }
    }
}

/// Every code this build names, for table-driven tests and UI legends.
pub const KNOWN_ERROR_CODES: [ErrorCode; 134] = [
    ErrorCode::UnknownServerError,
    ErrorCode::OffsetOutOfRange,
    ErrorCode::CorruptMessage,
    ErrorCode::UnknownTopicOrPartition,
    ErrorCode::InvalidFetchSize,
    ErrorCode::LeaderNotAvailable,
    ErrorCode::NotLeaderOrFollower,
    ErrorCode::RequestTimedOut,
    ErrorCode::BrokerNotAvailable,
    ErrorCode::ReplicaNotAvailable,
    ErrorCode::MessageTooLarge,
    ErrorCode::StaleControllerEpoch,
    ErrorCode::OffsetMetadataTooLarge,
    ErrorCode::NetworkException,
    ErrorCode::CoordinatorLoadInProgress,
    ErrorCode::CoordinatorNotAvailable,
    ErrorCode::NotCoordinator,
    ErrorCode::InvalidTopicException,
    ErrorCode::RecordListTooLarge,
    ErrorCode::NotEnoughReplicas,
    ErrorCode::NotEnoughReplicasAfterAppend,
    ErrorCode::InvalidRequiredAcks,
    ErrorCode::IllegalGeneration,
    ErrorCode::InconsistentGroupProtocol,
    ErrorCode::InvalidGroupId,
    ErrorCode::UnknownMemberId,
    ErrorCode::InvalidSessionTimeout,
    ErrorCode::RebalanceInProgress,
    ErrorCode::InvalidCommitOffsetSize,
    ErrorCode::TopicAuthorizationFailed,
    ErrorCode::GroupAuthorizationFailed,
    ErrorCode::ClusterAuthorizationFailed,
    ErrorCode::InvalidTimestamp,
    ErrorCode::UnsupportedSaslMechanism,
    ErrorCode::IllegalSaslState,
    ErrorCode::UnsupportedVersion,
    ErrorCode::TopicAlreadyExists,
    ErrorCode::InvalidPartitions,
    ErrorCode::InvalidReplicationFactor,
    ErrorCode::InvalidReplicaAssignment,
    ErrorCode::InvalidConfig,
    ErrorCode::NotController,
    ErrorCode::InvalidRequest,
    ErrorCode::UnsupportedForMessageFormat,
    ErrorCode::PolicyViolation,
    ErrorCode::OutOfOrderSequenceNumber,
    ErrorCode::DuplicateSequenceNumber,
    ErrorCode::InvalidProducerEpoch,
    ErrorCode::InvalidTxnState,
    ErrorCode::InvalidProducerIdMapping,
    ErrorCode::InvalidTransactionTimeout,
    ErrorCode::ConcurrentTransactions,
    ErrorCode::TransactionCoordinatorFenced,
    ErrorCode::TransactionalIdAuthorizationFailed,
    ErrorCode::SecurityDisabled,
    ErrorCode::OperationNotAttempted,
    ErrorCode::KafkaStorageError,
    ErrorCode::LogDirNotFound,
    ErrorCode::SaslAuthenticationFailed,
    ErrorCode::UnknownProducerId,
    ErrorCode::ReassignmentInProgress,
    ErrorCode::DelegationTokenAuthDisabled,
    ErrorCode::DelegationTokenNotFound,
    ErrorCode::DelegationTokenOwnerMismatch,
    ErrorCode::DelegationTokenRequestNotAllowed,
    ErrorCode::DelegationTokenAuthorizationFailed,
    ErrorCode::DelegationTokenExpired,
    ErrorCode::InvalidPrincipalType,
    ErrorCode::NonEmptyGroup,
    ErrorCode::GroupIdNotFound,
    ErrorCode::FetchSessionIdNotFound,
    ErrorCode::InvalidFetchSessionEpoch,
    ErrorCode::ListenerNotFound,
    ErrorCode::TopicDeletionDisabled,
    ErrorCode::FencedLeaderEpoch,
    ErrorCode::UnknownLeaderEpoch,
    ErrorCode::UnsupportedCompressionType,
    ErrorCode::StaleBrokerEpoch,
    ErrorCode::OffsetNotAvailable,
    ErrorCode::MemberIdRequired,
    ErrorCode::PreferredLeaderNotAvailable,
    ErrorCode::GroupMaxSizeReached,
    ErrorCode::FencedInstanceId,
    ErrorCode::EligibleLeadersNotAvailable,
    ErrorCode::ElectionNotNeeded,
    ErrorCode::NoReassignmentInProgress,
    ErrorCode::GroupSubscribedToTopic,
    ErrorCode::InvalidRecord,
    ErrorCode::UnstableOffsetCommit,
    ErrorCode::ThrottlingQuotaExceeded,
    ErrorCode::ProducerFenced,
    ErrorCode::ResourceNotFound,
    ErrorCode::DuplicateResource,
    ErrorCode::UnacceptableCredential,
    ErrorCode::InconsistentVoterSet,
    ErrorCode::InvalidUpdateVersion,
    ErrorCode::FeatureUpdateFailed,
    ErrorCode::PrincipalDeserializationFailure,
    ErrorCode::SnapshotNotFound,
    ErrorCode::PositionOutOfRange,
    ErrorCode::UnknownTopicId,
    ErrorCode::DuplicateBrokerRegistration,
    ErrorCode::BrokerIdNotRegistered,
    ErrorCode::InconsistentTopicId,
    ErrorCode::InconsistentClusterId,
    ErrorCode::TransactionalIdNotFound,
    ErrorCode::FetchSessionTopicIdError,
    ErrorCode::IneligibleReplica,
    ErrorCode::NewLeaderElected,
    ErrorCode::OffsetMovedToTieredStorage,
    ErrorCode::FencedMemberEpoch,
    ErrorCode::UnreleasedInstanceId,
    ErrorCode::UnsupportedAssignor,
    ErrorCode::StaleMemberEpoch,
    ErrorCode::MismatchedEndpointType,
    ErrorCode::UnsupportedEndpointType,
    ErrorCode::UnknownControllerId,
    ErrorCode::UnknownSubscriptionId,
    ErrorCode::TelemetryTooLarge,
    ErrorCode::InvalidRegistration,
    ErrorCode::TransactionAbortable,
    ErrorCode::InvalidRecordState,
    ErrorCode::ShareSessionNotFound,
    ErrorCode::InvalidShareSessionEpoch,
    ErrorCode::FencedStateEpoch,
    ErrorCode::InvalidVoterKey,
    ErrorCode::DuplicateVoter,
    ErrorCode::VoterNotFound,
    ErrorCode::InvalidRegularExpression,
    ErrorCode::RebootstrapRequired,
    ErrorCode::StreamsInvalidTopology,
    ErrorCode::StreamsInvalidTopologyEpoch,
    ErrorCode::StreamsTopologyFenced,
    ErrorCode::ShareSessionLimitReached,
];
