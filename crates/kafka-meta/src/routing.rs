//! Where each RPC has to go.
//!
//! Not every request can be sent to any broker, and the failure mode of
//! getting it wrong is not an error — it is a `NOT_CONTROLLER` or
//! `NOT_COORDINATOR` retry loop that presents as a flaky cluster. So this is a
//! first-class table in its own file, next to the error table, rather than a
//! decision scattered across call sites.
//!
//! CLAUDE.md names four classes. [`Routing::Specific`] carries the one
//! distinction that list glosses over: a broker the *caller* names
//! (`DescribeLogDirs` against a particular node) and a broker the *metadata
//! snapshot* names (a partition leader, for `Fetch`) are the same routing class
//! but need different resolution.
//!
//! The wildcard arm is [`Routing::Any`], and that is the safe default in a way
//! the read-only gate's wildcard is not: sending to the wrong broker at worst
//! costs a redirect, whereas mis-classifying a mutating API costs the property.

use kafka_conn::ApiKey;

/// Which coordinator a request belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoordinatorKind {
    /// Resolved per group id.
    Group,
    /// Resolved per transactional id.
    Transaction,
}

impl CoordinatorKind {
    /// The `key_type` byte `FindCoordinator` expects.
    pub const fn key_type(self) -> i8 {
        match self {
            CoordinatorKind::Group => 0,
            CoordinatorKind::Transaction => 1,
        }
    }
}

/// How a specific broker is chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerSelector {
    /// The caller names the broker id.
    Caller,
    /// The leader of the partition the request names.
    PartitionLeader,
}

/// Where a request has to be sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Routing {
    /// Any live broker will answer.
    Any,
    /// Only the active controller.
    Controller,
    /// The coordinator for a particular group or transactional id.
    Coordinator(CoordinatorKind),
    /// One particular broker.
    Specific(BrokerSelector),
}

/// The routing class for an api key.
pub const fn routing(api_key: ApiKey) -> Routing {
    match api_key {
        // Controller only. In KRaft a broker will forward most of these, but
        // "most" is doing a lot of work in that sentence and the forwarding
        // path has its own failure modes.
        ApiKey::CreateTopics
        | ApiKey::DeleteTopics
        | ApiKey::CreatePartitions
        | ApiKey::AlterPartitionReassignments
        | ApiKey::ListPartitionReassignments
        | ApiKey::ElectLeaders
        | ApiKey::UpdateFeatures => Routing::Controller,

        // Group coordinator, resolved per group id.
        ApiKey::OffsetCommit
        | ApiKey::OffsetFetch
        | ApiKey::OffsetDelete
        | ApiKey::JoinGroup
        | ApiKey::Heartbeat
        | ApiKey::LeaveGroup
        | ApiKey::SyncGroup
        | ApiKey::DescribeGroups
        | ApiKey::DeleteGroups
        | ApiKey::ConsumerGroupDescribe
        | ApiKey::ConsumerGroupHeartbeat
        | ApiKey::ShareGroupDescribe
        | ApiKey::ShareGroupHeartbeat
        | ApiKey::DescribeShareGroupOffsets
        | ApiKey::AlterShareGroupOffsets
        | ApiKey::DeleteShareGroupOffsets
        | ApiKey::TxnOffsetCommit => Routing::Coordinator(CoordinatorKind::Group),

        // Transaction coordinator, resolved per transactional id.
        ApiKey::InitProducerId
        | ApiKey::AddPartitionsToTxn
        | ApiKey::AddOffsetsToTxn
        | ApiKey::EndTxn
        | ApiKey::DescribeTransactions => Routing::Coordinator(CoordinatorKind::Transaction),

        // A broker the caller picks: these report that broker's own state and
        // answering from anywhere else would be answering a different question.
        ApiKey::DescribeLogDirs | ApiKey::AlterReplicaLogDirs | ApiKey::DescribeProducers => {
            Routing::Specific(BrokerSelector::Caller)
        }

        // The partition leader.
        ApiKey::Produce | ApiKey::Fetch | ApiKey::ListOffsets | ApiKey::OffsetForLeaderEpoch => {
            Routing::Specific(BrokerSelector::PartitionLeader)
        }

        // Everything else answers from any broker: metadata, describes, ACLs,
        // quotas, SCRAM credentials, ListGroups, ListTransactions.
        _ => Routing::Any,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_four_classes_from_claude_md() {
        // Controller only.
        for key in [
            ApiKey::CreateTopics,
            ApiKey::DeleteTopics,
            ApiKey::CreatePartitions,
            ApiKey::AlterPartitionReassignments,
            ApiKey::ElectLeaders,
            ApiKey::UpdateFeatures,
        ] {
            assert_eq!(routing(key), Routing::Controller, "{key}");
        }

        // Group/txn coordinator.
        assert_eq!(
            routing(ApiKey::OffsetFetch),
            Routing::Coordinator(CoordinatorKind::Group)
        );
        assert_eq!(
            routing(ApiKey::DescribeTransactions),
            Routing::Coordinator(CoordinatorKind::Transaction)
        );

        // One specific broker.
        assert_eq!(
            routing(ApiKey::DescribeLogDirs),
            Routing::Specific(BrokerSelector::Caller)
        );
        assert_eq!(
            routing(ApiKey::DescribeProducers),
            Routing::Specific(BrokerSelector::Caller)
        );

        // Any broker.
        for key in [
            ApiKey::DescribeConfigs,
            ApiKey::DescribeAcls,
            ApiKey::ListGroups,
            ApiKey::Metadata,
        ] {
            assert_eq!(routing(key), Routing::Any, "{key}");
        }
    }

    #[test]
    fn the_read_path_goes_to_the_leader() {
        for key in [ApiKey::Fetch, ApiKey::ListOffsets, ApiKey::Produce] {
            assert_eq!(
                routing(key),
                Routing::Specific(BrokerSelector::PartitionLeader),
                "{key}"
            );
        }
    }

    #[test]
    fn share_and_consumer_group_apis_route_like_classic_groups() {
        // KIP-848 and KIP-932 introduced new RPCs for the same resource. They
        // are coordinator-routed exactly as DescribeGroups is; treating them as
        // "any broker" because they are new is a NOT_COORDINATOR loop.
        for key in [
            ApiKey::ConsumerGroupDescribe,
            ApiKey::ShareGroupDescribe,
            ApiKey::DescribeShareGroupOffsets,
        ] {
            assert_eq!(
                routing(key),
                Routing::Coordinator(CoordinatorKind::Group),
                "{key}"
            );
        }
    }

    #[test]
    fn an_api_key_this_build_cannot_name_routes_anywhere() {
        // A streams-group RPC, say. "Any broker" is the right default: at worst
        // the broker redirects us.
        assert_eq!(routing(ApiKey::Unknown(9_999)), Routing::Any);
    }

    #[test]
    fn find_coordinator_key_types_match_the_protocol() {
        assert_eq!(CoordinatorKind::Group.key_type(), 0);
        assert_eq!(CoordinatorKind::Transaction.key_type(), 1);
    }
}
