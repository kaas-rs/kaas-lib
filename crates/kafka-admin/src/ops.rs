//! The mutating api keys each admin operation sends.
//!
//! What a caller hands [`Admin::scoped`](crate::Admin::scoped) to get a handle
//! that can perform one operation and nothing else, or
//! [`Admin::connect_with_mutations`](crate::Admin::connect_with_mutations) to
//! set a connection's ceiling. They live here, beside the methods, because
//! which key a method sends is this crate's business: a method that grows a
//! fallback to a second key changes its constant in the same commit, and a
//! caller that built its set from these never has to know.
//!
//! **Only the mutating keys.** Reads are never gated, so the metadata,
//! `FindCoordinator` and `DescribeGroups` requests an operation makes on the
//! way are not listed — `reset_offsets` describes the group to check it is
//! empty and needs only [`RESET_OFFSETS`] to commit. A `validate_*` method
//! sends the same key as the method it validates, with `validate_only` set,
//! and needs the same constant.

use kafka_conn::ApiKey;

/// [`Admin::create_topics`](crate::Admin::create_topics) and
/// [`validate_topics`](crate::Admin::validate_topics).
pub const CREATE_TOPICS: &[ApiKey] = &[ApiKey::CreateTopics];
/// [`Admin::delete_topics`](crate::Admin::delete_topics).
pub const DELETE_TOPICS: &[ApiKey] = &[ApiKey::DeleteTopics];
/// [`Admin::create_partitions`](crate::Admin::create_partitions).
pub const CREATE_PARTITIONS: &[ApiKey] = &[ApiKey::CreatePartitions];
/// [`Admin::delete_records`](crate::Admin::delete_records).
pub const DELETE_RECORDS: &[ApiKey] = &[ApiKey::DeleteRecords];
/// [`Admin::alter_configs`](crate::Admin::alter_configs) and
/// [`validate_config_changes`](crate::Admin::validate_config_changes).
pub const ALTER_CONFIGS: &[ApiKey] = &[ApiKey::IncrementalAlterConfigs];
/// [`Admin::reset_offsets`](crate::Admin::reset_offsets).
pub const RESET_OFFSETS: &[ApiKey] = &[ApiKey::OffsetCommit];
/// [`Admin::delete_offsets`](crate::Admin::delete_offsets).
pub const DELETE_OFFSETS: &[ApiKey] = &[ApiKey::OffsetDelete];
/// [`Admin::delete_groups`](crate::Admin::delete_groups).
pub const DELETE_GROUPS: &[ApiKey] = &[ApiKey::DeleteGroups];
/// [`Admin::create_acls`](crate::Admin::create_acls).
pub const CREATE_ACLS: &[ApiKey] = &[ApiKey::CreateAcls];
/// [`Admin::delete_acls`](crate::Admin::delete_acls).
pub const DELETE_ACLS: &[ApiKey] = &[ApiKey::DeleteAcls];
/// [`Admin::alter_client_quotas`](crate::Admin::alter_client_quotas).
pub const ALTER_CLIENT_QUOTAS: &[ApiKey] = &[ApiKey::AlterClientQuotas];
/// [`Admin::upsert_scram_credentials`](crate::Admin::upsert_scram_credentials)
/// and [`delete_scram_credentials`](crate::Admin::delete_scram_credentials).
pub const ALTER_SCRAM_CREDENTIALS: &[ApiKey] = &[ApiKey::AlterUserScramCredentials];
/// [`Admin::alter_partition_reassignments`](crate::Admin::alter_partition_reassignments).
pub const ALTER_PARTITION_REASSIGNMENTS: &[ApiKey] = &[ApiKey::AlterPartitionReassignments];
/// [`Admin::elect_leaders`](crate::Admin::elect_leaders).
pub const ELECT_LEADERS: &[ApiKey] = &[ApiKey::ElectLeaders];
/// [`Admin::create_delegation_token`](crate::Admin::create_delegation_token).
pub const CREATE_DELEGATION_TOKEN: &[ApiKey] = &[ApiKey::CreateDelegationToken];
/// [`Admin::renew_delegation_token`](crate::Admin::renew_delegation_token).
pub const RENEW_DELEGATION_TOKEN: &[ApiKey] = &[ApiKey::RenewDelegationToken];
/// [`Admin::expire_delegation_token`](crate::Admin::expire_delegation_token).
pub const EXPIRE_DELEGATION_TOKEN: &[ApiKey] = &[ApiKey::ExpireDelegationToken];

/// Every constant above, for tests that hold them to their claims.
pub const ALL: &[&[ApiKey]] = &[
    CREATE_TOPICS,
    DELETE_TOPICS,
    CREATE_PARTITIONS,
    DELETE_RECORDS,
    ALTER_CONFIGS,
    RESET_OFFSETS,
    DELETE_OFFSETS,
    DELETE_GROUPS,
    CREATE_ACLS,
    DELETE_ACLS,
    ALTER_CLIENT_QUOTAS,
    ALTER_SCRAM_CREDENTIALS,
    ALTER_PARTITION_REASSIGNMENTS,
    ELECT_LEADERS,
    CREATE_DELEGATION_TOKEN,
    RENEW_DELEGATION_TOKEN,
    EXPIRE_DELEGATION_TOKEN,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_constant_names_only_mutating_keys() {
        // A read in a constant would be harmless and misleading: reads are
        // never gated, so listing one suggests a scope without it would fail.
        for set in ALL {
            assert!(!set.is_empty());
            for key in *set {
                assert!(key.is_mutating(), "{key:?} does not mutate");
            }
        }
    }
}
