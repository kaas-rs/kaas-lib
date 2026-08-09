//! Owned admin domain types.
//!
//! Rule 4 lives here too: multi-resource calls return [`PerItem`], never
//! `Result<Vec<_>>`. Describing 500 topics where three are mid-deletion has to
//! return 497 answers and three errors, because the alternative — one global
//! error — makes a UI unusable on exactly the clusters that need one.

use kafka_conn::ErrorCode;

/// A per-resource result set.
///
/// The shape rule 4 mandates. Keyed by whatever identifies the resource, so a
/// caller can correlate answers back to what it asked for even when the broker
/// reorders them.
pub type PerItem<K, T> = Vec<(K, Result<T, kafka_conn::Error>)>;

/// Collect the successful half of a [`PerItem`].
pub fn oks<K, T>(items: &PerItem<K, T>) -> impl Iterator<Item = (&K, &T)> {
    items
        .iter()
        .filter_map(|(key, value)| value.as_ref().ok().map(|v| (key, v)))
}

/// Collect the failed half of a [`PerItem`].
pub fn errs<K, T>(items: &PerItem<K, T>) -> impl Iterator<Item = (&K, &kafka_conn::Error)> {
    items
        .iter()
        .filter_map(|(key, value)| value.as_ref().err().map(|e| (key, e)))
}

/// What a config belongs to.
///
/// **Not the same enum as [`AclResourceType`].** Kafka has two independent
/// resource-type numberings and they disagree on almost every value: a config
/// resource type of 4 is `BROKER`, an ACL resource type of 4 is `CLUSTER`.
/// Sharing one enum between them is a bug that type-checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConfigResourceType {
    /// A topic's configuration.
    Topic,
    /// A broker's configuration.
    Broker,
    /// A broker's log4j levels.
    BrokerLogger,
    /// A client-metrics subscription (KIP-714).
    ClientMetrics,
    /// A group's configuration (KIP-1071 and friends).
    Group,
}

impl ConfigResourceType {
    /// The wire value.
    pub const fn code(self) -> i8 {
        match self {
            ConfigResourceType::Topic => 2,
            ConfigResourceType::Broker => 4,
            ConfigResourceType::BrokerLogger => 8,
            ConfigResourceType::ClientMetrics => 16,
            ConfigResourceType::Group => 32,
        }
    }

    /// From a wire value.
    pub const fn from_code(code: i8) -> Option<Self> {
        match code {
            2 => Some(ConfigResourceType::Topic),
            4 => Some(ConfigResourceType::Broker),
            8 => Some(ConfigResourceType::BrokerLogger),
            16 => Some(ConfigResourceType::ClientMetrics),
            32 => Some(ConfigResourceType::Group),
            _ => None,
        }
    }
}

/// A configurable resource.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConfigResource {
    /// What kind of thing.
    pub resource_type: ConfigResourceType,
    /// Its name — a topic name, or a broker id as a string.
    pub name: String,
}

impl ConfigResource {
    /// A topic's configuration.
    pub fn topic(name: impl Into<String>) -> Self {
        Self {
            resource_type: ConfigResourceType::Topic,
            name: name.into(),
        }
    }

    /// A broker's configuration.
    pub fn broker(node_id: i32) -> Self {
        Self {
            resource_type: ConfigResourceType::Broker,
            name: node_id.to_string(),
        }
    }

    /// A group's configuration.
    pub fn group(name: impl Into<String>) -> Self {
        Self {
            resource_type: ConfigResourceType::Group,
            name: name.into(),
        }
    }
}

/// Where a config value came from.
///
/// A UI needs this to tell "someone set this" from "this is the default",
/// which is the difference between a config worth showing and noise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigSource {
    /// Set on the topic.
    TopicConfig,
    /// Set dynamically on this broker.
    DynamicBrokerConfig,
    /// Set dynamically as the cluster-wide broker default.
    DynamicDefaultBrokerConfig,
    /// From the broker's properties file.
    StaticBrokerConfig,
    /// Kafka's built-in default.
    DefaultConfig,
    /// A dynamic log4j level.
    DynamicBrokerLoggerConfig,
    /// A client-metrics subscription config.
    ClientMetricsConfig,
    /// A group config.
    GroupConfig,
    /// A source this build does not name.
    Unknown(i8),
}

impl ConfigSource {
    /// From a wire value.
    pub const fn from_code(code: i8) -> Self {
        match code {
            1 => ConfigSource::TopicConfig,
            2 => ConfigSource::DynamicBrokerConfig,
            3 => ConfigSource::DynamicDefaultBrokerConfig,
            4 => ConfigSource::StaticBrokerConfig,
            5 => ConfigSource::DefaultConfig,
            6 => ConfigSource::DynamicBrokerLoggerConfig,
            7 => ConfigSource::ClientMetricsConfig,
            8 => ConfigSource::GroupConfig,
            other => ConfigSource::Unknown(other),
        }
    }

    /// Whether this value was explicitly set rather than inherited.
    pub const fn is_explicit(self) -> bool {
        !matches!(self, ConfigSource::DefaultConfig | ConfigSource::Unknown(_))
    }
}

/// One configuration entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    /// Config key.
    pub name: String,
    /// Current value, or `None` when the broker redacts it.
    pub value: Option<String>,
    /// Where the value came from.
    pub source: ConfigSource,
    /// Whether the broker considers this sensitive. Sensitive values arrive as
    /// `None`, and rendering "null" for a password is worse than saying so.
    pub is_sensitive: bool,
    /// Whether the value can be changed at runtime.
    pub read_only: bool,
    /// The broker's own documentation, when asked for.
    pub documentation: Option<String>,
}

/// How to change one config key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterOp {
    /// Replace the value.
    Set,
    /// Remove the override, reverting to the default.
    Delete,
    /// Append to a list-valued config.
    Append,
    /// Remove entries from a list-valued config.
    Subtract,
}

impl AlterOp {
    /// The wire value.
    pub const fn code(self) -> i8 {
        match self {
            AlterOp::Set => 0,
            AlterOp::Delete => 1,
            AlterOp::Append => 2,
            AlterOp::Subtract => 3,
        }
    }
}

/// One incremental config change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigChange {
    /// Config key.
    pub name: String,
    /// The operation.
    pub op: AlterOp,
    /// The value, which `Delete` ignores.
    pub value: Option<String>,
}

impl ConfigChange {
    /// Set a key.
    pub fn set(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op: AlterOp::Set,
            value: Some(value.into()),
        }
    }

    /// Remove an override.
    pub fn delete(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op: AlterOp::Delete,
            value: None,
        }
    }
}

/// A topic to create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTopic {
    /// Topic name.
    pub name: String,
    /// Partition count, or `None` to take the cluster default.
    pub partitions: Option<i32>,
    /// Replication factor, or `None` to take the cluster default.
    pub replication_factor: Option<i16>,
    /// Explicit replica assignment, partition index to broker ids.
    ///
    /// Mutually exclusive with `partitions`/`replication_factor`: the broker
    /// rejects a request that sets both, so the builders keep them apart.
    pub assignments: Vec<(i32, Vec<i32>)>,
    /// Topic configs to set at creation.
    pub configs: Vec<(String, String)>,
}

impl NewTopic {
    /// A topic with an explicit partition count and replication factor.
    pub fn new(name: impl Into<String>, partitions: i32, replication_factor: i16) -> Self {
        Self {
            name: name.into(),
            partitions: Some(partitions),
            replication_factor: Some(replication_factor),
            assignments: Vec::new(),
            configs: Vec::new(),
        }
    }

    /// A topic whose replicas are placed explicitly.
    pub fn with_assignments(name: impl Into<String>, assignments: Vec<(i32, Vec<i32>)>) -> Self {
        Self {
            name: name.into(),
            partitions: None,
            replication_factor: None,
            assignments,
            configs: Vec::new(),
        }
    }

    /// Add a topic config.
    #[must_use]
    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.push((key.into(), value.into()));
        self
    }
}

/// A topic that was created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedTopic {
    /// Topic name.
    pub name: String,
    /// Partitions the broker actually created.
    pub partitions: i32,
    /// Replication factor the broker actually used.
    pub replication_factor: i16,
}

/// Which offset to ask `ListOffsets` for.
///
/// **Six sentinels, not two.** A UI that only knows `-1` and `-2` reports the
/// wrong retention on any tiered-storage cluster, because the local log and the
/// tiered log have different earliest offsets and only one of them is `-2`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetSpec {
    /// `-1`: the high watermark.
    Latest,
    /// `-2`: the first offset still retained.
    Earliest,
    /// `-3` (KIP-734): the offset of the record with the largest timestamp.
    /// Not the same as `Latest` when producers write out of order.
    MaxTimestamp,
    /// `-4` (KIP-405): the earliest offset held on the broker's *local* disk,
    /// which on a tiered topic is far ahead of [`OffsetSpec::Earliest`].
    EarliestLocalTimestamp,
    /// `-5` (KIP-1005): the latest offset that has been tiered.
    LatestTieredTimestamp,
    /// `-6` (KIP-1023): the earliest offset pending upload to tiered storage.
    ///
    /// **Unreachable in this build.** It needs `ListOffsets` v11 and
    /// `kafka-protocol` 0.17 caps at v10, so [`OffsetSpec::min_version`]
    /// reports 11 and requesting it yields an error naming the gap. Modelled
    /// rather than omitted, because a silently missing sentinel is
    /// indistinguishable from one that returns nothing.
    EarliestPendingUploadTimestamp,
    /// A wall-clock timestamp in milliseconds: the first offset at or after it.
    Timestamp(i64),
}

impl OffsetSpec {
    /// The value that goes in the request's `timestamp` field.
    pub const fn timestamp(self) -> i64 {
        match self {
            OffsetSpec::Latest => -1,
            OffsetSpec::Earliest => -2,
            OffsetSpec::MaxTimestamp => -3,
            OffsetSpec::EarliestLocalTimestamp => -4,
            OffsetSpec::LatestTieredTimestamp => -5,
            OffsetSpec::EarliestPendingUploadTimestamp => -6,
            OffsetSpec::Timestamp(ms) => ms,
        }
    }

    /// The lowest `ListOffsets` version that understands this sentinel.
    pub const fn min_version(self) -> i16 {
        match self {
            OffsetSpec::MaxTimestamp => 7,
            OffsetSpec::EarliestLocalTimestamp => 8,
            OffsetSpec::LatestTieredTimestamp => 9,
            OffsetSpec::EarliestPendingUploadTimestamp => 11,
            _ => 1,
        }
    }

    /// Every sentinel this build can actually send.
    pub const REACHABLE: [OffsetSpec; 5] = [
        OffsetSpec::Latest,
        OffsetSpec::Earliest,
        OffsetSpec::MaxTimestamp,
        OffsetSpec::EarliestLocalTimestamp,
        OffsetSpec::LatestTieredTimestamp,
    ];
}

/// One partition's answer to `ListOffsets`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListedOffset {
    /// Partition index.
    pub partition: i32,
    /// The offset, or `None` when the partition has none matching.
    pub offset: Option<i64>,
    /// The timestamp of the record at that offset, when the broker reports one.
    pub timestamp: Option<i64>,
    /// Leader epoch at that offset, for fencing.
    pub leader_epoch: Option<i32>,
}

/// One log directory on one broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDir {
    /// Path on the broker.
    pub path: String,
    /// Total bytes on the volume, when the broker reports it.
    pub total_bytes: Option<i64>,
    /// Usable bytes on the volume, when the broker reports it.
    pub usable_bytes: Option<i64>,
    /// Per-replica sizes.
    pub replicas: Vec<LogDirReplica>,
    /// A directory-level error — an offline disk, most often.
    pub error: Option<ErrorCode>,
}

/// One replica's footprint in a log directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirReplica {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Bytes on disk for this replica.
    pub size_bytes: i64,
    /// How far behind the leader this replica is, in offsets.
    pub offset_lag: i64,
    /// Whether this is a future replica being moved between directories.
    pub is_future: bool,
}

/// A topic's size on disk.
///
/// Everything here comes out of one `DescribeLogDirs` fan-out joined against
/// the metadata snapshot, so the detail costs no extra round trip: it is the
/// same bytes, aggregated at four altitudes rather than one.
///
/// `#[non_exhaustive]` because that fan-out reports more than any one caller
/// wants and the field set has grown once already. Callers read these fields;
/// only this crate builds one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TopicSize {
    /// Topic name.
    pub topic: String,
    /// Logical size: one replica per partition, the number a user means when
    /// they ask how big a topic is.
    pub logical_bytes: i64,
    /// Physical size: every replica on every broker, which is what the disks
    /// actually hold.
    pub replicated_bytes: i64,
    /// Per-partition sizes, in partition-index order.
    ///
    /// One entry per partition *some broker reported a copy of*, which is not
    /// quite the same as one entry per partition: a partition whose every
    /// holder failed the describe is absent rather than zero.
    pub partitions: Vec<PartitionSize>,
    /// One row per log-directory entry, ordered by partition, then broker,
    /// then directory.
    ///
    /// `replicas.len()` is the entry count, and an entry is *one replica in
    /// one directory* — `DescribeLogDirs` does not report segment files at
    /// all, so a "segment count" taken from here is a replica count under
    /// another name. Worth knowing before labelling it in a UI.
    ///
    /// Future replicas appear here, flagged, rather than being dropped: a
    /// directory move in flight is worth showing, and it is deliberately
    /// invisible in every total above.
    pub replicas: Vec<ReplicaSize>,
}

/// One partition's share of a [`TopicSize`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct PartitionSize {
    /// Partition index.
    pub partition: i32,
    /// The leader's copy — this partition's share of
    /// [`TopicSize::logical_bytes`].
    ///
    /// Zero when no reported copy was the leader's: a leaderless partition, or
    /// a leader whose describe failed. Read it against `replicated_bytes`
    /// before rendering it as "empty".
    pub logical_bytes: i64,
    /// Every non-future copy summed — what the disks hold for this partition.
    ///
    /// The figure a "which partition is the big one" question wants, where
    /// `logical_bytes` answers "how big is this topic".
    pub replicated_bytes: i64,
}

/// One replica of one partition, in one log directory.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReplicaSize {
    /// The broker holding this copy.
    pub node_id: i32,
    /// Partition index.
    pub partition: i32,
    /// The directory the copy lives in.
    ///
    /// Part of the row's identity rather than decoration: on a JBOD broker the
    /// same partition appears twice — once per directory — while a copy moves
    /// between disks, and `(node_id, partition)` alone cannot tell those two
    /// rows apart.
    pub log_dir: String,
    /// Bytes on disk for this copy.
    pub size_bytes: i64,
    /// How far behind the leader this copy is, in offsets.
    pub offset_lag: i64,
    /// Whether this is a future replica — the destination of a directory move,
    /// not yet the live copy. Excluded from every total in [`TopicSize`].
    pub is_future: bool,
    /// Whether the broker holding this copy leads the partition, according to
    /// the metadata snapshot the sizes were joined against.
    ///
    /// Reported so a caller does not have to fetch metadata a second time to
    /// tell a logical byte from a replicated one.
    pub is_leader: bool,
}

/// Broker and cluster identity, from `DescribeCluster`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterDescription {
    /// The cluster id.
    pub cluster_id: String,
    /// The active controller, when the broker reports one.
    pub controller_id: Option<i32>,
    /// Every broker.
    pub brokers: Vec<ClusterBroker>,
}

/// One broker in a [`ClusterDescription`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterBroker {
    /// Broker id.
    pub node_id: i32,
    /// Advertised host.
    pub host: String,
    /// Advertised port.
    pub port: i32,
    /// Rack, when set.
    pub rack: Option<String>,
    /// Whether the controller has fenced this broker.
    pub is_fenced: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_and_acl_resource_numberings_are_different_and_stay_that_way() {
        // The trap: `4` means BROKER for a config and CLUSTER for an ACL. This
        // asserts the two enums have not been quietly unified.
        assert_eq!(ConfigResourceType::Broker.code(), 4);
        assert_eq!(ConfigResourceType::Topic.code(), 2);
        assert_eq!(
            ConfigResourceType::from_code(4),
            Some(ConfigResourceType::Broker)
        );
        assert_eq!(ConfigResourceType::from_code(3), None);
    }

    #[test]
    fn every_sentinel_has_its_documented_wire_value() {
        assert_eq!(OffsetSpec::Latest.timestamp(), -1);
        assert_eq!(OffsetSpec::Earliest.timestamp(), -2);
        assert_eq!(OffsetSpec::MaxTimestamp.timestamp(), -3);
        assert_eq!(OffsetSpec::EarliestLocalTimestamp.timestamp(), -4);
        assert_eq!(OffsetSpec::LatestTieredTimestamp.timestamp(), -5);
        assert_eq!(OffsetSpec::EarliestPendingUploadTimestamp.timestamp(), -6);
        assert_eq!(
            OffsetSpec::Timestamp(1_700_000_000_000).timestamp(),
            1_700_000_000_000
        );
    }

    #[test]
    fn the_kip_1023_sentinel_is_modelled_but_out_of_reach() {
        // kafka-protocol 0.17 caps ListOffsets at v10. Documenting the gap in
        // the type beats omitting the variant, which would make "we cannot ask"
        // indistinguishable from "the broker said no".
        assert_eq!(OffsetSpec::EarliestPendingUploadTimestamp.min_version(), 11);
        let our_max = kafka_conn::our_range(kafka_conn::ApiKey::ListOffsets)
            .map(|r| r.max)
            .unwrap_or(0);
        assert_eq!(
            our_max, 10,
            "if this changed, the -6 sentinel became reachable"
        );
        assert!(!OffsetSpec::REACHABLE.contains(&OffsetSpec::EarliestPendingUploadTimestamp));
    }

    #[test]
    fn the_five_reachable_sentinels_are_within_our_ceiling() {
        let our_max = kafka_conn::our_range(kafka_conn::ApiKey::ListOffsets)
            .map(|r| r.max)
            .unwrap_or(0);
        for spec in OffsetSpec::REACHABLE {
            assert!(spec.min_version() <= our_max, "{spec:?}");
        }
    }

    #[test]
    fn defaults_are_distinguishable_from_explicit_settings() {
        assert!(!ConfigSource::DefaultConfig.is_explicit());
        assert!(ConfigSource::TopicConfig.is_explicit());
        assert!(ConfigSource::DynamicBrokerConfig.is_explicit());
        assert!(!ConfigSource::from_code(99).is_explicit());
        assert_eq!(ConfigSource::from_code(99), ConfigSource::Unknown(99));
    }

    #[test]
    fn per_item_splits_into_successes_and_failures() {
        let items: PerItem<String, i32> = vec![
            ("a".to_owned(), Ok(1)),
            (
                "b".to_owned(),
                Err(kafka_conn::Error::from_code(
                    ErrorCode::UnknownTopicOrPartition,
                    None,
                )),
            ),
            ("c".to_owned(), Ok(3)),
        ];
        assert_eq!(oks(&items).count(), 2);
        assert_eq!(errs(&items).count(), 1);
    }
}
