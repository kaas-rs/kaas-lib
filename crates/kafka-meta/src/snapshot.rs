//! The owned metadata domain types.
//!
//! Every one of these is ours: rule 1 means no `kafka_protocol` type crosses
//! this crate's public API, and metadata is where the temptation is strongest
//! because `MetadataResponse` is *almost* the right shape. It is not quite —
//! `BrokerId` and `TopicName` are newtypes, `StrBytes` is not `String`, `Uuid`
//! comes from a crate we do not otherwise depend on, and all of it is
//! `#[non_exhaustive]` and regenerated on every Kafka release.
//!
//! Snapshots are immutable and shared behind an `ArcSwap`. Readers never block
//! and never see a half-updated cluster.

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant, SystemTime};

use kafka_conn::ErrorCode;

/// A Kafka topic id.
///
/// Ours rather than `uuid::Uuid`: it is a public field of [`TopicInfo`], and a
/// dependency's type in a public struct is a dependency's semver in our
/// semver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopicId([u8; 16]);

impl TopicId {
    /// All-zero, which is how the protocol spells "no id".
    pub const ZERO: TopicId = TopicId([0; 16]);

    /// Build from raw bytes.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// The raw bytes.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Whether this is the zero id.
    pub fn is_zero(&self) -> bool {
        self.0 == [0; 16]
    }
}

impl fmt::Display for TopicId {
    /// Canonical 8-4-4-4-12 hex, the way `kafka-topics.sh` prints it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, byte) in self.0.iter().enumerate() {
            if matches!(index, 4 | 6 | 8 | 10) {
                f.write_str("-")?;
            }
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// A broker's advertised endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerInfo {
    /// The broker id.
    pub node_id: i32,
    /// Advertised host.
    pub host: String,
    /// Advertised port.
    pub port: i32,
    /// Rack, when the broker declares one.
    pub rack: Option<String>,
}

impl BrokerInfo {
    /// `host:port`, as the connection layer wants it.
    ///
    /// An IPv6 literal is bracketed, because `::1:9092` parses as nothing —
    /// neither `TcpStream::connect` nor a TLS name check can read it — while
    /// `[::1]:9092` is the form every socket API agrees on.
    pub fn address(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

/// One partition of one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInfo {
    /// Partition index.
    pub partition: i32,
    /// Current leader, or `None` when there is no leader right now.
    ///
    /// `None` rather than `-1`: a UI that renders "leader -1" is a UI that
    /// forgot to check, and the type should not let it.
    pub leader: Option<i32>,
    /// Leader epoch, for fencing stale reads.
    pub leader_epoch: i32,
    /// The full replica set.
    pub replicas: Vec<i32>,
    /// In-sync replicas.
    pub isr: Vec<i32>,
    /// Replicas the leader considers offline.
    pub offline_replicas: Vec<i32>,
    /// A per-partition error, which does not invalidate the rest of the topic.
    pub error: Option<ErrorCode>,
}

impl PartitionInfo {
    /// Whether the partition has fewer in-sync replicas than replicas.
    pub fn under_replicated(&self) -> bool {
        self.isr.len() < self.replicas.len()
    }
}

/// One topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicInfo {
    /// Topic name.
    pub name: String,
    /// Topic id, zero on brokers or versions that do not report one.
    pub topic_id: TopicId,
    /// Whether Kafka considers this an internal topic.
    pub internal: bool,
    /// Partitions, in index order.
    pub partitions: Vec<PartitionInfo>,
    /// A topic-level error — `UNKNOWN_TOPIC_OR_PARTITION` for a name that does
    /// not exist, most often.
    pub error: Option<ErrorCode>,
}

impl TopicInfo {
    /// Look up one partition.
    pub fn partition(&self, index: i32) -> Option<&PartitionInfo> {
        self.partitions.iter().find(|p| p.partition == index)
    }
}

/// An immutable view of the cluster.
#[derive(Debug, Clone)]
pub struct MetadataSnapshot {
    brokers: Vec<BrokerInfo>,
    brokers_by_id: HashMap<i32, usize>,
    topics: Vec<TopicInfo>,
    topics_by_name: HashMap<String, usize>,
    controller_id: Option<i32>,
    cluster_id: Option<String>,
    fetched_at: SystemTime,
    fetched_instant: Instant,
}

impl MetadataSnapshot {
    /// Assemble a snapshot.
    pub fn new(
        brokers: Vec<BrokerInfo>,
        topics: Vec<TopicInfo>,
        controller_id: Option<i32>,
        cluster_id: Option<String>,
    ) -> Self {
        let brokers_by_id = brokers
            .iter()
            .enumerate()
            .map(|(index, broker)| (broker.node_id, index))
            .collect();
        let topics_by_name = topics
            .iter()
            .enumerate()
            .map(|(index, topic)| (topic.name.clone(), index))
            .collect();
        Self {
            brokers,
            brokers_by_id,
            topics,
            topics_by_name,
            controller_id,
            cluster_id,
            fetched_at: SystemTime::now(),
            fetched_instant: Instant::now(),
        }
    }

    /// An empty snapshot, for the moment before the first refresh lands.
    pub fn empty() -> Self {
        Self::new(Vec::new(), Vec::new(), None, None)
    }

    /// All known brokers.
    pub fn brokers(&self) -> &[BrokerInfo] {
        &self.brokers
    }

    /// One broker by id.
    pub fn broker(&self, node_id: i32) -> Option<&BrokerInfo> {
        self.brokers_by_id
            .get(&node_id)
            .and_then(|index| self.brokers.get(*index))
    }

    /// All known topics.
    pub fn topics(&self) -> &[TopicInfo] {
        &self.topics
    }

    /// One topic by name.
    pub fn topic(&self, name: &str) -> Option<&TopicInfo> {
        self.topics_by_name
            .get(name)
            .and_then(|index| self.topics.get(*index))
    }

    /// The active controller, when the broker told us.
    pub fn controller_id(&self) -> Option<i32> {
        self.controller_id
    }

    /// The cluster id.
    pub fn cluster_id(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }

    /// Wall-clock time this snapshot was fetched.
    ///
    /// A UI renders staleness; without this it can only render "now", which is
    /// a lie whenever the refresh loop is stuck.
    pub fn fetched_at(&self) -> SystemTime {
        self.fetched_at
    }

    /// How long ago this snapshot was fetched, on a monotonic clock.
    pub fn age(&self) -> Duration {
        self.fetched_instant.elapsed()
    }

    /// The leader of one partition.
    pub fn leader_for(&self, topic: &str, partition: i32) -> Option<i32> {
        self.topic(topic)?.partition(partition)?.leader
    }

    /// Merge newer topic entries into this snapshot, keeping everything else.
    ///
    /// A targeted refresh asks about a handful of topics; discarding the rest
    /// of the cache because of that would make every scan re-fetch the world.
    pub fn with_topics_merged(&self, updated: Vec<TopicInfo>) -> Self {
        let mut topics = self.topics.clone();
        for topic in updated {
            match self.topics_by_name.get(&topic.name) {
                Some(index) => {
                    if let Some(slot) = topics.get_mut(*index) {
                        *slot = topic;
                    }
                }
                None => topics.push(topic),
            }
        }
        Self::new(
            self.brokers.clone(),
            topics,
            self.controller_id,
            self.cluster_id.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker(id: i32) -> BrokerInfo {
        BrokerInfo {
            node_id: id,
            host: format!("broker-{id}"),
            port: 9092,
            rack: None,
        }
    }

    /// #34: `::1:9092` parses as nothing anywhere in the socket APIs; an IPv6
    /// host has to leave here bracketed or the pool can never dial it.
    #[test]
    fn an_ipv6_host_is_bracketed_in_the_address() {
        let mut b = broker(1);
        b.host = "2001:db8::2".to_owned();
        assert_eq!(b.address(), "[2001:db8::2]:9092");
        assert_eq!(broker(2).address(), "broker-2:9092");
    }

    fn topic(name: &str, leaders: &[i32]) -> TopicInfo {
        TopicInfo {
            name: name.to_owned(),
            topic_id: TopicId::ZERO,
            internal: false,
            partitions: leaders
                .iter()
                .enumerate()
                .map(|(index, leader)| PartitionInfo {
                    partition: i32::try_from(index).unwrap_or(0),
                    leader: Some(*leader),
                    leader_epoch: 0,
                    replicas: vec![*leader],
                    isr: vec![*leader],
                    offline_replicas: Vec::new(),
                    error: None,
                })
                .collect(),
            error: None,
        }
    }

    #[test]
    fn lookups_are_by_index_not_by_scan() {
        let snapshot = MetadataSnapshot::new(
            vec![broker(1), broker(2)],
            vec![topic("orders", &[1, 2])],
            Some(1),
            Some("cluster".to_owned()),
        );
        assert_eq!(
            snapshot.broker(2).map(|b| b.address()),
            Some("broker-2:9092".to_owned())
        );
        assert!(snapshot.broker(99).is_none());
        assert_eq!(snapshot.leader_for("orders", 1), Some(2));
        assert_eq!(snapshot.leader_for("orders", 7), None);
        assert_eq!(snapshot.leader_for("nope", 0), None);
    }

    #[test]
    fn a_targeted_refresh_keeps_the_topics_it_did_not_ask_about() {
        let snapshot = MetadataSnapshot::new(
            vec![broker(1)],
            vec![topic("orders", &[1]), topic("events", &[1])],
            Some(1),
            None,
        );
        let merged = snapshot.with_topics_merged(vec![topic("orders", &[1, 1, 1])]);
        assert_eq!(merged.topics().len(), 2);
        assert_eq!(merged.topic("orders").map(|t| t.partitions.len()), Some(3));
        assert!(merged.topic("events").is_some());
    }

    #[test]
    fn a_targeted_refresh_can_add_a_topic() {
        let snapshot = MetadataSnapshot::new(vec![broker(1)], vec![], None, None);
        let merged = snapshot.with_topics_merged(vec![topic("new", &[1])]);
        assert!(merged.topic("new").is_some());
    }

    #[test]
    fn a_missing_leader_is_none_not_minus_one() {
        // The protocol spells "no leader" as -1. Letting that reach a UI is how
        // you get a broker detail page for node -1.
        let mut info = topic("orders", &[1]);
        if let Some(p) = info.partitions.get_mut(0) {
            p.leader = None;
        }
        let snapshot = MetadataSnapshot::new(vec![broker(1)], vec![info], None, None);
        assert_eq!(snapshot.leader_for("orders", 0), None);
    }

    #[test]
    fn topic_ids_render_as_uuids() {
        let id = TopicId::from_bytes([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
            0xcd, 0xef,
        ]);
        assert_eq!(id.to_string(), "01234567-89ab-cdef-0123-456789abcdef");
        assert!(TopicId::ZERO.is_zero());
        assert!(!id.is_zero());
    }

    #[test]
    fn under_replicated_is_a_comparison_not_a_guess() {
        let partition = PartitionInfo {
            partition: 0,
            leader: Some(1),
            leader_epoch: 0,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2],
            offline_replicas: vec![3],
            error: None,
        };
        assert!(partition.under_replicated());
    }
}
