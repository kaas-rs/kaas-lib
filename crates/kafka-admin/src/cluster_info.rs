//! `DescribeCluster`, `DescribeLogDirs`, and per-topic size.

use std::collections::HashMap;

use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::describe_log_dirs_request::DescribableLogDirTopic;
use kafka_conn::protocol::messages::{DescribeClusterRequest, DescribeLogDirsRequest, TopicName};
use kafka_conn::{Error, ErrorCode, Result};
use kafka_meta::TopicInfo;

use crate::Admin;
use crate::types::{
    ClusterBroker, ClusterDescription, LogDir, LogDirReplica, PartitionSize, PerItem, ReplicaSize,
    TopicSize,
};

/// Topic name -> partition -> leader node, from a metadata snapshot.
///
/// Nested rather than keyed by `(String, i32)` because the fold below looks a
/// leader up once per replica entry, and a tuple key means allocating the
/// topic name again on every one of those lookups.
type LeaderMap = HashMap<String, HashMap<i32, i32>>;

/// One topic's log-dir entries while the fold is in progress.
#[derive(Default)]
struct Sizes {
    partitions: HashMap<i32, PartitionSize>,
    replicas: Vec<ReplicaSize>,
}

impl Admin {
    /// Describe the cluster.
    ///
    /// More authoritative than a metadata snapshot for identity: it names the
    /// controller directly and reports fenced brokers, which `Metadata` omits
    /// entirely — a fenced broker simply disappears from a metadata response,
    /// so a UI built on metadata alone shows a shrinking cluster with no
    /// explanation.
    pub async fn describe_cluster(&self) -> Result<ClusterDescription> {
        let request = DescribeClusterRequest::default()
            .with_include_cluster_authorized_operations(false)
            .with_include_fenced_brokers(true);
        let response = self.cluster().send_any(request).await?;

        if let Some(code) = ErrorCode::from_code(response.error_code) {
            return Err(Error::from_code(
                code,
                response.error_message.map(|m| m.to_string()),
            ));
        }

        Ok(ClusterDescription {
            cluster_id: response.cluster_id.to_string(),
            controller_id: Some(response.controller_id.0).filter(|id| *id >= 0),
            brokers: response
                .brokers
                .into_iter()
                .map(|broker| ClusterBroker {
                    node_id: broker.broker_id.0,
                    host: broker.host.to_string(),
                    port: broker.port,
                    rack: broker.rack.map(|r| r.to_string()),
                    is_fenced: broker.is_fenced,
                })
                .collect(),
        })
    }

    /// Describe every log directory on one broker.
    ///
    /// Broker-specific by construction: a log directory belongs to a machine,
    /// and asking a different broker answers a different question.
    pub async fn describe_log_dirs(&self, node_id: i32) -> Result<Vec<LogDir>> {
        self.log_dirs_of(node_id, None).await
    }

    /// `DescribeLogDirs` against one broker, optionally scoped to one topic's
    /// partitions.
    ///
    /// The scope is a real saving, not tidiness: unscoped, a broker answers
    /// with every partition it holds, so a one-topic question on a large
    /// cluster decodes the whole cluster's log dirs once per broker.
    async fn log_dirs_of(
        &self,
        node_id: i32,
        scope: Option<(&str, &[i32])>,
    ) -> Result<Vec<LogDir>> {
        // `None` means "every partition", which is what a cluster-wide size
        // view wants.
        let topics = scope.map(|(topic, partitions)| {
            vec![
                DescribableLogDirTopic::default()
                    .with_topic(TopicName(StrBytes::from_string(topic.to_owned())))
                    .with_partitions(partitions.to_vec()),
            ]
        });
        let request = DescribeLogDirsRequest::default().with_topics(topics);
        let response = self.cluster().send_to_node(node_id, request).await?;

        if let Some(code) = ErrorCode::from_code(response.error_code) {
            return Err(Error::from_code(code, None));
        }

        Ok(response
            .results
            .into_iter()
            .map(|result| LogDir {
                path: result.log_dir.to_string(),
                // -1 is "the broker did not measure this", which is not the
                // same as a zero-byte volume.
                total_bytes: Some(result.total_bytes).filter(|b| *b >= 0),
                usable_bytes: Some(result.usable_bytes).filter(|b| *b >= 0),
                replicas: result
                    .topics
                    .into_iter()
                    .flat_map(|topic| {
                        let name = topic.name.0.to_string();
                        topic
                            .partitions
                            .into_iter()
                            .map(move |partition| LogDirReplica {
                                topic: name.clone(),
                                partition: partition.partition_index,
                                size_bytes: partition.partition_size,
                                offset_lag: partition.offset_lag,
                                is_future: partition.is_future_key,
                            })
                    })
                    .collect(),
                error: ErrorCode::from_code(result.error_code),
            })
            .collect())
    }

    /// Describe log directories on every broker.
    pub async fn describe_all_log_dirs(&self) -> Result<PerItem<i32, Vec<LogDir>>> {
        self.all_log_dirs(None).await
    }

    /// The fan-out both size calls need.
    ///
    /// Unavoidably one call per broker whatever the scope: a log directory
    /// belongs to a machine, and a broker answers only for the copies it
    /// holds.
    async fn all_log_dirs(
        &self,
        scope: Option<(&str, &[i32])>,
    ) -> Result<PerItem<i32, Vec<LogDir>>> {
        let snapshot = self.cluster().refresh_if_stale().await?;
        let mut results: PerItem<i32, Vec<LogDir>> = Vec::new();
        for broker in snapshot.brokers() {
            let outcome = self.log_dirs_of(broker.node_id, scope).await;
            results.push((broker.node_id, outcome));
        }
        Ok(results)
    }

    /// Per-topic size on disk.
    ///
    /// Joins `DescribeLogDirs` across every broker with the metadata snapshot's
    /// leader assignment.
    ///
    /// **Replicas are not double counted.** Every broker reports the bytes it
    /// holds, so summing them for an RF=3 topic gives three times the size a
    /// user means when they ask how big it is. The logical figure counts each
    /// partition once, at its *leader*, and the replicated figure — the bytes
    /// the disks actually hold — is reported separately rather than instead.
    ///
    /// A broker whose describe fails is skipped, so its copies are missing
    /// from the totals; the answer is still `Ok` because the other brokers
    /// answered. Call [`describe_all_log_dirs`](Self::describe_all_log_dirs)
    /// directly when it matters which broker did not.
    ///
    /// [`topic_size`](Self::topic_size) is the same join for one topic,
    /// without the cluster-sized metadata fetch.
    pub async fn topic_sizes(&self) -> Result<PerItem<String, TopicSize>> {
        let snapshot = self.cluster().refresh().await?;
        let per_broker = self.describe_all_log_dirs().await?;

        let leaders = leader_map(snapshot.topics());
        let mut folded = fold(
            per_broker
                .iter()
                .filter_map(|(node_id, dirs)| Some((*node_id, dirs.as_ref().ok()?.as_slice()))),
            &leaders,
        );

        Ok(snapshot
            .topics()
            .iter()
            .map(|topic| {
                let size = folded
                    .remove(&topic.name)
                    .unwrap_or_default()
                    .finish(topic.name.clone());
                (topic.name.clone(), Ok(size))
            })
            .collect())
    }

    /// One topic's size on disk.
    ///
    /// The same join as [`topic_sizes`](Self::topic_sizes), scoped at both
    /// ends: metadata for one topic instead of the cluster, and a
    /// `DescribeLogDirs` that names the partitions instead of asking each
    /// broker for everything it holds. The fan-out itself does not shrink —
    /// a log directory belongs to a broker, so every broker is still asked —
    /// but nothing else about the call is cluster-sized.
    ///
    /// Errors with `UNKNOWN_TOPIC_OR_PARTITION` for a topic that does not
    /// exist, and with the first broker's error if *no* broker answered:
    /// reporting zero bytes for a topic nothing could be measured on would be
    /// indistinguishable from an empty topic. A broker that fails while others
    /// answer is skipped, as in `topic_sizes`, so its copies are missing from
    /// the totals.
    pub async fn topic_size(&self, topic: &str) -> Result<TopicSize> {
        let snapshot = self.cluster().refresh_topics(&[topic]).await?;
        let info = snapshot
            .topic(topic)
            .ok_or_else(|| Error::from_code(ErrorCode::UnknownTopicOrPartition, None))?;
        if let Some(code) = info.error {
            return Err(Error::from_code(code, None));
        }

        let indexes: Vec<i32> = info.partitions.iter().map(|p| p.partition).collect();
        let leaders = leader_map(std::slice::from_ref(info));

        let mut dirs: Vec<(i32, Vec<LogDir>)> = Vec::new();
        let mut failure: Option<Error> = None;
        for (node_id, result) in self.all_log_dirs(Some((topic, &indexes))).await? {
            match result {
                Ok(logdirs) => dirs.push((node_id, logdirs)),
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        if dirs.is_empty()
            && let Some(error) = failure
        {
            return Err(error);
        }

        Ok(fold(
            dirs.iter()
                .map(|(node_id, dirs)| (*node_id, dirs.as_slice())),
            &leaders,
        )
        .remove(topic)
        .unwrap_or_default()
        .finish(topic.to_owned()))
    }
}

/// Topic -> partition -> leader, from a metadata snapshot's topics.
fn leader_map(topics: &[TopicInfo]) -> LeaderMap {
    topics
        .iter()
        .map(|topic| {
            let leaders = topic
                .partitions
                .iter()
                .filter_map(|partition| Some((partition.partition, partition.leader?)))
                .collect();
            (topic.name.clone(), leaders)
        })
        .collect()
}

/// Fold every broker's log-dir entries into per-topic sizes.
///
/// Separate from the RPC so the arithmetic — which is the part that is easy to
/// get wrong, and the reason this module has unit tests — can be checked
/// without a broker.
fn fold<'a>(
    per_broker: impl IntoIterator<Item = (i32, &'a [LogDir])>,
    leaders: &LeaderMap,
) -> HashMap<String, Sizes> {
    let mut folded: HashMap<String, Sizes> = HashMap::new();
    for (node_id, dirs) in per_broker {
        for dir in dirs {
            for replica in &dir.replicas {
                let is_leader = leaders
                    .get(replica.topic.as_str())
                    .and_then(|partitions| partitions.get(&replica.partition))
                    == Some(&node_id);
                folded
                    .entry(replica.topic.clone())
                    .or_default()
                    .add(node_id, &dir.path, replica, is_leader);
            }
        }
    }
    folded
}

impl Sizes {
    /// Account for one log-dir entry.
    fn add(&mut self, node_id: i32, log_dir: &str, replica: &LogDirReplica, is_leader: bool) {
        self.replicas.push(ReplicaSize {
            node_id,
            partition: replica.partition,
            log_dir: log_dir.to_owned(),
            size_bytes: replica.size_bytes,
            offset_lag: replica.offset_lag,
            is_future: replica.is_future,
            is_leader,
        });

        // Future replicas are a copy being moved between log directories on
        // the same broker. Counting them makes a topic appear to grow during a
        // reassignment and shrink again afterwards. The row above keeps the
        // move visible; the totals below stay out of it.
        if replica.is_future {
            return;
        }

        let partition = self
            .partitions
            .entry(replica.partition)
            .or_insert(PartitionSize {
                partition: replica.partition,
                logical_bytes: 0,
                replicated_bytes: 0,
            });
        partition.replicated_bytes += replica.size_bytes;
        if is_leader {
            // Assigned rather than accumulated: exactly one broker leads a
            // partition, so this runs once, and if a broker somehow reported
            // the same partition from two directories the second figure is the
            // one to believe rather than the sum.
            partition.logical_bytes = replica.size_bytes;
        }
    }

    /// Order the rows and total them up.
    fn finish(mut self, topic: String) -> TopicSize {
        let mut partitions: Vec<PartitionSize> = self.partitions.into_values().collect();
        partitions.sort_by_key(|partition| partition.partition);
        self.replicas.sort_by(|a, b| {
            (a.partition, a.node_id)
                .cmp(&(b.partition, b.node_id))
                .then_with(|| a.log_dir.cmp(&b.log_dir))
        });

        TopicSize {
            topic,
            logical_bytes: partitions.iter().map(|p| p.logical_bytes).sum(),
            replicated_bytes: partitions.iter().map(|p| p.replicated_bytes).sum(),
            partitions,
            replicas: self.replicas,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One log directory holding the given entries.
    fn dir(path: &str, replicas: Vec<LogDirReplica>) -> LogDir {
        LogDir {
            path: path.to_owned(),
            total_bytes: None,
            usable_bytes: None,
            replicas,
            error: None,
        }
    }

    /// One entry, live rather than future.
    fn entry(topic: &str, partition: i32, size_bytes: i64) -> LogDirReplica {
        LogDirReplica {
            topic: topic.to_owned(),
            partition,
            size_bytes,
            offset_lag: 0,
            is_future: false,
        }
    }

    /// A leader map from `(topic, partition, leader)` triples.
    fn leading(entries: &[(&str, i32, i32)]) -> LeaderMap {
        let mut map = LeaderMap::new();
        for (topic, partition, leader) in entries {
            map.entry((*topic).to_owned())
                .or_default()
                .insert(*partition, *leader);
        }
        map
    }

    /// Fold one topic out of per-broker results.
    fn sizes(brokers: &[(i32, Vec<LogDir>)], leaders: &LeaderMap, topic: &str) -> TopicSize {
        fold(
            brokers.iter().map(|(node, dirs)| (*node, dirs.as_slice())),
            leaders,
        )
        .remove(topic)
        .unwrap_or_default()
        .finish(topic.to_owned())
    }

    /// The double-counting arithmetic, without a broker.
    ///
    /// This is the calculation M6's acceptance test checks against a real RF=3
    /// topic; having it here as well means a regression is a unit-test failure
    /// rather than something only Docker can catch.
    #[test]
    fn logical_size_counts_each_partition_once_at_its_leader() {
        // One partition, three replicas of 100 bytes on brokers 1, 2 and 3,
        // with broker 1 the leader.
        let brokers: Vec<(i32, Vec<LogDir>)> = (1..=3)
            .map(|node| (node, vec![dir("/data", vec![entry("orders", 0, 100)])]))
            .collect();

        let size = sizes(&brokers, &leading(&[("orders", 0, 1)]), "orders");

        assert_eq!(
            size.logical_bytes, 100,
            "an RF=3 topic is not three times as large"
        );
        assert_eq!(
            size.replicated_bytes, 300,
            "but the disks really do hold 300 bytes"
        );
        assert_eq!(size.partitions.len(), 1);
        assert_eq!(size.partitions[0].logical_bytes, 100);
        assert_eq!(
            size.partitions[0].replicated_bytes, 300,
            "the per-partition figures split the topic's two totals the same way"
        );
        assert_eq!(size.replicas.len(), 3, "one row per log-dir entry");
        assert_eq!(
            size.replicas.iter().filter(|r| r.is_leader).count(),
            1,
            "exactly one copy is the leader's"
        );
    }

    #[test]
    fn future_replicas_are_a_row_but_not_a_byte() {
        // A move between two directories on the same broker: the live copy in
        // one, the copy being built in the other.
        let brokers = vec![(
            1,
            vec![
                dir("/data/a", vec![entry("orders", 0, 100)]),
                dir(
                    "/data/b",
                    vec![LogDirReplica {
                        is_future: true,
                        ..entry("orders", 0, 40)
                    }],
                ),
            ],
        )];

        let size = sizes(&brokers, &leading(&[("orders", 0, 1)]), "orders");

        assert_eq!(
            size.replicated_bytes, 100,
            "a reassignment in flight is not growth"
        );
        assert_eq!(size.logical_bytes, 100);
        assert_eq!(
            size.replicas.len(),
            2,
            "the move is still visible, which is the point of keeping the rows"
        );
        assert_eq!(
            size.replicas
                .iter()
                .filter(|replica| replica.is_future)
                .map(|replica| replica.log_dir.as_str())
                .collect::<Vec<_>>(),
            ["/data/b"],
            "and the directory is what tells the two copies apart"
        );
    }

    #[test]
    fn replica_rows_keep_the_broker_and_the_lag() {
        let brokers = vec![
            (1, vec![dir("/data", vec![entry("orders", 0, 100)])]),
            (
                2,
                vec![dir(
                    "/data",
                    vec![LogDirReplica {
                        offset_lag: 17,
                        ..entry("orders", 0, 96)
                    }],
                )],
            ),
        ];

        let size = sizes(&brokers, &leading(&[("orders", 0, 1)]), "orders");

        let follower = size
            .replicas
            .iter()
            .find(|replica| replica.node_id == 2)
            .expect("a row for the follower");
        assert_eq!(follower.size_bytes, 96);
        assert_eq!(follower.offset_lag, 17, "the lag survives the aggregation");
        assert!(!follower.is_leader);
        assert_eq!(follower.log_dir, "/data");
    }

    #[test]
    fn rows_are_ordered_by_partition_then_broker() {
        let brokers = vec![
            (
                3,
                vec![dir(
                    "/data",
                    vec![entry("orders", 1, 10), entry("orders", 0, 10)],
                )],
            ),
            (1, vec![dir("/data", vec![entry("orders", 1, 10)])]),
        ];

        let size = sizes(
            &brokers,
            &leading(&[("orders", 0, 3), ("orders", 1, 1)]),
            "orders",
        );

        assert_eq!(
            size.replicas
                .iter()
                .map(|replica| (replica.partition, replica.node_id))
                .collect::<Vec<_>>(),
            [(0, 3), (1, 1), (1, 3)],
            "a partition table renders in this order without re-sorting"
        );
        assert_eq!(
            size.partitions
                .iter()
                .map(|partition| partition.partition)
                .collect::<Vec<_>>(),
            [0, 1]
        );
    }

    #[test]
    fn a_partition_with_no_leading_copy_reports_replicated_but_not_logical() {
        // Broker 2 holds a copy; the metadata snapshot names no leader, which
        // is what a partition mid-election looks like.
        let brokers = vec![(2, vec![dir("/data", vec![entry("orders", 0, 100)])])];

        let size = sizes(&brokers, &leading(&[]), "orders");

        assert_eq!(size.replicated_bytes, 100);
        assert_eq!(
            size.logical_bytes, 0,
            "no copy was the leader's, so nothing counts logically"
        );
        assert_eq!(size.partitions[0].replicated_bytes, 100);
        assert_eq!(size.partitions[0].logical_bytes, 0);
    }

    #[test]
    fn each_topic_is_folded_separately() {
        let brokers = vec![(
            1,
            vec![dir(
                "/data",
                vec![entry("orders", 0, 100), entry("shipments", 0, 25)],
            )],
        )];
        let leaders = leading(&[("orders", 0, 1), ("shipments", 0, 1)]);

        assert_eq!(sizes(&brokers, &leaders, "orders").logical_bytes, 100);
        assert_eq!(sizes(&brokers, &leaders, "shipments").logical_bytes, 25);
        assert_eq!(
            sizes(&brokers, &leaders, "absent"),
            TopicSize {
                topic: "absent".to_owned(),
                logical_bytes: 0,
                replicated_bytes: 0,
                partitions: Vec::new(),
                replicas: Vec::new(),
            },
            "a topic nothing reported is empty, not missing"
        );
    }
}
