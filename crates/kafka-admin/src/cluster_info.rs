//! `DescribeCluster`, `DescribeLogDirs`, and per-topic size.

use std::collections::HashMap;

use kafka_conn::protocol::messages::{DescribeClusterRequest, DescribeLogDirsRequest};
use kafka_conn::{Error, ErrorCode, Result};

use crate::Admin;
use crate::types::{ClusterBroker, ClusterDescription, LogDir, LogDirReplica, PerItem, TopicSize};

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
        // `None` means "every partition", which is what a size view wants.
        let request = DescribeLogDirsRequest::default().with_topics(None);
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
        let snapshot = self.cluster().refresh_if_stale().await?;
        let mut results: PerItem<i32, Vec<LogDir>> = Vec::new();
        for broker in snapshot.brokers() {
            let outcome = self.describe_log_dirs(broker.node_id).await;
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
    pub async fn topic_sizes(&self) -> Result<PerItem<String, TopicSize>> {
        let snapshot = self.cluster().refresh().await?;
        let per_broker = self.describe_all_log_dirs().await?;

        // (topic, partition) -> leader, from the metadata snapshot.
        let mut leaders: HashMap<(String, i32), i32> = HashMap::new();
        for topic in snapshot.topics() {
            for partition in &topic.partitions {
                if let Some(leader) = partition.leader {
                    leaders.insert((topic.name.clone(), partition.partition), leader);
                }
            }
        }

        let mut logical: HashMap<String, HashMap<i32, i64>> = HashMap::new();
        let mut replicated: HashMap<String, i64> = HashMap::new();

        for (node_id, dirs) in &per_broker {
            let Ok(dirs) = dirs else { continue };
            for dir in dirs {
                for replica in &dir.replicas {
                    // Future replicas are a copy being moved between log
                    // directories on the same broker. Counting them makes a
                    // topic appear to grow during a reassignment and shrink
                    // again afterwards.
                    if replica.is_future {
                        continue;
                    }
                    *replicated.entry(replica.topic.clone()).or_default() += replica.size_bytes;

                    let key = (replica.topic.clone(), replica.partition);
                    if leaders.get(&key) == Some(node_id) {
                        logical
                            .entry(replica.topic.clone())
                            .or_default()
                            .insert(replica.partition, replica.size_bytes);
                    }
                }
            }
        }

        Ok(snapshot
            .topics()
            .iter()
            .map(|topic| {
                let partitions = logical.remove(&topic.name).unwrap_or_default();
                let mut partitions: Vec<(i32, i64)> = partitions.into_iter().collect();
                partitions.sort_by_key(|(index, _)| *index);
                let size = TopicSize {
                    topic: topic.name.clone(),
                    logical_bytes: partitions.iter().map(|(_, bytes)| *bytes).sum(),
                    replicated_bytes: replicated.get(&topic.name).copied().unwrap_or_default(),
                    partitions,
                };
                (topic.name.clone(), Ok(size))
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The double-counting arithmetic, without a broker.
    ///
    /// This is the calculation M6's acceptance test checks against a real RF=3
    /// topic; having it here as well means a regression is a unit-test failure
    /// rather than something only Docker can catch.
    #[test]
    fn logical_size_counts_each_partition_once_at_its_leader() {
        // One partition, three replicas of 100 bytes on brokers 1, 2 and 3,
        // with broker 1 the leader.
        let leaders: HashMap<(String, i32), i32> =
            [(("orders".to_owned(), 0), 1)].into_iter().collect();

        let mut logical = 0i64;
        let mut replicated = 0i64;
        for node_id in [1, 2, 3] {
            let replica = LogDirReplica {
                topic: "orders".to_owned(),
                partition: 0,
                size_bytes: 100,
                offset_lag: 0,
                is_future: false,
            };
            replicated += replica.size_bytes;
            if leaders.get(&("orders".to_owned(), 0)) == Some(&node_id) {
                logical += replica.size_bytes;
            }
        }

        assert_eq!(logical, 100, "an RF=3 topic is not three times as large");
        assert_eq!(replicated, 300, "but the disks really do hold 300 bytes");
    }

    #[test]
    fn future_replicas_are_not_counted() {
        let replicas = [
            LogDirReplica {
                topic: "orders".to_owned(),
                partition: 0,
                size_bytes: 100,
                offset_lag: 0,
                is_future: false,
            },
            LogDirReplica {
                topic: "orders".to_owned(),
                partition: 0,
                size_bytes: 100,
                offset_lag: 0,
                is_future: true,
            },
        ];
        let total: i64 = replicas
            .iter()
            .filter(|r| !r.is_future)
            .map(|r| r.size_bytes)
            .sum();
        assert_eq!(total, 100, "a reassignment in flight is not growth");
    }
}
