//! `ListOffsets`, all five reachable sentinels.

use std::collections::HashMap;

use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsTopic,
};
use kafka_conn::protocol::messages::{ListOffsetsRequest, TopicName};
use kafka_conn::{ApiKey, Error, ErrorCode, Result};

use crate::Admin;
use crate::topics::clone_error;
use crate::types::{ListedOffset, OffsetSpec, PerItem};

/// `ListOffsets` `replica_id` for a consumer rather than a follower.
const CONSUMER_REPLICA_ID: i32 = -1;

/// Isolation levels, as `ListOffsets` and `Fetch` spell them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Everything the log holds, including records from transactions that were
    /// later aborted.
    #[default]
    ReadUncommitted,
    /// Up to the last stable offset: aborted records are excluded, and the
    /// "latest" offset is the LSO rather than the high watermark.
    ReadCommitted,
}

impl IsolationLevel {
    /// The wire value.
    pub const fn code(self) -> i8 {
        match self {
            IsolationLevel::ReadUncommitted => 0,
            IsolationLevel::ReadCommitted => 1,
        }
    }
}

impl Admin {
    /// List offsets for partitions, all with the same spec.
    pub async fn list_offsets(
        &self,
        partitions: impl IntoIterator<Item = (String, i32)>,
        spec: OffsetSpec,
    ) -> Result<PerItem<(String, i32), ListedOffset>> {
        self.list_offsets_with(
            partitions.into_iter().map(|(t, p)| (t, p, spec)),
            IsolationLevel::default(),
        )
        .await
    }

    /// List offsets with a per-partition spec and an explicit isolation level.
    pub async fn list_offsets_with(
        &self,
        requests: impl IntoIterator<Item = (String, i32, OffsetSpec)>,
        isolation: IsolationLevel,
    ) -> Result<PerItem<(String, i32), ListedOffset>> {
        let requests: Vec<(String, i32, OffsetSpec)> = requests.into_iter().collect();
        let mut results: PerItem<(String, i32), ListedOffset> = Vec::new();

        // The version we can actually speak decides which sentinels are
        // available. `kafka-protocol` 0.17 caps at v10, so `-6` is unreachable
        // no matter how new the broker is — say so plainly rather than sending
        // it and letting the broker return something surprising.
        let negotiated = self.negotiated_version(ApiKey::ListOffsets).await;

        let mut by_leader: HashMap<i32, Vec<(String, i32, OffsetSpec)>> = HashMap::new();
        for (topic, partition, spec) in requests {
            if let Some(version) = negotiated
                && spec.min_version() > version
            {
                results.push((
                    (topic, partition),
                    Err(Error::Unsupported(format!(
                        "{spec:?} needs ListOffsets v{}, and this build negotiated v{version} \
                         (kafka-protocol 0.17 ships Kafka 4.0 schemas)",
                        spec.min_version()
                    ))),
                ));
                continue;
            }
            match self.cluster().leader_for(&topic, partition).await {
                Ok(leader) => by_leader
                    .entry(leader)
                    .or_default()
                    .push((topic, partition, spec)),
                Err(error) => results.push(((topic, partition), Err(error))),
            }
        }

        for (leader, group) in by_leader {
            let mut topics: HashMap<String, Vec<ListOffsetsPartition>> = HashMap::new();
            for (topic, partition, spec) in &group {
                topics.entry(topic.clone()).or_default().push(
                    ListOffsetsPartition::default()
                        .with_partition_index(*partition)
                        // -1: we are not fencing on a specific leader epoch.
                        // The dispatcher already refreshes metadata and retries
                        // when the leader moves.
                        .with_current_leader_epoch(-1)
                        .with_timestamp(spec.timestamp()),
                );
            }

            let request = ListOffsetsRequest::default()
                .with_replica_id(kafka_conn::protocol::messages::BrokerId(
                    CONSUMER_REPLICA_ID,
                ))
                .with_isolation_level(isolation.code())
                .with_topics(
                    topics
                        .into_iter()
                        .map(|(name, partitions)| {
                            ListOffsetsTopic::default()
                                .with_name(TopicName(StrBytes::from_string(name)))
                                .with_partitions(partitions)
                        })
                        .collect(),
                );

            match self.cluster().send_to_node(leader, request).await {
                Ok(response) => {
                    for topic in response.topics {
                        let name = topic.name.0.to_string();
                        for partition in topic.partitions {
                            let key = (name.clone(), partition.partition_index);
                            let outcome = match ErrorCode::from_code(partition.error_code) {
                                Some(code) => Err(Error::from_code(code, None)),
                                None => Ok(ListedOffset {
                                    partition: partition.partition_index,
                                    // -1 means "no such offset" — an empty
                                    // partition, or a timestamp past the end.
                                    offset: Some(partition.offset).filter(|o| *o >= 0),
                                    timestamp: Some(partition.timestamp).filter(|t| *t >= 0),
                                    leader_epoch: Some(partition.leader_epoch).filter(|e| *e >= 0),
                                }),
                            };
                            results.push((key, outcome));
                        }
                    }
                }
                Err(error) => {
                    for (topic, partition, _) in group {
                        results.push(((topic, partition), Err(clone_error(&error))));
                    }
                }
            }
        }

        Ok(results)
    }

    /// Earliest and latest offsets for every partition of a topic.
    ///
    /// The pair a UI shows as "this partition holds offsets X through Y".
    pub async fn topic_offset_range(
        &self,
        topic: &str,
    ) -> Result<PerItem<i32, (Option<i64>, Option<i64>)>> {
        let snapshot = self.cluster().refresh_topics(&[topic]).await?;
        let partitions: Vec<i32> = snapshot
            .topic(topic)
            .map(|info| info.partitions.iter().map(|p| p.partition).collect())
            .unwrap_or_default();
        if partitions.is_empty() {
            return Err(Error::from_code(
                ErrorCode::UnknownTopicOrPartition,
                Some(topic.to_owned()),
            ));
        }

        let earliest = self
            .list_offsets(
                partitions.iter().map(|p| (topic.to_owned(), *p)),
                OffsetSpec::Earliest,
            )
            .await?;
        let latest = self
            .list_offsets(
                partitions.iter().map(|p| (topic.to_owned(), *p)),
                OffsetSpec::Latest,
            )
            .await?;

        let earliest: HashMap<i32, std::result::Result<ListedOffset, Error>> = earliest
            .into_iter()
            .map(|((_, p), value)| (p, value))
            .collect();
        let mut latest: HashMap<i32, std::result::Result<ListedOffset, Error>> = latest
            .into_iter()
            .map(|((_, p), value)| (p, value))
            .collect();

        Ok(partitions
            .into_iter()
            .map(|partition| {
                let low = earliest.get(&partition);
                let high = latest.remove(&partition);
                let outcome = match (low, high) {
                    (Some(Ok(low)), Some(Ok(high))) => Ok((low.offset, high.offset)),
                    (Some(Err(error)), _) => Err(clone_error(error)),
                    (_, Some(Err(error))) => Err(clone_error(&error)),
                    _ => Err(Error::from_code(
                        ErrorCode::UnknownTopicOrPartition,
                        Some(format!("{topic}-{partition}")),
                    )),
                };
                (partition, outcome)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn isolation_levels_match_the_protocol() {
        assert_eq!(IsolationLevel::ReadUncommitted.code(), 0);
        assert_eq!(IsolationLevel::ReadCommitted.code(), 1);
        assert_eq!(IsolationLevel::default(), IsolationLevel::ReadUncommitted);
    }

    #[test]
    fn the_unreachable_sentinel_is_named_in_the_error_rather_than_sent() {
        // Documented in the type, refused at the boundary. The alternative is
        // sending -6 at v10, where the broker has no idea what it means.
        let spec = OffsetSpec::EarliestPendingUploadTimestamp;
        assert!(spec.min_version() > 10);
    }
}
