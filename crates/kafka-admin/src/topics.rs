//! Topic lifecycle: create, describe, grow, delete, truncate.

use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::create_partitions_request::CreatePartitionsTopic;
use kafka_conn::protocol::messages::create_topics_request::{
    CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig,
};
use kafka_conn::protocol::messages::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsTopic,
};
use kafka_conn::protocol::messages::delete_topics_request::DeleteTopicState;
use kafka_conn::protocol::messages::describe_topic_partitions_request::{
    Cursor as RequestCursor, TopicRequest,
};
use kafka_conn::protocol::messages::{
    CreatePartitionsRequest, CreateTopicsRequest, DeleteRecordsRequest, DeleteTopicsRequest,
    DescribeTopicPartitionsRequest, TopicName,
};
use kafka_conn::{ApiKey, Error, ErrorCode, Result};
use kafka_meta::{PartitionInfo, TopicId, TopicInfo};

use crate::Admin;
use crate::types::{CreatedTopic, NewTopic, PerItem};

/// How many partitions to ask `DescribeTopicPartitions` for per response.
///
/// The api paginates, and the broker's own default limit is 2000. Asking for
/// more in one response does not make the call cheaper, it just makes the
/// response bigger than the frame budget on a wide topic.
const DESCRIBE_PARTITION_LIMIT: i32 = 2_000;

impl Admin {
    /// Create topics.
    ///
    /// Per-topic results: creating fifty topics where two already exist gives
    /// forty-eight successes and two `TOPIC_ALREADY_EXISTS` errors, not one
    /// global failure.
    pub async fn create_topics(
        &self,
        topics: impl IntoIterator<Item = NewTopic>,
    ) -> Result<PerItem<String, CreatedTopic>> {
        self.create_topics_inner(topics, false).await
    }

    /// Check what `create_topics` would do without doing it.
    pub async fn validate_topics(
        &self,
        topics: impl IntoIterator<Item = NewTopic>,
    ) -> Result<PerItem<String, CreatedTopic>> {
        self.create_topics_inner(topics, true).await
    }

    async fn create_topics_inner(
        &self,
        topics: impl IntoIterator<Item = NewTopic>,
        validate_only: bool,
    ) -> Result<PerItem<String, CreatedTopic>> {
        // Validate every spec before anything is sent, as before.
        let topics: Vec<NewTopic> = topics.into_iter().collect();
        for topic in &topics {
            to_creatable(topic)?;
        }
        if topics.is_empty() {
            return Ok(Vec::new());
        }

        // Per-item retriable codes (a topic mid-election, THROTTLING) are
        // narrowed and re-asked (#23); the result key is the topic name.
        let results = crate::reask::per_item_retrying(
            self.cluster(),
            crate::reask::Axis::Metadata,
            topics,
            |subset| async move {
                let request = CreateTopicsRequest::default()
                    .with_topics(
                        subset
                            .iter()
                            .map(to_creatable)
                            .collect::<Result<Vec<_>>>()?,
                    )
                    .with_timeout_ms(self.request_timeout_ms())
                    .with_validate_only(validate_only);
                // Controller-routed: see the routing table. A CreateTopics
                // sent to a random broker is a NOT_CONTROLLER retry loop.
                let response = self.cluster().send_to_controller(request).await?;

                let mut by_name: std::collections::HashMap<String, _> = response
                    .topics
                    .into_iter()
                    .map(|result| (result.name.0.to_string(), result))
                    .collect();
                Ok(subset
                    .into_iter()
                    .map(|topic| {
                        let outcome = match by_name.remove(&topic.name) {
                            None => Err(Error::from_code(
                                ErrorCode::UnknownServerError,
                                Some("the response did not mention this topic".to_owned()),
                            )),
                            Some(result) => match ErrorCode::from_code(result.error_code) {
                                Some(code) => Err(Error::from_code(
                                    code,
                                    result.error_message.map(|m| m.to_string()),
                                )),
                                None => Ok(CreatedTopic {
                                    name: topic.name.clone(),
                                    partitions: result.num_partitions,
                                    replication_factor: result.replication_factor,
                                }),
                            },
                        };
                        (topic, outcome)
                    })
                    .collect())
            },
        )
        .await?;

        Ok(results
            .into_iter()
            .map(|(topic, outcome)| (topic.name, outcome))
            .collect())
    }

    /// Delete topics by name.
    pub async fn delete_topics(
        &self,
        names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<PerItem<String, ()>> {
        let names: Vec<String> = names.into_iter().map(Into::into).collect();
        if names.is_empty() {
            return Ok(Vec::new());
        }

        // v6+ takes `topics` (name or id); v1-5 take `topic_names`. Setting
        // both is *not* harmless: the codec bails on a field set outside its
        // own version range, so the two shapes have to be chosen between.
        let version = self.negotiated_for::<DeleteTopicsRequest>().await?;

        // A topic mid-deletion or mid-election answers with a retriable code
        // per item; narrow and re-ask those (#23).
        crate::reask::per_item_retrying(
            self.cluster(),
            crate::reask::Axis::Metadata,
            names,
            |subset| async move {
                let request =
                    DeleteTopicsRequest::default().with_timeout_ms(self.request_timeout_ms());
                let request = if version >= 6 {
                    request.with_topics(
                        subset
                            .iter()
                            .map(|name| {
                                DeleteTopicState::default()
                                    .with_name(Some(TopicName(StrBytes::from_string(name.clone()))))
                            })
                            .collect(),
                    )
                } else {
                    request.with_topic_names(
                        subset
                            .iter()
                            .map(|name| TopicName(StrBytes::from_string(name.clone())))
                            .collect(),
                    )
                };

                let response = self.cluster().send_to_controller(request).await?;
                Ok(response
                    .responses
                    .into_iter()
                    .map(|result| {
                        let name = result.name.map(|n| n.0.to_string()).unwrap_or_default();
                        let outcome = match ErrorCode::from_code(result.error_code) {
                            Some(code) => Err(Error::from_code(
                                code,
                                result.error_message.map(|m| m.to_string()),
                            )),
                            None => Ok(()),
                        };
                        (name, outcome)
                    })
                    .collect())
            },
        )
        .await
    }

    /// Grow topics to a larger partition count.
    ///
    /// Kafka cannot shrink a topic; asking for fewer partitions than it has is
    /// rejected by the broker with `INVALID_PARTITIONS`.
    pub async fn create_partitions(
        &self,
        counts: impl IntoIterator<Item = (String, i32)>,
    ) -> Result<PerItem<String, ()>> {
        let counts: Vec<(String, i32)> = counts.into_iter().collect();
        if counts.is_empty() {
            return Ok(Vec::new());
        }

        // Retriable per-topic codes are narrowed and re-asked (#23).
        let results = crate::reask::per_item_retrying(
            self.cluster(),
            crate::reask::Axis::Metadata,
            counts,
            |subset| async move {
                let request = CreatePartitionsRequest::default()
                    .with_topics(
                        subset
                            .iter()
                            .map(|(name, count)| {
                                CreatePartitionsTopic::default()
                                    .with_name(TopicName(StrBytes::from_string(name.clone())))
                                    .with_count(*count)
                                    // The second instance of the trap CLAUDE.md
                                    // names for `allow_auto_topic_creation`: a
                                    // *nullable* field defaults to `Some(empty)`,
                                    // not `None`. Null means "broker, place the
                                    // new replicas"; an empty list means "here
                                    // are your assignments, there are none of
                                    // them", and the broker rejects it with
                                    // INVALID_REPLICA_ASSIGNMENT.
                                    .with_assignments(None)
                            })
                            .collect(),
                    )
                    .with_timeout_ms(self.request_timeout_ms())
                    .with_validate_only(false);
                let response = self.cluster().send_to_controller(request).await?;

                let mut by_name: std::collections::HashMap<String, _> = response
                    .results
                    .into_iter()
                    .map(|result| (result.name.0.to_string(), result))
                    .collect();
                Ok(subset
                    .into_iter()
                    .map(|item| {
                        let outcome = match by_name.remove(&item.0) {
                            None => Err(Error::from_code(
                                ErrorCode::UnknownServerError,
                                Some("the response did not mention this topic".to_owned()),
                            )),
                            Some(result) => match ErrorCode::from_code(result.error_code) {
                                Some(code) => Err(Error::from_code(
                                    code,
                                    result.error_message.map(|m| m.to_string()),
                                )),
                                None => Ok(()),
                            },
                        };
                        (item, outcome)
                    })
                    .collect())
            },
        )
        .await?;

        Ok(results
            .into_iter()
            .map(|((name, _count), outcome)| (name, outcome))
            .collect())
    }

    /// Delete records before an offset, per partition.
    ///
    /// Returns each partition's new low watermark. Routed to the partition
    /// leader, one request per leader — a single broker cannot truncate a
    /// partition it does not lead.
    pub async fn delete_records(
        &self,
        cutoffs: impl IntoIterator<Item = (String, i32, i64)>,
    ) -> Result<PerItem<(String, i32), i64>> {
        let cutoffs: Vec<(String, i32, i64)> = cutoffs.into_iter().collect();
        let mut results: PerItem<(String, i32), i64> = Vec::new();

        // Group by leader so each broker gets one request rather than one per
        // partition.
        let mut by_leader: std::collections::HashMap<i32, Vec<(String, i32, i64)>> =
            std::collections::HashMap::new();
        for (topic, partition, offset) in cutoffs {
            match self.cluster().leader_for(&topic, partition).await {
                Ok(leader) => by_leader
                    .entry(leader)
                    .or_default()
                    .push((topic, partition, offset)),
                Err(error) => results.push(((topic, partition), Err(error))),
            }
        }

        for (leader, group) in by_leader {
            let mut topics: std::collections::HashMap<String, Vec<DeleteRecordsPartition>> =
                std::collections::HashMap::new();
            for (topic, partition, offset) in &group {
                topics.entry(topic.clone()).or_default().push(
                    DeleteRecordsPartition::default()
                        .with_partition_index(*partition)
                        .with_offset(*offset),
                );
            }

            let request = DeleteRecordsRequest::default()
                .with_timeout_ms(self.request_timeout_ms())
                .with_topics(
                    topics
                        .into_iter()
                        .map(|(name, partitions)| {
                            DeleteRecordsTopic::default()
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
                                None => Ok(partition.low_watermark),
                            };
                            results.push((key, outcome));
                        }
                    }
                }
                Err(error) => {
                    // One broker failing must not lose the answers from the
                    // others, so the failure is recorded per partition.
                    for (topic, partition, _) in group {
                        results.push(((topic, partition), Err(clone_error(&error))));
                    }
                }
            }
        }

        Ok(results)
    }

    /// Describe topics.
    ///
    /// Prefers `DescribeTopicPartitions` (the api the 4.x Java AdminClient
    /// uses) and falls back to `Metadata` when the broker does not offer it.
    /// The preference matters at scale: an unfiltered `Metadata` returns the
    /// *whole cluster* in one response, which on a ten-thousand-topic cluster
    /// is a multi-megabyte payload every refresh, whereas
    /// `DescribeTopicPartitions` names its topics and paginates.
    pub async fn describe_topics(
        &self,
        names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<PerItem<String, TopicInfo>> {
        let names: Vec<String> = names.into_iter().map(Into::into).collect();
        if names.is_empty() {
            return Ok(Vec::new());
        }

        if self.supports(ApiKey::DescribeTopicPartitions).await {
            match self.describe_topics_paginated(&names).await {
                Ok(results) => return Ok(results),
                Err(error) if matches!(error, Error::UnsupportedApi { .. }) => {
                    tracing::debug!(%error, "falling back to Metadata for topic descriptions");
                }
                Err(error) => return Err(error),
            }
        }
        self.describe_topics_via_metadata(&names).await
    }

    /// `DescribeTopicPartitions`, following the cursor to the end.
    async fn describe_topics_paginated(
        &self,
        names: &[String],
    ) -> Result<PerItem<String, TopicInfo>> {
        let mut merged: Vec<(String, std::result::Result<TopicInfo, Error>)> = Vec::new();
        let mut cursor = None;
        // A topic can span responses, so partitions accumulate by name.
        let mut partial: std::collections::HashMap<String, TopicInfo> =
            std::collections::HashMap::new();
        let mut order: Vec<String> = Vec::new();

        loop {
            let request = DescribeTopicPartitionsRequest::default()
                .with_topics(
                    names
                        .iter()
                        .map(|name| {
                            TopicRequest::default()
                                .with_name(TopicName(StrBytes::from_string(name.clone())))
                        })
                        .collect(),
                )
                .with_response_partition_limit(DESCRIBE_PARTITION_LIMIT)
                .with_cursor(cursor.clone());

            let response = self.cluster().send_any(request).await?;

            for topic in response.topics {
                let name = topic.name.map(|n| n.0.to_string()).unwrap_or_default();
                if let Some(code) = ErrorCode::from_code(topic.error_code) {
                    if !order.contains(&name) {
                        order.push(name.clone());
                    }
                    merged.push((name.clone(), Err(Error::from_code(code, None))));
                    continue;
                }

                let partitions: Vec<PartitionInfo> = topic
                    .partitions
                    .into_iter()
                    .map(|p| PartitionInfo {
                        partition: p.partition_index,
                        leader: Some(p.leader_id.0).filter(|id| *id >= 0),
                        leader_epoch: p.leader_epoch,
                        replicas: p.replica_nodes.iter().map(|id| id.0).collect(),
                        isr: p.isr_nodes.iter().map(|id| id.0).collect(),
                        offline_replicas: p.offline_replicas.iter().map(|id| id.0).collect(),
                        error: ErrorCode::from_code(p.error_code),
                    })
                    .collect();

                match partial.get_mut(&name) {
                    Some(existing) => existing.partitions.extend(partitions),
                    None => {
                        order.push(name.clone());
                        partial.insert(
                            name.clone(),
                            TopicInfo {
                                name: name.clone(),
                                topic_id: TopicId::from_bytes(topic.topic_id.into_bytes()),
                                internal: topic.is_internal,
                                partitions,
                                error: None,
                            },
                        );
                    }
                }
            }

            match response.next_cursor {
                // The cursor is how a wide topic is delivered across responses.
                // Stopping at the first page silently truncates the partition
                // list, which reads as "this topic has 2000 partitions".
                //
                // Request and response cursors are *different generated types*
                // with identical fields, so this is a copy rather than a move.
                Some(next) => {
                    cursor = Some(
                        RequestCursor::default()
                            .with_topic_name(next.topic_name)
                            .with_partition_index(next.partition_index),
                    );
                }
                None => break,
            }
        }

        let mut out: PerItem<String, TopicInfo> = Vec::new();
        for name in order {
            if let Some(info) = partial.remove(&name) {
                out.push((name, Ok(info)));
            } else if let Some(position) = merged.iter().position(|(n, _)| *n == name) {
                out.push(merged.swap_remove(position));
            }
        }
        Ok(out)
    }

    async fn describe_topics_via_metadata(
        &self,
        names: &[String],
    ) -> Result<PerItem<String, TopicInfo>> {
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let snapshot = self.cluster().refresh_topics(&refs).await?;
        Ok(names
            .iter()
            .map(|name| {
                let outcome = match snapshot.topic(name) {
                    Some(info) => match info.error {
                        Some(code) => Err(Error::from_code(code, Some(name.clone()))),
                        None => Ok(info.clone()),
                    },
                    None => Err(Error::from_code(
                        ErrorCode::UnknownTopicOrPartition,
                        Some(name.clone()),
                    )),
                };
                (name.clone(), outcome)
            })
            .collect())
    }

    /// Every topic name the cluster knows.
    pub async fn list_topics(&self) -> Result<Vec<String>> {
        let snapshot = self.cluster().refresh().await?;
        Ok(snapshot.topics().iter().map(|t| t.name.clone()).collect())
    }
}

fn to_creatable(topic: &NewTopic) -> Result<CreatableTopic> {
    if !topic.assignments.is_empty()
        && (topic.partitions.is_some() || topic.replication_factor.is_some())
    {
        // The broker rejects this combination, and its error does not say
        // which half was unwanted.
        return Err(Error::InvalidRequest(format!(
            "topic {}: explicit assignments cannot be combined with a partition count \
             or replication factor",
            topic.name
        )));
    }

    let mut creatable = CreatableTopic::default()
        .with_name(TopicName(StrBytes::from_string(topic.name.clone())))
        // -1 tells the broker to use the cluster default, which is what
        // `None` means here.
        .with_num_partitions(topic.partitions.unwrap_or(-1))
        .with_replication_factor(topic.replication_factor.unwrap_or(-1))
        .with_configs(
            topic
                .configs
                .iter()
                .map(|(key, value)| {
                    CreatableTopicConfig::default()
                        .with_name(StrBytes::from_string(key.clone()))
                        .with_value(Some(StrBytes::from_string(value.clone())))
                })
                .collect(),
        );

    if !topic.assignments.is_empty() {
        creatable = creatable.with_assignments(
            topic
                .assignments
                .iter()
                .map(|(partition, brokers)| {
                    CreatableReplicaAssignment::default()
                        .with_partition_index(*partition)
                        .with_broker_ids(
                            brokers
                                .iter()
                                .map(|id| kafka_conn::protocol::messages::BrokerId(*id))
                                .collect(),
                        )
                })
                .collect(),
        );
    }
    Ok(creatable)
}

/// Errors are not `Clone`, and a broker-level failure has to be reported
/// against every partition that request covered.
pub(crate) fn clone_error(error: &Error) -> Error {
    match error {
        Error::Broker { code, message } => Error::Broker {
            code: *code,
            message: message.clone(),
        },
        Error::Authorization { code, detail } => Error::Authorization {
            code: *code,
            detail: detail.clone(),
        },
        Error::Authentication(message) => Error::Authentication(message.clone()),
        Error::ConnectionClosed { peer } => Error::ConnectionClosed { peer: peer.clone() },
        Error::ReadOnly { api_key } => Error::ReadOnly { api_key: *api_key },
        Error::Timeout { api_key, elapsed } => Error::Timeout {
            api_key: *api_key,
            elapsed: *elapsed,
        },
        other => Error::InvalidRequest(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_partition_count_of_none_becomes_the_brokers_default_sentinel() {
        let topic = NewTopic {
            name: "orders".to_owned(),
            partitions: None,
            replication_factor: None,
            assignments: Vec::new(),
            configs: Vec::new(),
        };
        let creatable = to_creatable(&topic).expect("valid");
        assert_eq!(creatable.num_partitions, -1);
        assert_eq!(creatable.replication_factor, -1);
    }

    #[test]
    fn assignments_and_counts_together_are_rejected_before_the_network() {
        let topic = NewTopic {
            name: "orders".to_owned(),
            partitions: Some(3),
            replication_factor: Some(1),
            assignments: vec![(0, vec![1])],
            configs: Vec::new(),
        };
        let err = to_creatable(&topic).expect_err("mutually exclusive");
        assert!(matches!(err, Error::InvalidRequest(_)), "{err:?}");
    }

    #[test]
    fn configs_survive_the_conversion() {
        let topic = NewTopic::new("orders", 3, 1).with_config("retention.ms", "60000");
        let creatable = to_creatable(&topic).expect("valid");
        assert_eq!(creatable.configs.len(), 1);
        assert_eq!(creatable.configs[0].name.as_str(), "retention.ms");
        assert_eq!(
            creatable.configs[0].value.as_ref().map(|v| v.as_str()),
            Some("60000")
        );
    }

    #[test]
    fn growing_a_topic_lets_the_broker_place_the_new_replicas() {
        // `CreatePartitionsTopic::default()` sets `assignments: Some(vec![])`,
        // which the broker reads as "zero assignments supplied for N new
        // partitions" and rejects. Null is the value that means "you choose".
        assert!(
            CreatePartitionsTopic::default().assignments.is_some(),
            "if upstream changes this default, the explicit None below is \
             redundant rather than load-bearing — check before removing it"
        );

        let topic = CreatePartitionsTopic::default()
            .with_name(TopicName(StrBytes::from_static_str("orders")))
            .with_count(6)
            .with_assignments(None);
        assert!(topic.assignments.is_none());
    }

    #[test]
    fn explicit_assignments_reach_the_request() {
        let topic = NewTopic::with_assignments("orders", vec![(0, vec![1, 2, 3])]);
        let creatable = to_creatable(&topic).expect("valid");
        assert_eq!(creatable.assignments.len(), 1);
        assert_eq!(creatable.assignments[0].broker_ids.len(), 3);
    }
}
