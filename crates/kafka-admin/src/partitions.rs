//! Partition reassignment and leader election.

use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::alter_partition_reassignments_request::{
    ReassignablePartition, ReassignableTopic,
};
use kafka_conn::protocol::messages::elect_leaders_request::TopicPartitions;
use kafka_conn::protocol::messages::list_partition_reassignments_request::ListPartitionReassignmentsTopics;
use kafka_conn::protocol::messages::{
    AlterPartitionReassignmentsRequest, BrokerId, ElectLeadersRequest,
    ListPartitionReassignmentsRequest, TopicName,
};
use kafka_conn::{Error, ErrorCode, Result};

use crate::Admin;
use crate::types::PerItem;

/// A reassignment to submit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignment {
    /// Topic.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// The replica set to move to, or `None` to cancel an in-flight
    /// reassignment for this partition.
    ///
    /// `None` and `Some(vec![])` are different requests: the first cancels, the
    /// second is rejected. The protocol spells the cancellation as a null
    /// replica list, so the `Option` is load-bearing.
    pub replicas: Option<Vec<i32>>,
}

impl PartitionReassignment {
    /// Move a partition to a replica set.
    pub fn to(topic: impl Into<String>, partition: i32, replicas: Vec<i32>) -> Self {
        Self {
            topic: topic.into(),
            partition,
            replicas: Some(replicas),
        }
    }

    /// Cancel an in-flight reassignment.
    pub fn cancel(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
            replicas: None,
        }
    }
}

/// A reassignment the controller is still working on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OngoingReassignment {
    /// Topic.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// The current replica set, which includes both the replicas being added
    /// and those being removed until the move completes.
    pub replicas: Vec<i32>,
    /// Replicas being added.
    pub adding: Vec<i32>,
    /// Replicas being removed.
    pub removing: Vec<i32>,
}

/// Which leader election to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionType {
    /// Elect the preferred replica — the first in the replica list — where it
    /// is in-sync. Safe: no data is lost.
    Preferred,
    /// Elect any surviving replica even if it is not in-sync.
    ///
    /// **Loses data.** Records the old leader had and this replica does not are
    /// gone. Only worth doing when the alternative is a partition that stays
    /// offline indefinitely.
    Unclean,
}

impl ElectionType {
    /// The wire value.
    pub const fn code(self) -> i8 {
        match self {
            ElectionType::Preferred => 0,
            ElectionType::Unclean => 1,
        }
    }
}

impl Admin {
    /// Submit partition reassignments.
    pub async fn alter_partition_reassignments(
        &self,
        reassignments: impl IntoIterator<Item = PartitionReassignment>,
    ) -> Result<PerItem<(String, i32), ()>> {
        let reassignments: Vec<PartitionReassignment> = reassignments.into_iter().collect();
        if reassignments.is_empty() {
            return Ok(Vec::new());
        }

        let mut by_topic: std::collections::HashMap<String, Vec<ReassignablePartition>> =
            std::collections::HashMap::new();
        for reassignment in &reassignments {
            by_topic
                .entry(reassignment.topic.clone())
                .or_default()
                .push(
                    ReassignablePartition::default()
                        .with_partition_index(reassignment.partition)
                        .with_replicas(
                            reassignment
                                .replicas
                                .as_ref()
                                .map(|ids| ids.iter().map(|id| BrokerId(*id)).collect()),
                        ),
                );
        }

        let request = AlterPartitionReassignmentsRequest::default()
            .with_timeout_ms(self.request_timeout_ms())
            .with_topics(
                by_topic
                    .into_iter()
                    .map(|(name, partitions)| {
                        ReassignableTopic::default()
                            .with_name(TopicName(StrBytes::from_string(name)))
                            .with_partitions(partitions)
                    })
                    .collect(),
            );

        // Controller-only. A reassignment sent elsewhere is a NOT_CONTROLLER
        // loop that looks like a cluster problem.
        let response = self.cluster().send_to_controller(request).await?;
        if let Some(code) = ErrorCode::from_code(response.error_code) {
            return Err(Error::from_code(
                code,
                response.error_message.map(|m| m.to_string()),
            ));
        }

        Ok(response
            .responses
            .into_iter()
            .flat_map(|topic| {
                let name = topic.name.0.to_string();
                topic.partitions.into_iter().map(move |partition| {
                    let outcome = match ErrorCode::from_code(partition.error_code) {
                        Some(code) => Err(Error::from_code(
                            code,
                            partition.error_message.map(|m| m.to_string()),
                        )),
                        None => Ok(()),
                    };
                    ((name.clone(), partition.partition_index), outcome)
                })
            })
            .collect())
    }

    /// List reassignments currently in progress.
    ///
    /// `None` lists every ongoing reassignment in the cluster, which is how a
    /// UI answers "is anything moving right now".
    pub async fn list_partition_reassignments(
        &self,
        partitions: Option<Vec<(String, Vec<i32>)>>,
    ) -> Result<Vec<OngoingReassignment>> {
        let request = ListPartitionReassignmentsRequest::default()
            .with_timeout_ms(self.request_timeout_ms())
            .with_topics(partitions.map(|topics| {
                topics
                    .into_iter()
                    .map(|(name, indexes)| {
                        ListPartitionReassignmentsTopics::default()
                            .with_name(TopicName(StrBytes::from_string(name)))
                            .with_partition_indexes(indexes)
                    })
                    .collect()
            }));

        let response = self.cluster().send_to_controller(request).await?;
        if let Some(code) = ErrorCode::from_code(response.error_code) {
            return Err(Error::from_code(
                code,
                response.error_message.map(|m| m.to_string()),
            ));
        }

        Ok(response
            .topics
            .into_iter()
            .flat_map(|topic| {
                let name = topic.name.0.to_string();
                topic
                    .partitions
                    .into_iter()
                    .map(move |partition| OngoingReassignment {
                        topic: name.clone(),
                        partition: partition.partition_index,
                        replicas: partition.replicas.iter().map(|id| id.0).collect(),
                        adding: partition.adding_replicas.iter().map(|id| id.0).collect(),
                        removing: partition.removing_replicas.iter().map(|id| id.0).collect(),
                    })
            })
            .collect())
    }

    /// Elect leaders.
    ///
    /// `None` elects for every partition that needs it.
    pub async fn elect_leaders(
        &self,
        election: ElectionType,
        partitions: Option<Vec<(String, Vec<i32>)>>,
    ) -> Result<PerItem<(String, i32), ()>> {
        let request = ElectLeadersRequest::default()
            .with_election_type(election.code())
            .with_timeout_ms(self.request_timeout_ms())
            .with_topic_partitions(partitions.map(|topics| {
                topics
                    .into_iter()
                    .map(|(name, indexes)| {
                        TopicPartitions::default()
                            .with_topic(TopicName(StrBytes::from_string(name)))
                            .with_partitions(indexes)
                    })
                    .collect()
            }));

        let response = self.cluster().send_to_controller(request).await?;
        if let Some(code) = ErrorCode::from_code(response.error_code) {
            return Err(Error::from_code(code, None));
        }

        Ok(response
            .replica_election_results
            .into_iter()
            .flat_map(|topic| {
                let name = topic.topic.0.to_string();
                topic.partition_result.into_iter().map(move |partition| {
                    let outcome = match ErrorCode::from_code(partition.error_code) {
                        // ELECTION_NOT_NEEDED means the preferred replica is
                        // already the leader. That is the desired state, so
                        // reporting it as a failure makes a no-op look broken.
                        Some(ErrorCode::ElectionNotNeeded) => Ok(()),
                        Some(code) => Err(Error::from_code(
                            code,
                            partition.error_message.map(|m| m.to_string()),
                        )),
                        None => Ok(()),
                    };
                    ((name.clone(), partition.partition_id), outcome)
                })
            })
            .collect())
    }

    /// Whether any reassignment involving these topics is still in flight.
    pub async fn reassignments_in_progress(&self, topics: &[&str]) -> Result<bool> {
        let filter = if topics.is_empty() {
            None
        } else {
            Some(
                topics
                    .iter()
                    .map(|name| ((*name).to_owned(), Vec::new()))
                    .collect(),
            )
        };
        Ok(!self.list_partition_reassignments(filter).await?.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelling_is_a_null_replica_list_not_an_empty_one() {
        let cancel = PartitionReassignment::cancel("orders", 0);
        assert!(cancel.replicas.is_none());
        let move_to = PartitionReassignment::to("orders", 0, vec![1, 2, 3]);
        assert_eq!(move_to.replicas.as_deref(), Some([1, 2, 3].as_slice()));
    }

    #[test]
    fn election_types_match_the_protocol() {
        assert_eq!(ElectionType::Preferred.code(), 0);
        assert_eq!(ElectionType::Unclean.code(), 1);
    }
}
