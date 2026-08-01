//! Transactions and producers — describe only.
//!
//! Nothing here aborts, commits or fences anything. A UI needs to *see* a stuck
//! transaction; deciding to break one is an operation with data-loss
//! consequences that belongs behind a deliberate, separate api rather than
//! behind a describe call's neighbour in an autocomplete list.

use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::describe_producers_request::TopicRequest;
use kafka_conn::protocol::messages::{
    DescribeProducersRequest, DescribeTransactionsRequest, ListTransactionsRequest, TopicName,
    TransactionalId,
};
use kafka_conn::{Error, ErrorCode, Result};
use kafka_meta::CoordinatorKind;

use crate::Admin;
use crate::types::PerItem;

/// One row of `ListTransactions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionListing {
    /// The transactional id.
    pub transactional_id: String,
    /// The producer id currently holding it.
    pub producer_id: i64,
    /// The state, as the broker names it.
    pub state: String,
}

/// A described transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionDescription {
    /// The transactional id.
    pub transactional_id: String,
    /// State.
    pub state: String,
    /// Configured transaction timeout.
    pub timeout_ms: i32,
    /// When the current transaction started, in epoch milliseconds, or `None`
    /// when there is no transaction in flight.
    pub start_time_ms: Option<i64>,
    /// Producer id.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Partitions enrolled in the current transaction.
    pub partitions: Vec<(String, Vec<i32>)>,
}

impl TransactionDescription {
    /// How long the current transaction has been open, given the current time
    /// in epoch milliseconds.
    ///
    /// The number that matters operationally: a transaction open far past its
    /// timeout is what holds the last stable offset back and stalls every
    /// `read_committed` consumer on those partitions.
    pub fn open_for_ms(&self, now_ms: i64) -> Option<i64> {
        // Clamped at zero: `saturating_sub` on a signed type saturates at
        // i64::MIN, not at zero, so clock skew between the broker and this
        // process would otherwise render as a transaction open for negative
        // time.
        self.start_time_ms
            .map(|start| now_ms.saturating_sub(start).max(0))
    }
}

/// One active producer on a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerState {
    /// Producer id.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i32,
    /// Last sequence number the broker accepted.
    pub last_sequence: i32,
    /// Timestamp of the last write.
    pub last_timestamp: i64,
    /// Coordinator epoch, when this producer is transactional.
    pub coordinator_epoch: i32,
    /// Where the producer's open transaction starts, or `None` when it has
    /// none in flight.
    pub current_txn_start_offset: Option<i64>,
}

impl Admin {
    /// List transactions.
    pub async fn list_transactions(
        &self,
        state_filters: &[&str],
    ) -> Result<Vec<TransactionListing>> {
        let request = ListTransactionsRequest::default().with_state_filters(
            state_filters
                .iter()
                .map(|s| StrBytes::from_string((*s).to_owned()))
                .collect(),
        );

        // Like ListGroups, each broker answers for the transactional ids it
        // coordinates, so one broker's answer is a fraction of the cluster's.
        let snapshot = self.cluster().refresh_if_stale().await?;
        let mut listings = Vec::new();
        let mut last_error = None;
        let mut answered = false;

        for broker in snapshot.brokers() {
            match self
                .cluster()
                .send_to_node(broker.node_id, request.clone())
                .await
            {
                Ok(response) => {
                    answered = true;
                    if let Some(code) = ErrorCode::from_code(response.error_code) {
                        last_error = Some(Error::from_code(code, None));
                        continue;
                    }
                    for state in response.transaction_states {
                        listings.push(TransactionListing {
                            transactional_id: state.transactional_id.0.to_string(),
                            producer_id: state.producer_id.0,
                            state: state.transaction_state.to_string(),
                        });
                    }
                }
                Err(error) => last_error = Some(error),
            }
        }

        match (answered, last_error) {
            (false, Some(error)) => Err(error),
            _ => {
                listings.sort_by(|a, b| a.transactional_id.cmp(&b.transactional_id));
                listings.dedup_by(|a, b| a.transactional_id == b.transactional_id);
                Ok(listings)
            }
        }
    }

    /// Describe transactions by transactional id.
    pub async fn describe_transactions(
        &self,
        transactional_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<PerItem<String, TransactionDescription>> {
        let ids: Vec<String> = transactional_ids.into_iter().map(Into::into).collect();
        let mut results: PerItem<String, TransactionDescription> = Vec::new();

        // Each transactional id has its own coordinator, so a batch is only a
        // batch when they happen to coincide.
        for id in ids {
            let request = DescribeTransactionsRequest::default()
                .with_transactional_ids(vec![TransactionalId(StrBytes::from_string(id.clone()))]);
            let outcome = match self
                .cluster()
                .send_to_coordinator(CoordinatorKind::Transaction, &id, request)
                .await
            {
                Ok(response) => match response.transaction_states.into_iter().next() {
                    Some(state) => match ErrorCode::from_code(state.error_code) {
                        Some(code) => Err(Error::from_code(code, Some(id.clone()))),
                        None => Ok(TransactionDescription {
                            transactional_id: state.transactional_id.0.to_string(),
                            state: state.transaction_state.to_string(),
                            timeout_ms: state.transaction_timeout_ms,
                            // -1 means "no transaction in flight", which is not
                            // a transaction that started at the epoch.
                            start_time_ms: Some(state.transaction_start_time_ms)
                                .filter(|t| *t >= 0),
                            producer_id: state.producer_id.0,
                            producer_epoch: state.producer_epoch,
                            partitions: state
                                .topics
                                .into_iter()
                                .map(|topic| (topic.topic.0.to_string(), topic.partitions.clone()))
                                .collect(),
                        }),
                    },
                    None => Err(Error::from_code(
                        ErrorCode::TransactionalIdNotFound,
                        Some(id.clone()),
                    )),
                },
                Err(error) => Err(error),
            };
            results.push((id, outcome));
        }
        Ok(results)
    }

    /// Describe the producers writing to partitions.
    ///
    /// Routed to each partition's leader: producer state is leader state, and
    /// a follower does not have it.
    pub async fn describe_producers(
        &self,
        partitions: impl IntoIterator<Item = (String, i32)>,
    ) -> Result<PerItem<(String, i32), Vec<ProducerState>>> {
        let partitions: Vec<(String, i32)> = partitions.into_iter().collect();
        let mut results: PerItem<(String, i32), Vec<ProducerState>> = Vec::new();

        let mut by_leader: std::collections::HashMap<i32, Vec<(String, i32)>> =
            std::collections::HashMap::new();
        for (topic, partition) in partitions {
            match self.cluster().leader_for(&topic, partition).await {
                Ok(leader) => by_leader
                    .entry(leader)
                    .or_default()
                    .push((topic, partition)),
                Err(error) => results.push(((topic, partition), Err(error))),
            }
        }

        for (leader, group) in by_leader {
            let mut by_topic: std::collections::HashMap<String, Vec<i32>> =
                std::collections::HashMap::new();
            for (topic, partition) in &group {
                by_topic.entry(topic.clone()).or_default().push(*partition);
            }

            let request = DescribeProducersRequest::default().with_topics(
                by_topic
                    .into_iter()
                    .map(|(name, indexes)| {
                        TopicRequest::default()
                            .with_name(TopicName(StrBytes::from_string(name)))
                            .with_partition_indexes(indexes)
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
                                Some(code) => Err(Error::from_code(
                                    code,
                                    partition.error_message.map(|m| m.to_string()),
                                )),
                                None => Ok(partition
                                    .active_producers
                                    .into_iter()
                                    .map(|producer| ProducerState {
                                        producer_id: producer.producer_id.0,
                                        producer_epoch: producer.producer_epoch,
                                        last_sequence: producer.last_sequence,
                                        last_timestamp: producer.last_timestamp,
                                        coordinator_epoch: producer.coordinator_epoch,
                                        current_txn_start_offset: Some(
                                            producer.current_txn_start_offset,
                                        )
                                        .filter(|o| *o >= 0),
                                    })
                                    .collect()),
                            };
                            results.push((key, outcome));
                        }
                    }
                }
                Err(error) => {
                    for key in group {
                        results.push((key, Err(crate::topics::clone_error(&error))));
                    }
                }
            }
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_transaction_with_no_start_time_has_been_open_for_nothing() {
        let description = TransactionDescription {
            transactional_id: "tx".to_owned(),
            state: "Empty".to_owned(),
            timeout_ms: 60_000,
            start_time_ms: None,
            producer_id: 1,
            producer_epoch: 0,
            partitions: Vec::new(),
        };
        assert_eq!(description.open_for_ms(1_000), None);
    }

    #[test]
    fn an_open_transactions_age_is_measurable() {
        let description = TransactionDescription {
            transactional_id: "tx".to_owned(),
            state: "Ongoing".to_owned(),
            timeout_ms: 60_000,
            start_time_ms: Some(1_000),
            producer_id: 1,
            producer_epoch: 0,
            partitions: vec![("orders".to_owned(), vec![0, 1])],
        };
        assert_eq!(description.open_for_ms(61_000), Some(60_000));
        // Clock skew must not produce a negative age.
        assert_eq!(description.open_for_ms(0), Some(0));
    }
}
