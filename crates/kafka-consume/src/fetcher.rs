//! One `Fetch` per broker, covering every assigned partition on it.
//!
//! `kafka-read`'s fetcher takes one topic and one topic id per call, which is
//! exactly right for a scan of one partition and wrong for a consumer. A
//! consumer holding twelve partitions across two topics on three brokers
//! should send **three** requests, not twenty-four: the fetch count scales with
//! broker count, not with assignment size.

use std::collections::HashMap;

use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::fetch_request::{FetchPartition, FetchTopic, ForgottenTopic};
use kafka_conn::protocol::messages::{BrokerId, FetchRequest, TopicName};
use kafka_conn::{Error, ErrorCode, Result};
use kafka_meta::{Cluster, TopicId};
use kafka_read::{AbortedTransaction, Visibility};

use crate::session::{Delta, FetchSession, PartitionState, session_lost};

/// The replica id a consumer sends. `-1` is "not a broker".
const CONSUMER_REPLICA_ID: i32 = -1;

/// The first `Fetch` version that identifies topics by uuid rather than name.
const FETCH_TOPIC_ID_VERSION: i16 = 13;

/// What one partition's fetch produced.
#[derive(Debug)]
pub(crate) struct Fetched {
    pub topic: String,
    pub partition: i32,
    pub records: bytes::Bytes,
    pub high_watermark: i64,
    /// The last stable offset: where a `read_committed` consumer stops.
    ///
    /// Unread today — `poll` bounds itself by the high watermark — and kept
    /// because it is the field a lag calculation under
    /// `Visibility::CommittedOnly` has to use instead.
    #[allow(dead_code)]
    pub last_stable_offset: i64,
    /// Where the log currently starts, which moves under retention.
    #[allow(dead_code)]
    pub log_start_offset: i64,
    pub aborted: Vec<AbortedTransaction>,
    /// A per-partition error, which is that partition's problem and not the
    /// fetch's — rule 4 on the read side.
    pub error: Option<Error>,
}

/// What bounds one fetch.
///
/// Grouped rather than passed loose: they always travel together, and a
/// six-argument call where three are `i32` is a call where two can be swapped
/// without the compiler noticing.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub max_wait_ms: i32,
    pub max_bytes: i32,
    pub visibility: Visibility,
}

/// Fetches from one broker, keeping its incremental session.
#[derive(Debug, Default)]
pub(crate) struct BrokerFetcher {
    session: FetchSession,
}

impl BrokerFetcher {
    /// Whether this broker's session is established.
    pub(crate) fn session_id(&self) -> i32 {
        self.session.id()
    }

    /// Fetch every assigned partition on one broker.
    ///
    /// A dropped session is rebuilt and retried once, transparently: the
    /// caller never learns it happened, because a broker restart is not
    /// something a consumer's user can act on.
    pub(crate) async fn fetch(
        &mut self,
        cluster: &Cluster,
        leader: i32,
        wanted: &HashMap<(String, i32), PartitionState>,
        topic_ids: &HashMap<String, TopicId>,
        limits: Limits,
    ) -> Result<Vec<Fetched>> {
        for attempt in 0..2 {
            let delta = self.session.next(wanted);
            let sent_partitions = delta.include.len();
            let request = self.build(cluster, &delta, topic_ids, limits).await?;

            let response = cluster.send_to_node(leader, request).await?;

            if let Some(code) = ErrorCode::from_code(response.error_code) {
                if session_lost(code) && attempt == 0 {
                    tracing::debug!(
                        leader,
                        %code,
                        "the broker dropped our fetch session; rebuilding"
                    );
                    self.session.reset();
                    continue;
                }
                return Err(Error::from_code(code, None));
            }

            self.session
                .accept(response.session_id, delta.session_epoch);

            tracing::trace!(
                leader,
                session = response.session_id,
                epoch = delta.session_epoch,
                sent_partitions,
                "fetched"
            );

            return Ok(collect(response, topic_ids));
        }

        // Unreachable: the loop either returns or continues exactly once.
        Err(Error::InvalidRequest(
            "the fetch session could not be rebuilt".to_owned(),
        ))
    }

    async fn build(
        &self,
        cluster: &Cluster,
        delta: &Delta,
        topic_ids: &HashMap<String, TopicId>,
        limits: Limits,
    ) -> Result<FetchRequest> {
        let version = cluster.negotiated_for::<FetchRequest>().await?;

        let mut by_topic: HashMap<String, Vec<FetchPartition>> = HashMap::new();
        for ((topic, partition), state) in &delta.include {
            by_topic.entry(topic.clone()).or_default().push(
                FetchPartition::default()
                    .with_partition(*partition)
                    .with_current_leader_epoch(-1)
                    .with_fetch_offset(state.offset)
                    .with_last_fetched_epoch(-1)
                    .with_log_start_offset(-1)
                    .with_partition_max_bytes(state.max_bytes),
            );
        }

        let mut topics = Vec::with_capacity(by_topic.len());
        for (topic, partitions) in by_topic {
            topics.push(named(&topic, topic_ids, version)?.with_partitions(partitions));
        }

        let mut forgotten_by_topic: HashMap<String, Vec<i32>> = HashMap::new();
        for (topic, partition) in &delta.forget {
            forgotten_by_topic
                .entry(topic.clone())
                .or_default()
                .push(*partition);
        }
        let mut forgotten = Vec::with_capacity(forgotten_by_topic.len());
        for (topic, partitions) in forgotten_by_topic {
            let mut entry = ForgottenTopic::default().with_partitions(partitions);
            if version >= FETCH_TOPIC_ID_VERSION {
                entry = entry.with_topic_id(topic_uuid(&topic, topic_ids)?);
            } else {
                entry = entry.with_topic(TopicName(StrBytes::from_string(topic)));
            }
            forgotten.push(entry);
        }

        Ok(FetchRequest::default()
            .with_replica_id(BrokerId(CONSUMER_REPLICA_ID))
            .with_max_wait_ms(limits.max_wait_ms)
            .with_min_bytes(1)
            .with_max_bytes(limits.max_bytes)
            .with_isolation_level(match limits.visibility {
                Visibility::All => 0,
                Visibility::CommittedOnly => 1,
            })
            .with_session_id(delta.session_id)
            .with_session_epoch(delta.session_epoch)
            .with_topics(topics)
            .with_forgotten_topics_data(forgotten))
    }
}

/// Set exactly the field this version has: the codec rejects one outside its
/// own range rather than ignoring it.
fn named(topic: &str, topic_ids: &HashMap<String, TopicId>, version: i16) -> Result<FetchTopic> {
    if version >= FETCH_TOPIC_ID_VERSION {
        Ok(FetchTopic::default().with_topic_id(topic_uuid(topic, topic_ids)?))
    } else {
        Ok(FetchTopic::default().with_topic(TopicName(StrBytes::from_string(topic.to_owned()))))
    }
}

fn topic_uuid(topic: &str, topic_ids: &HashMap<String, TopicId>) -> Result<uuid::Uuid> {
    let id = topic_ids.get(topic).ok_or_else(|| {
        Error::InvalidRequest(format!(
            "{topic}: Fetch v{FETCH_TOPIC_ID_VERSION}+ identifies topics by id and none is known"
        ))
    })?;
    if id.is_zero() {
        return Err(Error::InvalidRequest(format!(
            "{topic}: the broker reported no topic id, which Fetch v{FETCH_TOPIC_ID_VERSION}+ requires"
        )));
    }
    Ok(uuid::Uuid::from_bytes(*id.as_bytes()))
}

/// Pull every partition out of the response.
///
/// A per-partition error is recorded rather than returned: one partition whose
/// leader just moved must not discard the eleven that answered.
///
/// # The v13 trap, on the response side
///
/// From v13 the *response* identifies each topic by `topic_id` and leaves the
/// name empty — the mirror of the request-side transition, and easy to miss
/// because the request side is the one that is documented as a trap. Reading
/// `topic_response.topic` alone yields an empty string for every topic, every
/// partition then fails to match the assignment, and the consumer silently
/// delivers **nothing at all** while every request succeeds. That is what the
/// first live run of this crate did.
fn collect(
    response: kafka_conn::protocol::messages::FetchResponse,
    topic_ids: &HashMap<String, TopicId>,
) -> Vec<Fetched> {
    let by_id: HashMap<uuid::Uuid, &String> = topic_ids
        .iter()
        .filter(|(_, id)| !id.is_zero())
        .map(|(name, id)| (uuid::Uuid::from_bytes(*id.as_bytes()), name))
        .collect();

    let mut out = Vec::new();
    for topic_response in response.responses {
        let named = topic_response.topic.0.to_string();
        let topic = if named.is_empty() {
            match by_id.get(&topic_response.topic_id) {
                Some(name) => (*name).clone(),
                // A topic we never asked for, or one whose id we do not know.
                // Skipping is right: there is nothing in the assignment it
                // could belong to.
                None => continue,
            }
        } else {
            named
        };

        for partition in topic_response.partitions {
            out.push(Fetched {
                topic: topic.clone(),
                partition: partition.partition_index,
                records: partition.records.unwrap_or_default(),
                high_watermark: partition.high_watermark,
                last_stable_offset: partition.last_stable_offset,
                log_start_offset: partition.log_start_offset,
                aborted: partition
                    .aborted_transactions
                    .unwrap_or_default()
                    .into_iter()
                    .map(|aborted| AbortedTransaction {
                        producer_id: aborted.producer_id.0,
                        first_offset: aborted.first_offset,
                    })
                    .collect(),
                error: ErrorCode::from_code(partition.error_code)
                    .map(|code| Error::from_code(code, None)),
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A v13+ fetch that cannot name a topic id must say so rather than send a
    /// nil one, which the broker answers `UNKNOWN_TOPIC_ID` — an error that
    /// reads like a deleted topic.
    #[test]
    fn a_missing_topic_id_is_refused_rather_than_sent_as_nil() {
        let ids = HashMap::new();
        let error = topic_uuid("t", &ids).expect_err("no id known");
        assert!(error.to_string().contains("identifies topics by id"));
    }

    #[test]
    fn a_zero_topic_id_is_refused_too() {
        let mut ids = HashMap::new();
        ids.insert("t".to_owned(), TopicId::from_bytes([0u8; 16]));
        assert!(topic_uuid("t", &ids).is_err());
    }

    /// Below v13 the name is the identifier and no id is needed at all, which
    /// is what lets this run against a broker that reports none.
    #[test]
    fn an_older_fetch_names_the_topic_and_needs_no_id() {
        let ids = HashMap::new();
        assert!(named("t", &ids, 12).is_ok());
        assert!(named("t", &ids, 13).is_err());
    }
}
