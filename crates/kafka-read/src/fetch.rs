//! One `Fetch` round trip against one leader.

use bytes::Bytes;
use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_conn::protocol::messages::{BrokerId, FetchRequest, TopicName};
use kafka_conn::{Error, ErrorCode, Result};
use kafka_meta::{Cluster, TopicId};

use crate::batch::{AbortedTransaction, Visibility};

/// `replica_id` for a consumer rather than a follower.
const CONSUMER_REPLICA_ID: i32 = -1;

/// The first `Fetch` version that identifies topics by uuid rather than name.
const FETCH_TOPIC_ID_VERSION: i16 = 13;

/// Name a topic the way this `Fetch` version expects.
///
/// From v13 the request carries a *topic id* and the name is not encoded at
/// all. Sending only the name against a modern broker leaves the id at nil and
/// the broker answers `UNKNOWN_TOPIC_ID` for every partition — which reads like
/// a missing topic and is not: it is a client that asked the wrong question.
fn fetch_topic(topic: &str, topic_id: TopicId, version: i16) -> Result<FetchTopic> {
    if version >= FETCH_TOPIC_ID_VERSION {
        if topic_id.is_zero() {
            // A broker that supports topic ids always reports them in
            // metadata, so a zero id here means our snapshot is from a path
            // that dropped it — worth saying plainly rather than sending nil
            // and blaming the broker for the answer.
            return Err(Error::InvalidRequest(format!(
                "{topic}: Fetch v{version} identifies topics by id, and no topic id is known"
            )));
        }
        return Ok(
            FetchTopic::default().with_topic_id(uuid::Uuid::from_bytes(*topic_id.as_bytes()))
        );
    }
    Ok(FetchTopic::default().with_topic(TopicName(StrBytes::from_string(topic.to_owned()))))
}

/// What one partition of a fetch response contained.
#[derive(Debug, Clone)]
pub(crate) struct FetchedPartition {
    /// Partition index.
    pub(crate) partition: i32,
    /// The raw record bytes, still encoded.
    pub(crate) records: Bytes,
    /// The partition's high watermark.
    pub(crate) high_watermark: i64,
    /// The last stable offset, which under `read_committed` is where a
    /// consumer stops.
    pub(crate) last_stable_offset: i64,
    /// The first offset the log still holds.
    pub(crate) log_start_offset: i64,
    /// Transactions the client must filter out under `read_committed`.
    pub(crate) aborted: Vec<AbortedTransaction>,
}

/// One partition to fetch.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FetchTarget {
    /// Partition index.
    pub(crate) partition: i32,
    /// Where to start reading.
    pub(crate) offset: i64,
    /// Per-partition byte budget.
    pub(crate) max_bytes: i32,
}

/// Fetch from one leader.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch(
    cluster: &Cluster,
    leader: i32,
    topic: &str,
    topic_id: TopicId,
    targets: &[FetchTarget],
    max_wait_ms: i32,
    max_bytes: i32,
    visibility: Visibility,
) -> Result<Vec<FetchedPartition>> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let version = cluster.negotiated_for::<FetchRequest>().await?;
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(CONSUMER_REPLICA_ID))
        .with_max_wait_ms(max_wait_ms)
        // 1 rather than 0: a fetch that returns as soon as *any* byte is
        // available. Zero would also work, but Kafka treats min_bytes=0 as
        // "return immediately even with nothing", which turns a scan into a
        // spin when the partition is briefly empty.
        .with_min_bytes(1)
        .with_max_bytes(max_bytes)
        .with_isolation_level(match visibility {
            Visibility::All => 0,
            Visibility::CommittedOnly => 1,
        })
        // A session id of 0 with epoch 0 is a full fetch: no incremental
        // session state. A UI's scans are one-shot, and an incremental session
        // would make each one depend on the last.
        .with_session_id(0)
        .with_session_epoch(-1)
        .with_topics(vec![
            fetch_topic(topic, topic_id, version)?.with_partitions(
                targets
                    .iter()
                    .map(|target| {
                        FetchPartition::default()
                            .with_partition(target.partition)
                            .with_current_leader_epoch(-1)
                            .with_fetch_offset(target.offset)
                            .with_last_fetched_epoch(-1)
                            .with_log_start_offset(-1)
                            .with_partition_max_bytes(target.max_bytes)
                    })
                    .collect(),
            ),
        ]);

    let response = cluster.send_to_node(leader, request).await?;
    if let Some(code) = ErrorCode::from_code(response.error_code) {
        return Err(Error::from_code(code, None));
    }

    let mut out = Vec::new();
    for topic_response in response.responses {
        for partition in topic_response.partitions {
            if let Some(code) = ErrorCode::from_code(partition.error_code) {
                return Err(Error::from_code(
                    code,
                    Some(format!("{topic}-{}", partition.partition_index)),
                ));
            }
            out.push(FetchedPartition {
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
            });
        }
    }
    Ok(out)
}
