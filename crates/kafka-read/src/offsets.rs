//! `ListOffsets` helpers for the read path.

use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsTopic,
};
use kafka_conn::protocol::messages::{BrokerId, ListOffsetsRequest, TopicName};
use kafka_conn::{Error, ErrorCode, Result};
use kafka_meta::Cluster;

const CONSUMER_REPLICA_ID: i32 = -1;
const EARLIEST: i64 = -2;
const LATEST: i64 = -1;

/// The `(earliest, latest)` offsets of a partition.
pub(crate) async fn bounds(
    cluster: &Cluster,
    topic: &str,
    partition: i32,
    leader: i32,
) -> Result<(i64, i64)> {
    let earliest = list_one(cluster, topic, partition, leader, EARLIEST)
        .await?
        .unwrap_or(0);
    let latest = list_one(cluster, topic, partition, leader, LATEST)
        .await?
        .unwrap_or(earliest);
    Ok((earliest, latest.max(earliest)))
}

/// The first offset at or after a timestamp, or `None` when there is none.
pub(crate) async fn at_timestamp(
    cluster: &Cluster,
    topic: &str,
    partition: i32,
    leader: i32,
    timestamp: i64,
) -> Result<Option<i64>> {
    list_one(cluster, topic, partition, leader, timestamp).await
}

async fn list_one(
    cluster: &Cluster,
    topic: &str,
    partition: i32,
    leader: i32,
    timestamp: i64,
) -> Result<Option<i64>> {
    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(CONSUMER_REPLICA_ID))
        .with_isolation_level(0)
        .with_topics(vec![
            ListOffsetsTopic::default()
                .with_name(TopicName(StrBytes::from_string(topic.to_owned())))
                .with_partitions(vec![
                    ListOffsetsPartition::default()
                        .with_partition_index(partition)
                        .with_current_leader_epoch(-1)
                        .with_timestamp(timestamp),
                ]),
        ]);

    let response = cluster.send_to_node(leader, request).await?;
    for topic_response in response.topics {
        for partition_response in topic_response.partitions {
            if partition_response.partition_index != partition {
                continue;
            }
            if let Some(code) = ErrorCode::from_code(partition_response.error_code) {
                return Err(Error::from_code(code, Some(format!("{topic}-{partition}"))));
            }
            // -1 means "no offset matches", which for a timestamp lookup past
            // the end of the log is an answer rather than a failure.
            return Ok(Some(partition_response.offset).filter(|offset| *offset >= 0));
        }
    }
    Ok(None)
}
