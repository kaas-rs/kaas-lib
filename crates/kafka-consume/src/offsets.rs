//! Committing and reading offsets **without joining a group**.
//!
//! A manually-assigned consumer still wants its position remembered — that is
//! what makes it resumable across restarts — but it is not a member of
//! anything. The protocol expresses that with sentinels rather than with a
//! separate api, and the sentinels differ by group protocol:
//!
//! * a **classic** group takes `generation_id = -1`
//! * a **KIP-848** consumer group takes `member_epoch = -1`
//!
//! They occupy the same wire field (`generation_id_or_member_epoch`), and
//! both spellings are `-1`, so a non-member commit is the one case where the
//! two protocols agree. Where they disagree is M17 and M18's problem.
//!
//! The member id must be **empty**. A made-up one is rejected with
//! `UNKNOWN_MEMBER_ID`, which reads like a membership bug in a client that
//! deliberately has no membership.

use std::collections::HashMap;

use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_conn::protocol::messages::offset_fetch_request::{
    OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
use kafka_conn::protocol::messages::{GroupId, OffsetCommitRequest, OffsetFetchRequest, TopicName};
use kafka_conn::{Error, ErrorCode, Result};
use kafka_meta::{Cluster, CoordinatorKind};

/// The sentinel that says "I am not a member of this group".
///
/// Spelled `generation_id` by the classic protocol and `member_epoch` by
/// KIP-848; the same field and the same value.
const NOT_A_MEMBER: i32 = -1;

/// The first `OffsetFetch` version that batches groups.
const OFFSET_FETCH_GROUPS_VERSION: i16 = 8;

/// An offset with the metadata a caller attached to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOffset {
    /// The committed position: the offset of the *next* record to read.
    pub offset: i64,
    /// Whatever the committer stored alongside it.
    pub metadata: Option<String>,
}

/// Commit positions for a group without being a member of it.
pub(crate) async fn commit(
    cluster: &Cluster,
    group_id: &str,
    offsets: &HashMap<(String, i32), CommittedOffset>,
) -> Result<Vec<((String, i32), Result<()>)>> {
    if offsets.is_empty() {
        return Ok(Vec::new());
    }

    let mut by_topic: HashMap<String, Vec<OffsetCommitRequestPartition>> = HashMap::new();
    for ((topic, partition), committed) in offsets {
        by_topic.entry(topic.clone()).or_default().push(
            OffsetCommitRequestPartition::default()
                .with_partition_index(*partition)
                .with_committed_offset(committed.offset)
                .with_committed_leader_epoch(-1)
                .with_committed_metadata(committed.metadata.clone().map(StrBytes::from_string)),
        );
    }

    let request = OffsetCommitRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group_id.to_owned())))
        .with_generation_id_or_member_epoch(NOT_A_MEMBER)
        // Empty, not absent and not invented: a made-up member id is rejected
        // with UNKNOWN_MEMBER_ID.
        .with_member_id(StrBytes::from_static_str(""))
        .with_topics(
            by_topic
                .into_iter()
                .map(|(topic, partitions)| {
                    OffsetCommitRequestTopic::default()
                        .with_name(TopicName(StrBytes::from_string(topic)))
                        .with_partitions(partitions)
                })
                .collect(),
        );

    let response = cluster
        .send_to_coordinator(CoordinatorKind::Group, group_id, request)
        .await?;

    // Per-partition results, per rule 4: one partition rejected must not hide
    // eleven that committed.
    let mut out = Vec::new();
    for topic in response.topics {
        let name = topic.name.0.to_string();
        for partition in topic.partitions {
            let key = (name.clone(), partition.partition_index);
            match ErrorCode::from_code(partition.error_code) {
                Some(code) => out.push((key, Err(Error::from_code(code, None)))),
                None => out.push((key, Ok(()))),
            }
        }
    }
    Ok(out)
}

/// Read a group's committed positions.
pub(crate) async fn fetch(
    cluster: &Cluster,
    group_id: &str,
    partitions: &[(String, i32)],
) -> Result<HashMap<(String, i32), CommittedOffset>> {
    let version = cluster.negotiated_for::<OffsetFetchRequest>().await?;

    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
    for (topic, partition) in partitions {
        by_topic.entry(topic.clone()).or_default().push(*partition);
    }

    // The version-shaped trap: `group_id`/`topics` are v1-7 and `groups` is
    // v8+, and the codec rejects a field outside its own range rather than
    // ignoring it. Build one shape.
    let request = OffsetFetchRequest::default().with_require_stable(true);
    let request = if version >= OFFSET_FETCH_GROUPS_VERSION {
        request.with_groups(vec![
            OffsetFetchRequestGroup::default()
                .with_group_id(GroupId(StrBytes::from_string(group_id.to_owned())))
                .with_topics(Some(
                    by_topic
                        .into_iter()
                        .map(|(topic, partitions)| {
                            OffsetFetchRequestTopics::default()
                                .with_name(TopicName(StrBytes::from_string(topic)))
                                .with_partition_indexes(partitions)
                        })
                        .collect(),
                )),
        ])
    } else {
        request
            .with_group_id(GroupId(StrBytes::from_string(group_id.to_owned())))
            .with_topics(Some(
                by_topic
                    .into_iter()
                    .map(|(topic, partitions)| {
                        OffsetFetchRequestTopic::default()
                            .with_name(TopicName(StrBytes::from_string(topic)))
                            .with_partition_indexes(partitions)
                    })
                    .collect(),
            ))
    };

    let response = cluster
        .send_to_coordinator(CoordinatorKind::Group, group_id, request)
        .await?;

    let mut out = HashMap::new();

    // v8+ answers inside `groups`; older versions at the top level. Reading
    // only one shape yields an empty map that looks like "nothing committed".
    if let Some(group) = response.groups.first() {
        if let Some(code) = ErrorCode::from_code(group.error_code) {
            return Err(Error::from_code(code, None));
        }
        for topic in &group.topics {
            for partition in &topic.partitions {
                record(
                    &mut out,
                    &topic.name.0,
                    partition.partition_index,
                    partition.committed_offset,
                    partition.metadata.as_ref(),
                );
            }
        }
        return Ok(out);
    }

    if let Some(code) = ErrorCode::from_code(response.error_code) {
        return Err(Error::from_code(code, None));
    }
    for topic in &response.topics {
        for partition in &topic.partitions {
            record(
                &mut out,
                &topic.name.0,
                partition.partition_index,
                partition.committed_offset,
                partition.metadata.as_ref(),
            );
        }
    }
    Ok(out)
}

/// `-1` means "nothing committed", which is not an offset and must not be
/// stored as one — a consumer that seeks to -1 reads the whole partition.
fn record(
    out: &mut HashMap<(String, i32), CommittedOffset>,
    topic: &str,
    partition: i32,
    offset: i64,
    metadata: Option<&StrBytes>,
) {
    if offset < 0 {
        return;
    }
    out.insert(
        (topic.to_owned(), partition),
        CommittedOffset {
            offset,
            metadata: metadata.map(std::string::ToString::to_string),
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_non_member_sentinel_is_minus_one_for_both_protocols() {
        // Classic spells it `generation_id`, KIP-848 spells it `member_epoch`,
        // and they share one wire field. This is the one case where the two
        // agree, and M17/M18 are where they stop agreeing.
        assert_eq!(NOT_A_MEMBER, -1);
    }

    #[test]
    fn nothing_committed_is_not_an_offset() {
        let mut out = HashMap::new();
        record(&mut out, "t", 0, -1, None);
        assert!(
            out.is_empty(),
            "-1 means no commit; storing it makes a consumer re-read the \
             whole partition"
        );

        record(&mut out, "t", 0, 0, None);
        assert_eq!(out.len(), 1, "offset zero is a real commit");
    }

    #[test]
    fn metadata_survives_the_round_trip_shape() {
        let mut out = HashMap::new();
        record(
            &mut out,
            "t",
            3,
            42,
            Some(&StrBytes::from_static_str("mine")),
        );
        assert_eq!(
            out.get(&("t".to_owned(), 3)),
            Some(&CommittedOffset {
                offset: 42,
                metadata: Some("mine".to_owned()),
            })
        );
    }
}
