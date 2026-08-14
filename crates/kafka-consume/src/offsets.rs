//! Committing and reading offsets, as a member or as nobody.
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
//! In the anonymous form the member id must be **empty**. A made-up one is
//! rejected with `UNKNOWN_MEMBER_ID`, which reads like a membership bug in a
//! client that deliberately has no membership.
//!
//! # A member must commit as itself
//!
//! The anonymous form is honoured **only while the group has no members** —
//! the coordinator rejects it with `UNKNOWN_MEMBER_ID` the moment anyone has
//! joined, precisely so a detached client cannot scribble over a live group's
//! positions. So a group member commits under its own identity
//! ([`CommitAs`]): its member id, its current epoch (KIP-848) or generation
//! (classic), and its instance id if it is static. Getting this wrong is
//! quiet in both directions — a member committing anonymously is refused
//! per partition with an error that reads like a membership bug, and an
//! auto-commit whose result nobody checks is refused silently.

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
use kafka_meta::{Cluster, RetryPolicy};

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

/// The membership a commit is made under.
///
/// `None` at the call site is the standalone consumer: empty member id,
/// epoch `-1`, honoured only while the group has no members. A member passes
/// what the coordinator knows it by — anything else is refused per partition
/// with `UNKNOWN_MEMBER_ID` or `STALE_MEMBER_EPOCH`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CommitAs<'a> {
    /// The member id as the coordinator currently knows it.
    pub member_id: &'a str,
    /// `member_epoch` under KIP-848, `generation_id` under classic — one
    /// wire field either way, exactly like the `-1` sentinel they share.
    pub epoch: i32,
    /// `group.instance.id`, for a static member.
    pub instance_id: Option<&'a str>,
}

/// Commit positions for a group, as a member or anonymously.
pub(crate) async fn commit(
    cluster: &Cluster,
    policy: RetryPolicy,
    group_id: &str,
    member: Option<CommitAs<'_>>,
    offsets: &HashMap<(String, i32), CommittedOffset>,
) -> Result<Vec<((String, i32), Result<()>)>> {
    if offsets.is_empty() {
        return Ok(Vec::new());
    }

    let request = commit_request(group_id, member, offsets);

    // `NOT_COORDINATOR` arrives per partition here rather than as a failed
    // round trip, so the routing layer's retry never sees it. It is a
    // whole-request condition even so — the coordinator is wrong for the
    // group, so every partition carries it — which is why re-asking is right
    // and why the first partition's code is enough to decide.
    let response =
        crate::coordinator::send_retrying(cluster, policy, group_id, request, |response| {
            response
                .topics
                .iter()
                .flat_map(|topic| topic.partitions.iter())
                .find_map(|partition| ErrorCode::from_code(partition.error_code))
        })
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

/// Build the commit request. Pure, so the identity handling stays testable:
/// the broker refuses a wrong identity per partition, which an ignored
/// auto-commit result turns into silence.
fn commit_request(
    group_id: &str,
    member: Option<CommitAs<'_>>,
    offsets: &HashMap<(String, i32), CommittedOffset>,
) -> OffsetCommitRequest {
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

    // Anonymous: empty member id — not absent and not invented, both of
    // which are rejected with UNKNOWN_MEMBER_ID.
    let (member_id, epoch, instance_id) = match member {
        Some(member) => (member.member_id, member.epoch, member.instance_id),
        None => ("", NOT_A_MEMBER, None),
    };

    OffsetCommitRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group_id.to_owned())))
        .with_generation_id_or_member_epoch(epoch)
        .with_member_id(StrBytes::from_string(member_id.to_owned()))
        .with_group_instance_id(instance_id.map(|id| StrBytes::from_string(id.to_owned())))
        .with_topics(
            by_topic
                .into_iter()
                .map(|(topic, partitions)| {
                    OffsetCommitRequestTopic::default()
                        .with_name(TopicName(StrBytes::from_string(topic)))
                        .with_partitions(partitions)
                })
                .collect(),
        )
}

/// Read a group's committed positions.
pub(crate) async fn fetch(
    cluster: &Cluster,
    policy: RetryPolicy,
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

    let response =
        crate::coordinator::send_retrying(cluster, policy, group_id, request, |response| {
            // v8+ answers inside `groups`, older versions at the top level —
            // the same two shapes the decoding below has to straddle.
            response
                .groups
                .first()
                .and_then(|group| ErrorCode::from_code(group.error_code))
                .or_else(|| ErrorCode::from_code(response.error_code))
        })
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

    /// The regression that reached CI as `UNKNOWN_MEMBER_ID` on every
    /// partition of a live group: a member's commit went out anonymously, and
    /// the coordinator only honours the anonymous form while the group is
    /// empty.
    #[test]
    fn a_member_commits_under_its_own_identity() {
        let mut offsets = HashMap::new();
        offsets.insert(
            ("t".to_owned(), 0),
            CommittedOffset {
                offset: 7,
                metadata: None,
            },
        );

        let request = commit_request(
            "g",
            Some(CommitAs {
                member_id: "member-uuid",
                epoch: 4,
                instance_id: None,
            }),
            &offsets,
        );
        assert_eq!(request.member_id.as_str(), "member-uuid");
        assert_eq!(request.generation_id_or_member_epoch, 4);
        assert_eq!(request.group_instance_id, None);
    }

    /// A static member also names its instance id, which is how the
    /// coordinator ties the commit to the parked membership.
    #[test]
    fn a_static_member_names_its_instance() {
        let mut offsets = HashMap::new();
        offsets.insert(
            ("t".to_owned(), 0),
            CommittedOffset {
                offset: 7,
                metadata: None,
            },
        );

        let request = commit_request(
            "g",
            Some(CommitAs {
                member_id: "member-uuid",
                epoch: 9,
                instance_id: Some("static-1"),
            }),
            &offsets,
        );
        assert_eq!(
            request.group_instance_id.as_ref().map(StrBytes::as_str),
            Some("static-1")
        );
    }

    /// The standalone consumer stays anonymous: empty id, epoch -1, no
    /// instance. This is the form the broker honours only for a group with
    /// no members, which is exactly what a standalone consumer's group is.
    #[test]
    fn a_non_member_commits_anonymously() {
        let mut offsets = HashMap::new();
        offsets.insert(
            ("t".to_owned(), 3),
            CommittedOffset {
                offset: 42,
                metadata: None,
            },
        );

        let request = commit_request("g", None, &offsets);
        assert_eq!(request.member_id.as_str(), "");
        assert_eq!(request.generation_id_or_member_epoch, NOT_A_MEMBER);
        assert_eq!(request.group_instance_id, None);
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
