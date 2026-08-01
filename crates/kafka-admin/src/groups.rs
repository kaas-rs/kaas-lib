//! Groups — all four kinds of them.
//!
//! # Four kinds, three describable
//!
//! Kafka 4.x has classic groups, consumer groups (KIP-848), share groups
//! (KIP-932) and streams groups (KIP-1071). They are *different RPCs* with
//! *different response shapes*, and flattening them into one struct throws away
//! the fields a UI needs — a KIP-848 member has an epoch and a target
//! assignment; a classic member has opaque assignment bytes and a generation.
//!
//! `kafka-protocol` 0.17 has no schema for `StreamsGroupDescribe` at all, so a
//! streams group can be *listed* and not described. That is not a corner case:
//! any 4.1+ cluster running Kafka Streams has them, and a group list that
//! hard-fails on one is a group list that hard-fails on most real clusters. So
//! [`GroupDescription::Unrecognized`] exists, it carries the group type the
//! broker reported, and it is a successful description of an undescribable
//! group rather than an error.
//!
//! # The offset-reset trap
//!
//! Committing an offset as a non-member differs by protocol. A classic group
//! wants `generation_id = -1`; a KIP-848 consumer group wants
//! `member_epoch = -1`. Both live in the same wire field
//! (`generation_id_or_member_epoch`), so sending the wrong one type-checks,
//! encodes, and comes back `ILLEGAL_GENERATION` against exactly one of the two
//! group types — invisible if the fixture only covers one.

use std::collections::HashMap;

use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_conn::protocol::messages::offset_delete_request::{
    OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
};
use kafka_conn::protocol::messages::offset_fetch_request::{
    OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
use kafka_conn::protocol::messages::{
    ConsumerGroupDescribeRequest, DeleteGroupsRequest, DescribeGroupsRequest, GroupId,
    ListGroupsRequest, OffsetCommitRequest, OffsetDeleteRequest, OffsetFetchRequest,
    ShareGroupDescribeRequest, TopicName,
};
use kafka_conn::{ApiKey, Error, ErrorCode, Result};
use kafka_meta::CoordinatorKind;

use crate::Admin;
use crate::types::PerItem;

/// A group's state, as the broker names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupState {
    /// No members and no committed offsets to speak of.
    Empty,
    /// Members are joining or leaving.
    PreparingRebalance,
    /// Waiting for the leader's assignment.
    CompletingRebalance,
    /// Steady state.
    Stable,
    /// Being removed.
    Dead,
    /// A state this build does not name.
    Other(String),
}

impl GroupState {
    /// Parse the broker's string.
    pub fn parse(value: &str) -> Self {
        match value {
            "Empty" => GroupState::Empty,
            "PreparingRebalance" => GroupState::PreparingRebalance,
            "CompletingRebalance" => GroupState::CompletingRebalance,
            "Stable" => GroupState::Stable,
            "Dead" => GroupState::Dead,
            other => GroupState::Other(other.to_owned()),
        }
    }

    /// Whether the group has no live members.
    ///
    /// The precondition for resetting offsets: committing on behalf of a group
    /// with a live member means the member overwrites it on its next commit,
    /// and the reset silently does nothing.
    pub fn is_empty(&self) -> bool {
        matches!(self, GroupState::Empty | GroupState::Dead)
    }
}

/// One row of `ListGroups`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupListing {
    /// Group id.
    pub group_id: String,
    /// The group's state.
    pub state: GroupState,
    /// The group type the broker reported: `classic`, `consumer`, `share`,
    /// `streams`, or something newer. Empty on brokers too old to report it.
    pub group_type: String,
    /// The protocol type — `consumer`, `connect`, and so on.
    pub protocol_type: String,
}

impl GroupListing {
    /// Whether this build can describe this group.
    pub fn describable(&self) -> bool {
        matches!(
            self.group_type.to_ascii_lowercase().as_str(),
            "" | "classic" | "consumer" | "share"
        )
    }
}

/// A member of a classic group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassicGroupMember {
    /// Member id assigned by the coordinator.
    pub member_id: String,
    /// Static membership id, when the member uses one.
    pub group_instance_id: Option<String>,
    /// The member's own client id.
    pub client_id: String,
    /// Where it connected from.
    pub client_host: String,
    /// The assignment, still encoded.
    ///
    /// Deliberately not decoded: the payload's format is the *assignor's*
    /// business, not the protocol's, and a custom assignor can put anything
    /// here. Handing back bytes is honest; guessing is not.
    pub assignment: bytes::Bytes,
}

/// A member of a KIP-848 consumer group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMember {
    /// Member id.
    pub member_id: String,
    /// Static membership id.
    pub instance_id: Option<String>,
    /// Rack, for rack-aware assignment.
    pub rack_id: Option<String>,
    /// The member's epoch.
    pub member_epoch: i32,
    /// Client id.
    pub client_id: String,
    /// Client host.
    pub client_host: String,
    /// Topics this member subscribed to.
    pub subscribed_topics: Vec<String>,
    /// Current assignment.
    pub assignment: Vec<(String, Vec<i32>)>,
    /// Assignment the coordinator is moving towards, which differs from
    /// `assignment` mid-rebalance and is how a UI shows one in progress.
    pub target_assignment: Vec<(String, Vec<i32>)>,
}

/// A member of a KIP-932 share group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupMember {
    /// Member id.
    pub member_id: String,
    /// Rack.
    pub rack_id: Option<String>,
    /// Member epoch.
    pub member_epoch: i32,
    /// Client id.
    pub client_id: String,
    /// Client host.
    pub client_host: String,
    /// Topics this member subscribed to.
    pub subscribed_topics: Vec<String>,
    /// Current assignment.
    pub assignment: Vec<(String, Vec<i32>)>,
}

/// A described group.
///
/// The distinction between kinds is *preserved*, not flattened: each variant
/// carries the fields its protocol actually has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupDescription {
    /// A classic (pre-KIP-848) group.
    Classic {
        /// Group id.
        group_id: String,
        /// State.
        state: GroupState,
        /// Protocol type — `consumer`, `connect`, …
        protocol_type: String,
        /// The assignor the group agreed on.
        protocol: String,
        /// Members.
        members: Vec<ClassicGroupMember>,
    },
    /// A KIP-848 consumer group.
    Consumer {
        /// Group id.
        group_id: String,
        /// State.
        state: GroupState,
        /// Group epoch.
        group_epoch: i32,
        /// Epoch of the assignment currently in force.
        assignment_epoch: i32,
        /// Server-side assignor name.
        assignor: String,
        /// Members.
        members: Vec<ConsumerGroupMember>,
    },
    /// A KIP-932 share group.
    Share {
        /// Group id.
        group_id: String,
        /// State.
        state: GroupState,
        /// Group epoch.
        group_epoch: i32,
        /// Epoch of the assignment currently in force.
        assignment_epoch: i32,
        /// Assignor name.
        assignor: String,
        /// Members.
        members: Vec<ShareGroupMember>,
    },
    /// A group whose type this build has no schema for.
    ///
    /// Streams groups (KIP-1071) are the live example: they list on any 4.1+
    /// broker running Kafka Streams and `kafka-protocol` 0.17 cannot describe
    /// them. This is a *successful* description of an undescribable group —
    /// the group exists, it is listed, and the UI can say what it is.
    Unrecognized {
        /// Group id.
        group_id: String,
        /// The group type the broker reported.
        group_type: String,
        /// The state the listing reported, which is available even when the
        /// describe RPC is not.
        state: GroupState,
    },
}

impl GroupDescription {
    /// The group's id, whatever kind it is.
    pub fn group_id(&self) -> &str {
        match self {
            GroupDescription::Classic { group_id, .. }
            | GroupDescription::Consumer { group_id, .. }
            | GroupDescription::Share { group_id, .. }
            | GroupDescription::Unrecognized { group_id, .. } => group_id,
        }
    }

    /// The group's state.
    pub fn state(&self) -> &GroupState {
        match self {
            GroupDescription::Classic { state, .. }
            | GroupDescription::Consumer { state, .. }
            | GroupDescription::Share { state, .. }
            | GroupDescription::Unrecognized { state, .. } => state,
        }
    }

    /// How many members the group has, or `None` when we cannot tell.
    pub fn member_count(&self) -> Option<usize> {
        match self {
            GroupDescription::Classic { members, .. } => Some(members.len()),
            GroupDescription::Consumer { members, .. } => Some(members.len()),
            GroupDescription::Share { members, .. } => Some(members.len()),
            GroupDescription::Unrecognized { .. } => None,
        }
    }
}

/// A committed offset to write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetReset {
    /// Topic.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// Offset to commit.
    pub offset: i64,
    /// Optional metadata string stored alongside.
    pub metadata: Option<String>,
}

impl OffsetReset {
    /// A reset with no metadata.
    pub fn new(topic: impl Into<String>, partition: i32, offset: i64) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
            metadata: None,
        }
    }
}

/// A committed offset read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOffset {
    /// The committed offset.
    pub offset: i64,
    /// Leader epoch at commit time.
    pub leader_epoch: Option<i32>,
    /// Metadata the committer stored.
    pub metadata: Option<String>,
}

/// The sentinel a *classic* group wants in `generation_id_or_member_epoch` when
/// the committer is not a member.
const CLASSIC_NON_MEMBER_GENERATION: i32 = -1;
/// The sentinel a *KIP-848 consumer* group wants in the same field.
///
/// Numerically identical to the classic one, which is exactly why this is a
/// trap: the two constants agree today and are not the same concept, and a
/// future protocol revision is free to move one of them.
const CONSUMER_NON_MEMBER_EPOCH: i32 = -1;

impl Admin {
    /// List groups.
    ///
    /// Never fails because of one undescribable group: the listing carries the
    /// type, and deciding what to do with it is [`Admin::describe_groups`]'s
    /// problem.
    pub async fn list_groups(&self) -> Result<Vec<GroupListing>> {
        self.list_groups_filtered(&[], &[]).await
    }

    /// List groups, filtered by state and/or type.
    pub async fn list_groups_filtered(
        &self,
        states: &[&str],
        types: &[&str],
    ) -> Result<Vec<GroupListing>> {
        let request = ListGroupsRequest::default()
            .with_states_filter(
                states
                    .iter()
                    .map(|s| StrBytes::from_string((*s).to_owned()))
                    .collect(),
            )
            .with_types_filter(
                types
                    .iter()
                    .map(|t| StrBytes::from_string((*t).to_owned()))
                    .collect(),
            );

        // ListGroups answers per broker, and each broker knows the groups it
        // coordinates. Asking one broker returns a fraction of the cluster's
        // groups, which looks like groups disappearing at random.
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
                    for group in response.groups {
                        listings.push(GroupListing {
                            group_id: group.group_id.0.to_string(),
                            state: GroupState::parse(group.group_state.as_str()),
                            group_type: group.group_type.to_string(),
                            protocol_type: group.protocol_type.to_string(),
                        });
                    }
                }
                Err(error) => last_error = Some(error),
            }
        }

        match (answered, last_error) {
            (false, Some(error)) => Err(error),
            _ => {
                listings.sort_by(|a, b| a.group_id.cmp(&b.group_id));
                listings.dedup_by(|a, b| a.group_id == b.group_id);
                Ok(listings)
            }
        }
    }

    /// Describe groups, dispatching on each group's type.
    ///
    /// Groups whose type this build cannot describe come back as
    /// [`GroupDescription::Unrecognized`] — successfully.
    pub async fn describe_groups(
        &self,
        group_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<PerItem<String, GroupDescription>> {
        let group_ids: Vec<String> = group_ids.into_iter().map(Into::into).collect();
        if group_ids.is_empty() {
            return Ok(Vec::new());
        }

        // The listing is what tells us which RPC to use. Without it we would
        // have to guess, and guessing wrong on a share group means
        // DescribeGroups answers with an empty classic group rather than an
        // error — a wrong answer that looks like a right one.
        let listings: HashMap<String, GroupListing> = self
            .list_groups()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|listing| (listing.group_id.clone(), listing))
            .collect();

        let mut classic = Vec::new();
        let mut consumer = Vec::new();
        let mut share = Vec::new();
        let mut results: PerItem<String, GroupDescription> = Vec::new();

        for group_id in group_ids {
            let listing = listings.get(&group_id);
            match listing
                .map(|l| l.group_type.to_ascii_lowercase())
                .as_deref()
            {
                Some("consumer") => consumer.push(group_id),
                Some("share") => share.push(group_id),
                // No listing means the group does not exist, or the broker is
                // too old to report a type. DescribeGroups is the right answer
                // in both cases: it is the api that reports GROUP_ID_NOT_FOUND.
                Some("classic") | Some("") | None => classic.push(group_id),
                Some(other) => {
                    let state = listing
                        .map(|l| l.state.clone())
                        .unwrap_or(GroupState::Other(String::new()));
                    results.push((
                        group_id.clone(),
                        Ok(GroupDescription::Unrecognized {
                            group_id,
                            group_type: other.to_owned(),
                            state,
                        }),
                    ));
                }
            }
        }

        results.extend(self.describe_classic_groups(classic).await?);
        results.extend(self.describe_consumer_groups(consumer, &listings).await?);
        results.extend(self.describe_share_groups(share, &listings).await?);
        Ok(results)
    }

    async fn describe_classic_groups(
        &self,
        group_ids: Vec<String>,
    ) -> Result<PerItem<String, GroupDescription>> {
        let mut results = Vec::new();
        for group_id in group_ids {
            let request = DescribeGroupsRequest::default()
                .with_groups(vec![GroupId(StrBytes::from_string(group_id.clone()))])
                .with_include_authorized_operations(false);
            let outcome = match self
                .cluster()
                .send_to_coordinator(CoordinatorKind::Group, &group_id, request)
                .await
            {
                Ok(response) => match response.groups.into_iter().next() {
                    Some(group) => match ErrorCode::from_code(group.error_code) {
                        Some(code) => Err(Error::from_code(
                            code,
                            group.error_message.map(|m| m.to_string()),
                        )),
                        None => Ok(GroupDescription::Classic {
                            group_id: group.group_id.0.to_string(),
                            state: GroupState::parse(group.group_state.as_str()),
                            protocol_type: group.protocol_type.to_string(),
                            protocol: group.protocol_data.to_string(),
                            members: group
                                .members
                                .into_iter()
                                .map(|member| ClassicGroupMember {
                                    member_id: member.member_id.to_string(),
                                    group_instance_id: member
                                        .group_instance_id
                                        .map(|id| id.to_string()),
                                    client_id: member.client_id.to_string(),
                                    client_host: member.client_host.to_string(),
                                    assignment: member.member_assignment,
                                })
                                .collect(),
                        }),
                    },
                    None => Err(Error::from_code(
                        ErrorCode::GroupIdNotFound,
                        Some(group_id.clone()),
                    )),
                },
                Err(error) => Err(error),
            };
            results.push((group_id, outcome));
        }
        Ok(results)
    }

    async fn describe_consumer_groups(
        &self,
        group_ids: Vec<String>,
        listings: &HashMap<String, GroupListing>,
    ) -> Result<PerItem<String, GroupDescription>> {
        let mut results = Vec::new();
        for group_id in group_ids {
            if !self.supports(ApiKey::ConsumerGroupDescribe).await {
                results.push((
                    group_id.clone(),
                    Ok(unrecognized(&group_id, "consumer", listings)),
                ));
                continue;
            }

            let request = ConsumerGroupDescribeRequest::default()
                .with_group_ids(vec![GroupId(StrBytes::from_string(group_id.clone()))])
                .with_include_authorized_operations(false);
            let outcome = match self
                .cluster()
                .send_to_coordinator(CoordinatorKind::Group, &group_id, request)
                .await
            {
                Ok(response) => match response.groups.into_iter().next() {
                    Some(group) => match ErrorCode::from_code(group.error_code) {
                        Some(code) => Err(Error::from_code(
                            code,
                            group.error_message.map(|m| m.to_string()),
                        )),
                        None => Ok(GroupDescription::Consumer {
                            group_id: group.group_id.0.to_string(),
                            state: GroupState::parse(group.group_state.as_str()),
                            group_epoch: group.group_epoch,
                            assignment_epoch: group.assignment_epoch,
                            assignor: group.assignor_name.to_string(),
                            members: group
                                .members
                                .into_iter()
                                .map(|member| ConsumerGroupMember {
                                    member_id: member.member_id.to_string(),
                                    instance_id: member.instance_id.map(|id| id.to_string()),
                                    rack_id: member.rack_id.map(|id| id.to_string()),
                                    member_epoch: member.member_epoch,
                                    client_id: member.client_id.to_string(),
                                    client_host: member.client_host.to_string(),
                                    subscribed_topics: member
                                        .subscribed_topic_names
                                        .iter()
                                        .map(|t| t.0.to_string())
                                        .collect(),
                                    assignment: member
                                        .assignment
                                        .topic_partitions
                                        .iter()
                                        .map(|tp| {
                                            (tp.topic_name.0.to_string(), tp.partitions.clone())
                                        })
                                        .collect(),
                                    target_assignment: member
                                        .target_assignment
                                        .topic_partitions
                                        .iter()
                                        .map(|tp| {
                                            (tp.topic_name.0.to_string(), tp.partitions.clone())
                                        })
                                        .collect(),
                                })
                                .collect(),
                        }),
                    },
                    None => Err(Error::from_code(
                        ErrorCode::GroupIdNotFound,
                        Some(group_id.clone()),
                    )),
                },
                Err(error) => Err(error),
            };
            results.push((group_id, outcome));
        }
        Ok(results)
    }

    async fn describe_share_groups(
        &self,
        group_ids: Vec<String>,
        listings: &HashMap<String, GroupListing>,
    ) -> Result<PerItem<String, GroupDescription>> {
        let mut results = Vec::new();
        for group_id in group_ids {
            if !self.supports(ApiKey::ShareGroupDescribe).await {
                // A share group on a broker that does not offer the describe
                // api — early access turned off, say. Undescribable, not
                // broken.
                results.push((
                    group_id.clone(),
                    Ok(unrecognized(&group_id, "share", listings)),
                ));
                continue;
            }

            let request = ShareGroupDescribeRequest::default()
                .with_group_ids(vec![GroupId(StrBytes::from_string(group_id.clone()))])
                .with_include_authorized_operations(false);
            let outcome = match self
                .cluster()
                .send_to_coordinator(CoordinatorKind::Group, &group_id, request)
                .await
            {
                Ok(response) => match response.groups.into_iter().next() {
                    Some(group) => match ErrorCode::from_code(group.error_code) {
                        Some(code) => Err(Error::from_code(
                            code,
                            group.error_message.map(|m| m.to_string()),
                        )),
                        None => Ok(GroupDescription::Share {
                            group_id: group.group_id.0.to_string(),
                            state: GroupState::parse(group.group_state.as_str()),
                            group_epoch: group.group_epoch,
                            assignment_epoch: group.assignment_epoch,
                            assignor: group.assignor_name.to_string(),
                            members: group
                                .members
                                .into_iter()
                                .map(|member| ShareGroupMember {
                                    member_id: member.member_id.to_string(),
                                    rack_id: member.rack_id.map(|id| id.to_string()),
                                    member_epoch: member.member_epoch,
                                    client_id: member.client_id.to_string(),
                                    client_host: member.client_host.to_string(),
                                    subscribed_topics: member
                                        .subscribed_topic_names
                                        .iter()
                                        .map(|t| t.0.to_string())
                                        .collect(),
                                    assignment: member
                                        .assignment
                                        .topic_partitions
                                        .iter()
                                        .map(|tp| {
                                            (tp.topic_name.0.to_string(), tp.partitions.clone())
                                        })
                                        .collect(),
                                })
                                .collect(),
                        }),
                    },
                    None => Err(Error::from_code(
                        ErrorCode::GroupIdNotFound,
                        Some(group_id.clone()),
                    )),
                },
                Err(error) => Err(error),
            };
            results.push((group_id, outcome));
        }
        Ok(results)
    }

    /// Fetch a group's committed offsets.
    ///
    /// `OffsetFetch`, never a scan of `__consumer_offsets`: the internal
    /// format is not a stable interface and has changed shape more than once.
    pub async fn fetch_offsets(
        &self,
        group_id: &str,
        partitions: Option<Vec<(String, Vec<i32>)>>,
    ) -> Result<PerItem<(String, i32), CommittedOffset>> {
        // The request changed shape at v8: `group_id` and `topics` are
        // versions 1-7, `groups` is 8+, and the codec rejects a field set
        // outside its own range. A modern broker negotiates v8 or above, so
        // building the old shape unconditionally means offsets never load at
        // all.
        let version = self.negotiated_for::<OffsetFetchRequest>().await?;
        let request = OffsetFetchRequest::default().with_require_stable(true);
        let request = if version >= 8 {
            let topics = partitions.map(|topics| {
                topics
                    .into_iter()
                    .map(|(name, indexes)| {
                        OffsetFetchRequestTopics::default()
                            .with_name(TopicName(StrBytes::from_string(name)))
                            .with_partition_indexes(indexes)
                    })
                    .collect()
            });
            request.with_groups(vec![
                OffsetFetchRequestGroup::default()
                    .with_group_id(GroupId(StrBytes::from_string(group_id.to_owned())))
                    .with_topics(topics),
            ])
        } else {
            let topics = partitions.map(|topics| {
                topics
                    .into_iter()
                    .map(|(name, indexes)| {
                        OffsetFetchRequestTopic::default()
                            .with_name(TopicName(StrBytes::from_string(name)))
                            .with_partition_indexes(indexes)
                    })
                    .collect()
            });
            request
                .with_group_id(GroupId(StrBytes::from_string(group_id.to_owned())))
                .with_topics(topics)
        };
        let response = self
            .cluster()
            .send_to_coordinator(CoordinatorKind::Group, group_id, request)
            .await?;

        if let Some(code) = ErrorCode::from_code(response.error_code) {
            return Err(Error::from_code(code, Some(group_id.to_owned())));
        }

        // v8+ nests everything under `groups`; older versions use the
        // top-level `topics`. Reading only one shape silently reports a group
        // with no committed offsets.
        let mut results: PerItem<(String, i32), CommittedOffset> = Vec::new();
        for group in &response.groups {
            if let Some(code) = ErrorCode::from_code(group.error_code) {
                return Err(Error::from_code(code, Some(group_id.to_owned())));
            }
            for topic in &group.topics {
                let name = topic.name.0.to_string();
                for partition in &topic.partitions {
                    results.push((
                        (name.clone(), partition.partition_index),
                        to_committed(
                            partition.error_code,
                            partition.committed_offset,
                            partition.committed_leader_epoch,
                            partition.metadata.as_ref().map(|m| m.to_string()),
                        ),
                    ));
                }
            }
        }
        if results.is_empty() {
            for topic in &response.topics {
                let name = topic.name.0.to_string();
                for partition in &topic.partitions {
                    results.push((
                        (name.clone(), partition.partition_index),
                        to_committed(
                            partition.error_code,
                            partition.committed_offset,
                            partition.committed_leader_epoch,
                            partition.metadata.as_ref().map(|m| m.to_string()),
                        ),
                    ));
                }
            }
        }
        Ok(results)
    }

    /// Reset a group's committed offsets.
    ///
    /// Refuses unless the group is empty. The broker will happily accept a
    /// commit for a group with a live member and then let that member overwrite
    /// it seconds later, so a reset that "succeeds" and does nothing is the
    /// default behaviour unless something checks — and the operator, watching
    /// the lag not move, has nothing to go on.
    pub async fn reset_offsets(
        &self,
        group_id: &str,
        offsets: impl IntoIterator<Item = OffsetReset>,
    ) -> Result<PerItem<(String, i32), ()>> {
        let offsets: Vec<OffsetReset> = offsets.into_iter().collect();
        if offsets.is_empty() {
            return Ok(Vec::new());
        }

        let description = self
            .describe_groups([group_id])
            .await?
            .into_iter()
            .next()
            .map(|(_, result)| result)
            .transpose()?;

        let description = description.ok_or_else(|| {
            Error::from_code(ErrorCode::GroupIdNotFound, Some(group_id.to_owned()))
        })?;

        if !description.state().is_empty() {
            return Err(Error::InvalidRequest(format!(
                "group {group_id} is {:?} with {} member(s); offsets can only be reset on an \
                 empty group, otherwise a live member overwrites them on its next commit",
                description.state(),
                description.member_count().unwrap_or_default()
            )));
        }

        // The trap. Same field, two protocols, and the value the *other* kind
        // wants comes back ILLEGAL_GENERATION.
        let generation_or_epoch = match &description {
            GroupDescription::Classic { .. } => CLASSIC_NON_MEMBER_GENERATION,
            GroupDescription::Consumer { .. } => CONSUMER_NON_MEMBER_EPOCH,
            GroupDescription::Share { .. } | GroupDescription::Unrecognized { .. } => {
                return Err(Error::Unsupported(format!(
                    "offsets for a {} group are not committed through OffsetCommit",
                    match &description {
                        GroupDescription::Share { .. } => "share",
                        _ => "streams or unrecognised",
                    }
                )));
            }
        };

        let mut by_topic: HashMap<String, Vec<OffsetCommitRequestPartition>> = HashMap::new();
        for reset in &offsets {
            by_topic.entry(reset.topic.clone()).or_default().push(
                OffsetCommitRequestPartition::default()
                    .with_partition_index(reset.partition)
                    .with_committed_offset(reset.offset)
                    .with_committed_leader_epoch(-1)
                    .with_committed_metadata(
                        reset
                            .metadata
                            .as_ref()
                            .map(|m| StrBytes::from_string(m.clone())),
                    ),
            );
        }

        let request = OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group_id.to_owned())))
            .with_generation_id_or_member_epoch(generation_or_epoch)
            // An empty member id is what a non-member commit uses; a made-up
            // one is rejected as UNKNOWN_MEMBER_ID.
            .with_member_id(StrBytes::from_static_str(""))
            .with_topics(
                by_topic
                    .into_iter()
                    .map(|(name, partitions)| {
                        OffsetCommitRequestTopic::default()
                            .with_name(TopicName(StrBytes::from_string(name)))
                            .with_partitions(partitions)
                    })
                    .collect(),
            );

        let response = self
            .cluster()
            .send_to_coordinator(CoordinatorKind::Group, group_id, request)
            .await?;

        Ok(response
            .topics
            .into_iter()
            .flat_map(|topic| {
                let name = topic.name.0.to_string();
                topic.partitions.into_iter().map(move |partition| {
                    let outcome = match ErrorCode::from_code(partition.error_code) {
                        Some(code) => Err(Error::from_code(code, None)),
                        None => Ok(()),
                    };
                    ((name.clone(), partition.partition_index), outcome)
                })
            })
            .collect())
    }

    /// Delete a group's committed offsets for specific partitions.
    pub async fn delete_offsets(
        &self,
        group_id: &str,
        partitions: impl IntoIterator<Item = (String, i32)>,
    ) -> Result<PerItem<(String, i32), ()>> {
        let mut by_topic: HashMap<String, Vec<OffsetDeleteRequestPartition>> = HashMap::new();
        for (topic, partition) in partitions {
            by_topic
                .entry(topic)
                .or_default()
                .push(OffsetDeleteRequestPartition::default().with_partition_index(partition));
        }
        if by_topic.is_empty() {
            return Ok(Vec::new());
        }

        let request = OffsetDeleteRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group_id.to_owned())))
            .with_topics(
                by_topic
                    .into_iter()
                    .map(|(name, partitions)| {
                        OffsetDeleteRequestTopic::default()
                            .with_name(TopicName(StrBytes::from_string(name)))
                            .with_partitions(partitions)
                    })
                    .collect(),
            );

        let response = self
            .cluster()
            .send_to_coordinator(CoordinatorKind::Group, group_id, request)
            .await?;
        if let Some(code) = ErrorCode::from_code(response.error_code) {
            return Err(Error::from_code(code, Some(group_id.to_owned())));
        }

        Ok(response
            .topics
            .into_iter()
            .flat_map(|topic| {
                let name = topic.name.0.to_string();
                topic.partitions.into_iter().map(move |partition| {
                    let outcome = match ErrorCode::from_code(partition.error_code) {
                        Some(code) => Err(Error::from_code(code, None)),
                        None => Ok(()),
                    };
                    ((name.clone(), partition.partition_index), outcome)
                })
            })
            .collect())
    }

    /// Delete groups.
    pub async fn delete_groups(
        &self,
        group_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<PerItem<String, ()>> {
        let group_ids: Vec<String> = group_ids.into_iter().map(Into::into).collect();
        let mut results: PerItem<String, ()> = Vec::new();

        // Each group belongs to its own coordinator, so a batch cannot be one
        // request unless every group happens to coordinate on one broker.
        for group_id in group_ids {
            let request = DeleteGroupsRequest::default()
                .with_groups_names(vec![GroupId(StrBytes::from_string(group_id.clone()))]);
            let outcome = match self
                .cluster()
                .send_to_coordinator(CoordinatorKind::Group, &group_id, request)
                .await
            {
                Ok(response) => match response.results.into_iter().next() {
                    Some(result) => match ErrorCode::from_code(result.error_code) {
                        Some(code) => Err(Error::from_code(code, None)),
                        None => Ok(()),
                    },
                    None => Err(Error::from_code(
                        ErrorCode::GroupIdNotFound,
                        Some(group_id.clone()),
                    )),
                },
                Err(error) => Err(error),
            };
            results.push((group_id, outcome));
        }
        Ok(results)
    }
}

fn unrecognized(
    group_id: &str,
    group_type: &str,
    listings: &HashMap<String, GroupListing>,
) -> GroupDescription {
    GroupDescription::Unrecognized {
        group_id: group_id.to_owned(),
        group_type: group_type.to_owned(),
        state: listings
            .get(group_id)
            .map(|l| l.state.clone())
            .unwrap_or(GroupState::Other(String::new())),
    }
}

fn to_committed(
    error_code: i16,
    offset: i64,
    leader_epoch: i32,
    metadata: Option<String>,
) -> std::result::Result<CommittedOffset, Error> {
    match ErrorCode::from_code(error_code) {
        Some(code) => Err(Error::from_code(code, None)),
        // -1 means "this group has never committed here", which is not the
        // same as committing offset zero.
        None if offset < 0 => Err(Error::from_code(
            ErrorCode::UnknownTopicOrPartition,
            Some("no committed offset".to_owned()),
        )),
        None => Ok(CommittedOffset {
            offset,
            leader_epoch: Some(leader_epoch).filter(|e| *e >= 0),
            metadata,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_states_parse_and_keep_what_they_do_not_know() {
        assert_eq!(GroupState::parse("Stable"), GroupState::Stable);
        assert_eq!(
            GroupState::parse("Reconciling"),
            GroupState::Other("Reconciling".to_owned())
        );
        assert!(GroupState::Empty.is_empty());
        assert!(GroupState::Dead.is_empty());
        assert!(!GroupState::Stable.is_empty());
        assert!(!GroupState::Other("Reconciling".to_owned()).is_empty());
    }

    #[test]
    fn the_three_describable_types_are_recognised_and_streams_is_not() {
        let listing = |group_type: &str| GroupListing {
            group_id: "g".to_owned(),
            state: GroupState::Stable,
            group_type: group_type.to_owned(),
            protocol_type: "consumer".to_owned(),
        };
        assert!(listing("classic").describable());
        assert!(listing("consumer").describable());
        assert!(listing("share").describable());
        assert!(listing("").describable(), "an old broker reports no type");

        // The one we cannot describe. `kafka-protocol` 0.17 has no
        // StreamsGroupDescribe schema at all.
        assert!(!listing("streams").describable());
        assert!(!listing("something-from-kafka-5").describable());
    }

    #[test]
    fn an_undescribable_group_is_still_a_group() {
        let description = GroupDescription::Unrecognized {
            group_id: "word-count".to_owned(),
            group_type: "streams".to_owned(),
            state: GroupState::Stable,
        };
        assert_eq!(description.group_id(), "word-count");
        assert_eq!(description.state(), &GroupState::Stable);
        // `None` rather than zero: we do not know how many members it has, and
        // reporting zero would be a lie a UI renders as an idle group.
        assert_eq!(description.member_count(), None);
    }

    #[test]
    fn the_group_kinds_are_not_flattened() {
        // Each variant carries fields the others do not have. A single struct
        // would have to drop the epoch or invent an assignment for a classic
        // member; neither is a description.
        let classic = GroupDescription::Classic {
            group_id: "g".to_owned(),
            state: GroupState::Stable,
            protocol_type: "consumer".to_owned(),
            protocol: "range".to_owned(),
            members: vec![ClassicGroupMember {
                member_id: "m".to_owned(),
                group_instance_id: None,
                client_id: "c".to_owned(),
                client_host: "/127.0.0.1".to_owned(),
                assignment: bytes::Bytes::from_static(b"opaque"),
            }],
        };
        let consumer = GroupDescription::Consumer {
            group_id: "g".to_owned(),
            state: GroupState::Stable,
            group_epoch: 7,
            assignment_epoch: 7,
            assignor: "uniform".to_owned(),
            members: Vec::new(),
        };
        assert_eq!(classic.member_count(), Some(1));
        assert_eq!(consumer.member_count(), Some(0));
        assert_ne!(classic, consumer);
    }

    #[test]
    fn the_two_non_member_sentinels_are_tracked_separately() {
        // They happen to be equal today. Keeping them as two named constants is
        // the point: a future protocol revision moving one of them is then a
        // one-line change with an obvious blast radius, rather than a hunt
        // through call sites for a bare `-1`.
        assert_eq!(CLASSIC_NON_MEMBER_GENERATION, -1);
        assert_eq!(CONSUMER_NON_MEMBER_EPOCH, -1);
    }

    #[test]
    fn a_never_committed_offset_is_not_offset_zero() {
        let never = to_committed(0, -1, -1, None);
        assert!(never.is_err(), "-1 means never committed");
        let committed = to_committed(0, 0, -1, None).expect("offset zero is a real offset");
        assert_eq!(committed.offset, 0);
        assert_eq!(committed.leader_epoch, None);
    }
}
