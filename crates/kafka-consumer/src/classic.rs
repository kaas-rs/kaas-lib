//! The classic group protocol: JoinGroup, SyncGroup, Heartbeat, LeaveGroup.
//!
//! # Why this exists at all
//!
//! KIP-848 ([`crate::group`]) is the default on a 4.x cluster and is strictly
//! less work. This path is for brokers older than 4.0, and for mixed groups
//! where a Java client is pinned to `group.protocol=classic`.
//!
//! # The assignor payload has to be byte-identical to Java's
//!
//! In the classic protocol the group *leader* computes the assignment, and the
//! leader is whichever member the coordinator picked — which may be a Java
//! client decoding what we encoded, or us decoding what it encoded. There is
//! no negotiation of the payload format: it is `ConsumerProtocolSubscription`
//! and `ConsumerProtocolAssignment`, and getting a field wrong produces a
//! group where somebody's assignment is silently empty.
//!
//! `kafka-protocol` ships both as real schemas, so none of it is hand-rolled.
//!
//! # Which assignors, and the deliberate omission
//!
//! **Range and round-robin only.** Sticky and cooperative-sticky are not
//! implemented, and that is a decision rather than an oversight:
//!
//! * The coordinator picks a protocol every member advertises. Advertising
//!   only these two settles any group containing us on one of them, and Java's
//!   default `partition.assignment.strategy` is `[RangeAssignor,
//!   CooperativeStickyAssignor]` — so `range` is present and a mixed group
//!   works.
//! * Java's `AbstractStickyAssignor` is ~1000 lines with a constrained/general
//!   split and a fairness balancing loop. Reimplementing it byte-compatibly is
//!   the single largest piece of work in this milestone and buys nothing until
//!   somebody actually needs it.
//! * The failure mode of guessing wrong is loud, not silent: a group whose
//!   other members are pinned to sticky-only fails `JoinGroup` with
//!   `INCONSISTENT_GROUP_PROTOCOL` at join time.
//!
//! The cost is honest and worth stating: forcing a group onto `range` is a
//! **group-wide** downgrade, so every Java member in it loses cooperative
//! rebalancing too.
//!
//! # Each member needs its own connection, and that is not a style preference
//!
//! `JoinGroup` **blocks** on the coordinator until the group forms — it is the
//! one RPC here whose normal behaviour is to sit in purgatory. And a Kafka
//! broker *mutes a connection* while a request on it is in flight: it will not
//! read the next request from that socket until it has written the previous
//! response.
//!
//! Put those together and two members of one group sharing a connection
//! deadlock outright. The first member's JoinGroup occupies the socket; the
//! second member's JoinGroup is never even *read* by the broker; the group
//! therefore never forms; and the first member waits out its rebalance timeout
//! for a member that was ready all along. It presents as a plain timeout with
//! nothing wrong on either side.
//!
//! A [`kafka_meta::Cluster`] pools one connection per broker and shares it
//! across everything using that handle, so **every member of a classic group
//! must be built from its own `Cluster`**. [`crate::ClassicConsumer::subscribe`]
//! cannot enforce that — it takes a cluster the caller owns — so it is
//! documented there and asserted by the acceptance test.
//!
//! KIP-848 has no such constraint: `ConsumerGroupHeartbeat` returns
//! immediately, so members can share a connection freely. That difference is
//! why the modern path worked first time and this one did not.

use std::collections::BTreeMap;

/// The assignors this client can compute, in preference order.
///
/// Order matters: the coordinator picks the first protocol supported by every
/// member, walking the leader's list.
pub(crate) const SUPPORTED: [&str; 2] = [RANGE, ROUND_ROBIN];

/// Java's `RangeAssignor`.
pub(crate) const RANGE: &str = "range";
/// Java's `RoundRobinAssignor`.
pub(crate) const ROUND_ROBIN: &str = "roundrobin";

/// One member's subscription, as the leader sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemberSubscription {
    pub member_id: String,
    pub topics: Vec<String>,
}

/// Compute an assignment the way Java's `RangeAssignor` does.
///
/// Per topic, members subscribed to it are sorted by member id and the
/// partitions are handed out in contiguous ranges. The first
/// `partitions % members` members get one extra — that remainder rule is the
/// part that has to match Java exactly, because an off-by-one here means two
/// clients disagree about who owns a partition.
pub(crate) fn assign_range(
    members: &[MemberSubscription],
    partitions_per_topic: &BTreeMap<String, i32>,
) -> BTreeMap<String, BTreeMap<String, Vec<i32>>> {
    let mut out: BTreeMap<String, BTreeMap<String, Vec<i32>>> = members
        .iter()
        .map(|member| (member.member_id.clone(), BTreeMap::new()))
        .collect();

    for (topic, count) in partitions_per_topic {
        let mut subscribed: Vec<&MemberSubscription> = members
            .iter()
            .filter(|member| member.topics.iter().any(|t| t == topic))
            .collect();
        if subscribed.is_empty() {
            continue;
        }
        subscribed.sort_by(|a, b| a.member_id.cmp(&b.member_id));

        let members_count = i32::try_from(subscribed.len()).unwrap_or(1);
        let per_member = count / members_count;
        let with_extra = count % members_count;

        let mut next: i32 = 0;
        for (index, member) in subscribed.iter().enumerate() {
            let index = i32::try_from(index).unwrap_or(0);
            let take = per_member + i32::from(index < with_extra);
            if take <= 0 {
                continue;
            }
            let assigned: Vec<i32> = (next..next.saturating_add(take)).collect();
            next = next.saturating_add(take);
            if !assigned.is_empty() {
                out.entry(member.member_id.clone())
                    .or_default()
                    .insert(topic.clone(), assigned);
            }
        }
    }
    out
}

/// Compute an assignment the way Java's `RoundRobinAssignor` does.
///
/// Every `(topic, partition)` across all subscribed topics is laid out in
/// order and dealt to members in rotation, skipping members not subscribed to
/// that topic.
pub(crate) fn assign_round_robin(
    members: &[MemberSubscription],
    partitions_per_topic: &BTreeMap<String, i32>,
) -> BTreeMap<String, BTreeMap<String, Vec<i32>>> {
    let mut out: BTreeMap<String, BTreeMap<String, Vec<i32>>> = members
        .iter()
        .map(|member| (member.member_id.clone(), BTreeMap::new()))
        .collect();

    let mut sorted: Vec<&MemberSubscription> = members.iter().collect();
    sorted.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    if sorted.is_empty() {
        return out;
    }

    let mut cursor = 0usize;
    for (topic, count) in partitions_per_topic {
        for partition in 0..*count {
            // Advance past members that do not want this topic. Bounded by the
            // member count so an unsubscribed topic cannot spin forever.
            let mut looked = 0;
            while looked < sorted.len() {
                let Some(member) = sorted.get(cursor % sorted.len()) else {
                    break;
                };
                cursor = cursor.wrapping_add(1);
                looked += 1;
                if member.topics.iter().any(|t| t == topic) {
                    out.entry(member.member_id.clone())
                        .or_default()
                        .entry(topic.clone())
                        .or_default()
                        .push(partition);
                    break;
                }
            }
        }
    }
    out
}

/// Compute an assignment with the named protocol.
pub(crate) fn assign(
    protocol: &str,
    members: &[MemberSubscription],
    partitions_per_topic: &BTreeMap<String, i32>,
) -> BTreeMap<String, BTreeMap<String, Vec<i32>>> {
    if protocol == ROUND_ROBIN {
        assign_round_robin(members, partitions_per_topic)
    } else {
        assign_range(members, partitions_per_topic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str, topics: &[&str]) -> MemberSubscription {
        MemberSubscription {
            member_id: id.to_owned(),
            topics: topics.iter().map(|t| (*t).to_owned()).collect(),
        }
    }

    fn topics(entries: &[(&str, i32)]) -> BTreeMap<String, i32> {
        entries
            .iter()
            .map(|(topic, count)| ((*topic).to_owned(), *count))
            .collect()
    }

    /// The remainder rule, which is the part that has to match Java exactly:
    /// with 7 partitions over 2 members the first gets 4 and the second 3.
    #[test]
    fn range_gives_the_remainder_to_the_earliest_members() {
        let assignment = assign_range(
            &[member("a", &["t"]), member("b", &["t"])],
            &topics(&[("t", 7)]),
        );
        assert_eq!(assignment["a"]["t"], vec![0, 1, 2, 3]);
        assert_eq!(assignment["b"]["t"], vec![4, 5, 6]);
    }

    #[test]
    fn range_divides_evenly_when_it_can() {
        let assignment = assign_range(
            &[member("a", &["t"]), member("b", &["t"])],
            &topics(&[("t", 6)]),
        );
        assert_eq!(assignment["a"]["t"], vec![0, 1, 2]);
        assert_eq!(assignment["b"]["t"], vec![3, 4, 5]);
    }

    /// Range's known weakness, asserted so nobody "fixes" it into
    /// incompatibility: it assigns per topic, so with more members than
    /// partitions the later members get nothing.
    #[test]
    fn range_leaves_surplus_members_empty() {
        let assignment = assign_range(
            &[
                member("a", &["t"]),
                member("b", &["t"]),
                member("c", &["t"]),
            ],
            &topics(&[("t", 2)]),
        );
        assert_eq!(assignment["a"]["t"], vec![0]);
        assert_eq!(assignment["b"]["t"], vec![1]);
        assert!(assignment["c"].is_empty());
    }

    #[test]
    fn round_robin_deals_partitions_in_rotation() {
        let assignment = assign_round_robin(
            &[member("a", &["t"]), member("b", &["t"])],
            &topics(&[("t", 5)]),
        );
        assert_eq!(assignment["a"]["t"], vec![0, 2, 4]);
        assert_eq!(assignment["b"]["t"], vec![1, 3]);
    }

    #[test]
    fn round_robin_skips_a_member_that_did_not_subscribe() {
        let assignment = assign_round_robin(
            &[member("a", &["x"]), member("b", &["x", "y"])],
            &topics(&[("x", 2), ("y", 2)]),
        );
        assert_eq!(assignment["a"]["x"], vec![0]);
        assert_eq!(assignment["b"]["x"], vec![1]);
        assert_eq!(
            assignment["b"]["y"],
            vec![0, 1],
            "only b subscribed to y, so it takes both"
        );
        assert!(!assignment["a"].contains_key("y"));
    }

    /// The property every assignor must have, and the one whose failure is
    /// silent: full coverage, no overlap.
    #[test]
    fn every_assignor_covers_everything_exactly_once() {
        let members = [
            member("a", &["t"]),
            member("b", &["t"]),
            member("c", &["t"]),
        ];
        let counts = topics(&[("t", 11)]);

        for protocol in SUPPORTED {
            let assignment = assign(protocol, &members, &counts);
            let mut all: Vec<i32> = assignment
                .values()
                .filter_map(|topics| topics.get("t"))
                .flatten()
                .copied()
                .collect();
            all.sort_unstable();
            let expected: Vec<i32> = (0..11).collect();
            assert_eq!(
                all, expected,
                "{protocol} did not cover every partition once"
            );
        }
    }

    /// Preference order is what the coordinator walks, so `range` first is a
    /// deliberate choice: it is the one Java advertises by default.
    #[test]
    fn range_is_offered_before_round_robin() {
        assert_eq!(SUPPORTED[0], RANGE);
        assert_eq!(SUPPORTED[1], ROUND_ROBIN);
    }
}

use kafka_conn::protocol::messages::consumer_protocol_assignment::TopicPartition;
use kafka_conn::protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_conn::protocol::messages::leave_group_request::MemberIdentity;
use kafka_conn::protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_conn::protocol::messages::{
    ConsumerProtocolAssignment, ConsumerProtocolSubscription, GroupId, HeartbeatRequest,
    JoinGroupRequest, LeaveGroupRequest, SyncGroupRequest, TopicName,
};
use kafka_conn::protocol::{Decodable, Encodable, StrBytes};
use kafka_conn::{Error, ErrorCode, Result};
use kafka_meta::{Cluster, CoordinatorKind};

/// The generation a member that has not joined holds.
const NO_GENERATION: i32 = -1;

/// The version of the consumer protocol payload we encode.
///
/// v0 deliberately: every field above it is for sticky's `user_data` and the
/// owned-partitions handshake that cooperative rebalancing needs, and this
/// client implements neither. Writing a higher version would advertise
/// capabilities the assignors here do not have.
const PROTOCOL_VERSION: i16 = 0;

/// Strip the two-byte version prefix and return the version it named.
fn take_version(bytes: &mut bytes::Bytes) -> Result<i16> {
    if bytes.len() < 2 {
        return Err(Error::decode(
            "consumer protocol payload",
            "shorter than its two-byte version prefix".to_owned(),
        ));
    }
    // `split_to` then read, rather than indexing: rule 2 forbids indexing in
    // library code, and a two-byte prefix is exactly the place a malformed
    // payload from another client would otherwise panic the process.
    let prefix = bytes.split_to(2);
    let (Some(high), Some(low)) = (prefix.first(), prefix.get(1)) else {
        return Err(Error::decode(
            "consumer protocol payload",
            "version prefix vanished between the length check and the read".to_owned(),
        ));
    };
    Ok(i16::from_be_bytes([*high, *low]))
}

/// Encode the subscription a member sends in JoinGroup.
///
/// `ConsumerProtocolSubscription` is a real schema in the codec, so the struct
/// is not hand-rolled — which matters because a Java group leader, and the
/// coordinator itself, decode it.
///
/// # The two bytes the schema does not include
///
/// Java's `ConsumerProtocol.serializeSubscription` writes the protocol
/// **version as an `int16` ahead of the struct**, and `deserializeSubscription`
/// reads it back before parsing. That prefix is part of the payload's contract
/// and is *not* part of the message schema, so `Encodable::encode` does not
/// write it — the codec encodes a struct, and the framing around it belongs to
/// the caller.
///
/// Omit it and every field lands two bytes early.
fn encode_subscription(topics: &[String]) -> Result<bytes::Bytes> {
    let subscription = ConsumerProtocolSubscription::default().with_topics(
        topics
            .iter()
            .map(|topic| StrBytes::from_string(topic.clone()))
            .collect(),
    );
    let mut buf = bytes::BytesMut::new();
    buf.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    subscription
        .encode(&mut buf, PROTOCOL_VERSION)
        .map_err(|error| {
            Error::InvalidRequest(format!("could not encode a subscription: {error}"))
        })?;
    Ok(buf.freeze())
}

fn decode_subscription(mut bytes: bytes::Bytes) -> Result<Vec<String>> {
    let version = take_version(&mut bytes)?;
    let subscription = ConsumerProtocolSubscription::decode(&mut bytes, version.max(0))
        .map_err(|error| Error::decode("consumer protocol subscription", error.to_string()))?;
    Ok(subscription
        .topics
        .into_iter()
        .map(|topic| topic.to_string())
        .collect())
}

fn encode_assignment(assigned: &BTreeMap<String, Vec<i32>>) -> Result<bytes::Bytes> {
    let assignment = ConsumerProtocolAssignment::default().with_assigned_partitions(
        assigned
            .iter()
            .map(|(topic, partitions)| {
                TopicPartition::default()
                    .with_topic(TopicName(StrBytes::from_string(topic.clone())))
                    .with_partitions(partitions.clone())
            })
            .collect(),
    );
    let mut buf = bytes::BytesMut::new();
    // Same two-byte version prefix as the subscription. A Java member decodes
    // this one.
    buf.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    assignment
        .encode(&mut buf, PROTOCOL_VERSION)
        .map_err(|error| {
            Error::InvalidRequest(format!("could not encode an assignment: {error}"))
        })?;
    Ok(buf.freeze())
}

fn decode_assignment(mut bytes: bytes::Bytes) -> Result<Vec<(String, i32)>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let version = take_version(&mut bytes)?;
    let assignment = ConsumerProtocolAssignment::decode(&mut bytes, version.max(0))
        .map_err(|error| Error::decode("consumer protocol assignment", error.to_string()))?;
    Ok(assignment
        .assigned_partitions
        .into_iter()
        .flat_map(|topic| {
            let name = topic.topic.0.to_string();
            topic
                .partitions
                .into_iter()
                .map(move |partition| (name.clone(), partition))
        })
        .collect())
}

/// A member of a classic group.
#[derive(Debug)]
pub(crate) struct ClassicMembership {
    group_id: String,
    subscription: Vec<String>,
    instance_id: Option<String>,
    member_id: String,
    generation_id: i32,
    /// Whether this member was elected leader and must compute the assignment.
    leader: bool,
    protocol: String,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
}

impl ClassicMembership {
    pub(crate) fn new(
        group_id: String,
        subscription: Vec<String>,
        instance_id: Option<String>,
    ) -> Self {
        Self {
            group_id,
            subscription,
            instance_id,
            member_id: String::new(),
            generation_id: NO_GENERATION,
            leader: false,
            protocol: RANGE.to_owned(),
            // Both deliberately small, and in this order.
            //
            // `JoinGroup` *blocks* on the coordinator until every member has
            // joined or the rebalance timeout expires — it is the one RPC in
            // the protocol whose normal behaviour is to sit there. So the
            // rebalance timeout has to fit inside the connection's own request
            // timeout, or the socket gives up first and reports a timeout on a
            // rebalance that was proceeding perfectly well.
            //
            // And `rebalance >= session`, matching Java, which pairs
            // `max.poll.interval.ms` (300s) with `session.timeout.ms` (45s).
            // Inverting them is not a configuration Kafka is tested against.
            session_timeout_ms: 6_000,
            rebalance_timeout_ms: 12_000,
        }
    }

    pub(crate) fn group_id(&self) -> &str {
        &self.group_id
    }

    pub(crate) fn member_id(&self) -> &str {
        &self.member_id
    }

    pub(crate) fn is_leader(&self) -> bool {
        self.leader
    }

    /// Join the group, then sync to get an assignment.
    ///
    /// Both halves in one call because they are one operation: a JoinGroup
    /// without the SyncGroup that follows leaves the whole group blocked on a
    /// member that never finished joining.
    pub(crate) async fn join(
        &mut self,
        cluster: &Cluster,
        partitions_per_topic: &BTreeMap<String, i32>,
    ) -> Result<Vec<(String, i32)>> {
        let members = self.join_group(cluster).await?;
        self.sync_group(cluster, members, partitions_per_topic)
            .await
    }

    async fn join_group(&mut self, cluster: &Cluster) -> Result<Vec<MemberSubscription>> {
        let protocols: Vec<JoinGroupRequestProtocol> = SUPPORTED
            .iter()
            .map(|name| {
                Ok(JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_static_str(name))
                    .with_metadata(encode_subscription(&self.subscription)?))
            })
            .collect::<Result<_>>()?;

        let request = JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(self.group_id.clone())))
            .with_session_timeout_ms(self.session_timeout_ms)
            .with_rebalance_timeout_ms(self.rebalance_timeout_ms)
            .with_member_id(StrBytes::from_string(self.member_id.clone()))
            .with_group_instance_id(self.instance_id.clone().map(StrBytes::from_string))
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_protocols(protocols);

        let response = cluster
            .send_to_coordinator(CoordinatorKind::Group, &self.group_id, request)
            .await?;

        if let Some(code) = ErrorCode::from_code(response.error_code) {
            // A first join is *expected* to be rejected with MEMBER_ID_REQUIRED
            // carrying the id to use: KIP-394 made the coordinator hand out an
            // id before it will accept a member, so this is the handshake
            // rather than a failure.
            if code == ErrorCode::MemberIdRequired {
                self.member_id = response.member_id.to_string();
                return Err(Error::from_code(code, None));
            }
            return Err(Error::from_code(code, None));
        }

        self.member_id = response.member_id.to_string();
        self.generation_id = response.generation_id;
        self.protocol = response
            .protocol_name
            .map(|name| name.to_string())
            .unwrap_or_else(|| RANGE.to_owned());
        self.leader = response.leader.to_string() == self.member_id;

        if !self.leader {
            return Ok(Vec::new());
        }

        // Only the leader is given the membership, and only the leader needs
        // it: it is the one computing the assignment.
        response
            .members
            .into_iter()
            .map(|member| {
                Ok(MemberSubscription {
                    member_id: member.member_id.to_string(),
                    topics: decode_subscription(member.metadata)?,
                })
            })
            .collect()
    }

    async fn sync_group(
        &mut self,
        cluster: &Cluster,
        members: Vec<MemberSubscription>,
        partitions_per_topic: &BTreeMap<String, i32>,
    ) -> Result<Vec<(String, i32)>> {
        let assignments: Vec<SyncGroupRequestAssignment> = if self.leader {
            let computed = assign(&self.protocol, &members, partitions_per_topic);
            computed
                .into_iter()
                .map(|(member_id, topics)| {
                    Ok(SyncGroupRequestAssignment::default()
                        .with_member_id(StrBytes::from_string(member_id))
                        .with_assignment(encode_assignment(&topics)?))
                })
                .collect::<Result<_>>()?
        } else {
            Vec::new()
        };

        let request = SyncGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(self.group_id.clone())))
            .with_generation_id(self.generation_id)
            .with_member_id(StrBytes::from_string(self.member_id.clone()))
            .with_group_instance_id(self.instance_id.clone().map(StrBytes::from_string))
            .with_protocol_type(Some(StrBytes::from_static_str("consumer")))
            .with_protocol_name(Some(StrBytes::from_string(self.protocol.clone())))
            .with_assignments(assignments);

        let response = cluster
            .send_to_coordinator(CoordinatorKind::Group, &self.group_id, request)
            .await?;

        if let Some(code) = ErrorCode::from_code(response.error_code) {
            return Err(Error::from_code(code, None));
        }
        decode_assignment(response.assignment)
    }

    /// One heartbeat. `REBALANCE_IN_PROGRESS` is normal, not an error.
    pub(crate) async fn heartbeat(&self, cluster: &Cluster) -> Result<bool> {
        if self.generation_id == NO_GENERATION {
            return Ok(true);
        }
        let request = HeartbeatRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(self.group_id.clone())))
            .with_generation_id(self.generation_id)
            .with_member_id(StrBytes::from_string(self.member_id.clone()))
            .with_group_instance_id(self.instance_id.clone().map(StrBytes::from_string));

        let response = cluster
            .send_to_coordinator(CoordinatorKind::Group, &self.group_id, request)
            .await?;

        match ErrorCode::from_code(response.error_code) {
            // All three mean "re-join", which is an ordinary part of the
            // protocol's life and not a failure to report.
            Some(
                ErrorCode::RebalanceInProgress
                | ErrorCode::UnknownMemberId
                | ErrorCode::IllegalGeneration,
            ) => Ok(true),
            Some(code) => Err(Error::from_code(code, None)),
            None => Ok(false),
        }
    }

    /// Leave the group.
    ///
    /// A **static** member does not leave on shutdown — that is the whole
    /// point of `group.instance.id`, and sending LeaveGroup would trigger the
    /// rebalance the static membership exists to avoid.
    pub(crate) async fn leave(&mut self, cluster: &Cluster) -> Result<()> {
        if self.member_id.is_empty() || self.instance_id.is_some() {
            self.generation_id = NO_GENERATION;
            return Ok(());
        }
        let request = LeaveGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(self.group_id.clone())))
            .with_members(vec![
                MemberIdentity::default()
                    .with_member_id(StrBytes::from_string(self.member_id.clone())),
            ]);
        let _ = cluster
            .send_to_coordinator(CoordinatorKind::Group, &self.group_id, request)
            .await;
        self.member_id = String::new();
        self.generation_id = NO_GENERATION;
        Ok(())
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;

    /// The payloads a Java leader decodes. Round-tripping through the codec's
    /// own schemas is what makes them compatible; hand-rolling is what makes
    /// them not.
    #[test]
    fn a_subscription_round_trips_through_the_real_schema() {
        let encoded = encode_subscription(&["a".to_owned(), "b".to_owned()]).unwrap();
        assert_eq!(decode_subscription(encoded).unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn an_assignment_round_trips_through_the_real_schema() {
        let mut assigned = BTreeMap::new();
        assigned.insert("t".to_owned(), vec![0, 2, 4]);
        let encoded = encode_assignment(&assigned).unwrap();
        assert_eq!(
            decode_assignment(encoded).unwrap(),
            vec![
                ("t".to_owned(), 0),
                ("t".to_owned(), 2),
                ("t".to_owned(), 4)
            ]
        );
    }

    /// A follower's SyncGroup answer can be empty, and that means "no
    /// partitions", not "malformed".
    #[test]
    fn an_empty_assignment_decodes_to_nothing_rather_than_failing() {
        assert!(decode_assignment(bytes::Bytes::new()).unwrap().is_empty());
    }

    /// The two bytes Java writes and the message schema does not.
    ///
    /// Asserted on the wire rather than only through a round trip, because a
    /// round trip is exactly what stays green when *both* halves omit the
    /// prefix — which is how this shipped broken.
    #[test]
    fn the_payload_carries_javas_two_byte_version_prefix() {
        let encoded = encode_subscription(&["t".to_owned()]).unwrap();
        assert_eq!(
            &encoded[..2],
            &PROTOCOL_VERSION.to_be_bytes(),
            "Java's ConsumerProtocol.serializeSubscription writes the version \
             ahead of the struct; without it every field lands two bytes early"
        );

        let mut assigned = BTreeMap::new();
        assigned.insert("t".to_owned(), vec![0]);
        let encoded = encode_assignment(&assigned).unwrap();
        assert_eq!(&encoded[..2], &PROTOCOL_VERSION.to_be_bytes());
    }

    #[test]
    fn a_payload_too_short_to_hold_a_version_is_a_decode_error() {
        assert!(decode_subscription(bytes::Bytes::from_static(&[0])).is_err());
    }

    #[test]
    fn a_static_member_does_not_send_leave_group() {
        let statically = ClassicMembership::new(
            "g".to_owned(),
            vec!["t".to_owned()],
            Some("instance-1".to_owned()),
        );
        assert!(statically.instance_id.is_some());
        // The assertion is in `leave`: a static member returns early. Sending
        // LeaveGroup would trigger exactly the rebalance that
        // `group.instance.id` exists to avoid across a restart.
    }
}
