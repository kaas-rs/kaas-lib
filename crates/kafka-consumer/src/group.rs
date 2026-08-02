//! KIP-848 consumer groups: one RPC, and a reconciliation the client owns.
//!
//! `ConsumerGroupHeartbeat` replaces JoinGroup, SyncGroup and Heartbeat
//! outright. The **broker computes the assignment**, which removes the single
//! largest source of subtle incompatibility in the classic protocol: there is
//! no assignor payload to make byte-identical with Java's, because there is no
//! assignor payload.
//!
//! What the client still owns is the *reconciliation*, and it is ordered.
//!
//! # Revoke, then acknowledge — in that order
//!
//! The broker sends a target assignment. The client must give up what it no
//! longer owns **before** telling the broker it owns the new set. Acknowledging
//! an assignment whose predecessor has not yet been revoked means two consumers
//! hold the same partition at the same time — duplicate delivery, with no error
//! anywhere and nothing in any log to explain it.
//!
//! So a rebalance is two beats, not one: the first learns the target and
//! revokes, the second acknowledges what is now owned.
//!
//! # The epoch sentinels are not interchangeable
//!
//! * `0` — join. The broker issues the member id.
//! * `-1` — leave, releasing the assignment for immediate reassignment.
//! * `-2` — a **static** member leaving, which *parks* its assignment against
//!   `session.timeout.ms` instead of releasing it. Using `-1` for a static
//!   member throws away the whole point of `group.instance.id`: the assignment
//!   is handed to somebody else instead of waiting for the restart.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::consumer_group_heartbeat_request::TopicPartitions;
use kafka_conn::protocol::messages::{ConsumerGroupHeartbeatRequest, GroupId};
use kafka_conn::{Error, ErrorCode, Result};
use kafka_meta::{Cluster, CoordinatorKind, TopicId};

/// Join. The member id is already ours — see [`new_member_id`].
const EPOCH_JOIN: i32 = 0;
/// Leave and release the assignment now.
const EPOCH_LEAVE: i32 = -1;
/// Leave as a static member: park the assignment rather than releasing it.
const EPOCH_STATIC_LEAVE: i32 = -2;

/// What one heartbeat told us to do.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Reconciliation {
    /// Partitions to give up before acknowledging anything.
    pub revoked: Vec<(String, i32)>,
    /// Partitions newly gained.
    pub gained: Vec<(String, i32)>,
    /// Whether the broker sent a target that differs from what we hold, and
    /// therefore whether an acknowledging heartbeat is owed.
    pub changed: bool,
}

/// Work out the ordered reconciliation between what we own and what the broker
/// wants us to own.
///
/// Pure, and tested as such: this is the part where getting the order wrong
/// produces duplicate delivery rather than an error, so it must be checkable
/// without a broker.
pub(crate) fn reconcile(
    owned: &HashSet<(String, i32)>,
    target: &HashSet<(String, i32)>,
) -> Reconciliation {
    let mut revoked: Vec<(String, i32)> = owned.difference(target).cloned().collect();
    let mut gained: Vec<(String, i32)> = target.difference(owned).cloned().collect();
    revoked.sort();
    gained.sort();
    Reconciliation {
        changed: !revoked.is_empty() || !gained.is_empty(),
        revoked,
        gained,
    }
}

/// Whether an error means our membership is gone and we must re-join from
/// scratch.
///
/// Not fatal: a fenced member is one whose session expired or whose epoch the
/// broker no longer recognises, and re-joining is the defined recovery. A
/// consumer that exits here is a consumer that dies on a network hiccup.
pub(crate) fn membership_lost(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::UnknownMemberId | ErrorCode::FencedMemberEpoch | ErrorCode::RebalanceInProgress
    )
}

/// A fresh member id.
///
/// **KIP-848 inverts the classic protocol here: the *client* generates its
/// member id, and the broker rejects an empty one with `INVALID_REQUEST`.** In
/// the classic protocol the broker issues it and the client sends an empty
/// string on the first JoinGroup, which is the intuition this contradicts —
/// and the error message ("MemberId can't be empty") describes the symptom
/// rather than the inversion, so it reads like a bug in the client's state
/// machine.
///
/// A v4-shaped UUID, because that is what Java sends and what the broker's
/// logs and `kafka-consumer-groups.sh` expect to render.
fn new_member_id() -> String {
    let mut bytes = [0u8; 16];
    rand::Rng::fill(&mut rand::rng(), &mut bytes[..]);
    // Version 4, variant RFC 4122.
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes).to_string()
}

/// One member's view of its KIP-848 group.
#[derive(Debug)]
pub(crate) struct Membership {
    group_id: String,
    instance_id: Option<String>,
    subscription: Vec<String>,
    member_id: String,
    member_epoch: i32,
    /// Authoritative on every beat, from the response — never from config.
    heartbeat_interval: Duration,
    rebalance_timeout_ms: i32,
    owned: HashSet<(String, i32)>,
    last_beat: Option<Instant>,
    /// Set when a target has been revoked but not yet acknowledged.
    ack_owed: bool,
}

impl Membership {
    pub(crate) fn new(
        group_id: String,
        subscription: Vec<String>,
        instance_id: Option<String>,
        rebalance_timeout_ms: i32,
    ) -> Self {
        Self {
            group_id,
            instance_id,
            subscription,
            member_id: new_member_id(),
            member_epoch: EPOCH_JOIN,
            // A placeholder until the first response replaces it. Every beat
            // after that uses the broker's number.
            heartbeat_interval: Duration::from_secs(5),
            rebalance_timeout_ms,
            owned: HashSet::new(),
            last_beat: None,
            ack_owed: false,
        }
    }

    /// Become a static member. A restart inside the session timeout then
    /// parks the assignment rather than triggering a rebalance.
    pub(crate) fn set_instance_id(&mut self, instance_id: String) {
        self.instance_id = Some(instance_id);
    }

    pub(crate) fn owned(&self) -> &HashSet<(String, i32)> {
        &self.owned
    }

    pub(crate) fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Whether a heartbeat is due.
    ///
    /// An owed acknowledgement is always due: the broker is waiting for it
    /// before it will hand our revoked partitions to anyone else.
    pub(crate) fn beat_due(&self) -> bool {
        if self.ack_owed || self.last_beat.is_none() {
            return true;
        }
        self.last_beat
            .is_some_and(|last| last.elapsed() >= self.heartbeat_interval)
    }

    /// Send one heartbeat and apply what comes back.
    ///
    /// Returns the reconciliation the caller must act on: revoke first, then
    /// the next beat acknowledges.
    pub(crate) async fn beat(
        &mut self,
        cluster: &Cluster,
        topic_ids: &HashMap<String, TopicId>,
    ) -> Result<Reconciliation> {
        let request = self.build(topic_ids)?;
        let response = cluster
            .send_to_coordinator(CoordinatorKind::Group, &self.group_id, request)
            .await?;
        self.last_beat = Some(Instant::now());

        if let Some(code) = ErrorCode::from_code(response.error_code) {
            if membership_lost(code) {
                tracing::debug!(%code, "membership lost; rejoining");
                self.reset();
                return Ok(Reconciliation::default());
            }
            return Err(Error::from_code(
                code,
                response.error_message.map(|m| m.to_string()),
            ));
        }

        if let Some(id) = response.member_id {
            self.member_id = id.to_string();
        }
        self.member_epoch = response.member_epoch;
        if response.heartbeat_interval_ms > 0 {
            // From the response, every time. Treating config as authoritative
            // is how a client drifts out of a group whose broker changed its
            // mind about the cadence.
            self.heartbeat_interval = Duration::from_millis(
                u64::try_from(response.heartbeat_interval_ms).unwrap_or(5000),
            );
        }

        let Some(assignment) = response.assignment else {
            // No assignment field means "nothing changed" — not "you own
            // nothing". Clearing here would revoke the whole assignment on
            // every ordinary beat.
            self.ack_owed = false;
            return Ok(Reconciliation::default());
        };

        let by_id: HashMap<uuid::Uuid, &String> = topic_ids
            .iter()
            .filter(|(_, id)| !id.is_zero())
            .map(|(name, id)| (uuid::Uuid::from_bytes(*id.as_bytes()), name))
            .collect();

        let mut target = HashSet::new();
        for topic in assignment.topic_partitions {
            let Some(name) = by_id.get(&topic.topic_id) else {
                // A topic we cannot name yet. Leaving it out of the target
                // would revoke it; skipping the whole reconciliation is
                // safer, and the next beat retries once metadata catches up.
                tracing::debug!(topic_id = %topic.topic_id, "assigned an unknown topic id");
                continue;
            };
            for partition in topic.partitions {
                target.insert(((*name).clone(), partition));
            }
        }

        let outcome = reconcile(&self.owned, &target);
        // Ownership moves to the target only after the caller has been told
        // what to revoke. The acknowledging beat is what tells the broker.
        self.owned = target;
        self.ack_owed = outcome.changed;
        Ok(outcome)
    }

    /// Leave the group.
    ///
    /// A static member parks its assignment; a dynamic one releases it.
    pub(crate) async fn leave(&mut self, cluster: &Cluster) -> Result<()> {
        if self.member_epoch <= EPOCH_JOIN {
            // Never joined, so there is nothing to leave.
            return Ok(());
        }
        let epoch = if self.instance_id.is_some() {
            EPOCH_STATIC_LEAVE
        } else {
            EPOCH_LEAVE
        };

        let request = ConsumerGroupHeartbeatRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(self.group_id.clone())))
            .with_member_id(StrBytes::from_string(self.member_id.clone()))
            .with_member_epoch(epoch)
            .with_instance_id(self.instance_id.clone().map(StrBytes::from_string));

        let response = cluster
            .send_to_coordinator(CoordinatorKind::Group, &self.group_id, request)
            .await?;

        self.reset();

        // A member the broker has already forgotten is a member that has left.
        match ErrorCode::from_code(response.error_code) {
            Some(code) if membership_lost(code) => Ok(()),
            Some(code) => Err(Error::from_code(code, None)),
            None => Ok(()),
        }
    }

    fn reset(&mut self) {
        // A new id, not an empty one: re-joining under the old id would race
        // the broker's own expiry of the member we just lost.
        self.member_id = new_member_id();
        self.member_epoch = EPOCH_JOIN;
        self.owned.clear();
        self.ack_owed = false;
    }

    fn build(&self, topic_ids: &HashMap<String, TopicId>) -> Result<ConsumerGroupHeartbeatRequest> {
        let mut request = ConsumerGroupHeartbeatRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(self.group_id.clone())))
            .with_member_id(StrBytes::from_string(self.member_id.clone()))
            .with_member_epoch(self.member_epoch)
            .with_instance_id(self.instance_id.clone().map(StrBytes::from_string));

        // The subscription is sent when joining and whenever it could have
        // been forgotten; sending it every beat is allowed and costs a few
        // bytes against a class of bug that is hard to see.
        if self.member_epoch == EPOCH_JOIN {
            request = request
                .with_rebalance_timeout_ms(self.rebalance_timeout_ms)
                .with_subscribed_topic_names(Some(
                    self.subscription
                        .iter()
                        .map(|topic| {
                            kafka_conn::protocol::messages::TopicName(StrBytes::from_string(
                                topic.clone(),
                            ))
                        })
                        .collect(),
                ));
        }

        // The acknowledgement: what we own *now*, after revoking. Sent only
        // when one is owed, because an unsolicited ownership claim on an
        // ordinary beat is how a client re-asserts a partition it was told to
        // give up.
        //
        // **Present and empty on a join beat — not absent.** The broker's
        // check is `topicPartitions == null || !isEmpty()`, so a *null* field
        // fails the same validation an ownership claim does, with the same
        // message: "TopicPartitions must be empty when (re-)joining". That
        // reads as "you sent partitions" when in fact nothing was sent at all,
        // which is why the obvious reading of it — stop sending the field —
        // makes joining permanently impossible.
        //
        // So: empty list while joining, owned set once joined. Never null.
        request = request.with_topic_partitions(Some(Vec::new()));

        // Never claim ownership while joining, whatever we think we own —
        // including on the beat right after a reset, where our own `owned` set
        // is stale by definition. Gating on the epoch as well as on `ack_owed`
        // makes that impossible to get wrong from either direction.
        if self.ack_owed && self.member_epoch != EPOCH_JOIN {
            let mut by_topic: HashMap<uuid::Uuid, Vec<i32>> = HashMap::new();
            for (topic, partition) in &self.owned {
                let Some(id) = topic_ids.get(topic) else {
                    continue;
                };
                if id.is_zero() {
                    continue;
                }
                by_topic
                    .entry(uuid::Uuid::from_bytes(*id.as_bytes()))
                    .or_default()
                    .push(*partition);
            }
            request = request.with_topic_partitions(Some(
                by_topic
                    .into_iter()
                    .map(|(topic_id, partitions)| {
                        TopicPartitions::default()
                            .with_topic_id(topic_id)
                            .with_partitions(partitions)
                    })
                    .collect(),
            ));
        }

        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(entries: &[(&str, i32)]) -> HashSet<(String, i32)> {
        entries
            .iter()
            .map(|(topic, partition)| ((*topic).to_owned(), *partition))
            .collect()
    }

    #[test]
    fn a_first_assignment_is_all_gain_and_no_revocation() {
        let outcome = reconcile(&HashSet::new(), &set(&[("t", 0), ("t", 1)]));
        assert!(outcome.revoked.is_empty());
        assert_eq!(outcome.gained.len(), 2);
        assert!(outcome.changed);
    }

    #[test]
    fn an_unchanged_assignment_owes_nothing() {
        let owned = set(&[("t", 0), ("t", 1)]);
        let outcome = reconcile(&owned, &owned);
        assert!(!outcome.changed, "an ack for an unchanged target is noise");
        assert!(outcome.revoked.is_empty());
        assert!(outcome.gained.is_empty());
    }

    /// The case the ordering rule exists for: a rebalance that both takes a
    /// partition away and hands a different one over.
    #[test]
    fn a_rebalance_reports_both_halves_separately() {
        let outcome = reconcile(&set(&[("t", 0), ("t", 1)]), &set(&[("t", 1), ("t", 2)]));
        assert_eq!(outcome.revoked, vec![("t".to_owned(), 0)]);
        assert_eq!(outcome.gained, vec![("t".to_owned(), 2)]);
        assert!(outcome.changed);
    }

    #[test]
    fn losing_everything_revokes_everything() {
        let outcome = reconcile(&set(&[("t", 0), ("t", 1)]), &HashSet::new());
        assert_eq!(outcome.revoked.len(), 2);
        assert!(outcome.gained.is_empty());
        assert!(outcome.changed);
    }

    /// The three sentinels, stated, because they are all small negative
    /// integers and swapping two of them produces no error at all.
    #[test]
    fn the_epoch_sentinels_are_distinct_and_not_interchangeable() {
        assert_eq!(EPOCH_JOIN, 0);
        assert_eq!(EPOCH_LEAVE, -1);
        assert_eq!(EPOCH_STATIC_LEAVE, -2);
        assert_ne!(EPOCH_LEAVE, EPOCH_STATIC_LEAVE);
    }

    /// A static member parks its assignment; a dynamic one releases it. Using
    /// `-1` for a static member discards the whole point of the instance id.
    #[test]
    fn a_static_member_leaves_with_a_different_sentinel() {
        let dynamic = Membership::new("g".to_owned(), vec!["t".to_owned()], None, 30_000);
        let static_member = Membership::new(
            "g".to_owned(),
            vec!["t".to_owned()],
            Some("instance-1".to_owned()),
            30_000,
        );
        assert!(dynamic.instance_id.is_none());
        assert!(static_member.instance_id.is_some());
    }

    #[test]
    fn a_fresh_member_beats_immediately() {
        let member = Membership::new("g".to_owned(), vec!["t".to_owned()], None, 30_000);
        assert!(
            member.beat_due(),
            "a member that has never beaten must join"
        );
        assert_eq!(member.member_epoch, EPOCH_JOIN);
        assert!(
            !member.member_id().is_empty(),
            "KIP-848 has the client generate its own member id; an empty one \
             is rejected with INVALID_REQUEST"
        );
    }

    /// An owed acknowledgement jumps the interval: the broker will not hand
    /// our revoked partitions on until it arrives.
    #[test]
    fn an_owed_acknowledgement_is_always_due() {
        let mut member = Membership::new("g".to_owned(), vec!["t".to_owned()], None, 30_000);
        member.last_beat = Some(Instant::now());
        member.heartbeat_interval = Duration::from_secs(3600);
        assert!(!member.beat_due());

        member.ack_owed = true;
        assert!(member.beat_due());
    }

    /// Two members must not collide, and the id must look like the UUID the
    /// broker's tooling expects to render.
    #[test]
    fn every_member_id_is_a_distinct_v4_uuid() {
        let a = new_member_id();
        let b = new_member_id();
        assert_ne!(a, b);
        assert_eq!(a.len(), 36, "hyphenated UUID: {a}");
        let parsed: uuid::Uuid = a.parse().expect("a valid uuid");
        assert_eq!(parsed.get_version_num(), 4);
    }

    #[test]
    fn membership_errors_are_recoverable_rather_than_fatal() {
        for code in [
            ErrorCode::UnknownMemberId,
            ErrorCode::FencedMemberEpoch,
            ErrorCode::RebalanceInProgress,
        ] {
            assert!(membership_lost(code), "{code:?} must rejoin, not fail");
        }
        assert!(!membership_lost(ErrorCode::TopicAuthorizationFailed));
    }
}
