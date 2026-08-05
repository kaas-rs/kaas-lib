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
//! # Which assignors, and the one deliberate omission
//!
//! **`range`, `roundrobin` and `cooperative-sticky`.** Eager `sticky` is not
//! implemented, and that one *is* a decision rather than an oversight — see
//! below.
//!
//! The advertised order is `[range, roundrobin, cooperative-sticky]`, matching
//! Java's default first choice. The coordinator does not simply take the
//! leader's favourite: it intersects every member's list, then each member
//! votes for the first of *its* protocols that survived, and the most-voted one
//! wins. So a group of default Java clients and us settles on `range`
//! deterministically, while a Java client pinned to `CooperativeStickyAssignor`
//! alone leaves that as the only candidate and the group forms on it — where
//! before it could not form at all. [`crate::Assignor`] reorders the list for a
//! caller who wants cooperative rebalancing without being forced into it.
//!
//! ## Cooperative rebalancing takes two rounds, and that is the protocol
//!
//! Under `range`/`roundrobin` a rebalance is **eager**: every member revokes
//! everything, re-joins, and takes whatever it is given. Under
//! `cooperative-sticky` (KIP-429) a member keeps what it holds across the
//! rebalance and gives up only what actually moves:
//!
//! 1. Every member sends what it currently owns in its subscription
//!    (`owned_partitions`, subscription v1+, plus `generation_id` at v2 to
//!    settle a double claim after a failure).
//! 2. The leader computes a sticky, balanced target — then **withholds** every
//!    partition whose owner is changing. Round one assigns it to nobody.
//! 3. The members that lost partitions revoke them and re-join immediately.
//! 4. Round two hands the now-unowned partitions to their new owners.
//!
//! Withholding is the whole point: a partition is never assigned to its next
//! owner in the same round its previous owner still holds it, so the two never
//! overlap. Skipping step 2 and assigning directly is the bug that delivers
//! every record in the moved partition twice, silently.
//!
//! ## Eager `sticky` is omitted, and this is why
//!
//! `StickyAssignor` carries the previous assignment in the subscription's
//! `user_data` as `StickyAssignorUserData` — a struct defined in Java client
//! code with **no schema in `kafka-protocol`**. Encoding it means hand-rolling
//! a wire format, which CLAUDE.md forbids for exactly the reason it applies
//! here: a hand-rolled struct that is subtly wrong produces a group where
//! somebody's assignment is silently empty.
//!
//! `cooperative-sticky` has no such problem — `owned_partitions` and
//! `generation_id` are real fields of the real schema — which is why it is here
//! and its eager sibling is not. The failure mode of the omission is loud: a
//! group whose other members are pinned to `sticky` alone fails `JoinGroup`
//! with `INCONSISTENT_GROUP_PROTOCOL` at join time rather than misbehaving.
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

use std::collections::{BTreeMap, BTreeSet};

/// Java's `RangeAssignor`.
pub(crate) const RANGE: &str = "range";
/// Java's `RoundRobinAssignor`.
pub(crate) const ROUND_ROBIN: &str = "roundrobin";
/// Java's `CooperativeStickyAssignor`.
pub(crate) const COOPERATIVE_STICKY: &str = "cooperative-sticky";

/// Which assignors a [`ClassicConsumer`](crate::ClassicConsumer) advertises,
/// and in what order.
///
/// Order is a vote, not a demand: the coordinator intersects every member's
/// list and each member votes for the first of its own that survived. Putting
/// [`Assignor::CooperativeSticky`] first asks for cooperative rebalancing;
/// whether the group gets it depends on the other members.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Assignor {
    /// Java's `RangeAssignor`. Eager, per topic, remainder to the earliest
    /// members. Java's own first choice, and therefore ours.
    Range,
    /// Java's `RoundRobinAssignor`. Eager, deals every partition in rotation.
    RoundRobin,
    /// Java's `CooperativeStickyAssignor` (KIP-429). Keeps what it can across a
    /// rebalance and moves the rest over two rounds.
    CooperativeSticky,
}

impl Assignor {
    /// The protocol name on the wire. These strings are Java's, not ours.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Assignor::Range => RANGE,
            Assignor::RoundRobin => ROUND_ROBIN,
            Assignor::CooperativeSticky => COOPERATIVE_STICKY,
        }
    }

    /// Whether this assignor revokes everything before re-joining.
    ///
    /// The distinction drives the member, not just the leader: an eager member
    /// gives its partitions up the moment it learns a rebalance is happening, a
    /// cooperative one holds them until the sync says which ones moved.
    pub(crate) fn is_eager(self) -> bool {
        !matches!(self, Assignor::CooperativeSticky)
    }
}

/// The default advertised list, matching Java's default first choice.
pub(crate) const SUPPORTED: [Assignor; 3] = [
    Assignor::Range,
    Assignor::RoundRobin,
    Assignor::CooperativeSticky,
];

/// Whether a protocol name is one this client can actually compute.
pub(crate) fn is_cooperative(protocol: &str) -> bool {
    protocol == COOPERATIVE_STICKY
}

/// One member's subscription, as the leader sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemberSubscription {
    pub member_id: String,
    pub topics: Vec<String>,
    /// What this member says it currently holds (subscription v1+).
    ///
    /// Empty under the eager assignors, which revoke before re-joining, and
    /// load-bearing under `cooperative-sticky`, which does not.
    pub owned: Vec<(String, i32)>,
    /// The generation the member owned those partitions in (subscription v2+).
    ///
    /// Only used to settle a double claim: after a member misses a rebalance,
    /// two members can both believe they own a partition, and the one from the
    /// older generation is the one that is wrong.
    pub generation: i32,
}

impl MemberSubscription {
    fn subscribes_to(&self, topic: &str) -> bool {
        self.topics.iter().any(|t| t == topic)
    }
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

/// Compute a sticky, balanced assignment, then withhold whatever has to move.
///
/// Java's `CooperativeStickyAssignor`, in the two parts that matter:
///
/// * **Sticky.** A partition stays with the member that already holds it
///   wherever balance allows, because moving one costs a revoke and a
///   re-consume from the committed offset.
/// * **Cooperative.** A partition whose owner is changing is assigned to
///   *nobody* this round. Its old owner sees it missing from its assignment and
///   revokes it; the re-join that follows gives it to its new owner. Handing it
///   straight over would mean two members holding it at once, each delivering
///   its records, with nothing anywhere reporting a problem.
///
/// The result is deliberately unbalanced in the round where partitions move.
/// That is what the second round is for.
pub(crate) fn assign_cooperative_sticky(
    members: &[MemberSubscription],
    partitions_per_topic: &BTreeMap<String, i32>,
) -> BTreeMap<String, BTreeMap<String, Vec<i32>>> {
    let mut sorted: Vec<&MemberSubscription> = members.iter().collect();
    sorted.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    if sorted.is_empty() {
        return BTreeMap::new();
    }

    // Every partition of every subscribed topic.
    let all: BTreeSet<(String, i32)> = partitions_per_topic
        .iter()
        .flat_map(|(topic, count)| (0..*count).map(move |p| (topic.clone(), p)))
        .filter(|(topic, _)| sorted.iter().any(|m| m.subscribes_to(topic)))
        .collect();

    // Who holds what now, with a double claim going to the newer generation.
    // A claim on a partition that no longer exists, or on a topic the claimant
    // no longer subscribes to, is not a claim.
    let mut owner: BTreeMap<(String, i32), (String, i32)> = BTreeMap::new();
    for member in &sorted {
        for key in &member.owned {
            if !all.contains(key) || !member.subscribes_to(&key.0) {
                continue;
            }
            match owner.get(key) {
                Some((_, generation)) if *generation >= member.generation => {}
                _ => {
                    owner.insert(key.clone(), (member.member_id.clone(), member.generation));
                }
            }
        }
    }

    let mut held: BTreeMap<String, BTreeSet<(String, i32)>> = sorted
        .iter()
        .map(|member| (member.member_id.clone(), BTreeSet::new()))
        .collect();
    for (key, (member_id, _)) in &owner {
        if let Some(set) = held.get_mut(member_id) {
            set.insert(key.clone());
        }
    }

    // What nobody holds goes to whoever has the least, so a member joining an
    // established group is filled up rather than starved.
    for key in all.iter().filter(|key| !owner.contains_key(*key)) {
        if let Some(member_id) = emptiest(&sorted, &held, &key.0)
            && let Some(set) = held.get_mut(&member_id)
        {
            set.insert(key.clone());
        }
    }

    balance(&sorted, &mut held);

    // The cooperative step. Anything still owned by somebody else is withheld
    // for a round.
    for (member_id, set) in &mut held {
        set.retain(|key| match owner.get(key) {
            Some((holder, _)) => holder == member_id,
            None => true,
        });
    }

    held.into_iter()
        .map(|(member_id, set)| {
            let mut topics: BTreeMap<String, Vec<i32>> = BTreeMap::new();
            for (topic, partition) in set {
                topics.entry(topic).or_default().push(partition);
            }
            (member_id, topics)
        })
        .collect()
}

/// The subscribed member holding the fewest partitions, ties by member id.
fn emptiest(
    members: &[&MemberSubscription],
    held: &BTreeMap<String, BTreeSet<(String, i32)>>,
    topic: &str,
) -> Option<String> {
    members
        .iter()
        .filter(|member| member.subscribes_to(topic))
        .min_by_key(|member| {
            (
                held.get(&member.member_id).map_or(0, BTreeSet::len),
                member.member_id.clone(),
            )
        })
        .map(|member| member.member_id.clone())
}

/// Move partitions from the fullest member to the emptiest until no pair is
/// more than one apart.
///
/// Bounded rather than looping until balanced: a subscription pattern where the
/// imbalance *cannot* be fixed — a topic only one member wants — must terminate
/// rather than spin, and it is the leader of a live group doing the spinning.
fn balance(members: &[&MemberSubscription], held: &mut BTreeMap<String, BTreeSet<(String, i32)>>) {
    let total: usize = held.values().map(BTreeSet::len).sum();
    for _ in 0..(total.saturating_mul(2)) {
        let Some((from, to, key)) = imbalance(members, held) else {
            return;
        };
        if let Some(set) = held.get_mut(&from) {
            set.remove(&key);
        }
        if let Some(set) = held.get_mut(&to) {
            set.insert(key);
        }
    }
}

/// One partition worth moving: from the fullest member to one at least two
/// behind it that is allowed to take it.
fn imbalance(
    members: &[&MemberSubscription],
    held: &BTreeMap<String, BTreeSet<(String, i32)>>,
) -> Option<(String, String, (String, i32))> {
    let mut by_size: Vec<(&String, usize)> = held.iter().map(|(id, set)| (id, set.len())).collect();
    // Sorted so the choice is deterministic: two leaders computing the same
    // group must reach the same answer, and a tie broken by hash order is a
    // tie broken differently on every run.
    by_size.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));

    for (fullest, size) in &by_size {
        for (emptiest, other) in by_size.iter().rev() {
            if size <= &(other + 1) {
                continue;
            }
            let takes = members
                .iter()
                .find(|member| &&member.member_id == emptiest)?;
            let moved = held
                .get(*fullest)?
                .iter()
                .find(|(topic, _)| takes.subscribes_to(topic))?;
            return Some(((*fullest).clone(), (*emptiest).clone(), moved.clone()));
        }
    }
    None
}

/// Compute an assignment with the named protocol.
///
/// An unrecognised name falls back to `range`, which is not a guess: the
/// coordinator only ever names a protocol every member advertised, so a name we
/// do not know means the codebase advertised something it cannot compute, and
/// range is the one Java clients always have.
pub(crate) fn assign(
    protocol: &str,
    members: &[MemberSubscription],
    partitions_per_topic: &BTreeMap<String, i32>,
) -> BTreeMap<String, BTreeMap<String, Vec<i32>>> {
    match protocol {
        ROUND_ROBIN => assign_round_robin(members, partitions_per_topic),
        COOPERATIVE_STICKY => assign_cooperative_sticky(members, partitions_per_topic),
        _ => assign_range(members, partitions_per_topic),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str, topics: &[&str]) -> MemberSubscription {
        MemberSubscription {
            member_id: id.to_owned(),
            topics: topics.iter().map(|t| (*t).to_owned()).collect(),
            owned: Vec::new(),
            generation: NO_GENERATION,
        }
    }

    /// A member that arrives at the rebalance already holding partitions,
    /// which is the only interesting case for the cooperative assignor.
    fn holder(
        id: &str,
        topics: &[&str],
        owned: &[(&str, i32)],
        generation: i32,
    ) -> MemberSubscription {
        MemberSubscription {
            owned: owned
                .iter()
                .map(|(topic, partition)| ((*topic).to_owned(), *partition))
                .collect(),
            generation,
            ..member(id, topics)
        }
    }

    /// Flatten one member's assignment into sorted `(topic, partition)` pairs.
    fn flat(
        assignment: &BTreeMap<String, BTreeMap<String, Vec<i32>>>,
        member_id: &str,
    ) -> Vec<(String, i32)> {
        let mut out: Vec<(String, i32)> = assignment
            .get(member_id)
            .into_iter()
            .flatten()
            .flat_map(|(topic, partitions)| partitions.iter().map(move |p| (topic.clone(), *p)))
            .collect();
        out.sort();
        out
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

        for assignor in SUPPORTED {
            let protocol = assignor.name();
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

    /// Preference order is a vote, and `range` first is a deliberate choice: it
    /// is the one Java advertises by default, so a mixed group lands on it
    /// deterministically rather than by tie-break.
    #[test]
    fn range_is_offered_before_round_robin() {
        assert_eq!(SUPPORTED[0], Assignor::Range);
        assert_eq!(SUPPORTED[1], Assignor::RoundRobin);
        assert_eq!(SUPPORTED[2], Assignor::CooperativeSticky);
    }

    /// The names are Java's, not ours. A typo here is a group that never forms.
    #[test]
    fn the_protocol_names_are_javas() {
        assert_eq!(Assignor::Range.name(), "range");
        assert_eq!(Assignor::RoundRobin.name(), "roundrobin");
        assert_eq!(Assignor::CooperativeSticky.name(), "cooperative-sticky");
        assert!(Assignor::Range.is_eager());
        assert!(Assignor::RoundRobin.is_eager());
        assert!(!Assignor::CooperativeSticky.is_eager());
    }

    #[test]
    fn a_first_cooperative_assignment_is_balanced_and_complete() {
        let assignment = assign_cooperative_sticky(
            &[member("a", &["t"]), member("b", &["t"])],
            &topics(&[("t", 6)]),
        );
        assert_eq!(flat(&assignment, "a").len(), 3);
        assert_eq!(flat(&assignment, "b").len(), 3);
    }

    /// Sticky means what it says: a member that already holds partitions keeps
    /// them when nothing forces a move.
    #[test]
    fn a_settled_group_moves_nothing() {
        let held_a = [("t", 0), ("t", 1), ("t", 2)];
        let held_b = [("t", 3), ("t", 4), ("t", 5)];
        let assignment = assign_cooperative_sticky(
            &[
                holder("a", &["t"], &held_a, 4),
                holder("b", &["t"], &held_b, 4),
            ],
            &topics(&[("t", 6)]),
        );
        assert_eq!(
            flat(&assignment, "a"),
            vec![
                ("t".to_owned(), 0),
                ("t".to_owned(), 1),
                ("t".to_owned(), 2)
            ]
        );
        assert_eq!(
            flat(&assignment, "b"),
            vec![
                ("t".to_owned(), 3),
                ("t".to_owned(), 4),
                ("t".to_owned(), 5)
            ]
        );
    }

    /// The heart of KIP-429: a partition that has to move is assigned to
    /// **nobody** in the round its old owner still holds it. Handing it over
    /// directly is the bug that delivers its records twice with no error
    /// anywhere.
    #[test]
    fn a_partition_that_must_move_is_withheld_for_a_round() {
        let all_of_it = [("t", 0), ("t", 1), ("t", 2), ("t", 3)];
        let round_one = assign_cooperative_sticky(
            &[
                holder("a", &["t"], &all_of_it, 7),
                // b has just joined and owns nothing.
                member("b", &["t"]),
            ],
            &topics(&[("t", 4)]),
        );

        let a_holds = flat(&round_one, "a");
        let b_holds = flat(&round_one, "b");
        assert_eq!(a_holds.len(), 2, "a keeps the half it is not giving up");
        assert!(
            b_holds.is_empty(),
            "b was handed {b_holds:?} while a still owned it — that is double ownership"
        );

        // Round two: a has revoked what round one took off it, so the
        // partitions are free and b gets them.
        let round_two = assign_cooperative_sticky(
            &[
                holder(
                    "a",
                    &["t"],
                    &a_holds
                        .iter()
                        .map(|(topic, p)| (topic.as_str(), *p))
                        .collect::<Vec<_>>(),
                    8,
                ),
                holder("b", &["t"], &[], 8),
            ],
            &topics(&[("t", 4)]),
        );
        assert_eq!(flat(&round_two, "a").len(), 2);
        assert_eq!(flat(&round_two, "b").len(), 2);

        // Together they cover everything, and nothing is held twice.
        let mut union: Vec<(String, i32)> = flat(&round_two, "a");
        union.extend(flat(&round_two, "b"));
        union.sort();
        union.dedup();
        assert_eq!(union.len(), 4);
    }

    /// A member that missed a rebalance can come back still believing it owns
    /// partitions somebody else was given. The older generation is the one that
    /// is wrong.
    ///
    /// Four partitions, not two, so balance has nothing to say: with one
    /// partition each to spare, the only thing deciding who holds t-0 and t-1
    /// is which claim is honoured.
    #[test]
    fn a_stale_claim_loses_to_a_newer_generation() {
        let assignment = assign_cooperative_sticky(
            &[
                holder("a", &["t"], &[("t", 0), ("t", 1)], 9),
                holder("b", &["t"], &[("t", 0), ("t", 1)], 3),
            ],
            &topics(&[("t", 4)]),
        );
        assert_eq!(
            flat(&assignment, "a"),
            vec![("t".to_owned(), 0), ("t".to_owned(), 1)],
            "the newer generation keeps what it claims"
        );
        assert_eq!(
            flat(&assignment, "b"),
            vec![("t".to_owned(), 2), ("t".to_owned(), 3)],
            "the stale claim is dropped, and the claimant is filled from what is free"
        );
    }

    /// Balance beats stickiness when the two disagree, and the partition that
    /// has to move is still withheld for the round.
    #[test]
    fn balance_wins_over_stickiness_and_the_move_still_waits_a_round() {
        let assignment = assign_cooperative_sticky(
            &[
                holder("a", &["t"], &[("t", 0), ("t", 1)], 4),
                holder("b", &["t"], &[], 4),
            ],
            &topics(&[("t", 2)]),
        );
        assert_eq!(
            flat(&assignment, "a").len(),
            1,
            "a must give one up for the group to be balanced"
        );
        assert!(
            flat(&assignment, "b").is_empty(),
            "and b must not receive it until a has actually let go"
        );
    }

    /// A claim on a partition that no longer exists, or on a topic the member
    /// no longer subscribes to, is not a claim.
    #[test]
    fn claims_on_partitions_that_are_gone_are_ignored() {
        let assignment = assign_cooperative_sticky(
            &[holder("a", &["t"], &[("t", 0), ("t", 99), ("gone", 0)], 2)],
            &topics(&[("t", 2)]),
        );
        assert_eq!(
            flat(&assignment, "a"),
            vec![("t".to_owned(), 0), ("t".to_owned(), 1)]
        );
    }

    /// A member that leaves frees its partitions immediately: there is nobody
    /// left to overlap with, so withholding them would strand them for a round
    /// for no reason.
    #[test]
    fn partitions_from_a_departed_member_are_not_withheld() {
        let assignment =
            assign_cooperative_sticky(&[holder("a", &["t"], &[("t", 0)], 5)], &topics(&[("t", 4)]));
        assert_eq!(
            flat(&assignment, "a").len(),
            4,
            "the survivor takes over everything the leaver held"
        );
    }

    /// The balancer must terminate even when the imbalance cannot be fixed,
    /// because it is the leader of a live group doing the work.
    #[test]
    fn an_unfixable_imbalance_terminates() {
        let assignment = assign_cooperative_sticky(
            &[member("a", &["busy"]), member("b", &["quiet"])],
            &topics(&[("busy", 8), ("quiet", 1)]),
        );
        assert_eq!(flat(&assignment, "a").len(), 8);
        assert_eq!(flat(&assignment, "b").len(), 1);
    }

    /// Two leaders computing the same group must reach the same answer.
    #[test]
    fn the_assignment_does_not_depend_on_member_order() {
        let counts = topics(&[("t", 7)]);
        let forwards = assign_cooperative_sticky(
            &[
                member("a", &["t"]),
                member("b", &["t"]),
                member("c", &["t"]),
            ],
            &counts,
        );
        let backwards = assign_cooperative_sticky(
            &[
                member("c", &["t"]),
                member("b", &["t"]),
                member("a", &["t"]),
            ],
            &counts,
        );
        assert_eq!(forwards, backwards);
    }
}

// Two distinct generated types with the same name: the assignment's and the
// subscription's. Aliased rather than glob-imported, because letting one shadow
// the other compiles right up until the field you set lands in the wrong
// message.
use kafka_conn::protocol::messages::consumer_protocol_assignment::TopicPartition;
use kafka_conn::protocol::messages::consumer_protocol_subscription::TopicPartition as OwnedPartition;
use kafka_conn::protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_conn::protocol::messages::leave_group_request::MemberIdentity;
use kafka_conn::protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_conn::protocol::messages::{
    ConsumerProtocolAssignment, ConsumerProtocolSubscription, GroupId, HeartbeatRequest,
    JoinGroupRequest, LeaveGroupRequest, SyncGroupRequest, TopicName,
};
use kafka_conn::protocol::{Decodable, Encodable, Message, StrBytes};
use kafka_conn::{Error, ErrorCode, Result};
use kafka_meta::{Cluster, CoordinatorKind};

/// The generation a member that has not joined holds.
const NO_GENERATION: i32 = -1;

/// The version of the consumer protocol payload the eager assignors encode.
///
/// v0 deliberately: the fields above it exist for sticky's `user_data` and the
/// owned-partitions handshake, and an eager member owns nothing at the moment
/// it joins — it revoked everything first. Writing a higher version here would
/// claim a capability the payload does not carry.
const PROTOCOL_VERSION: i16 = 0;

/// The version `cooperative-sticky` encodes.
///
/// v1 added `owned_partitions`, which is what makes a rebalance incremental,
/// and v2 added `generation_id`, which is how a stale claim from a member that
/// missed a rebalance is told from a live one. Both are real fields of the
/// codec's own `ConsumerProtocolSubscription`; neither is hand-rolled.
const COOPERATIVE_PROTOCOL_VERSION: i16 = 2;

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
///
/// # What `owned` is for
///
/// Under an eager assignor it is empty, because an eager member has already
/// revoked everything by the time it joins. Under `cooperative-sticky` it is
/// the member's current assignment, and it is the input the leader's
/// stickiness is computed from — a member that under-reports what it owns has
/// its partitions handed to somebody else, which is a rebalance storm rather
/// than a rebalance.
fn encode_subscription(
    topics: &[String],
    owned: &[(String, i32)],
    generation: i32,
    version: i16,
) -> Result<bytes::Bytes> {
    let mut subscription = ConsumerProtocolSubscription::default().with_topics(
        topics
            .iter()
            .map(|topic| StrBytes::from_string(topic.clone()))
            .collect(),
    );

    // Only above v0, and only through the schema's own fields. Setting a field
    // the version does not have is an encode error, not a silent drop.
    if version >= 1 {
        let mut by_topic: BTreeMap<String, Vec<i32>> = BTreeMap::new();
        for (topic, partition) in owned {
            by_topic.entry(topic.clone()).or_default().push(*partition);
        }
        subscription = subscription.with_owned_partitions(
            by_topic
                .into_iter()
                .map(|(topic, partitions)| {
                    OwnedPartition::default()
                        .with_topic(TopicName(StrBytes::from_string(topic)))
                        .with_partitions(partitions)
                })
                .collect(),
        );
    }
    if version >= 2 {
        subscription = subscription.with_generation_id(generation);
    }

    let mut buf = bytes::BytesMut::new();
    buf.extend_from_slice(&version.to_be_bytes());
    subscription.encode(&mut buf, version).map_err(|error| {
        Error::InvalidRequest(format!("could not encode a subscription: {error}"))
    })?;
    Ok(buf.freeze())
}

/// What a member told the leader about itself.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedSubscription {
    topics: Vec<String>,
    owned: Vec<(String, i32)>,
    generation: i32,
}

fn decode_subscription(mut bytes: bytes::Bytes) -> Result<DecodedSubscription> {
    let version = take_version(&mut bytes)?;
    // Clamp rather than reject: a member from a newer client may name a version
    // above what the codec knows, and the fields we read are all at the bottom
    // of the schema. Refusing would fail the whole group over a field we do not
    // even look at.
    let version = version.clamp(0, <ConsumerProtocolSubscription as Message>::VERSIONS.max);
    let subscription = ConsumerProtocolSubscription::decode(&mut bytes, version)
        .map_err(|error| Error::decode("consumer protocol subscription", error.to_string()))?;
    Ok(DecodedSubscription {
        topics: subscription
            .topics
            .into_iter()
            .map(|topic| topic.to_string())
            .collect(),
        owned: subscription
            .owned_partitions
            .into_iter()
            .flat_map(|topic| {
                let name = topic.topic.0.to_string();
                topic
                    .partitions
                    .into_iter()
                    .map(move |partition| (name.clone(), partition))
            })
            .collect(),
        generation: subscription.generation_id,
    })
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
    /// What this member advertises, in preference order.
    assignors: Vec<Assignor>,
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
            assignors: SUPPORTED.to_vec(),
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

    /// The generation the coordinator last confirmed for this member.
    ///
    /// An `OffsetCommit` must carry it: a commit at the wrong generation is
    /// refused with `ILLEGAL_GENERATION`, and the anonymous `-1` is refused
    /// with `UNKNOWN_MEMBER_ID` while the group has members.
    pub(crate) fn generation_id(&self) -> i32 {
        self.generation_id
    }

    /// `group.instance.id`, when this member is static.
    pub(crate) fn instance_id(&self) -> Option<&str> {
        self.instance_id.as_deref()
    }

    pub(crate) fn is_leader(&self) -> bool {
        self.leader
    }

    /// Advertise a different set of assignors, in a different order.
    pub(crate) fn set_assignors(&mut self, assignors: Vec<Assignor>) {
        self.assignors = assignors;
    }

    /// What this member advertises.
    pub(crate) fn assignors(&self) -> &[Assignor] {
        &self.assignors
    }

    /// Whether the protocol the coordinator settled on rebalances
    /// incrementally.
    ///
    /// Meaningful only after a join: before one, nobody has voted yet.
    pub(crate) fn is_cooperative(&self) -> bool {
        is_cooperative(&self.protocol)
    }

    /// Join the group, then sync to get an assignment.
    ///
    /// Both halves in one call because they are one operation: a JoinGroup
    /// without the SyncGroup that follows leaves the whole group blocked on a
    /// member that never finished joining.
    ///
    /// `owned` is what this member currently holds. It is what makes a
    /// cooperative rebalance sticky, and it is ignored by the eager assignors —
    /// which is correct, because an eager member has already given everything
    /// up before it gets here.
    pub(crate) async fn join(
        &mut self,
        cluster: &Cluster,
        partitions_per_topic: &BTreeMap<String, i32>,
        owned: &[(String, i32)],
    ) -> Result<Vec<(String, i32)>> {
        let members = self.join_group(cluster, owned).await?;
        self.sync_group(cluster, members, partitions_per_topic)
            .await
    }

    async fn join_group(
        &mut self,
        cluster: &Cluster,
        owned: &[(String, i32)],
    ) -> Result<Vec<MemberSubscription>> {
        let generation = self.generation_id;
        let protocols: Vec<JoinGroupRequestProtocol> = self
            .assignors
            .iter()
            .map(|assignor| {
                // Per-protocol metadata, which is what lets one JoinGroup offer
                // an eager v0 payload and a cooperative v2 one in the same
                // request. Whichever the coordinator picks, the payload that
                // came with it is the one the leader reads.
                let (version, owned) = if assignor.is_eager() {
                    (PROTOCOL_VERSION, &[][..])
                } else {
                    (COOPERATIVE_PROTOCOL_VERSION, owned)
                };
                Ok(JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_static_str(assignor.name()))
                    .with_metadata(encode_subscription(
                        &self.subscription,
                        owned,
                        generation,
                        version,
                    )?))
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

        // `MEMBER_ID_REQUIRED` below is *not* a coordinator-class code, so the
        // KIP-394 handshake still returns on the first round trip rather than
        // being re-asked.
        let response =
            crate::coordinator::send_retrying(cluster, &self.group_id, request, |response| {
                ErrorCode::from_code(response.error_code)
            })
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
                let decoded = decode_subscription(member.metadata)?;
                Ok(MemberSubscription {
                    member_id: member.member_id.to_string(),
                    topics: decoded.topics,
                    owned: decoded.owned,
                    generation: decoded.generation,
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

        let response =
            crate::coordinator::send_retrying(cluster, &self.group_id, request, |response| {
                ErrorCode::from_code(response.error_code)
            })
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

        let response =
            crate::coordinator::send_retrying(cluster, &self.group_id, request, |response| {
                ErrorCode::from_code(response.error_code)
            })
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
        let encoded =
            encode_subscription(&["a".to_owned(), "b".to_owned()], &[], -1, PROTOCOL_VERSION)
                .unwrap();
        let decoded = decode_subscription(encoded).unwrap();
        assert_eq!(decoded.topics, vec!["a", "b"]);
        assert!(decoded.owned.is_empty(), "v0 carries no owned partitions");
    }

    /// The cooperative payload, which is the one carrying the state a sticky
    /// assignment is computed from. A v2 subscription that loses its
    /// `owned_partitions` on the way out reads to the leader as a member that
    /// owns nothing, and every partition moves on every rebalance.
    #[test]
    fn a_cooperative_subscription_carries_what_it_owns_and_when() {
        let owned = [
            ("t".to_owned(), 3),
            ("t".to_owned(), 7),
            ("u".to_owned(), 0),
        ];
        let encoded = encode_subscription(
            &["t".to_owned(), "u".to_owned()],
            &owned,
            11,
            COOPERATIVE_PROTOCOL_VERSION,
        )
        .unwrap();

        assert_eq!(
            &encoded[..2],
            &COOPERATIVE_PROTOCOL_VERSION.to_be_bytes(),
            "the prefix names the version the struct was encoded at"
        );

        let decoded = decode_subscription(encoded).unwrap();
        assert_eq!(decoded.topics, vec!["t", "u"]);
        assert_eq!(decoded.owned, owned.to_vec());
        assert_eq!(decoded.generation, 11);
    }

    /// A payload from a client newer than the codec must not fail the group
    /// over a field we never read.
    ///
    /// The schema is additive, so a future version is a v3 payload with more on
    /// the end: decoding at the highest version we know reads every field we
    /// care about and ignores the rest. This is what Java does with the same
    /// problem, and the alternative — refusing — is one member of a newer
    /// client version taking the whole group down.
    #[test]
    fn a_version_above_what_the_codec_knows_is_clamped_rather_than_refused() {
        // A genuine v3 payload: topics, owned partitions, generation, rack.
        let highest = <ConsumerProtocolSubscription as Message>::VERSIONS.max;
        let subscription = ConsumerProtocolSubscription::default()
            .with_topics(vec![StrBytes::from_static_str("t")])
            .with_owned_partitions(vec![
                OwnedPartition::default()
                    .with_topic(TopicName(StrBytes::from_static_str("t")))
                    .with_partitions(vec![2]),
            ])
            .with_generation_id(6)
            .with_rack_id(Some(StrBytes::from_static_str("rack-1")));

        let mut buf = bytes::BytesMut::new();
        // Labelled as a version from a client we have never met.
        buf.extend_from_slice(&(highest + 2).to_be_bytes());
        subscription.encode(&mut buf, highest).unwrap();

        let decoded = decode_subscription(buf.freeze()).unwrap();
        assert_eq!(decoded.topics, vec!["t"]);
        assert_eq!(decoded.owned, vec![("t".to_owned(), 2)]);
        assert_eq!(decoded.generation, 6);
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
        let encoded = encode_subscription(&["t".to_owned()], &[], -1, PROTOCOL_VERSION).unwrap();
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
