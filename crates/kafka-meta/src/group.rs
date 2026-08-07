//! A consumer's identity within its group, as a transactional offset commit
//! needs it.
//!
//! # Why this type lives here rather than in `kafka-consume`
//!
//! It is the one value that has to travel from a consumer to a *producer*:
//! KIP-447's `send_offsets_to_transaction` commits a consumer's offsets inside
//! the producer's transaction, and `TxnOffsetCommit` v3+ wants the committing
//! member's id, generation and instance id. `kafka-produce` does not depend on
//! `kafka-consume` and should not start — the write path has no business
//! knowing how membership works — so the type belongs at the layer both already
//! sit on.
//!
//! # One `generation` field, not one per group kind
//!
//! CLAUDE.md says not to flatten the four group kinds, because they are
//! described by different RPCs with different response shapes. This is the
//! place where that does not apply, and the protocol says so: the classic
//! protocol's `generation_id` and KIP-848's `member_epoch` are **the same wire
//! field**, spelled differently in two KIPs, and `TxnOffsetCommit` has exactly
//! one of them. `crate::offsets`' non-member sentinel makes the same point from
//! the other direction — both protocols spell "not a member" `-1`.
//!
//! Two types here would mean a producer method that takes an enum and then
//! writes the same field either way.

/// The sentinel that says "I am not a member of this group".
///
/// Spelled `generation_id` by the classic protocol and `member_epoch` by
/// KIP-848; one field, one value.
const NOT_A_MEMBER: i32 = -1;

/// What a consumer must tell a producer for the producer to commit that
/// consumer's offsets inside a transaction.
///
/// Build it from a consumer — `Consumer::group_metadata`,
/// `GroupConsumer::group_metadata`, `ClassicConsumer::group_metadata` — rather
/// than by hand. The hand-built form exists for a caller whose consumer is not
/// this library's.
///
/// ```
/// use kafka_meta::ConsumerGroupMetadata;
///
/// // A group member, as its consumer reports itself.
/// let member = ConsumerGroupMetadata::new("billing")
///     .with_member_id("billing-3f9c")
///     .with_generation(7);
///
/// // A standalone consumer that only borrows the group's offset storage.
/// let standalone = ConsumerGroupMetadata::new("reporting");
/// assert_eq!(standalone.generation, -1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMetadata {
    /// The group whose offsets are being committed.
    pub group_id: String,
    /// The member id the coordinator knows this consumer by.
    ///
    /// Empty for a consumer that is not a member — which the coordinator
    /// honours only while the group has *no* members, exactly as it does for an
    /// ordinary anonymous `OffsetCommit`. A made-up id is refused with
    /// `UNKNOWN_MEMBER_ID`.
    pub member_id: String,
    /// The classic `generation_id`, or the KIP-848 `member_epoch`.
    ///
    /// `-1` means "not a member". A *stale* value here is refused with
    /// `ILLEGAL_GENERATION` or `STALE_MEMBER_EPOCH`, which is the protocol
    /// doing its job: a member that has been rebalanced away from these
    /// partitions must not be allowed to commit them.
    pub generation: i32,
    /// `group.instance.id`, for a static member.
    pub instance_id: Option<String>,
}

impl ConsumerGroupMetadata {
    /// A non-member's metadata: the group id, and the sentinels that say this
    /// consumer is only borrowing the group's offset storage.
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            member_id: String::new(),
            generation: NOT_A_MEMBER,
            instance_id: None,
        }
    }

    /// The member id the coordinator issued, or that this client generated.
    #[must_use]
    pub fn with_member_id(mut self, member_id: impl Into<String>) -> Self {
        self.member_id = member_id.into();
        self
    }

    /// The generation (classic) or member epoch (KIP-848) this member holds.
    #[must_use]
    pub fn with_generation(mut self, generation: i32) -> Self {
        self.generation = generation;
        self
    }

    /// `group.instance.id`, for a static member.
    #[must_use]
    pub fn with_instance_id(mut self, instance_id: impl Into<String>) -> Self {
        self.instance_id = Some(instance_id.into());
        self
    }

    /// Set the instance id from an `Option`, for a caller relaying one it was
    /// handed rather than deciding on it.
    #[must_use]
    pub fn with_maybe_instance_id(mut self, instance_id: Option<String>) -> Self {
        self.instance_id = instance_id;
        self
    }

    /// Whether this metadata describes an actual group member.
    ///
    /// `false` is the standalone consumer borrowing the group's storage, and
    /// the commit it produces is honoured only while the group is empty.
    pub fn is_member(&self) -> bool {
        self.generation != NOT_A_MEMBER
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_one_is_the_non_member_form() {
        // The defaults matter more than they look: `TxnOffsetCommit` encodes
        // `member_id` and `generation_id` only at v3+, and below that the codec
        // *refuses* a value other than these. A non-member commit is therefore
        // the one shape that encodes at any version.
        let metadata = ConsumerGroupMetadata::new("reporting");
        assert_eq!(metadata.member_id, "");
        assert_eq!(metadata.generation, NOT_A_MEMBER);
        assert_eq!(metadata.instance_id, None);
        assert!(!metadata.is_member());
    }

    #[test]
    fn a_member_carries_what_the_coordinator_knows_it_by() {
        let metadata = ConsumerGroupMetadata::new("billing")
            .with_member_id("billing-3f9c")
            .with_generation(7)
            .with_instance_id("pod-3");
        assert!(metadata.is_member());
        assert_eq!(metadata.member_id, "billing-3f9c");
        assert_eq!(metadata.generation, 7);
        assert_eq!(metadata.instance_id.as_deref(), Some("pod-3"));
    }

    #[test]
    fn a_relayed_instance_id_reaches_the_same_value_as_a_chosen_one() {
        assert_eq!(
            ConsumerGroupMetadata::new("g").with_maybe_instance_id(Some("pod-3".to_owned())),
            ConsumerGroupMetadata::new("g").with_instance_id("pod-3")
        );
        // Assignment rather than a merge, same as `ProducerRecord`'s relayed
        // partition: a `None` clears what an earlier call set.
        assert_eq!(
            ConsumerGroupMetadata::new("g")
                .with_instance_id("pod-3")
                .with_maybe_instance_id(None)
                .instance_id,
            None
        );
    }

    #[test]
    fn generation_zero_is_a_member_not_a_sentinel() {
        // KIP-848 issues epoch 0 on join. Treating "falsy" as "not a member"
        // would send an anonymous commit for a member that has just joined,
        // which the coordinator refuses with UNKNOWN_MEMBER_ID.
        assert!(
            ConsumerGroupMetadata::new("g")
                .with_generation(0)
                .is_member()
        );
    }
}
