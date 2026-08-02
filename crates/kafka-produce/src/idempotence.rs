//! Idempotence: a producer id, an epoch, and a sequence number per partition.
//!
//! # What this buys, and why it changes the retry rule
//!
//! Without it, a produce whose outcome is *unknown* — a timeout, a connection
//! that died in flight — can never be re-sent, because the records may already
//! be in the log and re-sending would duplicate them. That is why M13's
//! dispatcher retries rejections and surfaces ambiguity to the caller.
//!
//! With a producer id, every record carries a `(producer_id, epoch, sequence)`
//! the broker remembers. Re-sending a batch it already appended is *detected*
//! and answered with the original offsets rather than appended twice, so an
//! ambiguous failure becomes safe to retry. That is the whole milestone: an
//! ordinary leader election stops being a delivery failure.
//!
//! # Routing: not the transaction coordinator
//!
//! `kafka-meta`'s routing table sends `InitProducerId` to the transaction
//! coordinator, which is right for M15's transactional producer and wrong here.
//! An idempotent-only producer has **no transactional id**, and the coordinator
//! is resolved *by* that id — there is nothing to look one up with. Java sends
//! this to any broker, and so do we. The table is keyed on api key alone and
//! cannot express "depends on whether a field is null", so this is a documented
//! exception rather than a table change.

use std::collections::HashMap;

use kafka_conn::protocol::messages::InitProducerIdRequest;
use kafka_conn::{Error, ErrorCode, Result};
use kafka_meta::Cluster;

/// Sequence numbers start here for every partition, and after every reset.
const FIRST_SEQUENCE: i32 = 0;

/// The identity a broker issued this producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProducerIdentity {
    pub producer_id: i64,
    pub producer_epoch: i16,
}

/// What one batch stamps onto its records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BatchIdentity {
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// The sequence of the batch's first record; the rest count up from it.
    pub base_sequence: i32,
}

/// Claim a producer id for idempotent, non-transactional writes.
pub(crate) async fn init_producer_id(cluster: &Cluster) -> Result<ProducerIdentity> {
    // `transaction_timeout_ms` is documented as relevant only when a
    // transactional id is set. Java sends `i32::MAX` on the idempotent-only
    // path and the broker ignores it; matching that keeps us off any
    // broker-side path a different value might select.
    let request = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_transaction_timeout_ms(i32::MAX);

    let response = cluster.send_any(request).await?;

    if let Some(code) = ErrorCode::from_code(response.error_code) {
        return Err(Error::from_code(code, None));
    }

    Ok(ProducerIdentity {
        producer_id: response.producer_id.0,
        producer_epoch: response.producer_epoch,
    })
}

/// Whether a broker error means our producer state is gone and has to be
/// re-established from scratch.
///
/// All three are recoverable, and treating any of them as fatal is the bug
/// this function exists to prevent — a producer id expires on a partition
/// that has been idle longer than `transactional.id.expiration.ms`, which is
/// an ordinary thing for a UI backend's producer to do overnight.
pub(crate) fn invalidates_producer_state(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::UnknownProducerId
            | ErrorCode::OutOfOrderSequenceNumber
            | ErrorCode::InvalidProducerEpoch
    )
}

/// Per-partition sequence numbers.
///
/// The broker requires the sequence for a partition to advance by exactly the
/// record count of each accepted batch. A gap makes it reject everything after
/// it with `OUT_OF_ORDER_SEQUENCE_NUMBER` forever, so a batch that is *not*
/// accepted must give its numbers back rather than burning them.
#[derive(Debug, Default)]
pub(crate) struct Sequences {
    next: HashMap<(String, i32), i32>,
}

impl Sequences {
    /// Reserve `count` numbers for a partition and return the base.
    pub(crate) fn reserve(&mut self, key: (String, i32), count: usize) -> i32 {
        let base = self.next.entry(key).or_insert(FIRST_SEQUENCE);
        let reserved = *base;
        // Kafka sequences are `i32` and are defined to wrap, so this is the
        // arithmetic the protocol asks for rather than a saturation guard.
        *base = reserved.wrapping_add(i32::try_from(count).unwrap_or(i32::MAX));
        reserved
    }

    /// Give a reservation back, because the batch was never appended.
    ///
    /// Sound only because at most one batch per partition is in flight: with
    /// nothing sent after it, rolling back to its base leaves no gap.
    pub(crate) fn release(&mut self, key: (String, i32), base: i32) {
        self.next.insert(key, base);
    }

    /// Forget everything. The broker no longer recognises our producer id, so
    /// every partition restarts from zero under the new one.
    pub(crate) fn reset(&mut self) {
        self.next.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> (String, i32) {
        ("t".to_owned(), 0)
    }

    #[test]
    fn sequences_start_at_zero_and_advance_by_the_record_count() {
        let mut sequences = Sequences::default();
        assert_eq!(sequences.reserve(key(), 10), 0);
        assert_eq!(sequences.reserve(key(), 5), 10);
        assert_eq!(sequences.reserve(key(), 1), 15);
    }

    #[test]
    fn partitions_number_independently() {
        let mut sequences = Sequences::default();
        assert_eq!(sequences.reserve(("t".to_owned(), 0), 4), 0);
        assert_eq!(sequences.reserve(("t".to_owned(), 1), 4), 0);
        assert_eq!(sequences.reserve(("t".to_owned(), 0), 4), 4);
    }

    /// The assertion that protects every later batch: a released reservation
    /// is handed to the next batch, so the broker sees no gap.
    #[test]
    fn a_released_reservation_is_reused_rather_than_burned() {
        let mut sequences = Sequences::default();
        let base = sequences.reserve(key(), 100);
        assert_eq!(base, 0);
        sequences.release(key(), base);
        assert_eq!(
            sequences.reserve(key(), 100),
            0,
            "a failed batch's numbers must be reused, or every later batch is \
             rejected as out of order"
        );
    }

    #[test]
    fn a_reset_restarts_every_partition() {
        let mut sequences = Sequences::default();
        sequences.reserve(("t".to_owned(), 0), 7);
        sequences.reserve(("t".to_owned(), 1), 7);
        sequences.reset();
        assert_eq!(sequences.reserve(("t".to_owned(), 0), 1), 0);
        assert_eq!(sequences.reserve(("t".to_owned(), 1), 1), 0);
    }

    #[test]
    fn sequences_wrap_rather_than_saturating() {
        let mut sequences = Sequences::default();
        sequences.release(key(), i32::MAX - 1);
        assert_eq!(sequences.reserve(key(), 4), i32::MAX - 1);
        // Wrapped, which is what the protocol defines; saturating would stall
        // every later batch on a duplicate sequence.
        assert_eq!(sequences.reserve(key(), 1), i32::MIN + 2);
    }

    #[test]
    fn the_three_recoverable_producer_state_errors_are_recognised() {
        for code in [
            ErrorCode::UnknownProducerId,
            ErrorCode::OutOfOrderSequenceNumber,
            ErrorCode::InvalidProducerEpoch,
        ] {
            assert!(
                invalidates_producer_state(code),
                "{code:?} means the broker lost our producer state and must \
                 re-init rather than fail"
            );
        }
        assert!(!invalidates_producer_state(ErrorCode::NotLeaderOrFollower));
        assert!(!invalidates_producer_state(ErrorCode::MessageTooLarge));
    }
}
