//! KIP-227 incremental fetch sessions.
//!
//! # What a session is for, and why `scan` does not have one
//!
//! A consumer fetches the same partitions over and over. Without a session,
//! every request re-sends the full assignment — topic ids, partition indexes,
//! offsets, byte budgets — and the broker re-resolves all of it. KIP-227 lets
//! the broker remember the assignment: the first request establishes it, and
//! every request after that sends **only what changed**, which in steady state
//! is nothing at all.
//!
//! `kafka-read`'s `scan` and `tail` deliberately keep the legacy sentinel
//! (`session_id = 0, session_epoch = -1`). They are one-shot: a UI asks for a
//! page of a partition and goes away. A session would make each scan depend on
//! the last, and leave state on the broker for a client that is not coming
//! back.
//!
//! # The epoch rules, which are easy to get subtly wrong
//!
//! * `(0, 0)` opens a session. Not `(0, -1)` — that is the *legacy* sentinel
//!   meaning "no session at all", and sending it forever is how a consumer
//!   silently re-sends its whole assignment on every fetch while appearing to
//!   work perfectly.
//! * After the broker answers with a session id, every subsequent request uses
//!   `(session_id, epoch + 1)`.
//! * A partition that leaves the assignment goes in `forgotten_topics_data`
//!   **once**, on the next request. Leaving it out instead means the broker
//!   keeps fetching a partition nobody is reading.
//! * `FETCH_SESSION_ID_NOT_FOUND` and `INVALID_FETCH_SESSION_EPOCH` mean the
//!   broker dropped the session — it restarted, or evicted us under cache
//!   pressure. Both are recovered by starting a new session with the full
//!   assignment, and neither is ever surfaced to the caller. A broker restart
//!   must not kill a consumer.

use std::collections::{HashMap, HashSet};

use kafka_conn::ErrorCode;

/// The epoch that opens a new session.
const INITIAL_EPOCH: i32 = 0;

/// The session id meaning "no session yet".
const NO_SESSION: i32 = 0;

/// What one partition's fetch state looks like to the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PartitionState {
    /// Where the next fetch starts.
    pub offset: i64,
    /// Per-partition byte budget.
    pub max_bytes: i32,
}

/// One broker's incremental fetch session.
///
/// Tracks what the broker has been told, so the next request can carry the
/// difference rather than the whole assignment.
#[derive(Debug, Default)]
pub(crate) struct FetchSession {
    id: i32,
    epoch: i32,
    /// What the broker currently believes, keyed by `(topic, partition)`.
    known: HashMap<(String, i32), PartitionState>,
    /// Partitions removed since the last request, to be forgotten in the next.
    forgotten: HashSet<(String, i32)>,
}

/// What one request should carry.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Delta {
    pub session_id: i32,
    pub session_epoch: i32,
    /// Partitions whose state the broker does not already know.
    ///
    /// Empty in steady state — and asserting on that emptiness is the only
    /// thing that proves the session is live rather than silently re-sending
    /// everything.
    pub include: Vec<((String, i32), PartitionState)>,
    /// Partitions the broker should stop tracking.
    pub forget: Vec<(String, i32)>,
    /// Whether this is a full fetch that replaces the broker's whole view.
    pub full: bool,
}

impl FetchSession {
    /// Compute what the next request carries, and record that we sent it.
    ///
    /// `wanted` is the complete current assignment for this broker. Anything
    /// the broker knows and `wanted` does not is forgotten.
    pub(crate) fn next(&mut self, wanted: &HashMap<(String, i32), PartitionState>) -> Delta {
        let full = self.id == NO_SESSION;

        let include: Vec<((String, i32), PartitionState)> = if full {
            // Opening a session sends everything: there is nothing for the
            // broker to diff against.
            wanted.iter().map(|(key, st)| (key.clone(), *st)).collect()
        } else {
            wanted
                .iter()
                .filter(|(key, state)| self.known.get(*key) != Some(*state))
                .map(|(key, state)| (key.clone(), *state))
                .collect()
        };

        for key in self.known.keys() {
            if !wanted.contains_key(key) {
                self.forgotten.insert(key.clone());
            }
        }
        let forget: Vec<(String, i32)> = self.forgotten.drain().collect();

        let delta = Delta {
            session_id: self.id,
            session_epoch: if full { INITIAL_EPOCH } else { self.epoch },
            include,
            forget,
            full,
        };

        // Record optimistically. A request that fails takes the session with
        // it — `reset` puts us back to a full fetch — so there is no state to
        // unwind.
        self.known = wanted.clone();
        delta
    }

    /// Adopt the session the broker answered with.
    pub(crate) fn accept(&mut self, session_id: i32, responded_epoch: i32) {
        self.id = session_id;
        // The broker echoes the epoch it has recorded; the next request is one
        // past it. Deriving it from our own counter instead drifts the moment
        // a request is retried.
        self.epoch = responded_epoch.saturating_add(1);
    }

    /// Throw the session away and go back to a full fetch.
    pub(crate) fn reset(&mut self) {
        self.id = NO_SESSION;
        self.epoch = INITIAL_EPOCH;
        self.known.clear();
        self.forgotten.clear();
    }

    pub(crate) fn id(&self) -> i32 {
        self.id
    }
}

/// Whether an error means the broker dropped our session.
///
/// Both are recovered by rebuilding, and **neither is ever surfaced to the
/// caller**: a broker restart or a session eviction under cache pressure is
/// not something a consumer's user can act on, and reporting it would make an
/// ordinary operational event look like data loss.
pub(crate) fn session_lost(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::FetchSessionIdNotFound | ErrorCode::InvalidFetchSessionEpoch
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(offset: i64) -> PartitionState {
        PartitionState {
            offset,
            max_bytes: 1024,
        }
    }

    fn wanted(entries: &[(&str, i32, i64)]) -> HashMap<(String, i32), PartitionState> {
        entries
            .iter()
            .map(|(topic, partition, offset)| (((*topic).to_owned(), *partition), state(*offset)))
            .collect()
    }

    /// The first request opens a session with `(0, 0)` — *not* the legacy
    /// `(0, -1)`, which asks for no session at all.
    #[test]
    fn the_first_request_opens_a_session_rather_than_declining_one() {
        let mut session = FetchSession::default();
        let delta = session.next(&wanted(&[("t", 0, 5)]));

        assert_eq!(delta.session_id, 0);
        assert_eq!(
            delta.session_epoch, 0,
            "epoch -1 is the legacy sentinel and would mean no session at all"
        );
        assert!(delta.full);
        assert_eq!(delta.include.len(), 1);
    }

    /// The assertion the whole milestone rests on: once the broker knows the
    /// assignment, a request that changes nothing carries nothing.
    #[test]
    fn a_steady_state_request_carries_no_partitions() {
        let mut session = FetchSession::default();
        let assignment = wanted(&[("t", 0, 5), ("t", 1, 9)]);

        session.next(&assignment);
        session.accept(77, 0);

        let delta = session.next(&assignment);
        assert_eq!(delta.session_id, 77);
        assert_eq!(delta.session_epoch, 1);
        assert!(!delta.full);
        assert!(
            delta.include.is_empty(),
            "an unchanged assignment must send an empty topics array; anything \
             else is a full fetch wearing a session id"
        );
        assert!(delta.forget.is_empty());
    }

    /// An advanced offset is a change, so it *is* sent — the session elides
    /// what is unchanged, not what matters.
    #[test]
    fn an_advanced_offset_is_sent() {
        let mut session = FetchSession::default();
        session.next(&wanted(&[("t", 0, 5)]));
        session.accept(77, 0);

        let delta = session.next(&wanted(&[("t", 0, 12)]));
        assert_eq!(delta.include.len(), 1);
        assert_eq!(delta.include[0].1.offset, 12);
    }

    #[test]
    fn a_dropped_partition_is_forgotten_once() {
        let mut session = FetchSession::default();
        session.next(&wanted(&[("t", 0, 5), ("t", 1, 5)]));
        session.accept(77, 0);

        let delta = session.next(&wanted(&[("t", 0, 5)]));
        assert_eq!(delta.forget, vec![("t".to_owned(), 1)]);

        // Once, not forever: repeating it every request wastes the bytes the
        // session exists to save.
        let delta = session.next(&wanted(&[("t", 0, 5)]));
        assert!(delta.forget.is_empty());
    }

    #[test]
    fn a_new_partition_is_added_without_a_full_fetch() {
        let mut session = FetchSession::default();
        session.next(&wanted(&[("t", 0, 5)]));
        session.accept(77, 0);

        let delta = session.next(&wanted(&[("t", 0, 5), ("t", 1, 0)]));
        assert!(!delta.full);
        assert_eq!(delta.include, vec![(("t".to_owned(), 1), state(0))]);
    }

    /// The epoch comes from the broker's answer, not from our own counter.
    /// Deriving it locally drifts the moment a request is retried.
    #[test]
    fn the_epoch_follows_the_brokers_answer() {
        let mut session = FetchSession::default();
        session.next(&wanted(&[("t", 0, 0)]));
        session.accept(77, 4);
        assert_eq!(session.next(&wanted(&[("t", 0, 0)])).session_epoch, 5);

        session.accept(77, 9);
        assert_eq!(session.next(&wanted(&[("t", 0, 0)])).session_epoch, 10);
    }

    /// A dropped session rebuilds from nothing, with the full assignment.
    #[test]
    fn a_reset_goes_back_to_a_full_fetch() {
        let mut session = FetchSession::default();
        let assignment = wanted(&[("t", 0, 5), ("t", 1, 9)]);
        session.next(&assignment);
        session.accept(77, 0);
        session.next(&assignment);

        session.reset();
        let delta = session.next(&assignment);
        assert!(delta.full);
        assert_eq!(delta.session_id, 0);
        assert_eq!(delta.session_epoch, 0);
        assert_eq!(
            delta.include.len(),
            2,
            "a rebuilt session must re-send the whole assignment"
        );
    }

    #[test]
    fn both_session_errors_are_recognised_as_recoverable() {
        assert!(session_lost(ErrorCode::FetchSessionIdNotFound));
        assert!(session_lost(ErrorCode::InvalidFetchSessionEpoch));
        assert!(!session_lost(ErrorCode::NotLeaderOrFollower));
        assert!(!session_lost(ErrorCode::OffsetOutOfRange));
    }
}
