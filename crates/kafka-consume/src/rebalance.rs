//! Rebalance callbacks: the caller's chance to flush before a partition goes.
//!
//! # Why auto-commit is not enough
//!
//! Auto-commit flushes the offsets *this crate* tracks. A caller that keeps its
//! own state keyed by partition — a windowed aggregate, a write-behind buffer,
//! a file handle per partition — has state we know nothing about, and by the
//! time [`GroupConsumer::poll`](crate::GroupConsumer::poll) returns, the
//! partition is gone and another member may already own it. Whatever was in
//! flight is either lost or written after somebody else started writing.
//!
//! So the callback fires **before** the revocation takes effect, while this
//! member still owns the partitions and the broker is still waiting for the
//! acknowledgement that gives them away.
//!
//! # The order, and why it is that way
//!
//! ```text
//! listener.on_revoke  →  auto-commit  →  drop the partitions  →  acknowledge
//! ```
//!
//! The caller flushes *first*, and the offset commit follows. That direction is
//! the safe one for at-least-once: a committed offset always trails data the
//! caller has already written, so a crash between the two re-delivers records
//! rather than skipping them. Committing first and flushing second inverts
//! exactly that, and the window is as long as the caller's flush.
//!
//! [`RebalanceListener::on_assign`] fires after the new partitions are in
//! place, because there is nothing to protect there — a caller warming a cache
//! for a partition it now owns can do it whenever.
//!
//! # Delivery is at-least-once, deliberately
//!
//! Rule 5 says dropping a `poll` future must be safe. A rebalance that has been
//! computed but not yet finished is therefore held on the consumer and retried
//! by the next `poll`, which means a `poll` cancelled *during* `on_revoke` runs
//! `on_revoke` again with the same partitions. That is the tolerable half of
//! the trade: a listener that flushes twice writes the same bytes twice, while
//! a listener that never fires loses them. Make `on_revoke` idempotent.
//!
//! # An error from a listener does not stop the rebalance
//!
//! By the time the callback runs, the group has already moved on: the broker
//! computed this assignment and is waiting for the acknowledgement. Refusing to
//! revoke would leave the member holding partitions the group has given away —
//! the double-ownership KIP-848's revoke-then-acknowledge ordering exists to
//! prevent. So an `Err` is logged at `warn` and the rebalance proceeds.

use futures::future::BoxFuture;
use kafka_conn::Result;

/// A partition this member is about to give up, and where it had read to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevokedPartition {
    /// The topic.
    pub topic: String,
    /// The partition.
    pub partition: i32,
    /// The offset this member would have read next.
    ///
    /// This is the position auto-commit is about to store, so a listener doing
    /// its own commit should store this same value rather than the offset of
    /// the last record it saw — off by one in the direction that re-delivers,
    /// or worse, skips.
    pub position: i64,
}

/// A hook invoked around a rebalance.
///
/// Both methods are async so a listener can do real work — flush a buffer,
/// write a checkpoint, commit its own offsets. See the [module
/// docs](crate::rebalance) for the ordering guarantees, the at-least-once
/// delivery of `on_revoke`, and what an `Err` does (it is logged; the rebalance
/// continues).
///
/// ```no_run
/// use futures::future::BoxFuture;
/// use kafka_consume::{RebalanceListener, Result, RevokedPartition};
///
/// struct FlushOnRevoke;
///
/// impl RebalanceListener for FlushOnRevoke {
///     fn on_revoke(&mut self, revoked: Vec<RevokedPartition>) -> BoxFuture<'_, Result<()>> {
///         Box::pin(async move {
///             for partition in revoked {
///                 println!("flushing {}-{} at {}", partition.topic, partition.partition, partition.position);
///             }
///             Ok(())
///         })
///     }
/// }
/// ```
/// `Send + Sync` because a consumer holding one must itself be movable to —
/// and pollable from — a spawned task: driving each member from its own task
/// is a documented requirement of the classic protocol (see
/// [`crate::classic`]), and the poll future holds `&self` across awaits.
pub trait RebalanceListener: Send + Sync {
    /// Fires while this member still owns `revoked`, before anything is given
    /// up and before auto-commit runs.
    fn on_revoke(&mut self, revoked: Vec<RevokedPartition>) -> BoxFuture<'_, Result<()>>;

    /// Fires once `assigned` is owned and readable.
    ///
    /// Defaults to doing nothing: gaining a partition needs no protection, so a
    /// listener that only cares about flushing implements one method.
    fn on_assign(&mut self, assigned: Vec<(String, i32)>) -> BoxFuture<'_, Result<()>> {
        let _ = assigned;
        Box::pin(std::future::ready(Ok(())))
    }
}

/// The listener slot a group consumer holds.
pub(crate) type Listener = Option<Box<dyn RebalanceListener>>;

/// Call `on_revoke`, swallowing (but reporting) whatever it says.
pub(crate) async fn revoke(listener: &mut Listener, revoked: Vec<RevokedPartition>) {
    let Some(listener) = listener.as_mut() else {
        return;
    };
    if let Err(error) = listener.on_revoke(revoked).await {
        tracing::warn!(%error, "a rebalance listener failed on revoke; revoking anyway");
    }
}

/// Call `on_assign`, swallowing (but reporting) whatever it says.
pub(crate) async fn assign(listener: &mut Listener, assigned: Vec<(String, i32)>) {
    let Some(listener) = listener.as_mut() else {
        return;
    };
    if let Err(error) = listener.on_assign(assigned).await {
        tracing::warn!(%error, "a rebalance listener failed on assign");
    }
}

/// A rebalance computed but not yet carried out.
///
/// Held on the consumer rather than run inline so a cancelled `poll` cannot
/// skip the callback: the next `poll` finds it and finishes it. The broker is
/// still waiting for our acknowledgement at that point, so "before revocation"
/// is still true a poll later.
#[derive(Debug, Default)]
pub(crate) struct Pending {
    /// Partitions to give up, with the positions they held.
    pub revoked: Vec<RevokedPartition>,
    /// Partitions newly owned, reported after the fact.
    pub gained: Vec<(String, i32)>,
}

impl Pending {
    pub(crate) fn is_empty(&self) -> bool {
        self.revoked.is_empty() && self.gained.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// What a listener saw, in the order it saw it.
    #[derive(Debug, Default)]
    struct Log {
        events: Vec<String>,
    }

    struct Recorder {
        log: Arc<Mutex<Log>>,
        fail: bool,
    }

    impl RebalanceListener for Recorder {
        fn on_revoke(&mut self, revoked: Vec<RevokedPartition>) -> BoxFuture<'_, Result<()>> {
            let log = Arc::clone(&self.log);
            let fail = self.fail;
            Box::pin(async move {
                log.lock().unwrap().events.push(format!(
                    "revoke {:?}",
                    revoked
                        .iter()
                        .map(|p| (p.topic.clone(), p.partition, p.position))
                        .collect::<Vec<_>>()
                ));
                if fail {
                    return Err(kafka_conn::Error::InvalidRequest("no".to_owned()));
                }
                Ok(())
            })
        }

        fn on_assign(&mut self, assigned: Vec<(String, i32)>) -> BoxFuture<'_, Result<()>> {
            let log = Arc::clone(&self.log);
            Box::pin(async move {
                log.lock()
                    .unwrap()
                    .events
                    .push(format!("assign {assigned:?}"));
                Ok(())
            })
        }
    }

    fn revoked(topic: &str, partition: i32, position: i64) -> RevokedPartition {
        RevokedPartition {
            topic: topic.to_owned(),
            partition,
            position,
        }
    }

    #[tokio::test]
    async fn a_listener_sees_the_partitions_and_the_positions_they_held() {
        let log = Arc::new(Mutex::new(Log::default()));
        let mut listener: Listener = Some(Box::new(Recorder {
            log: Arc::clone(&log),
            fail: false,
        }));

        revoke(&mut listener, vec![revoked("t", 3, 99)]).await;
        assign(&mut listener, vec![("t".to_owned(), 4)]).await;

        let events = log.lock().unwrap().events.clone();
        assert_eq!(
            events,
            vec!["revoke [(\"t\", 3, 99)]", "assign [(\"t\", 4)]"]
        );
    }

    /// The whole point of the `Err` handling: a listener that fails must not
    /// leave the member holding partitions the group has already reassigned.
    #[tokio::test]
    async fn a_failing_listener_does_not_abort_the_rebalance() {
        let log = Arc::new(Mutex::new(Log::default()));
        let mut listener: Listener = Some(Box::new(Recorder {
            log: Arc::clone(&log),
            fail: true,
        }));

        // No panic, no propagation — the caller of `revoke` gets `()`.
        revoke(&mut listener, vec![revoked("t", 0, 1)]).await;
        assert_eq!(log.lock().unwrap().events.len(), 1);
    }

    #[tokio::test]
    async fn no_listener_is_not_an_error() {
        let mut listener: Listener = None;
        revoke(&mut listener, vec![revoked("t", 0, 1)]).await;
        assign(&mut listener, vec![("t".to_owned(), 0)]).await;
    }

    /// The default `on_assign` exists so a flush-only listener implements one
    /// method rather than two.
    #[tokio::test]
    async fn on_assign_defaults_to_doing_nothing() {
        struct RevokeOnly;
        impl RebalanceListener for RevokeOnly {
            fn on_revoke(&mut self, _: Vec<RevokedPartition>) -> BoxFuture<'_, Result<()>> {
                Box::pin(std::future::ready(Ok(())))
            }
        }
        let mut listener: Listener = Some(Box::new(RevokeOnly));
        assign(&mut listener, vec![("t".to_owned(), 0)]).await;
    }

    #[test]
    fn an_empty_rebalance_is_recognisable() {
        assert!(Pending::default().is_empty());
        assert!(
            !Pending {
                revoked: vec![revoked("t", 0, 0)],
                gained: Vec::new(),
            }
            .is_empty()
        );
        assert!(
            !Pending {
                revoked: Vec::new(),
                gained: vec![("t".to_owned(), 0)],
            }
            .is_empty()
        );
    }
}
