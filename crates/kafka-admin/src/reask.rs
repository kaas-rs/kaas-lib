//! Re-asking the retriable slice of a per-item admin result (#23).
//!
//! Admin RPCs ride `Cluster::dispatch`, which retries the errors it can see —
//! transport failures and top-level codes like `NOT_CONTROLLER`. But the
//! *normal* error shape for admin APIs is a code per item inside an `Ok`
//! response: one topic of a `CreateTopics` mid-election, one partition of a
//! `ListOffsets` whose leader just moved. The dispatcher has finished by the
//! time anything decodes those, so without this layer a transiently-retriable
//! item and a genuinely-failed item looked identical in the final `PerItem`
//! (rule 4's shape, with rule 4's promise quietly broken for the transient
//! half).
//!
//! [`per_item_retrying`] narrows and re-asks: the round closure is re-run
//! with only the still-retriable items, under the cluster's configured
//! [`kafka_meta::RetryPolicy`] — the same jittered pacing and coordinator
//! deadline as every other re-ask since #22. Retriability is judged on the
//! [`retriable_for_named_resource`](kafka_conn::ErrorCode::retriable_for_named_resource)
//! axis, because every admin call names its resources and a typo'd topic must
//! stay an answer, not become a spinner. On budget exhaustion the last
//! per-item errors are returned unchanged.
//!
//! # The stale-view allowance
//!
//! That axis is right about typos and wrong about one real case, which the
//! first live run against a three-broker cluster found immediately: growing a
//! topic and then listing offsets answers `UNKNOWN_TOPIC_OR_PARTITION` for a
//! partition that certainly exists, because the broker that answered has not
//! caught up with the controller yet. Two runs in three failed that way.
//!
//! The code is identical in both situations, so the code alone cannot settle
//! it — but a **metadata refresh is new information**. So a code that is not
//! retriable-for-a-named-resource but does say
//! [`needs_metadata_refresh`](kafka_conn::ErrorCode::needs_metadata_refresh)
//! buys exactly one refresh-and-re-ask for the whole call. A stale view is
//! corrected; a name that does not exist comes back the same way it went and
//! costs one extra round trip, never a spinner.

use std::future::Future;
use std::time::Instant;

use kafka_conn::ErrorCode;
use kafka_meta::{Cluster, CoordinatorKind};

use crate::types::PerItem;

/// What the re-ask loop should do about one item's error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Disposition {
    /// Final. Hand it to the caller as it came.
    Settle,
    /// Ask again under the retry budget.
    Reask,
    /// Ask again, but only after refreshing metadata, and only once for the
    /// whole call — see the module docs on the stale-view allowance.
    RefreshOnce,
}

/// Decide what to do with a per-item code.
///
/// Pure, and separate from the loop, because this is the whole judgement
/// call: everything else is bookkeeping.
pub(crate) fn disposition(code: ErrorCode, stale_view_allowance_spent: bool) -> Disposition {
    if code.retriable_for_named_resource() {
        Disposition::Reask
    } else if code.needs_metadata_refresh() && !stale_view_allowance_spent {
        Disposition::RefreshOnce
    } else {
        Disposition::Settle
    }
}

/// Which cache a retriable per-item code makes stale.
#[derive(Clone, Copy)]
pub(crate) enum Axis<'a> {
    /// Leader- or controller-routed: the metadata snapshot is what to
    /// refresh — the round closure re-resolves leaders/controller from it.
    Metadata,
    /// Coordinator-routed for this group or transactional id: the cached
    /// coordinator is what to drop.
    Coordinator(CoordinatorKind, &'a str),
}

/// Run `round` over `items`, then keep re-asking only the items that came
/// back with a retriable error code.
///
/// `round` performs one whole request/decode cycle for the subset it is
/// given and returns a `PerItem` keyed by the *work item*, so the helper can
/// hand the retriable ones straight back to it. Result order follows
/// settlement, not submission — callers that grouped by leader already gave
/// up submission order.
pub(crate) async fn per_item_retrying<K, T, F, Fut>(
    cluster: &Cluster,
    axis: Axis<'_>,
    items: Vec<K>,
    mut round: F,
) -> kafka_conn::Result<PerItem<K, T>>
where
    K: Clone,
    F: FnMut(Vec<K>) -> Fut,
    Fut: Future<Output = kafka_conn::Result<PerItem<K, T>>>,
{
    let policy = cluster.retry();
    let budget = Instant::now() + policy.coordinator_timeout;
    let mut settled: PerItem<K, T> = Vec::new();
    let mut pending = items;
    let mut attempt: u32 = 1;
    let mut stale_view_allowance_spent = false;

    loop {
        let outcomes = round(std::mem::take(&mut pending)).await?;

        let mut latest: PerItem<K, T> = Vec::new();
        let mut refresh_metadata = false;
        let mut refresh_coordinator = false;
        let mut spending_allowance = false;
        for (key, outcome) in outcomes {
            let code = match &outcome {
                Err(error) => error.code(),
                Ok(_) => None,
            };
            let disposition = code.map_or(Disposition::Settle, |code| {
                disposition(code, stale_view_allowance_spent)
            });
            match disposition {
                Disposition::Settle => settled.push((key, outcome)),
                Disposition::Reask | Disposition::RefreshOnce => {
                    if let Some(code) = code {
                        refresh_metadata |= code.needs_metadata_refresh();
                        refresh_coordinator |= code.needs_coordinator_refresh();
                    }
                    spending_allowance |= disposition == Disposition::RefreshOnce;
                    pending.push(key.clone());
                    latest.push((key, outcome));
                }
            }
        }
        // Spent per call, not per item: one refresh answers the question for
        // every item that was asking it.
        stale_view_allowance_spent |= spending_allowance;

        if pending.is_empty() {
            return Ok(settled);
        }

        // The upcoming delay counts against the budget, so the loop never
        // sleeps out the clock only to send a request whose answer it will
        // not wait for. On exhaustion the retriable items keep their last
        // error as it came (rule 4).
        let delay = policy.delay(attempt.saturating_add(1));
        if Instant::now() + delay >= budget {
            settled.extend(latest);
            return Ok(settled);
        }

        match axis {
            Axis::Metadata => {
                if refresh_metadata {
                    cluster.refresh().await.ok();
                }
            }
            Axis::Coordinator(kind, key) => {
                if refresh_coordinator {
                    cluster.invalidate_coordinator(kind, key);
                }
                if refresh_metadata {
                    cluster.refresh().await.ok();
                }
            }
        }

        tracing::debug!(
            still_retriable = pending.len(),
            attempt,
            "per-item admin codes are retriable; narrowing and re-asking"
        );
        tokio::time::sleep(delay).await;
        attempt = attempt.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The judgement the live run forced (#23). `UNKNOWN_TOPIC_OR_PARTITION`
    /// is the same code whether the partition is seconds old or imaginary, so
    /// the rule is about *information*, not about the code: one metadata
    /// refresh is new information, and a second identical answer after it is
    /// not.
    #[test]
    fn a_stale_view_buys_one_refresh_and_no_more() {
        let code = ErrorCode::UnknownTopicOrPartition;
        assert!(
            !code.retriable_for_named_resource(),
            "the premise: naming a resource makes this non-retriable, which is \
             what keeps a typo from spinning"
        );
        assert_eq!(disposition(code, false), Disposition::RefreshOnce);
        assert_eq!(
            disposition(code, true),
            Disposition::Settle,
            "after the refresh the answer is the answer"
        );
    }

    /// The ordinary retriable case is unaffected, and keeps its full budget
    /// rather than being spent down to one attempt.
    #[test]
    fn a_leader_move_still_retries_under_the_normal_budget() {
        for code in [
            ErrorCode::NotLeaderOrFollower,
            ErrorCode::LeaderNotAvailable,
            ErrorCode::CoordinatorNotAvailable,
        ] {
            assert_eq!(disposition(code, false), Disposition::Reask, "{code:?}");
            assert_eq!(disposition(code, true), Disposition::Reask, "{code:?}");
        }
    }

    /// And a genuine, permanent refusal settles immediately either way — the
    /// allowance must not turn an authorization failure into a round trip.
    #[test]
    fn a_terminal_code_never_buys_anything() {
        for code in [
            ErrorCode::TopicAuthorizationFailed,
            ErrorCode::InvalidTopicException,
            ErrorCode::TopicAlreadyExists,
        ] {
            assert_eq!(disposition(code, false), Disposition::Settle, "{code:?}");
        }
    }
}
