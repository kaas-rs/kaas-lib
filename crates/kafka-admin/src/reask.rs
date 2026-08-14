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

use std::future::Future;
use std::time::Instant;

use kafka_meta::{Cluster, CoordinatorKind};

use crate::types::PerItem;

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

    loop {
        let outcomes = round(std::mem::take(&mut pending)).await?;

        let mut latest: PerItem<K, T> = Vec::new();
        let mut refresh_metadata = false;
        let mut refresh_coordinator = false;
        for (key, outcome) in outcomes {
            let code = match &outcome {
                Err(error) => error.code(),
                Ok(_) => None,
            };
            match code.filter(|code| code.retriable_for_named_resource()) {
                Some(code) => {
                    refresh_metadata |= code.needs_metadata_refresh();
                    refresh_coordinator |= code.needs_coordinator_refresh();
                    pending.push(key.clone());
                    latest.push((key, outcome));
                }
                None => settled.push((key, outcome)),
            }
        }

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
