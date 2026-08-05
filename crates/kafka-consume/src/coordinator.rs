//! Re-asking a coordinator that has moved.
//!
//! `Cluster::dispatch` retries on `Err`, and a coordinator that has moved does
//! not produce one: the round trip *succeeds*, and `NOT_COORDINATOR` arrives as
//! a field inside the response — top-level on a heartbeat, per partition on an
//! `OffsetCommit`. The routing layer has finished with the request by the time
//! anything decodes that field, so nothing invalidates the cached coordinator
//! and nothing asks again. Every KIP-848 acceptance test failed this way, in
//! under ten seconds, which is itself the tell: a budget being consulted would
//! have spent it.
//!
//! So the re-ask lives here, above the decode. `kafka-produce` reached the same
//! conclusion for the transaction coordinator and keeps its own copy — which is
//! why transactions never showed this bug. Two private helpers rather than one
//! shared public one is deliberate for now: this is a lockstep release, and a
//! new public method on `kafka-meta` that `kafka-consume` calls in the same
//! version is exactly what `cargo publish --workspace --dry-run` refuses to
//! verify. Worth unifying once both are published.

use std::time::{Duration, Instant};

use kafka_conn::{ErrorCode, Result, Rpc};
use kafka_meta::{Cluster, CoordinatorKind};

/// How long to keep re-asking before handing the answer back as it came.
///
/// A deadline rather than an attempt count, because this class of error is not
/// "the request failed" but "ask again in a moment": the group is being handed
/// to a new coordinator, or `__consumer_offsets` is still being read. How many
/// attempts that takes is a function of the backoff curve; how long it takes is
/// a property of the cluster. Half of Java's `default.api.timeout.ms`, because
/// this library backs a UI and a genuinely coordinator-less cluster should
/// still report something before a person gives up on the page.
const COORDINATOR_TIMEOUT: Duration = Duration::from_secs(30);

/// Wait between re-asks. Flat rather than exponential: a handover completes on
/// the cluster's schedule, and backing off past it only adds latency.
const BACKOFF: Duration = Duration::from_millis(500);

/// Send to the group coordinator, re-asking while the decoded response says we
/// asked the wrong broker.
///
/// `code_of` pulls the code out of the decoded response. On give-up the
/// response is returned **unchanged** rather than converted to an error, so
/// per-item results stay per-item (rule 4) and the caller's own decoding is
/// untouched — this only decides whether to ask again.
pub(crate) async fn send_retrying<R, F>(
    cluster: &Cluster,
    group_id: &str,
    request: R,
    code_of: F,
) -> Result<R::Response>
where
    R: Rpc + Clone,
    F: Fn(&R::Response) -> Option<ErrorCode>,
{
    let started = Instant::now();
    loop {
        let response = cluster
            .send_to_coordinator(CoordinatorKind::Group, group_id, request.clone())
            .await?;

        let Some(code) = code_of(&response) else {
            return Ok(response);
        };
        if !code.needs_coordinator_refresh() || started.elapsed() >= COORDINATOR_TIMEOUT {
            return Ok(response);
        }

        cluster.invalidate_coordinator(CoordinatorKind::Group, group_id);
        tracing::debug!(group_id, %code, "coordinator moved; re-asking");
        tokio::time::sleep(BACKOFF).await;
    }
}
