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
//! The loop itself is [`kafka_meta::reask`] (#22 unified the private copies
//! this module and `kafka-produce` used to keep): pacing follows the
//! cluster's configured [`kafka_meta::RetryPolicy`] — jittered exponential,
//! not the flat 500ms this module once hardcoded — and the budget is the
//! policy's `coordinator_timeout` unless the caller's own deadline replaces
//! it. What stays here is only what is consume-specific: which cache to
//! invalidate, and that a given-up response goes back unchanged so per-item
//! results survive (rule 4).

use std::time::Instant;

use kafka_conn::{ErrorCode, Result, Rpc};
use kafka_meta::{Cluster, CoordinatorKind, RetryPolicy, Verdict};

/// Send to the group coordinator, re-asking while the decoded response says we
/// asked the wrong broker.
///
/// `policy` is the consumer's resolved retry policy — [`ConsumerConfig::retry`]
/// when set, the cluster's otherwise (#24) — so a caller-configured posture
/// reaches every coordinator re-ask.
///
/// `code_of` pulls the code out of the decoded response. On give-up the
/// response is returned **unchanged** rather than converted to an error, so
/// per-item results stay per-item (rule 4) and the caller's own decoding is
/// untouched — this only decides whether to ask again.
pub(crate) async fn send_retrying<R, F>(
    cluster: &Cluster,
    policy: RetryPolicy,
    group_id: &str,
    request: R,
    code_of: F,
) -> Result<R::Response>
where
    R: Rpc + Clone,
    F: Fn(&R::Response) -> Option<ErrorCode>,
{
    send_retrying_until(cluster, policy, group_id, request, None, code_of).await
}

/// [`send_retrying`], with a deadline for the RPCs that block on purpose.
///
/// `JoinGroup` and `SyncGroup` sit in the coordinator's purgatory until the
/// group forms, so their budget is the member's rebalance timeout rather than
/// the connection's `request_timeout`. Every other coordinator RPC answers
/// promptly and wants the connection default, which is what `None` selects.
/// The deadline *replaces* the policy budget — see [`kafka_meta::reask`].
pub(crate) async fn send_retrying_until<R, F>(
    cluster: &Cluster,
    policy: RetryPolicy,
    group_id: &str,
    request: R,
    deadline: Option<Instant>,
    code_of: F,
) -> Result<R::Response>
where
    R: Rpc + Clone,
    F: Fn(&R::Response) -> Option<ErrorCode>,
{
    kafka_meta::reask(&policy, deadline, |_attempt| {
        let request = request.clone();
        let code_of = &code_of;
        async move {
            let response = match deadline {
                Some(deadline) => {
                    cluster
                        .send_to_coordinator_until(
                            CoordinatorKind::Group,
                            group_id,
                            request,
                            deadline,
                        )
                        .await?
                }
                None => {
                    cluster
                        .send_to_coordinator(CoordinatorKind::Group, group_id, request)
                        .await?
                }
            };

            match code_of(&response) {
                Some(code) if code.needs_coordinator_refresh() => {
                    cluster.invalidate_coordinator(CoordinatorKind::Group, group_id);
                    tracing::debug!(
                        group_id = %kafka_conn::control_safe(group_id),
                        %code,
                        "coordinator moved; re-asking"
                    );
                    Ok(Verdict::Reask(Ok(response)))
                }
                _ => Ok(Verdict::Settled(response)),
            }
        }
    })
    .await
}
