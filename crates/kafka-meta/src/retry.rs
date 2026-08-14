//! Capped, jittered backoff.
//!
//! Jitter is not decoration. Without it, every connection in a pool that lost
//! the same broker retries on the same schedule forever, and the cluster gets a
//! synchronised thundering herd on top of whatever knocked the broker over.
//!
//! [`reask`] is the other half (#22): `Cluster::dispatch` retries errors the
//! transport surfaces as `Err`, but the *normal* failure shape for
//! coordinator- and leader-routed RPCs is a code inside an `Ok` response —
//! `NOT_COORDINATOR` on a heartbeat, `NOT_LEADER_OR_FOLLOWER` per partition —
//! which only the caller's decode can see. Every crate above this one grew its
//! own flat-constant loop for that; `reask` is the one driver they now share,
//! so the pacing is jittered, the budget is [`RetryPolicy::coordinator_timeout`],
//! and a caller-configured policy is honoured everywhere.

use std::future::Future;
use std::time::{Duration, Instant};

use kafka_conn::Result;
use rand::Rng;

/// How hard to retry.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Total attempts, including the first.
    pub max_attempts: u32,
    /// Delay before the second attempt.
    pub base_delay: Duration,
    /// Ceiling for the exponential growth.
    pub max_delay: Duration,
    /// Fraction of the delay to randomise, `0.0..=1.0`.
    pub jitter: f64,
    /// How long to keep retrying an error that names a **cluster-side
    /// handover**: a coordinator that moved or has not finished loading
    /// (`NOT_COORDINATOR`, `COORDINATOR_NOT_AVAILABLE`,
    /// `COORDINATOR_LOAD_IN_PROGRESS`), or a partition leader that is being
    /// re-elected (`NOT_LEADER_OR_FOLLOWER`, `LEADER_NOT_AVAILABLE`, a
    /// connection refused by a broker that just died — anything
    /// [`needs_metadata_refresh`](kafka_conn::Error::needs_metadata_refresh)
    /// on the produce path).
    ///
    /// A **deadline** rather than an attempt count, because this class of
    /// error is not "the request failed" but "ask again in a moment": the
    /// group is being handed to a new coordinator, or the partition to a new
    /// leader. How many attempts that takes is a function of the backoff
    /// curve, not of the cluster; how long it takes is a property of the
    /// cluster.
    ///
    /// [`max_attempts`](Self::max_attempts) governs it otherwise, and five
    /// attempts is ~1.5s at the default curve — shorter than a routine
    /// election, so a caller saw a raw `NOT_COORDINATOR` for something that
    /// resolves itself, and an idempotent producer dropped records into a
    /// leader restart it exists to ride out. Java bounds the same cases by
    /// `default.api.timeout.ms` / `delivery.timeout.ms`, 60s and 120s.
    pub coordinator_timeout: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            jitter: 0.3,
            // Half Java's, because this library backs a UI: long enough to
            // ride out an election or a cold `__consumer_offsets`, short
            // enough that a genuinely coordinator-less cluster still reports
            // something before a person gives up on the page.
            coordinator_timeout: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// Never retry — for callers that would rather see the first failure.
    ///
    /// Zeroes the coordinator deadline too: "never retry" has to mean it on
    /// both axes, or a caller that asked for the first failure still waits
    /// half a minute for a coordinator one.
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            coordinator_timeout: Duration::ZERO,
            ..Self::default()
        }
    }

    /// The delay before attempt `attempt`, one-based.
    ///
    /// `attempt <= 1` is the first try and has no delay.
    pub fn delay(&self, attempt: u32) -> Duration {
        if attempt <= 1 {
            return Duration::ZERO;
        }
        let exponent = attempt.saturating_sub(2).min(16);
        let scaled = self
            .base_delay
            .saturating_mul(2u32.saturating_pow(exponent))
            .min(self.max_delay);
        self.apply_jitter(scaled)
    }

    fn apply_jitter(&self, delay: Duration) -> Duration {
        let jitter = self.jitter.clamp(0.0, 1.0);
        if jitter == 0.0 {
            return delay;
        }
        // Jitter downwards only, so the cap stays a cap.
        let factor = 1.0 - rand::rng().random_range(0.0..jitter);
        delay.mul_f64(factor)
    }

    /// Whether another attempt is allowed.
    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts
    }
}

/// What one attempt concluded, as the caller's decode saw it.
#[derive(Debug)]
pub enum Verdict<T> {
    /// The answer is final — a success, or a failure no re-ask can change.
    Settled(T),
    /// The answer says "ask again in a moment": a coordinator moved, a leader
    /// is mid-election, `__consumer_offsets` is still loading. Carries the
    /// answer *as it stands*, because giving up must return it unchanged —
    /// per-item results survive (rule 4), and this layer only decides whether
    /// to ask again, never what the answer means.
    Reask(Result<T>),
}

/// Drive a re-ask loop for errors that arrive inside an `Ok` response.
///
/// `attempt_fn` runs one attempt — send, decode, and any cache invalidation
/// or metadata refresh the retriable class calls for — and reports a
/// [`Verdict`]. This function owns what every ad-hoc copy of the loop got
/// slightly differently: pacing (the policy's jittered exponential curve,
/// never a flat constant) and budget.
///
/// The budget is a **deadline**, not an attempt count, because the handover
/// class of error resolves on the cluster's schedule: how many attempts that
/// takes is a function of the backoff curve, how long it takes is a property
/// of the cluster. A caller-supplied `deadline` *replaces* the policy's
/// [`RetryPolicy::coordinator_timeout`] rather than capping it — a member
/// with a 300s rebalance timeout is willing to outwait 30s, and one that has
/// spent its budget in the coordinator's purgatory has nothing left here.
/// The upcoming delay counts against the budget, so the loop never sleeps
/// past the deadline only to send a request that cannot be answered in time.
///
/// An `Err` from `attempt_fn` is terminal: transport-level retries live below
/// in `Cluster::dispatch`, and re-running them here would double every retry.
pub async fn reask<T, F, Fut>(
    policy: &RetryPolicy,
    deadline: Option<Instant>,
    mut attempt_fn: F,
) -> Result<T>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<Verdict<T>>>,
{
    let budget = deadline.unwrap_or_else(|| Instant::now() + policy.coordinator_timeout);
    let mut attempt: u32 = 1;
    loop {
        match attempt_fn(attempt).await? {
            Verdict::Settled(value) => return Ok(value),
            Verdict::Reask(as_it_stands) => {
                let delay = policy.delay(attempt.saturating_add(1));
                if Instant::now()
                    .checked_add(delay)
                    .is_none_or(|then| then >= budget)
                {
                    return as_it_stands;
                }
                tokio::time::sleep(delay).await;
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_attempt_never_waits() {
        assert_eq!(RetryPolicy::default().delay(0), Duration::ZERO);
        assert_eq!(RetryPolicy::default().delay(1), Duration::ZERO);
    }

    #[test]
    fn backoff_grows_and_then_stops_growing() {
        let policy = RetryPolicy {
            jitter: 0.0,
            ..RetryPolicy::default()
        };
        assert_eq!(policy.delay(2), Duration::from_millis(100));
        assert_eq!(policy.delay(3), Duration::from_millis(200));
        assert_eq!(policy.delay(4), Duration::from_millis(400));
        // And is capped rather than growing without bound.
        assert_eq!(policy.delay(20), policy.max_delay);
        assert_eq!(policy.delay(u32::MAX), policy.max_delay);
    }

    /// The regression this field exists for: at the default curve the attempt
    /// budget expires about a second and a half in, which is shorter than a
    /// routine coordinator election, so a caller saw a raw `NOT_COORDINATOR`
    /// for something that resolves itself. Asserted against the *sum* of the
    /// backoff rather than a hand-copied number, so a change to the curve
    /// re-checks the premise instead of silently invalidating it.
    #[test]
    fn the_attempt_budget_is_far_shorter_than_a_coordinator_election() {
        let policy = RetryPolicy {
            jitter: 0.0,
            ..RetryPolicy::default()
        };
        let spent: Duration = (1..=policy.max_attempts).map(|a| policy.delay(a)).sum();
        assert_eq!(spent, Duration::from_millis(1_500));
        assert!(
            policy.coordinator_timeout > spent * 10,
            "coordinator errors need a budget of a different order, not a bigger attempt count"
        );
    }

    /// "Never retry" has to mean it on both axes.
    #[test]
    fn none_zeroes_the_coordinator_deadline_too() {
        assert_eq!(RetryPolicy::none().coordinator_timeout, Duration::ZERO);
        assert!(!RetryPolicy::none().should_retry(1));
    }

    #[test]
    fn jitter_only_shortens_so_the_cap_stays_a_cap() {
        let policy = RetryPolicy::default();
        for attempt in 2..12 {
            for _ in 0..50 {
                let delay = policy.delay(attempt);
                assert!(delay <= policy.max_delay, "{delay:?}");
            }
        }
    }

    #[test]
    fn jitter_actually_varies() {
        let policy = RetryPolicy {
            base_delay: Duration::from_secs(1),
            ..RetryPolicy::default()
        };
        let samples: Vec<Duration> = (0..20).map(|_| policy.delay(3)).collect();
        assert!(
            samples.windows(2).any(|w| w[0] != w[1]),
            "a synchronised pool is the failure this exists to prevent"
        );
    }

    #[test]
    fn attempts_are_bounded() {
        let policy = RetryPolicy::default();
        assert!(policy.should_retry(1));
        assert!(!policy.should_retry(policy.max_attempts));
        assert!(!RetryPolicy::none().should_retry(1));
    }

    fn fast() -> RetryPolicy {
        RetryPolicy {
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
            jitter: 0.0,
            coordinator_timeout: Duration::from_millis(50),
            ..RetryPolicy::default()
        }
    }

    #[tokio::test]
    async fn a_settled_verdict_ends_the_loop_at_once() {
        let result = reask(&fast(), None, |attempt| async move {
            assert_eq!(attempt, 1);
            Ok(Verdict::Settled(42))
        })
        .await;
        assert_eq!(result.unwrap(), 42);
    }

    /// Rule 4 made executable: on budget exhaustion the answer goes back *as
    /// it came*, because per-item results must survive the retry layer.
    #[tokio::test]
    async fn exhausting_the_budget_returns_the_answer_unchanged() {
        let result: Result<&str> = reask(&fast(), None, |_| async {
            Ok(Verdict::Reask(Ok("the response, codes and all")))
        })
        .await;
        assert_eq!(result.unwrap(), "the response, codes and all");
    }

    /// The caller's deadline replaces the policy budget — both directions
    /// matter, but the short direction is the testable one without waiting.
    #[tokio::test]
    async fn a_caller_deadline_replaces_the_policy_budget() {
        let policy = RetryPolicy {
            coordinator_timeout: Duration::from_secs(3600),
            ..fast()
        };
        let started = Instant::now();
        let attempts = std::cell::Cell::new(0u32);
        let _: Result<()> = reask(
            &policy,
            Some(Instant::now() + Duration::from_millis(20)),
            |attempt| {
                attempts.set(attempt);
                async { Ok(Verdict::Reask(Ok(()))) }
            },
        )
        .await;
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the hour-long policy budget must not apply"
        );
        assert!(attempts.get() >= 1);
    }

    /// Transport errors are dispatch's job; re-running them here would
    /// double every retry below.
    #[tokio::test]
    async fn an_error_from_the_attempt_is_terminal() {
        let attempts = std::cell::Cell::new(0u32);
        let result: Result<()> = reask(&fast(), None, |attempt| {
            attempts.set(attempt);
            async { Err(kafka_conn::Error::InvalidRequest("boom".to_owned())) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(attempts.get(), 1);
    }
}
