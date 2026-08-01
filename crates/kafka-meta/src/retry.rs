//! Capped, jittered backoff.
//!
//! Jitter is not decoration. Without it, every connection in a pool that lost
//! the same broker retries on the same schedule forever, and the cluster gets a
//! synchronised thundering herd on top of whatever knocked the broker over.

use std::time::Duration;

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
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            jitter: 0.3,
        }
    }
}

impl RetryPolicy {
    /// Never retry — for callers that would rather see the first failure.
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
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
}
