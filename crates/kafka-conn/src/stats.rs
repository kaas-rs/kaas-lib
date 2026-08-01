//! Per-connection counters.
//!
//! These exist from M2 rather than from M11 on purpose. M10's acceptance test
//! asserts that a backward scan of 500 records reads less than 5% of the
//! partition — an assertion that cannot be written at all without a byte
//! counter, and a milestone whose acceptance criterion cannot be evaluated is
//! a milestone that quietly ships broken.
//!
//! Counting happens at the frame layer, so "bytes" means what actually crossed
//! the socket including the length prefix, not the size of a decoded struct.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative traffic on one connection.
#[derive(Default)]
pub struct ConnectionStats {
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    requests_sent: AtomicU64,
    responses_received: AtomicU64,
}

impl ConnectionStats {
    /// A fresh set of counters.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn record_sent(&self, bytes: usize) {
        self.bytes_sent
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
        self.requests_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_received(&self, bytes: usize) {
        self.bytes_received
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
        self.responses_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Bytes written to the socket, length prefixes included.
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent.load(Ordering::Relaxed)
    }

    /// Bytes read from the socket, length prefixes included.
    pub fn bytes_received(&self) -> u64 {
        self.bytes_received.load(Ordering::Relaxed)
    }

    /// Requests handed to the writer.
    pub fn requests_sent(&self) -> u64 {
        self.requests_sent.load(Ordering::Relaxed)
    }

    /// Response frames read, including those whose caller had gone away.
    pub fn responses_received(&self) -> u64 {
        self.responses_received.load(Ordering::Relaxed)
    }

    /// A snapshot, for arithmetic that must not race against live traffic.
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            bytes_sent: self.bytes_sent(),
            bytes_received: self.bytes_received(),
            requests_sent: self.requests_sent(),
            responses_received: self.responses_received(),
        }
    }
}

impl fmt::Debug for ConnectionStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.snapshot().fmt(f)
    }
}

/// A point-in-time copy of [`ConnectionStats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Bytes written to the socket.
    pub bytes_sent: u64,
    /// Bytes read from the socket.
    pub bytes_received: u64,
    /// Requests handed to the writer.
    pub requests_sent: u64,
    /// Response frames read.
    pub responses_received: u64,
}

impl StatsSnapshot {
    /// Traffic accumulated between an earlier snapshot and this one.
    pub fn since(&self, earlier: &StatsSnapshot) -> StatsSnapshot {
        StatsSnapshot {
            bytes_sent: self.bytes_sent.saturating_sub(earlier.bytes_sent),
            bytes_received: self.bytes_received.saturating_sub(earlier.bytes_received),
            requests_sent: self.requests_sent.saturating_sub(earlier.requests_sent),
            responses_received: self
                .responses_received
                .saturating_sub(earlier.responses_received),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deltas_are_taken_against_a_snapshot_not_a_live_counter() {
        let stats = ConnectionStats::new();
        stats.record_sent(100);
        stats.record_received(400);
        let mark = stats.snapshot();

        stats.record_received(50);
        let delta = stats.snapshot().since(&mark);
        assert_eq!(delta.bytes_received, 50);
        assert_eq!(delta.bytes_sent, 0);
        assert_eq!(delta.responses_received, 1);
    }

    #[test]
    fn deltas_never_underflow() {
        let later = StatsSnapshot::default();
        let earlier = StatsSnapshot {
            bytes_received: 10,
            ..Default::default()
        };
        assert_eq!(later.since(&earlier).bytes_received, 0);
    }
}
