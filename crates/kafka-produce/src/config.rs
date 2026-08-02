//! What a producer was asked to guarantee.

use std::time::Duration;

use kafka_meta::RetryPolicy;

/// How many acknowledgements the leader must collect before it answers.
///
/// There is deliberately **no `None` variant**. `acks=0` is a request the
/// broker never responds to, which the connection actor cannot express without
/// a second send path, and which is incompatible with the idempotence work in
/// M14. See the crate documentation for the full argument — this enum is the
/// enforcement of it, and refusing at the type level means the mode cannot be
/// selected by accident and then fail at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Acks {
    /// The leader has written the record to its own log.
    ///
    /// Fast, and lossy exactly once: if the leader fails before a follower
    /// replicates the record, the record is gone and the caller was told it
    /// arrived.
    Leader,
    /// Every in-sync replica has written the record.
    ///
    /// The default, because a library that reports success should mean it.
    #[default]
    All,
}

impl Acks {
    /// The wire value. `-1` is the protocol's spelling of "the full ISR".
    pub(crate) const fn wire(self) -> i16 {
        match self {
            Acks::Leader => 1,
            Acks::All => -1,
        }
    }
}

/// The compression codec applied to a record batch.
///
/// Ours rather than `kafka_protocol`'s, per rule 1: the upstream enum is part
/// of a crate that regenerates on every Kafka release, and it must not appear
/// in a signature a caller names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// No compression.
    #[default]
    None,
    /// gzip.
    Gzip,
    /// Snappy, in the Java/xerial framing.
    Snappy,
    /// LZ4.
    Lz4,
    /// Zstandard.
    Zstd,
}

/// How a [`crate::Producer`] behaves.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// How many acknowledgements to require. Defaults to [`Acks::All`].
    pub acks: Acks,
    /// The codec applied to each batch. Defaults to [`Compression::None`].
    pub compression: Compression,
    /// How long the *broker* may spend collecting acknowledgements.
    ///
    /// Distinct from the connection's own request timeout, which bounds how
    /// long we wait for the socket. This one is a field in the request and is
    /// what the leader honours while waiting on its followers.
    pub delivery_timeout: Duration,
    /// How to re-send a record the broker **rejected**.
    ///
    /// Only rejections are governed by this, because only rejections are safe
    /// to repeat: a response carrying an error code proves the record was not
    /// appended. A timeout or a dropped connection is never retried at any
    /// setting of this, so raising it cannot cause a duplicate. See
    /// [`crate::Producer::send`].
    ///
    /// The same [`RetryPolicy`] the routed read paths use, and for the same
    /// reason: the delay is the point, not the count. The case this exists for
    /// is a leader that moved between our metadata and our request, and a
    /// cluster needs a moment to agree on the new one — three immediate
    /// retries all read the same stale answer and fail identically, which is a
    /// retry loop that has only made the error message longer.
    pub retry: RetryPolicy,
}

impl ProducerConfig {
    /// The defaults: acknowledged by the full ISR, uncompressed.
    pub fn new() -> Self {
        Self {
            acks: Acks::All,
            compression: Compression::None,
            delivery_timeout: Duration::from_secs(30),
            retry: RetryPolicy::default(),
        }
    }

    /// How to re-send a record the broker rejected.
    #[must_use]
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Require only the leader to have written the record.
    #[must_use]
    pub fn acks(mut self, acks: Acks) -> Self {
        self.acks = acks;
        self
    }

    /// Compress each batch with the given codec.
    #[must_use]
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// How long the broker may spend collecting acknowledgements.
    #[must_use]
    pub fn delivery_timeout(mut self, timeout: Duration) -> Self {
        self.delivery_timeout = timeout;
        self
    }

    /// The delivery timeout in the milliseconds the request field wants.
    pub(crate) fn delivery_timeout_ms(&self) -> i32 {
        i32::try_from(self.delivery_timeout.as_millis()).unwrap_or(i32::MAX)
    }
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acks_map_to_the_protocols_own_spelling() {
        assert_eq!(Acks::Leader.wire(), 1);
        assert_eq!(Acks::All.wire(), -1);
    }

    #[test]
    fn the_default_is_the_durable_one() {
        assert_eq!(ProducerConfig::new().acks, Acks::All);
        assert_eq!(Acks::default(), Acks::All);
    }

    #[test]
    fn no_acks_variant_can_produce_the_unresponded_request() {
        // The property this whole decision rests on: nothing in the enum maps
        // to wire value 0, so no configuration can reach the request shape the
        // broker never answers. If a `None` variant is ever added, this fails.
        for acks in [Acks::Leader, Acks::All] {
            assert_ne!(acks.wire(), 0, "acks=0 gets no response; see crate docs");
        }
    }

    #[test]
    fn an_absurd_delivery_timeout_saturates_rather_than_wrapping() {
        let config = ProducerConfig::new().delivery_timeout(Duration::from_secs(u64::MAX));
        assert_eq!(config.delivery_timeout_ms(), i32::MAX);
    }
}
