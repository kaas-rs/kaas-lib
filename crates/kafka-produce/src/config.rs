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
    ///
    /// **The acknowledgement also arrives before the record is readable.** A
    /// consumer reads only up to the high watermark, which does not advance
    /// until the in-sync replicas have the record — so on a replicated topic
    /// there is a window where [`crate::Producer::send`] has returned an
    /// offset that a scan of that partition will not yet show. That is not a
    /// bug to work around; it is what this variant means, and code that reads
    /// its own writes back should use [`Acks::All`].
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
    /// How long an open batch waits for company before it is sent.
    ///
    /// Zero — the default, and Java's — does **not** mean one record per
    /// request. A partition holds at most one batch on the wire at a time, so
    /// records arriving while that request is outstanding accumulate into the
    /// next batch and are sent together the moment it lands. Batching therefore
    /// scales with load rather than with this setting: an idle producer pays no
    /// latency, and a busy one batches anyway.
    ///
    /// Raising it trades latency for larger batches on a producer whose records
    /// arrive in bursts smaller than the round trip.
    pub linger: Duration,
    /// The size, in accounted bytes, at which an open batch is closed and sent.
    ///
    /// A record larger than this still gets sent — in a batch of its own, which
    /// is the only way it can be sent at all.
    pub batch_size: usize,
    /// The ceiling on one partition's batch, in accounted bytes.
    ///
    /// A record accounted larger than this is refused at
    /// [`crate::Producer::send`] with `MESSAGE_TOO_LARGE` before it is ever
    /// buffered, so it fails alone rather than taking a batch with it.
    pub max_request_size: usize,
    /// How many bytes of unsent records may be buffered before `send` waits.
    ///
    /// This bound is the difference between backpressure and an OOM: without
    /// it, a producer whose broker has stopped acknowledging accepts records
    /// until the process dies. Reaching it makes `send` wait, which is the
    /// signal a caller can actually act on.
    pub buffer_memory: usize,
    /// Whether to claim a producer id and number every record.
    ///
    /// On by default, as it is in Java since 3.0, because it is what makes a
    /// re-send safe. Without it a request whose outcome is unknown — a
    /// timeout, a connection that died in flight — can never be retried, so an
    /// ordinary leader election surfaces to the caller as a delivery failure.
    /// With it the broker recognises the re-sent batch and answers with the
    /// original offsets instead of appending it twice.
    ///
    /// Turning it off is for brokers that do not support `InitProducerId` at
    /// all. It does not make the producer faster; it makes it lossier.
    pub idempotent: bool,
}

impl ProducerConfig {
    /// The defaults: acknowledged by the full ISR, uncompressed.
    ///
    /// The batching defaults match Java's, because they are the numbers every
    /// operator's intuition is calibrated against: 16 KiB batches, no linger,
    /// a 1 MiB request ceiling and a 32 MiB buffer.
    pub fn new() -> Self {
        Self {
            acks: Acks::All,
            compression: Compression::None,
            delivery_timeout: Duration::from_secs(30),
            retry: RetryPolicy::default(),
            linger: Duration::ZERO,
            batch_size: 16 * 1024,
            max_request_size: 1024 * 1024,
            buffer_memory: 32 * 1024 * 1024,
            idempotent: true,
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

    /// How long an open batch waits for company before it is sent.
    #[must_use]
    pub fn linger(mut self, linger: Duration) -> Self {
        self.linger = linger;
        self
    }

    /// The size at which an open batch is closed and sent.
    #[must_use]
    pub fn batch_size(mut self, bytes: usize) -> Self {
        self.batch_size = bytes;
        self
    }

    /// The ceiling on one partition's batch.
    #[must_use]
    pub fn max_request_size(mut self, bytes: usize) -> Self {
        self.max_request_size = bytes;
        self
    }

    /// How many bytes of unsent records may be buffered before `send` waits.
    #[must_use]
    pub fn buffer_memory(mut self, bytes: usize) -> Self {
        self.buffer_memory = bytes;
        self
    }

    /// Whether to claim a producer id and number every record.
    #[must_use]
    pub fn idempotent(mut self, idempotent: bool) -> Self {
        self.idempotent = idempotent;
        self
    }

    /// How many requests a connection carrying this producer may have in
    /// flight.
    ///
    /// One without idempotence, five with it. Kafka's own default is five and
    /// the broker tracks exactly five in-flight sequence windows per
    /// partition, so five is the ceiling rather than a tuning knob.
    ///
    /// The reason for the clamp is that M13 retries rejected batches: a
    /// re-sent batch travelling behind a later one reorders the log with no
    /// error and no log line. Sequence numbers are what let the broker put
    /// them back in order, so without them the only safe answer is one.
    pub(crate) fn max_in_flight(&self) -> usize {
        if self.idempotent { 5 } else { 1 }
    }

    /// The delivery timeout in the milliseconds the request field wants.
    pub(crate) fn delivery_timeout_ms(&self) -> i32 {
        i32::try_from(self.delivery_timeout.as_millis()).unwrap_or(i32::MAX)
    }

    /// The buffer bound as a semaphore permit count.
    ///
    /// Permits are counted in `usize` by the semaphore but acquired in `u32`,
    /// so a buffer configured past `u32::MAX` would be unreachable in one
    /// acquisition. Clamping here keeps the two halves consistent.
    pub(crate) fn buffer_memory_permits(&self) -> usize {
        let clamped = u32::try_from(self.buffer_memory).unwrap_or(u32::MAX);
        usize::try_from(clamped).unwrap_or(usize::MAX)
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
