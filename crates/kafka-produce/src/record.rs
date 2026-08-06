//! What a caller hands over, and what comes back.

use bytes::Bytes;

/// A record to write.
///
/// `key` and `value` are both optional, and the distinction between `None` and
/// an empty `Bytes` is load-bearing: a record with `value: None` is a
/// **tombstone**, and on a compacted topic it deletes the key rather than
/// storing nothing under it. [`kafka_read::Record`] preserves the same
/// distinction on the way back, and the round-trip test asserts it.
///
/// [`kafka_read::Record`]: https://docs.rs/kafka-read
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProducerRecord {
    /// The topic to write to.
    pub topic: String,
    /// The partition, when the caller is choosing it.
    ///
    /// `None` hands the choice to the partitioner: by key when there is one,
    /// stickily when there is not.
    pub partition: Option<i32>,
    /// The record key.
    pub key: Option<Bytes>,
    /// The record value. `None` is a tombstone.
    pub value: Option<Bytes>,
    /// Headers, in the order they will be written.
    ///
    /// A `Vec` rather than a map because Kafka headers are an ordered list and
    /// may repeat a name.
    ///
    /// # A duplicate name cannot currently be written
    ///
    /// This type preserves duplicates, and [`kafka_read`] returns them
    /// faithfully — but the encoder cannot emit them.
    /// `kafka_protocol::records::Record::headers` is an `IndexMap`, so a
    /// repeated name collapses to its last value on the way to the wire, with
    /// no error. Reading records a Java producer wrote is unaffected; only
    /// writing them is impossible.
    ///
    /// An upstream limitation rather than something to route around: the
    /// alternative is hand-rolling the record format, which CLAUDE.md rules
    /// out. `a_duplicate_header_name_is_dropped_and_that_is_an_upstream_limit`
    /// in `tests/roundtrip.rs` pins the behaviour so the day upstream changes
    /// the field to a list, the test tells us.
    ///
    /// [`kafka_read`]: https://docs.rs/kafka-read
    pub headers: Vec<(String, Option<Bytes>)>,
    /// The record timestamp in milliseconds, when the caller is choosing it.
    ///
    /// `None` means now. Note the broker may override it anyway: a topic
    /// configured `message.timestamp.type=LogAppendTime` stamps its own.
    pub timestamp: Option<i64>,
}

impl ProducerRecord {
    /// A record destined for `topic`, with no key, value or headers.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            ..Self::default()
        }
    }

    /// Write to an explicit partition, bypassing the partitioner.
    ///
    /// Takes an `i32` because naming a partition is a decision. A caller
    /// *relaying* one it was handed wants [`with_maybe_partition`] instead.
    ///
    /// [`with_maybe_partition`]: Self::with_maybe_partition
    #[must_use]
    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Set the partition from an `Option`, leaving the choice to the
    /// partitioner when there is none.
    ///
    /// [`with_partition`] is the method for code that has decided on a
    /// partition. This one is for code that is passing one through — from a
    /// `--partition` flag, a config field, a request parameter — where "no
    /// partition" is a value the caller holds rather than a branch it takes.
    /// Without it, such a caller has to break the chain to apply what it was
    /// given:
    ///
    /// ```
    /// # use kafka_produce::ProducerRecord;
    /// # let configured: Option<i32> = Some(3);
    /// let mut record = ProducerRecord::new("t").with_value("v");
    /// if let Some(partition) = configured {
    ///     record = record.with_partition(partition);
    /// }
    /// # assert_eq!(record.partition, Some(3));
    /// ```
    ///
    /// The `let mut` and the rebinding are the whole reason this exists:
    ///
    /// ```
    /// # use kafka_produce::ProducerRecord;
    /// # let configured: Option<i32> = Some(3);
    /// let record = ProducerRecord::new("t")
    ///     .with_value("v")
    ///     .with_maybe_partition(configured);
    /// # assert_eq!(record.partition, Some(3));
    /// ```
    ///
    /// Assignment rather than a merge: `with_maybe_partition(None)` clears a
    /// partition set earlier in the chain, the same way a second
    /// [`with_partition`] call overwrites the first. Last call wins, which is
    /// the only rule that does not require knowing what came before it.
    ///
    /// [`with_partition`]: Self::with_partition
    #[must_use]
    pub fn with_maybe_partition(mut self, partition: Option<i32>) -> Self {
        self.partition = partition;
        self
    }

    /// Set the key.
    #[must_use]
    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set the value.
    ///
    /// Leaving it unset is not the same as setting an empty one: a record with
    /// no value is a tombstone. See the type documentation.
    #[must_use]
    pub fn with_value(mut self, value: impl Into<Bytes>) -> Self {
        self.value = Some(value.into());
        self
    }

    /// Append a header.
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push((name.into(), Some(value.into())));
        self
    }

    /// Append a header with a null value, which is distinct from an empty one.
    #[must_use]
    pub fn with_null_header(mut self, name: impl Into<String>) -> Self {
        self.headers.push((name.into(), None));
        self
    }

    /// Set an explicit timestamp, in milliseconds since the epoch.
    #[must_use]
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
}

/// Where a record landed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMetadata {
    /// The topic written to.
    pub topic: String,
    /// The partition written to.
    pub partition: i32,
    /// The offset the record was assigned.
    pub offset: i64,
    /// The timestamp the broker recorded, when it reported one.
    ///
    /// `None` where the broker did not send `log_append_time_ms`, which is the
    /// normal case on a `CreateTime` topic — there the timestamp the caller
    /// supplied is the one stored, so there is nothing for the broker to
    /// report back and guessing would be a fabrication.
    pub timestamp: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tombstone_is_not_an_empty_value() {
        let tombstone = ProducerRecord::new("t").with_key("k");
        let empty = ProducerRecord::new("t")
            .with_key("k")
            .with_value(Bytes::new());
        assert_eq!(tombstone.value, None);
        assert_eq!(empty.value, Some(Bytes::new()));
        assert_ne!(tombstone, empty);
    }

    #[test]
    fn a_relayed_partition_reaches_the_same_record_as_a_chosen_one() {
        // The point of the method: a caller holding `Option<i32>` builds the
        // record a caller holding `i32` builds, without leaving the chain.
        assert_eq!(
            ProducerRecord::new("t").with_maybe_partition(Some(3)),
            ProducerRecord::new("t").with_partition(3)
        );
        assert_eq!(
            ProducerRecord::new("t")
                .with_maybe_partition(None)
                .partition,
            None
        );
    }

    #[test]
    fn a_relayed_none_clears_a_partition_set_earlier() {
        // Assignment, not a merge. A `maybe_partition` that silently ignored
        // `None` would make the last call in a chain conditional on the ones
        // before it, which is the kind of rule nobody remembers at the call
        // site.
        let record = ProducerRecord::new("t")
            .with_partition(3)
            .with_maybe_partition(None);
        assert_eq!(record.partition, None);
    }

    #[test]
    fn headers_keep_their_order_and_their_duplicates() {
        let record = ProducerRecord::new("t")
            .with_header("trace", "a")
            .with_header("trace", "b")
            .with_null_header("tombstoned");

        assert_eq!(record.headers.len(), 3);
        assert_eq!(record.headers[0], ("trace".to_owned(), Some("a".into())));
        assert_eq!(record.headers[1], ("trace".to_owned(), Some("b".into())));
        assert_eq!(record.headers[2], ("tombstoned".to_owned(), None));
    }
}
