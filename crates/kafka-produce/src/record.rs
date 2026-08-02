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
    /// may repeat a name — collapsing them into a map would silently drop
    /// duplicates a Java producer is entitled to write.
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
    #[must_use]
    pub fn partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Set the key.
    #[must_use]
    pub fn key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set the value.
    #[must_use]
    pub fn value(mut self, value: impl Into<Bytes>) -> Self {
        self.value = Some(value.into());
        self
    }

    /// Append a header.
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push((name.into(), Some(value.into())));
        self
    }

    /// Append a header with a null value, which is distinct from an empty one.
    #[must_use]
    pub fn null_header(mut self, name: impl Into<String>) -> Self {
        self.headers.push((name.into(), None));
        self
    }

    /// Set an explicit timestamp, in milliseconds since the epoch.
    #[must_use]
    pub fn timestamp(mut self, timestamp: i64) -> Self {
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
        let tombstone = ProducerRecord::new("t").key("k");
        let empty = ProducerRecord::new("t").key("k").value(Bytes::new());
        assert_eq!(tombstone.value, None);
        assert_eq!(empty.value, Some(Bytes::new()));
        assert_ne!(tombstone, empty);
    }

    #[test]
    fn headers_keep_their_order_and_their_duplicates() {
        let record = ProducerRecord::new("t")
            .header("trace", "a")
            .header("trace", "b")
            .null_header("tombstoned");

        assert_eq!(record.headers.len(), 3);
        assert_eq!(record.headers[0], ("trace".to_owned(), Some("a".into())));
        assert_eq!(record.headers[1], ("trace".to_owned(), Some("b".into())));
        assert_eq!(record.headers[2], ("tombstoned".to_owned(), None));
    }
}
