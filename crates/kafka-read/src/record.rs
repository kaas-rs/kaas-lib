//! The owned record type, and what happens when one will not decode.

use std::fmt;

use bytes::Bytes;

/// A record read from a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Topic.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// Offset.
    pub offset: i64,
    /// Timestamp, in epoch milliseconds.
    pub timestamp: i64,
    /// Whether the timestamp is the producer's or the broker's.
    pub timestamp_type: TimestampType,
    /// Key, or `None` for a record written without one.
    pub key: Option<Bytes>,
    /// Value, or `None` for a tombstone.
    ///
    /// The distinction matters: a tombstone is a deletion marker on a compacted
    /// topic, and rendering it as an empty value hides that.
    pub value: Option<Bytes>,
    /// Headers, in the order the producer wrote them.
    ///
    /// A `Vec` rather than a map, because Kafka permits duplicate header keys
    /// and some producers rely on it.
    pub headers: Vec<(String, Option<Bytes>)>,
    /// The producer id, when the record came from an idempotent or
    /// transactional producer.
    pub producer_id: Option<i64>,
    /// Whether the record was written inside a transaction.
    pub transactional: bool,
    /// Leader epoch of the batch this record came from.
    pub leader_epoch: Option<i32>,
}

impl Record {
    /// Whether this is a tombstone — a null value on a compacted topic.
    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    /// Serialized size of the key and value, for progress accounting.
    pub fn payload_len(&self) -> usize {
        self.key.as_ref().map_or(0, Bytes::len) + self.value.as_ref().map_or(0, Bytes::len)
    }
}

/// Where a record's timestamp came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType {
    /// Set by the producer.
    Creation,
    /// Set by the broker on append.
    LogAppend,
}

/// Why a batch could not be decoded.
#[derive(Debug, Clone)]
pub struct DecodeError {
    /// A human-readable explanation.
    pub reason: String,
}

impl DecodeError {
    pub(crate) fn new(reason: impl fmt::Display) -> Self {
        Self {
            reason: reason.to_string(),
        }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for DecodeError {}

/// The outcome of decoding one unit of the log.
///
/// # Granularity, decided deliberately
///
/// PLAN.md asks for this to be settled rather than discovered: the outcome is
/// **batch-level**, not per-record.
///
/// `RecordBatchDecoder` decodes a whole batch into a `Vec<Record>` and reports
/// failure for the batch, not the record; per-record tolerance is not something
/// the crate's API offers, and `decode_into_vec` is private. The alternative was
/// vendoring the record loop — varints, header parsing, the batch header, the
/// CRC — which is a second implementation of a wire schema to keep in step with
/// upstream on every Kafka release, and CLAUDE.md is explicit that a hand-rolled
/// schema is not an acceptable answer to a gap in the crate.
///
/// What the design is actually built around still holds: one bad batch does not
/// fail the scan, and the [`RecordOutcome::Malformed`] variant carries the raw
/// bytes and the offset range so a UI can say *which* records it could not read
/// rather than failing the whole partition. The cost is granularity — a corrupt
/// record takes its batch with it, bounded by `max.message.bytes`.
#[derive(Debug, Clone)]
pub enum RecordOutcome {
    /// A decoded record.
    Ok(Record),
    /// A batch that would not decode.
    Malformed {
        /// The first offset the batch claims to hold, from its header — which
        /// is readable even when the records inside are not.
        offset: i64,
        /// The last offset the batch claims, when the header was intact enough
        /// to say.
        last_offset: Option<i64>,
        /// The raw batch, so it can be dumped, hexed or reported.
        raw: Bytes,
        /// Why.
        reason: DecodeError,
    },
}

impl RecordOutcome {
    /// The offset this outcome is about.
    pub fn offset(&self) -> i64 {
        match self {
            RecordOutcome::Ok(record) => record.offset,
            RecordOutcome::Malformed { offset, .. } => *offset,
        }
    }

    /// The record, if this outcome has one.
    pub fn record(&self) -> Option<&Record> {
        match self {
            RecordOutcome::Ok(record) => Some(record),
            RecordOutcome::Malformed { .. } => None,
        }
    }

    /// Whether this outcome is a decode failure.
    pub fn is_malformed(&self) -> bool {
        matches!(self, RecordOutcome::Malformed { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(value: Option<&'static [u8]>) -> Record {
        Record {
            topic: "orders".to_owned(),
            partition: 0,
            offset: 7,
            timestamp: 1,
            timestamp_type: TimestampType::Creation,
            key: Some(Bytes::from_static(b"k")),
            value: value.map(Bytes::from_static),
            headers: Vec::new(),
            producer_id: None,
            transactional: false,
            leader_epoch: None,
        }
    }

    #[test]
    fn a_tombstone_is_not_an_empty_value() {
        assert!(record(None).is_tombstone());
        assert!(!record(Some(b"")).is_tombstone());
        assert_eq!(record(Some(b"")).payload_len(), 1);
    }

    #[test]
    fn duplicate_header_keys_survive() {
        // Kafka permits them and some producers depend on it, so headers are a
        // Vec. A map would silently drop one.
        let mut record = record(Some(b"v"));
        record.headers = vec![
            ("trace".to_owned(), Some(Bytes::from_static(b"a"))),
            ("trace".to_owned(), Some(Bytes::from_static(b"b"))),
        ];
        assert_eq!(record.headers.len(), 2);
    }

    #[test]
    fn a_malformed_outcome_still_knows_where_it_was() {
        let outcome = RecordOutcome::Malformed {
            offset: 42,
            last_offset: Some(50),
            raw: Bytes::from_static(b"garbage"),
            reason: DecodeError::new("crc mismatch"),
        };
        assert_eq!(outcome.offset(), 42);
        assert!(outcome.is_malformed());
        assert!(outcome.record().is_none());
        match &outcome {
            RecordOutcome::Malformed { reason, .. } => {
                assert_eq!(reason.to_string(), "crc mismatch");
            }
            other => panic!("{other:?}"),
        }
    }
}
