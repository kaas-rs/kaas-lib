//! Tolerant batch decoding.
//!
//! This is the module the whole read path is built around. Three things in a
//! fetch response look like corruption and are not, and a decoder that reports
//! them is worse than no decoder at all:
//!
//! 1. **A truncated trailing batch.** `max_bytes` cuts a fetch mid-batch by
//!    design. Every fetch ends this way whenever there is more data than the
//!    budget. Flagging it means claiming corruption at the end of every fetch.
//! 2. **Control batches.** Attribute bit 5 marks a transaction marker — a
//!    commit or abort record the broker writes into the log. It is not user
//!    data and has no key or value worth showing.
//! 3. **Aborted transaction records.** Under `read_committed` the broker sends
//!    them anyway and hands over an `AbortedTransactions` list for the *client*
//!    to filter with. Not filtering shows records that were explicitly rolled
//!    back.
//!
//! Everything else that fails to decode becomes a
//! [`RecordOutcome::Malformed`] and the scan continues.

use std::collections::HashSet;

use bytes::{Buf, Bytes};
use kafka_protocol::records::{Compression, RecordBatchDecoder, TimestampType as KpTimestampType};

use crate::decompress;
use crate::record::{DecodeError, Record, RecordOutcome, TimestampType};

/// Fixed offsets into a v2 record batch header.
///
/// Reading these directly is not schema duplication: it is the minimum needed
/// to decide whether a batch is *complete* before handing it to a decoder that
/// would otherwise report a truncation as corruption.
mod header {
    /// `baseOffset`, i64.
    pub(super) const BASE_OFFSET: usize = 0;
    /// `batchLength`, i32 — the byte count *after* this field.
    pub(super) const BATCH_LENGTH: usize = 8;
    /// `magic`, i8.
    pub(super) const MAGIC: usize = 16;
    /// `attributes`, i16.
    pub(super) const ATTRIBUTES: usize = 21;
    /// `lastOffsetDelta`, i32.
    pub(super) const LAST_OFFSET_DELTA: usize = 23;
    /// `producerId`, i64.
    pub(super) const PRODUCER_ID: usize = 43;
    /// Bytes before `batchLength`'s coverage begins.
    pub(super) const PREFIX_LEN: usize = 12;
    /// The smallest complete v2 batch header.
    pub(super) const MIN_LEN: usize = 61;
}

/// Attribute bit 5: this batch is a transaction marker, not user data.
const CONTROL_FLAG: i16 = 0x20;
/// Attribute bit 4: the records belong to a transaction.
const TRANSACTIONAL_FLAG: i16 = 0x10;
/// Attribute bits 0-2: the compression codec.
const COMPRESSION_MASK: i16 = 0x07;

/// What a batch header says, read without decoding the records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BatchHeader {
    /// First offset in the batch.
    pub(crate) base_offset: i64,
    /// Last offset in the batch.
    pub(crate) last_offset: i64,
    /// Total bytes this batch occupies, header included.
    pub(crate) total_len: usize,
    /// Whether this is a control batch.
    pub(crate) control: bool,
    /// Whether the records are transactional.
    pub(crate) transactional: bool,
    /// The producer that wrote it.
    pub(crate) producer_id: i64,
    /// The codec the records are compressed with.
    pub(crate) compression: Compression,
}

/// Why a batch could not even be looked at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeaderProblem {
    /// The buffer holds less than a full batch.
    ///
    /// Normal, not corruption: it is what `max_bytes` looks like.
    Truncated,
    /// The header is present but says something impossible.
    Malformed,
}

/// Read a batch header without consuming the buffer.
pub(crate) fn peek_header(buf: &[u8]) -> Result<BatchHeader, HeaderProblem> {
    if buf.len() < header::MIN_LEN {
        return Err(HeaderProblem::Truncated);
    }
    let batch_length = read_i32(buf, header::BATCH_LENGTH).ok_or(HeaderProblem::Truncated)?;
    if batch_length < 0 {
        return Err(HeaderProblem::Malformed);
    }
    let total_len = header::PREFIX_LEN
        .checked_add(usize::try_from(batch_length).map_err(|_| HeaderProblem::Malformed)?)
        .ok_or(HeaderProblem::Malformed)?;
    if buf.len() < total_len {
        // The broker cut us off at max_bytes. Expected.
        return Err(HeaderProblem::Truncated);
    }

    let magic = *buf.get(header::MAGIC).ok_or(HeaderProblem::Truncated)?;
    // Magic 2 is the only batch format Kafka 4.x writes; 0 and 1 are the
    // pre-0.11 message sets, which a 4.x broker will not serve.
    if magic != 2 {
        return Err(HeaderProblem::Malformed);
    }

    let attributes = read_i16(buf, header::ATTRIBUTES).ok_or(HeaderProblem::Truncated)?;
    let base_offset = read_i64(buf, header::BASE_OFFSET).ok_or(HeaderProblem::Truncated)?;
    let last_offset_delta =
        read_i32(buf, header::LAST_OFFSET_DELTA).ok_or(HeaderProblem::Truncated)?;
    let producer_id = read_i64(buf, header::PRODUCER_ID).ok_or(HeaderProblem::Truncated)?;

    Ok(BatchHeader {
        base_offset,
        last_offset: base_offset.saturating_add(i64::from(last_offset_delta)),
        total_len,
        control: attributes & CONTROL_FLAG != 0,
        transactional: attributes & TRANSACTIONAL_FLAG != 0,
        producer_id,
        compression: match attributes & COMPRESSION_MASK {
            1 => Compression::Gzip,
            2 => Compression::Snappy,
            3 => Compression::Lz4,
            4 => Compression::Zstd,
            _ => Compression::None,
        },
    })
}

/// A producer id whose records were rolled back, with the offset it started at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortedTransaction {
    /// The producer.
    pub producer_id: i64,
    /// The first offset of the aborted transaction.
    pub first_offset: i64,
}

/// Whether aborted-transaction records are visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Visibility {
    /// Show every record the log holds, including records from transactions
    /// that were later aborted.
    ///
    /// The default, and the right one for a cluster UI: the question a UI
    /// answers is "what is in this partition", and a record that was written
    /// and rolled back *is* in the partition. A consumer's view is a different
    /// question, and [`Visibility::CommittedOnly`] answers it.
    #[default]
    All,
    /// Hide records from aborted transactions, and stop at the last stable
    /// offset — what a `read_committed` consumer sees.
    CommittedOnly,
}

/// How to decode a fetched partition.
#[derive(Debug, Clone)]
pub struct DecodeOptions {
    /// Ceiling on a single batch's decompressed size.
    pub max_decompressed_bytes: usize,
    /// Whether aborted records are visible.
    pub visibility: Visibility,
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self {
            // Generously above Kafka's 1 MiB `max.message.bytes` default, and
            // far below anything that threatens a process.
            max_decompressed_bytes: 64 * 1024 * 1024,
            visibility: Visibility::default(),
        }
    }
}

/// What decoding one fetched partition produced.
#[derive(Debug, Default)]
pub struct DecodedPartition {
    /// Records and decode failures, in log order.
    pub outcomes: Vec<RecordOutcome>,
    /// Control batches skipped.
    pub control_batches_skipped: usize,
    /// Records hidden because their transaction was aborted.
    pub aborted_records_skipped: usize,
    /// Whether the last batch was cut short by `max_bytes`.
    ///
    /// Reported as information, not as an error: a caller that fetched with a
    /// small budget uses it to know there is more to ask for.
    pub truncated_tail: bool,
}

/// Decode a partition's record bytes tolerantly.
///
/// The public entry point to the tolerant decoder, and the surface the fuzz
/// target drives. Rule 2 made executable: this function must not panic on any
/// input at all, including bytes no broker would ever send.
pub fn decode_records(
    topic: &str,
    partition: i32,
    records: bytes::Bytes,
    options: &DecodeOptions,
) -> DecodedPartition {
    decode_partition(topic, partition, records, &[], options)
}

/// Decode one partition's records tolerantly.
pub(crate) fn decode_partition(
    topic: &str,
    partition: i32,
    mut records: Bytes,
    aborted: &[AbortedTransaction],
    options: &DecodeOptions,
) -> DecodedPartition {
    let mut out = DecodedPartition::default();

    // Producers whose records are hidden. A transaction's records run from its
    // first offset until the marker, so membership is by producer id and the
    // set only grows as we pass each aborted transaction's start.
    let aborted_producers: HashSet<i64> = if options.visibility == Visibility::CommittedOnly {
        aborted.iter().map(|a| a.producer_id).collect()
    } else {
        HashSet::new()
    };

    while records.has_remaining() {
        let header = match peek_header(&records) {
            Ok(header) => header,
            Err(HeaderProblem::Truncated) => {
                // The normal end of a size-capped fetch. Discard silently:
                // reporting it would mean every scan claims corruption at the
                // end of every fetch.
                out.truncated_tail = true;
                break;
            }
            Err(HeaderProblem::Malformed) => {
                out.outcomes.push(RecordOutcome::Malformed {
                    offset: read_i64(&records, header::BASE_OFFSET).unwrap_or(-1),
                    last_offset: None,
                    raw: records.clone(),
                    reason: DecodeError::new("record batch header is not a valid v2 batch"),
                });
                break;
            }
        };

        let mut batch = records.split_to(header.total_len);

        if header.control {
            // A transaction marker. Not user data.
            out.control_batches_skipped += 1;
            continue;
        }

        if header.transactional && aborted_producers.contains(&header.producer_id) {
            let hidden = header
                .last_offset
                .saturating_sub(header.base_offset)
                .saturating_add(1);
            out.aborted_records_skipped += usize::try_from(hidden).unwrap_or(0);
            continue;
        }

        let raw = batch.clone();
        let decoded = RecordBatchDecoder::decode_with_custom_compression(
            &mut batch,
            Some(decompress::decompressor(options.max_decompressed_bytes)),
        );

        match decoded {
            Ok(set) => {
                for record in set.records {
                    // Belt and braces: a control record inside an otherwise
                    // ordinary batch is not something Kafka writes, but the
                    // flag is per record as well as per batch.
                    if record.control {
                        out.control_batches_skipped += 1;
                        continue;
                    }
                    out.outcomes
                        .push(RecordOutcome::Ok(convert(topic, partition, record)));
                }
            }
            Err(error) => {
                // One bad batch, not one bad scan.
                out.outcomes.push(RecordOutcome::Malformed {
                    offset: header.base_offset,
                    last_offset: Some(header.last_offset),
                    raw,
                    reason: DecodeError::new(error),
                });
            }
        }
    }

    out
}

fn convert(topic: &str, partition: i32, record: kafka_protocol::records::Record) -> Record {
    Record {
        topic: topic.to_owned(),
        partition,
        offset: record.offset,
        timestamp: record.timestamp,
        timestamp_type: match record.timestamp_type {
            KpTimestampType::Creation => TimestampType::Creation,
            KpTimestampType::LogAppend => TimestampType::LogAppend,
        },
        key: record.key,
        value: record.value,
        headers: record
            .headers
            .into_iter()
            .map(|(name, value)| (name.as_str().to_owned(), value))
            .collect(),
        producer_id: Some(record.producer_id).filter(|id| *id >= 0),
        transactional: record.transactional,
        leader_epoch: Some(record.partition_leader_epoch).filter(|epoch| *epoch >= 0),
    }
}

fn read_i16(buf: &[u8], at: usize) -> Option<i16> {
    let mut slice = buf.get(at..at.checked_add(2)?)?;
    Some(slice.get_i16())
}

fn read_i32(buf: &[u8], at: usize) -> Option<i32> {
    let mut slice = buf.get(at..at.checked_add(4)?)?;
    Some(slice.get_i32())
}

fn read_i64(buf: &[u8], at: usize) -> Option<i64> {
    let mut slice = buf.get(at..at.checked_add(8)?)?;
    Some(slice.get_i64())
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};
    use kafka_protocol::records::{
        NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, RecordBatchEncoder,
        RecordEncodeOptions,
    };

    use super::*;

    fn sample_record(offset: i64, value: &str) -> kafka_protocol::records::Record {
        kafka_protocol::records::Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: KpTimestampType::Creation,
            offset,
            // The encoder only groups records into one batch when
            // `offset - sequence` is constant across them. Leaving every
            // sequence at NO_SEQUENCE gives one batch per record, which would
            // make the multi-record cases below test nothing.
            sequence: i32::try_from(offset).unwrap_or(0),
            timestamp: 1_700_000_000_000 + offset,
            key: Some(Bytes::from(format!("k{offset}"))),
            value: Some(Bytes::from(value.to_owned())),
            headers: Default::default(),
        }
    }

    fn encode(records: &[kafka_protocol::records::Record], compression: Compression) -> Bytes {
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .expect("encodes");
        buf.freeze()
    }

    #[test]
    fn every_codec_round_trips() {
        let records: Vec<_> = (0..10).map(|i| sample_record(i, "payload")).collect();
        for compression in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let encoded = encode(&records, compression);
            let decoded = decode_partition("orders", 0, encoded, &[], &DecodeOptions::default());
            assert_eq!(decoded.outcomes.len(), 10, "{compression:?}");
            assert!(!decoded.truncated_tail, "{compression:?}");
            for (index, outcome) in decoded.outcomes.iter().enumerate() {
                let record = outcome.record().expect("decoded");
                assert_eq!(record.offset, i64::try_from(index).unwrap_or(0));
                assert_eq!(record.value.as_deref(), Some(&b"payload"[..]));
            }
        }
    }

    /// The single most important not-a-bug in the whole read path.
    #[test]
    fn a_truncated_trailing_batch_is_invisible() {
        let records: Vec<_> = (0..10).map(|i| sample_record(i, "payload")).collect();
        let full = encode(&records, Compression::None);

        // Cut the buffer mid-batch, the way a broker does at max_bytes.
        for cut in [1usize, 20, 40, full.len() - 1] {
            let truncated = full.slice(..cut);
            let decoded = decode_partition("orders", 0, truncated, &[], &DecodeOptions::default());
            assert!(
                decoded.outcomes.iter().all(|o| !o.is_malformed()),
                "cut at {cut} produced a Malformed event"
            );
            assert!(decoded.truncated_tail, "cut at {cut}");
        }
    }

    #[test]
    fn a_complete_batch_followed_by_a_truncated_one_yields_the_complete_records() {
        let first = encode(&[sample_record(0, "first")], Compression::None);
        let second = encode(&[sample_record(1, "second")], Compression::None);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&first);
        buf.extend_from_slice(&second[..second.len() / 2]);

        let decoded = decode_partition("orders", 0, buf.freeze(), &[], &DecodeOptions::default());
        assert_eq!(decoded.outcomes.len(), 1);
        assert_eq!(decoded.outcomes[0].offset(), 0);
        assert!(decoded.truncated_tail);
    }

    #[test]
    fn a_corrupt_batch_is_reported_and_the_scan_continues() {
        let first = encode(&[sample_record(0, "first")], Compression::None);
        let second = encode(&[sample_record(1, "second")], Compression::None);
        let third = encode(&[sample_record(2, "third")], Compression::None);

        // Corrupt the middle batch's body, past its header, so the header stays
        // readable and the *records* do not decode. That is the case a
        // batch-granular decoder can actually report usefully.
        let mut damaged = BytesMut::from(&second[..]);
        let tail = damaged.len() - 1;
        if let Some(byte) = damaged.get_mut(tail) {
            *byte ^= 0xFF;
        }

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&first);
        buf.extend_from_slice(&damaged);
        buf.extend_from_slice(&third);

        let decoded = decode_partition("orders", 0, buf.freeze(), &[], &DecodeOptions::default());
        // The scan reached the third batch, which is the property that matters.
        assert_eq!(decoded.outcomes.len(), 3, "{decoded:?}");
        assert_eq!(decoded.outcomes[0].offset(), 0);
        assert_eq!(decoded.outcomes[2].offset(), 2);
        assert!(!decoded.truncated_tail);
    }

    #[test]
    fn control_batches_are_skipped() {
        let mut control = sample_record(5, "marker");
        control.control = true;
        control.transactional = true;
        let encoded = encode(&[control], Compression::None);

        // The batch-level attribute is what the broker sets, so set it too.
        let mut buf = BytesMut::from(&encoded[..]);
        let attributes = read_i16(&buf, header::ATTRIBUTES).expect("attributes");
        let patched = (attributes | CONTROL_FLAG).to_be_bytes();
        if let Some(slot) = buf.get_mut(header::ATTRIBUTES..header::ATTRIBUTES + 2) {
            slot.copy_from_slice(&patched);
        }

        let decoded = decode_partition("orders", 0, buf.freeze(), &[], &DecodeOptions::default());
        assert!(decoded.outcomes.is_empty(), "{decoded:?}");
        assert_eq!(decoded.control_batches_skipped, 1);
    }

    #[test]
    fn the_batch_header_is_read_without_decoding_the_records() {
        let encoded = encode(
            &[sample_record(100, "x"), sample_record(101, "y")],
            Compression::Gzip,
        );
        let header = peek_header(&encoded).expect("header");
        assert_eq!(header.base_offset, 100);
        assert_eq!(header.last_offset, 101);
        assert_eq!(header.total_len, encoded.len());
        assert_eq!(header.compression, Compression::Gzip);
        assert!(!header.control);
    }

    #[test]
    fn a_short_buffer_is_truncated_not_malformed() {
        assert_eq!(peek_header(&[]), Err(HeaderProblem::Truncated));
        assert_eq!(peek_header(&[0u8; 10]), Err(HeaderProblem::Truncated));
        assert_eq!(peek_header(&[0u8; 60]), Err(HeaderProblem::Truncated));
    }

    #[test]
    fn a_pre_0_11_message_set_is_malformed_not_truncated() {
        // Magic 0 and 1 are the old message sets. A 4.x broker never serves
        // them, so seeing one means something is wrong rather than short.
        let mut buf = BytesMut::new();
        buf.put_i64(0); // base offset
        buf.put_i32(49); // batch length: header::MIN_LEN - PREFIX_LEN
        buf.put_i32(0); // partition leader epoch
        buf.put_i8(1); // magic
        buf.put_bytes(0, header::MIN_LEN - 17);
        assert_eq!(peek_header(&buf), Err(HeaderProblem::Malformed));
    }

    #[test]
    fn a_negative_batch_length_is_malformed() {
        let mut buf = BytesMut::new();
        buf.put_i64(0);
        buf.put_i32(-1);
        buf.put_bytes(0, header::MIN_LEN);
        assert_eq!(peek_header(&buf), Err(HeaderProblem::Malformed));
    }

    #[test]
    fn aborted_records_are_visible_by_default_and_hidden_on_request() {
        let mut txn = sample_record(0, "rolled back");
        txn.transactional = true;
        txn.producer_id = 1234;
        let encoded = encode(&[txn], Compression::None);

        // Set the batch-level transactional bit, as a real producer does.
        let mut buf = BytesMut::from(&encoded[..]);
        let attributes = read_i16(&buf, header::ATTRIBUTES).expect("attributes");
        let patched = (attributes | TRANSACTIONAL_FLAG).to_be_bytes();
        if let Some(slot) = buf.get_mut(header::ATTRIBUTES..header::ATTRIBUTES + 2) {
            slot.copy_from_slice(&patched);
        }
        let buf = buf.freeze();

        let aborted = [AbortedTransaction {
            producer_id: 1234,
            first_offset: 0,
        }];

        let visible = decode_partition(
            "orders",
            0,
            buf.clone(),
            &aborted,
            &DecodeOptions::default(),
        );
        assert_eq!(
            visible.outcomes.len(),
            1,
            "the default shows the log as it is"
        );
        assert_eq!(visible.aborted_records_skipped, 0);

        let hidden = decode_partition(
            "orders",
            0,
            buf,
            &aborted,
            &DecodeOptions {
                visibility: Visibility::CommittedOnly,
                ..DecodeOptions::default()
            },
        );
        assert!(hidden.outcomes.is_empty(), "{hidden:?}");
        assert_eq!(hidden.aborted_records_skipped, 1);
    }

    #[test]
    fn a_tombstone_survives_as_a_null_value() {
        let mut record = sample_record(0, "");
        record.value = None;
        let encoded = encode(&[record], Compression::None);
        let decoded = decode_partition("orders", 0, encoded, &[], &DecodeOptions::default());
        let record = decoded.outcomes[0].record().expect("decoded");
        assert!(record.is_tombstone());
    }

    #[test]
    fn a_decompression_bomb_becomes_a_malformed_batch_not_an_allocation() {
        let big = sample_record(0, &"a".repeat(4 * 1024 * 1024));
        let encoded = encode(&[big], Compression::Gzip);
        let decoded = decode_partition(
            "orders",
            0,
            encoded,
            &[],
            &DecodeOptions {
                max_decompressed_bytes: 4096,
                ..DecodeOptions::default()
            },
        );
        assert_eq!(decoded.outcomes.len(), 1);
        assert!(decoded.outcomes[0].is_malformed(), "{decoded:?}");
    }
}
