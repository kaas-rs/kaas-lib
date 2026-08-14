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
    /// `recordsCount`, i32 — the last field of the header.
    pub(super) const RECORD_COUNT: usize = 57;
    /// Bytes before `batchLength`'s coverage begins.
    pub(super) const PREFIX_LEN: usize = 12;
    /// The smallest complete v2 batch header.
    pub(super) const MIN_LEN: usize = 61;
}

/// The fewest bytes a v2 record can possibly encode to.
///
/// A record is a length varint, a fixed attributes byte, then timestampDelta,
/// offsetDelta, keyLength, valueLen and headerCount — each a varint costing at
/// least one byte. Seven. Six is used deliberately, one below the true floor,
/// so that no batch a real producer writes is ever rejected by the ceiling
/// this feeds.
const MIN_RECORD_BYTES: usize = 6;

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
    /// How many records the header claims. Not to be trusted — see
    /// [`max_plausible_records`].
    pub(crate) record_count: usize,
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
    let record_count = read_i32(buf, header::RECORD_COUNT).ok_or(HeaderProblem::Truncated)?;
    let record_count = usize::try_from(record_count).map_err(|_| HeaderProblem::Malformed)?;

    Ok(BatchHeader {
        record_count,
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
    ///
    /// This is a bound on decompressed *bytes*, not on total transient heap:
    /// the decoded `Record` representation costs roughly 30x the 6-byte wire
    /// minimum per record, so a pathological batch of empty records can
    /// transiently occupy ~30x this value while it decodes. Size the limit
    /// for the memory the process can actually spare, not for the largest
    /// batch it might meet.
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

/// Decode a partition's records with the fetch response's aborted-transaction
/// list applied.
///
/// [`decode_records`] is the one-shot form for a caller that has no such list
/// — a fuzz target, or a scan reading `Visibility::All`. A consumer does have
/// one, and passing it is what makes `Visibility::CommittedOnly` mean anything:
/// without it the filter has nothing to filter *by* and silently shows aborted
/// records.
pub fn decode_records_with_aborted(
    topic: &str,
    partition: i32,
    records: bytes::Bytes,
    aborted: &[AbortedTransaction],
    options: &DecodeOptions,
) -> DecodedPartition {
    decode_partition(topic, partition, records, aborted, options)
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

    // Aborted spans, entered and left as the walk passes them. An entry
    // `(producer_id, first_offset)` hides that producer's records from
    // `first_offset` up to its abort marker — and nothing before it. This
    // used to collect the producer ids up front instead, which also hid the
    // same producer's *earlier, committed* transactions whenever they shared
    // a fetch response with the aborted entry: a committed transaction
    // followed by an aborted one from the same producer read back as nothing
    // at all, exactly when the broker answered both in one response.
    let mut spans: Vec<&AbortedTransaction> = if options.visibility == Visibility::CommittedOnly {
        aborted.iter().collect()
    } else {
        Vec::new()
    };
    spans.sort_by_key(|span| span.first_offset);
    let mut spans = spans.into_iter().peekable();
    let mut aborted_producers: HashSet<i64> = HashSet::new();

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

        // Enter every aborted span whose start this batch has reached. A
        // span's first offset is the first *data* record of the transaction,
        // so by the time a batch at or past it appears, its producer is
        // hiding.
        while spans
            .peek()
            .is_some_and(|span| span.first_offset <= header.base_offset)
        {
            if let Some(span) = spans.next() {
                aborted_producers.insert(span.producer_id);
            }
        }

        if header.control {
            // A transaction marker. Not user data — and the end of its
            // producer's aborted span, if one is open. Marker type does not
            // need decoding: the only control batch that can appear between
            // a span's first offset and its end is that span's own abort
            // marker, so a commit marker only ever lands here as a no-op.
            out.control_batches_skipped += 1;
            aborted_producers.remove(&header.producer_id);
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

        // Before handing the batch over, refuse a record count the batch
        // cannot possibly hold. `RecordBatchDecoder` reserves the whole
        // `Vec<Record>` from this number before parsing a single record, so a
        // count nothing else validates is an allocation primitive.
        let ceiling = max_plausible_records(&header, options);
        if header.record_count > ceiling {
            out.outcomes.push(RecordOutcome::Malformed {
                offset: header.base_offset,
                last_offset: Some(header.last_offset),
                raw: batch,
                reason: DecodeError::new(format!(
                    "batch claims {} records; {} bytes can hold at most {ceiling}",
                    header.record_count, header.total_len
                )),
            });
            continue;
        }

        let raw = batch.clone();
        // The pre-decode ceiling above can only price a *compressed* batch at
        // the configured decompression maximum, and upstream reserves
        // `record_count * sizeof(Record)` (~30x the 6-byte wire floor) after
        // the decompress hook returns. So the tight bound lives inside the
        // hook, where the actual decompressed length is known and an error
        // still arrives before the reservation.
        let claimed = header.record_count;
        let limit = options.max_decompressed_bytes;
        let decoded = RecordBatchDecoder::decode_with_custom_compression(
            &mut batch,
            Some(move |buf: &mut Bytes, compression: Compression| {
                let out = decompress::bounded(buf, compression, limit)?;
                let ceiling = out.len() / MIN_RECORD_BYTES;
                if claimed > ceiling {
                    anyhow::bail!(
                        "batch claims {claimed} records; {} decompressed bytes can hold \
                         at most {ceiling}",
                        out.len()
                    );
                }
                Ok(out)
            }),
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

/// The most records a batch could physically contain.
///
/// `RecordBatchDecoder::decode` reserves a `Vec<Record>` sized from the
/// header's `recordsCount` before it has parsed anything
/// (`kafka-protocol-0.17.0/src/records.rs:479`), and the only check upstream
/// applies to that number is that it is not negative. So a 99-byte batch
/// claiming 285 million records asks for a multi-gigabyte allocation — which
/// is how the M11 fuzz target found this, as an out-of-memory rather than a
/// panic, and which a corrupt segment or a hostile broker could aim at a
/// backend serving other clusters. That is rule 2.
///
/// The bound is what the batch could actually hold: uncompressed, its own
/// payload; compressed, the decompression ceiling `DecodeOptions` already
/// promises. Either way the demand becomes proportional to bytes we have
/// already accepted, rather than to a number the sender chose freely.
///
/// For a compressed batch this is only the *coarse* filter: pricing every
/// compressed batch at the configured maximum leaves a gap (a tiny wire
/// payload may claim `limit / 6` records — ~11M at the 64 MiB default — and
/// upstream reserves ~30x that in heap before parsing). The exact bound —
/// the *actual* decompressed length — is enforced inside the decompress hook
/// in [`decode_partition`], which runs before upstream's reservation.
fn max_plausible_records(header: &BatchHeader, options: &DecodeOptions) -> usize {
    let payload = match header.compression {
        Compression::None => header.total_len.saturating_sub(header::MIN_LEN),
        _ => options.max_decompressed_bytes,
    };
    payload / MIN_RECORD_BYTES
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

    /// The M11 fuzz target's first real finding, pinned as a unit test.
    ///
    /// libFuzzer reported it as an out-of-memory rather than a panic: a batch
    /// whose 38-byte payload claims 285 million records makes the upstream
    /// decoder reserve a `Vec<Record>` of that length before parsing anything.
    /// If this regresses, the test does not fail — the machine runs out of
    /// memory — so the assertion is that the batch is refused *by its header*,
    /// cheaply, before the decoder is ever handed it.
    #[test]
    fn an_absurd_record_count_is_refused_before_it_becomes_an_allocation() {
        let honest = encode(&[sample_record(0, "payload")], Compression::None);
        let mut buf = BytesMut::from(&honest[..]);
        let absurd = 285_212_423i32.to_be_bytes();
        if let Some(slot) = buf.get_mut(header::RECORD_COUNT..header::RECORD_COUNT + 4) {
            slot.copy_from_slice(&absurd);
        }

        let header = peek_header(&buf).expect("the header itself is still well formed");
        assert_eq!(header.record_count, 285_212_423);
        assert!(
            header.record_count > max_plausible_records(&header, &DecodeOptions::default()),
            "the ceiling must reject a count this batch cannot hold"
        );

        let decoded = decode_partition("orders", 0, buf.freeze(), &[], &DecodeOptions::default());
        assert_eq!(decoded.outcomes.len(), 1);
        assert!(decoded.outcomes[0].is_malformed(), "{decoded:?}");
    }

    /// The other half: the ceiling must not reject batches real producers write.
    #[test]
    fn an_honest_record_count_is_within_the_ceiling() {
        for count in [1usize, 2, 50, 500] {
            let records: Vec<_> = (0..count)
                .map(|i| sample_record(i64::try_from(i).unwrap_or(0), "payload"))
                .collect();
            for compression in [
                Compression::None,
                Compression::Gzip,
                Compression::Snappy,
                Compression::Lz4,
                Compression::Zstd,
            ] {
                let encoded = encode(&records, compression);
                let header = peek_header(&encoded).expect("header");
                assert_eq!(header.record_count, count, "{compression:?}");
                assert!(
                    header.record_count
                        <= max_plausible_records(&header, &DecodeOptions::default()),
                    "{count} records compressed with {compression:?} was rejected as implausible"
                );
                // And it still decodes to exactly those records.
                let decoded =
                    decode_partition("orders", 0, encoded, &[], &DecodeOptions::default());
                assert_eq!(decoded.outcomes.len(), count, "{compression:?}");
            }
        }
    }

    /// The compressed sibling of the fuzz finding above, from the security
    /// audit (#30): the header-level ceiling prices a compressed batch at the
    /// configured decompression maximum (64 MiB / 6 ≈ 11.18M records), but
    /// upstream reserves ~180 heap bytes per claimed record *after*
    /// decompression — so a tiny wire batch claiming 11M records passed the
    /// coarse check and reserved ~2 GB before failing to parse. The tight
    /// check runs inside the decompress hook against the actual decompressed
    /// length; this pins that it fires. A hostile broker computes a valid
    /// CRC over its forged header, so the test must too — otherwise the CRC
    /// check rejects the batch first and the ceiling is never exercised.
    #[test]
    fn a_compressed_batch_cannot_claim_more_records_than_it_decompresses_to() {
        let honest = encode(&[sample_record(0, "payload")], Compression::Gzip);
        let mut buf = BytesMut::from(&honest[..]);
        let claimed = 11_000_000i32;
        if let Some(slot) = buf.get_mut(header::RECORD_COUNT..header::RECORD_COUNT + 4) {
            slot.copy_from_slice(&claimed.to_be_bytes());
        }
        // Re-seal: crc (bytes 17..21) covers attributes (21) to the end.
        let crc = crc32c::crc32c(&buf[21..]);
        if let Some(slot) = buf.get_mut(17..21) {
            slot.copy_from_slice(&crc.to_be_bytes());
        }

        let header = peek_header(&buf).expect("the header itself is still well formed");
        assert_eq!(header.record_count, 11_000_000);
        assert!(
            header.record_count <= max_plausible_records(&header, &DecodeOptions::default()),
            "this count must slip past the coarse ceiling, or the test proves nothing"
        );

        let decoded = decode_partition("orders", 0, buf.freeze(), &[], &DecodeOptions::default());
        assert_eq!(decoded.outcomes.len(), 1, "{decoded:?}");
        assert!(decoded.outcomes[0].is_malformed(), "{decoded:?}");
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

    /// The shape that read back as *nothing at all*: one producer, a
    /// committed transaction followed by an aborted one, both in the same
    /// response. The aborted entry names the producer, but its span starts at
    /// `first_offset` — hiding the committed half too is the bug that made
    /// `Visibility::CommittedOnly` return zero records from a partition
    /// holding a hundred committed ones.
    #[test]
    fn an_aborted_transaction_does_not_hide_the_same_producers_committed_one() {
        const PID: i64 = 1234;

        fn flagged(records: &[kafka_protocol::records::Record], flags: i16) -> Bytes {
            let encoded = encode(records, Compression::None);
            let mut buf = BytesMut::from(&encoded[..]);
            let attributes = read_i16(&buf, header::ATTRIBUTES).expect("attributes");
            let patched = (attributes | flags).to_be_bytes();
            if let Some(slot) = buf.get_mut(header::ATTRIBUTES..header::ATTRIBUTES + 2) {
                slot.copy_from_slice(&patched);
            }
            buf.freeze()
        }

        fn txn_record(offset: i64, value: &str) -> kafka_protocol::records::Record {
            let mut record = sample_record(offset, value);
            record.transactional = true;
            record.producer_id = PID;
            record
        }

        // committed data (0), commit marker (1), aborted data (2), abort
        // marker (3) — the log as the broker serves it after the two
        // transactions in the acceptance test.
        let mut stream = BytesMut::new();
        stream.extend_from_slice(&flagged(&[txn_record(0, "committed")], TRANSACTIONAL_FLAG));
        stream.extend_from_slice(&flagged(
            &[txn_record(1, "commit marker")],
            TRANSACTIONAL_FLAG | CONTROL_FLAG,
        ));
        stream.extend_from_slice(&flagged(&[txn_record(2, "aborted")], TRANSACTIONAL_FLAG));
        stream.extend_from_slice(&flagged(
            &[txn_record(3, "abort marker")],
            TRANSACTIONAL_FLAG | CONTROL_FLAG,
        ));

        let aborted = [AbortedTransaction {
            producer_id: PID,
            first_offset: 2,
        }];

        let committed = decode_partition(
            "orders",
            0,
            stream.clone().freeze(),
            &aborted,
            &DecodeOptions {
                visibility: Visibility::CommittedOnly,
                ..DecodeOptions::default()
            },
        );
        let values: Vec<String> = committed
            .outcomes
            .iter()
            .filter_map(|outcome| outcome.record())
            .filter_map(|record| record.value.as_ref())
            .map(|value| String::from_utf8_lossy(value).into_owned())
            .collect();
        assert_eq!(
            values,
            ["committed"],
            "the committed transaction must stay visible: {committed:?}"
        );
        assert_eq!(committed.aborted_records_skipped, 1);
        assert_eq!(committed.control_batches_skipped, 2);

        // The same stream under `All` shows both halves.
        let everything = decode_partition(
            "orders",
            0,
            stream.freeze(),
            &aborted,
            &DecodeOptions::default(),
        );
        assert_eq!(everything.outcomes.len(), 2, "{everything:?}");
    }

    /// A producer with two aborted transactions back to back: the first
    /// marker must not end the second span.
    #[test]
    fn consecutive_aborted_transactions_each_hide_their_own_span() {
        const PID: i64 = 1234;

        fn flagged(records: &[kafka_protocol::records::Record], flags: i16) -> Bytes {
            let encoded = encode(records, Compression::None);
            let mut buf = BytesMut::from(&encoded[..]);
            let attributes = read_i16(&buf, header::ATTRIBUTES).expect("attributes");
            let patched = (attributes | flags).to_be_bytes();
            if let Some(slot) = buf.get_mut(header::ATTRIBUTES..header::ATTRIBUTES + 2) {
                slot.copy_from_slice(&patched);
            }
            buf.freeze()
        }

        fn txn_record(offset: i64, value: &str) -> kafka_protocol::records::Record {
            let mut record = sample_record(offset, value);
            record.transactional = true;
            record.producer_id = PID;
            record
        }

        let mut stream = BytesMut::new();
        stream.extend_from_slice(&flagged(
            &[txn_record(0, "aborted one")],
            TRANSACTIONAL_FLAG,
        ));
        stream.extend_from_slice(&flagged(
            &[txn_record(1, "abort marker")],
            TRANSACTIONAL_FLAG | CONTROL_FLAG,
        ));
        stream.extend_from_slice(&flagged(
            &[txn_record(2, "aborted two")],
            TRANSACTIONAL_FLAG,
        ));
        stream.extend_from_slice(&flagged(
            &[txn_record(3, "abort marker")],
            TRANSACTIONAL_FLAG | CONTROL_FLAG,
        ));
        stream.extend_from_slice(&flagged(
            &[txn_record(4, "committed after")],
            TRANSACTIONAL_FLAG,
        ));

        let aborted = [
            AbortedTransaction {
                producer_id: PID,
                first_offset: 0,
            },
            AbortedTransaction {
                producer_id: PID,
                first_offset: 2,
            },
        ];

        let committed = decode_partition(
            "orders",
            0,
            stream.freeze(),
            &aborted,
            &DecodeOptions {
                visibility: Visibility::CommittedOnly,
                ..DecodeOptions::default()
            },
        );
        let values: Vec<String> = committed
            .outcomes
            .iter()
            .filter_map(|outcome| outcome.record())
            .filter_map(|record| record.value.as_ref())
            .map(|value| String::from_utf8_lossy(value).into_owned())
            .collect();
        assert_eq!(values, ["committed after"], "{committed:?}");
        assert_eq!(committed.aborted_records_skipped, 2);
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
