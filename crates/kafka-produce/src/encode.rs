//! Turning records into the bytes a `Produce` request carries.
//!
//! v2 batches only. v0 and v1 message sets are pre-0.11 and this library does
//! not write them — a broker old enough to require one is older than the
//! `Produce` versions we negotiate anyway, so the fallback would be dead code
//! that still had to be kept correct.

use bytes::{Bytes, BytesMut};
use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::indexmap::IndexMap;
use kafka_conn::protocol::records::{
    Compression as WireCompression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
    NO_SEQUENCE, Record as WireRecord, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use kafka_conn::{Error, Result};

use crate::config::Compression;
use crate::idempotence::BatchIdentity;
use crate::record::ProducerRecord;

/// The record batch version. 2 is the only one we write.
const RECORD_BATCH_V2: i8 = 2;

impl Compression {
    fn wire(self) -> WireCompression {
        match self {
            Compression::None => WireCompression::None,
            Compression::Gzip => WireCompression::Gzip,
            Compression::Snappy => WireCompression::Snappy,
            Compression::Lz4 => WireCompression::Lz4,
            Compression::Zstd => WireCompression::Zstd,
        }
    }
}

/// Encode records into one v2 batch.
///
/// `now` is passed in rather than read here so a batch's records share one
/// clock reading, and so the unit tests are not racing `SystemTime`.
///
/// `identity` is `Some` for an idempotent producer and stamps the batch with
/// the producer id, epoch and base sequence the broker deduplicates on. `None`
/// writes the pre-M14 shape: no producer id, and a base sequence of -1.
pub(crate) fn encode_batch(
    records: &[ProducerRecord],
    compression: Compression,
    now: i64,
    identity: Option<BatchIdentity>,
) -> Result<Bytes> {
    let wire: Vec<WireRecord> = records
        .iter()
        .enumerate()
        .map(|(index, record)| WireRecord {
            transactional: identity.is_some_and(|id| id.transactional),
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: identity.map_or(NO_PRODUCER_ID, |id| id.producer_id),
            producer_epoch: identity.map_or(NO_PRODUCER_EPOCH, |id| id.producer_epoch),
            timestamp_type: TimestampType::Creation,
            // Offsets within a batch are relative and start at zero; the
            // broker rewrites them to absolute ones on append. `index` cannot
            // exceed the batch length, so the fallback is unreachable.
            offset: i64::try_from(index).unwrap_or(0),
            // Not `NO_SEQUENCE` for every record, which is the obvious thing
            // and is wrong in a way nothing complains about.
            //
            // `RecordBatchEncoder` decides where one batch ends and the next
            // begins by walking records while `offset - sequence` holds
            // constant (`records.rs:256`). Offsets necessarily increase, so a
            // fixed sequence makes that difference increase too and **every
            // record is emitted as its own batch** — each with its own 61-byte
            // header, its own CRC, and nothing for compression to work across.
            // The records all arrive, in order, so it reads as a success; it
            // is a throughput bug wearing a correctness result.
            //
            // The batch header stores `base_sequence` plus a per-record offset
            // delta, and the decoder reconstructs `sequence` as the sum. So
            // counting up from the base is not a trick to satisfy the check: it
            // is the value the wire format implies.
            //
            // Without an identity the base is `NO_SEQUENCE`, which makes the
            // encoder write `base_sequence = -1` (`records.rs:287`) — what a
            // non-idempotent producer must send. With one it is the number the
            // broker expects next for this partition, and a gap there makes it
            // reject every later batch as out of order.
            sequence: identity
                .map_or(NO_SEQUENCE, |id| id.base_sequence)
                .wrapping_add(i32::try_from(index).unwrap_or(0)),
            timestamp: record.timestamp.unwrap_or(now),
            key: record.key.clone(),
            value: record.value.clone(),
            headers: record
                .headers
                .iter()
                .map(|(name, value)| (StrBytes::from_string(name.clone()), value.clone()))
                .collect::<IndexMap<_, _>>(),
        })
        .collect();

    let options = RecordEncodeOptions {
        version: RECORD_BATCH_V2,
        compression: compression.wire(),
    };

    let mut buffer = BytesMut::new();
    RecordBatchEncoder::encode(&mut buffer, wire.iter(), &options).map_err(|error| {
        Error::InvalidRequest(format!("could not encode a record batch: {error}"))
    })?;

    Ok(buffer.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    // The decoder these round trip against lives in `kafka-read`, which
    // depends on this crate's siblings rather than on this crate — so the
    // full encode/decode assertion is the acceptance test's job. What is
    // checkable here is that every codec produces a batch, that the shape is
    // v2, and that a tombstone survives encoding as a null rather than as an
    // empty value.

    fn one(record: ProducerRecord) -> Bytes {
        encode_batch(&[record], Compression::None, 1_700_000_000_000, None).unwrap()
    }

    #[test]
    fn a_batch_carries_the_v2_magic_byte() {
        let encoded = one(ProducerRecord::new("t").with_value("hello"));
        // base offset (8) + batch length (4) + leader epoch (4) = 16, then the
        // magic byte. Anything else means we wrote a message set, not a batch.
        assert_eq!(encoded.get(16), Some(&2_u8));
    }

    #[test]
    fn every_codec_encodes() {
        for compression in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let record = ProducerRecord::new("t").with_key("k").with_value("v");
            let encoded = encode_batch(&[record], compression, 1_700_000_000_000, None)
                .expect("encode failed");
            assert!(
                encoded.len() > 16,
                "{compression:?} produced a batch too short to be one"
            );
        }
    }

    #[test]
    fn a_tombstone_encodes_shorter_than_an_empty_value() {
        // A null value is a length of -1; an empty one is a length of 0
        // followed by nothing. They must not encode identically, or the
        // compaction semantics of every tombstone this library writes are
        // wrong.
        let tombstone = one(ProducerRecord::new("t").with_key("k"));
        let empty = one(ProducerRecord::new("t")
            .with_key("k")
            .with_value(Bytes::new()));
        assert_ne!(tombstone, empty);
    }

    #[test]
    fn many_records_encode_as_one_batch_not_one_batch_each() {
        let records: Vec<ProducerRecord> = (0..3)
            .map(|i| ProducerRecord::new("t").with_value(format!("v{i}")))
            .collect();
        let encoded = encode_batch(&records, Compression::None, 1_700_000_000_000, None).unwrap();

        // `lastOffsetDelta` is a big-endian i32 at byte 23; for three records
        // in one batch it is 2. Zero here means the encoder split the records
        // into a batch each — see the `sequence` comment above. That still
        // round trips, so only this assertion catches it.
        assert_eq!(
            encoded.get(23..27),
            Some([0, 0, 0, 2].as_slice()),
            "records were split into one batch each"
        );

        // And the batch must claim to be non-idempotent: `baseSequence` is a
        // big-endian i32 at byte 53, and -1 is the "no producer id" spelling.
        assert_eq!(
            encoded.get(53..57),
            Some([0xff, 0xff, 0xff, 0xff].as_slice()),
            "baseSequence should be -1 until M14 claims a producer id"
        );

        // Record count, at byte 57.
        assert_eq!(encoded.get(57..61), Some([0, 0, 0, 3].as_slice()));
    }

    #[test]
    fn batching_holds_as_the_batch_grows() {
        // The split is driven by a *per-record* comparison, so a bug that
        // survives three records can still appear at a thousand.
        let records: Vec<ProducerRecord> = (0..1_000)
            .map(|i| ProducerRecord::new("t").with_value(format!("v{i}")))
            .collect();
        let encoded = encode_batch(&records, Compression::None, 1_700_000_000_000, None).unwrap();

        assert_eq!(encoded.get(23..27), Some([0, 0, 0x03, 0xe7].as_slice()));
        assert_eq!(encoded.get(57..61), Some([0, 0, 0x03, 0xe8].as_slice()));
    }
}
