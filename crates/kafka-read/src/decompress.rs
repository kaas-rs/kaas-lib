//! Size-bounded decompression.
//!
//! A record batch arrives compressed and expands on our heap. Gzip's maximum
//! expansion ratio is about 1032:1, so a producer that can write a 1 MiB batch
//! — Kafka's default `max.message.bytes` — can make a client allocate a
//! gigabyte. That is a denial of service against every other cluster the same
//! UI backend is serving, and it costs the attacker almost nothing.
//!
//! So decompression is bounded, and `RecordBatchDecoder::decode_with_custom_compression`
//! is the hook the crate provides for exactly this.
//!
//! # Why three codecs stream and one does not
//!
//! Gzip, LZ4 and zstd are decompressed here, through a `Read` wrapped in
//! [`std::io::Read::take`], so the limit is enforced *during* decompression and
//! the allocation never happens.
//!
//! Snappy is delegated to `kafka-protocol`, bounded on its *compressed* input
//! instead. Kafka's snappy is xerial-framed, not the standard snappy frame
//! format, and `kafka-protocol` 0.17 rewrote that code to match the Java client
//! and decodes by autodetecting between the two. Reimplementing that here to
//! get a streaming limit would mean maintaining a second, divergent copy of the
//! newest and least-settled code in the dependency — a worse trade than the
//! bound it would buy. The input cap is a real bound because snappy's expansion
//! is limited by its format: a copy operation emits at most 64 bytes, and
//! xerial chunks decompress to at most 32 KiB each.

use std::io::Read;

use bytes::Bytes;
use kafka_protocol::compression::{Decompressor, Snappy};
use kafka_protocol::records::Compression;

/// Snappy's worst-case expansion, used to turn an output limit into an input
/// limit. Generous: the real figure is nearer 32.
const SNAPPY_MAX_RATIO: usize = 68;

/// Decompress a batch's records, refusing to exceed `limit` bytes.
pub(crate) fn bounded(
    compressed: &mut Bytes,
    compression: Compression,
    limit: usize,
) -> anyhow::Result<Bytes> {
    match compression {
        Compression::None => Ok(compressed.split_to(compressed.len())),
        Compression::Gzip => {
            let input = compressed.split_to(compressed.len());
            read_bounded(flate2::read::GzDecoder::new(&input[..]), limit, "gzip")
        }
        Compression::Lz4 => {
            let input = compressed.split_to(compressed.len());
            let decoder = lz4::Decoder::new(&input[..])?;
            read_bounded(decoder, limit, "lz4")
        }
        Compression::Zstd => {
            let input = compressed.split_to(compressed.len());
            let decoder = zstd::stream::read::Decoder::new(&input[..])?;
            read_bounded(decoder, limit, "zstd")
        }
        Compression::Snappy => snappy(compressed, limit),
    }
}

/// The xerial framing's magic header, as `kafka-protocol` writes it.
const XERIAL_MAGIC: &[u8; 16] = b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01";

/// Decompress a snappy batch in either framing Kafka clients actually write.
///
/// Kafka's snappy is not one format. The Java client frames it with
/// snappy-java's xerial header; `librdkafka` — and with it most of the
/// non-Java ecosystem — writes raw, unframed snappy. A reader has to take
/// both, which is why upstream autodetects.
///
/// It autodetects wrongly. `Snappy::decompress` reads the magic header with
/// `try_get_bytes(16)`, and that *advances* the buffer (`protocol/buf.rs:38`
/// calls `get_bytes`). When the header does not match, the raw fallback then
/// runs on a buffer whose first sixteen bytes are already gone, and fails as
/// "failed to decompress raw snappy bytes". Upstream's own fallback test
/// passes only because its fixture is fifteen bytes — one short of the header
/// — so the read returns `Err` and consumes nothing.
///
/// So the framing is decided here, where the buffer is still whole, and only
/// the xerial case is delegated. This is not the reimplementation this module
/// refuses to do: the raw branch is a single `snap` call with no framing in it.
fn snappy(compressed: &mut Bytes, limit: usize) -> anyhow::Result<Bytes> {
    if compressed.starts_with(&XERIAL_MAGIC[..]) {
        // Framed: upstream walks the blocks and each block's declared length
        // drives an allocation, so the bound has to stay on the input.
        let max_input = limit / SNAPPY_MAX_RATIO;
        if compressed.len() > max_input {
            anyhow::bail!(
                "snappy batch of {} compressed bytes could exceed the {limit} byte \
                 decompression limit",
                compressed.len()
            );
        }
        return Snappy::decompress(compressed, |buf| Ok(buf.clone()));
    }

    // Unframed: the block declares its own decompressed size, so this branch
    // gets an exact bound instead of a ratio. Checked before anything is
    // consumed or allocated.
    let declared = snap::raw::decompress_len(compressed)
        .map_err(|e| anyhow::anyhow!("failed to read the snappy block length: {e}"))?;
    if declared > limit {
        anyhow::bail!("snappy batch declares {declared} bytes, past the {limit} byte limit");
    }
    let input = compressed.split_to(compressed.len());
    let mut out = vec![0u8; declared];
    snap::raw::Decoder::new().decompress(&input, &mut out)?;
    Ok(Bytes::from(out))
}

/// Read at most `limit` bytes, and fail rather than truncating.
///
/// Truncating silently would be worse than refusing: a half-decompressed batch
/// decodes into records that look real and are not.
fn read_bounded<R: Read>(reader: R, limit: usize, codec: &str) -> anyhow::Result<Bytes> {
    let mut out = Vec::new();
    // One byte past the limit, so hitting it proves the stream had more.
    let read = reader
        .take(u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1))
        .read_to_end(&mut out)?;
    if read > limit {
        anyhow::bail!("{codec} batch decompressed past the {limit} byte limit");
    }
    Ok(Bytes::from(out))
}

/// Compress with a codec, for tests that need a real compressed batch.
#[cfg(test)]
pub(crate) fn compress_for_test(data: &[u8], compression: Compression) -> Vec<u8> {
    use std::io::Write;
    match compression {
        Compression::None => data.to_vec(),
        Compression::Gzip => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
            encoder.write_all(data).expect("gzip write");
            encoder.finish().expect("gzip finish")
        }
        Compression::Zstd => zstd::encode_all(data, 19).expect("zstd"),
        Compression::Lz4 => {
            let mut encoder = lz4::EncoderBuilder::new()
                .level(9)
                .build(Vec::new())
                .expect("lz4 encoder");
            encoder.write_all(data).expect("lz4 write");
            let (out, result) = encoder.finish();
            result.expect("lz4 finish");
            out
        }
        Compression::Snappy => {
            // Raw, unframed snappy — the framing `librdkafka` writes. It is
            // also the framing `kafka-protocol` 0.17 claims to fall back to
            // and cannot actually decode, which is why `decompress::snappy`
            // handles this branch itself.
            let mut out = vec![0u8; snap::raw::max_compress_len(data.len())];
            let written = snap::raw::Encoder::new()
                .compress(data, &mut out)
                .expect("snappy compress");
            out.truncate(written);
            out
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Roughly a gigabyte of zeros compresses to a few kilobytes. This is the
    /// attack the limit exists for.
    fn bomb(compression: Compression) -> Bytes {
        let zeros = vec![0u8; 8 * 1024 * 1024];
        Bytes::from(compress_for_test(&zeros, compression))
    }

    #[test]
    fn a_decompression_bomb_is_refused_rather_than_allocated() {
        for compression in [Compression::Gzip, Compression::Zstd, Compression::Lz4] {
            let mut compressed = bomb(compression);
            let result = bounded(&mut compressed, compression, 64 * 1024);
            assert!(
                result.is_err(),
                "{compression:?} expanded past the limit without complaint"
            );
        }
    }

    #[test]
    fn a_snappy_bomb_is_refused_before_it_is_decompressed() {
        // Unframed snappy declares its decompressed size up front, so the
        // check is exact rather than a ratio — but either way what makes it a
        // bound is that it fires *before* anything is decompressed.
        let mut compressed = bomb(Compression::Snappy);
        let before = compressed.len();
        let error = bounded(&mut compressed, Compression::Snappy, 1024)
            .expect_err("a batch this large cannot fit in 1 KiB");
        assert!(format!("{error}").contains("declares"), "{error}");
        assert_eq!(
            compressed.len(),
            before,
            "the input must not have been consumed — nothing was decompressed"
        );
    }

    #[test]
    fn an_honest_batch_decompresses_unchanged() {
        let payload: Vec<u8> = (0..4096u32)
            .map(|i| u8::try_from(i % 251).unwrap_or(0))
            .collect();
        // Snappy is absent on purpose. Kafka's snappy is xerial-framed, and
        // producing that framing here would mean writing the encoder this
        // module explicitly refuses to write. The snappy path is covered
        // end-to-end in batch.rs, through `RecordBatchEncoder`, which emits
        // exactly what a Kafka producer emits.
        for compression in [
            Compression::None,
            Compression::Gzip,
            Compression::Zstd,
            Compression::Lz4,
        ] {
            let mut compressed = Bytes::from(compress_for_test(&payload, compression));
            let out = bounded(&mut compressed, compression, 1024 * 1024)
                .unwrap_or_else(|e| panic!("{compression:?}: {e}"));
            assert_eq!(&out[..], &payload[..], "{compression:?}");
        }
    }

    /// Raw, unframed snappy — what `librdkafka` writes, and therefore what
    /// most non-Java producers in a cluster write.
    ///
    /// Excluded from `an_honest_batch_decompresses_unchanged` above on the
    /// grounds that batch.rs covers snappy through `RecordBatchEncoder`. It
    /// does — but that encoder emits *xerial* framing, so the unframed half of
    /// the format had no test anywhere, and it was broken.
    #[test]
    fn raw_unframed_snappy_round_trips() {
        let payload: Vec<u8> = (0..4096u32)
            .map(|i| u8::try_from(i % 251).unwrap_or(0))
            .collect();
        let mut compressed = Bytes::from(compress_for_test(&payload, Compression::Snappy));
        assert!(
            compressed.len() > 16,
            "the payload must exceed the magic header for this to be the interesting case"
        );
        let out = bounded(&mut compressed, Compression::Snappy, 1024 * 1024)
            .expect("raw snappy is a framing Kafka clients really write");
        assert_eq!(&out[..], &payload[..]);
    }

    /// The most expansion-dense snappy block that the format can express:
    /// one literal byte, then nothing but maximum-length overlapping copies —
    /// each 3-byte op emitting 64 bytes. This is the adversary's best move,
    /// not an average payload; a real encoder compressing zeros does worse.
    fn max_expansion_block() -> Vec<u8> {
        let mut block = vec![0x80, 0x80, 0x02]; // uvarint: 32768 uncompressed
        block.extend_from_slice(&[0x00, 0x5a]); // 1-byte literal 'Z'
        for _ in 0..511 {
            // Copy, 2-byte offset: tag ((64-1)<<2)|0b10, offset 1 → emits 64.
            block.extend_from_slice(&[0xfe, 0x01, 0x00]);
        }
        block.extend_from_slice(&[0xfa, 0x01, 0x00]); // len 63 closes at 32768
        block
    }

    /// #34: `SNAPPY_MAX_RATIO` guards an allocation and was an unproven
    /// constant. This drives the most pathological xerial-framed blob the
    /// snappy format can express against it: the densest block decodes 3-byte
    /// ops into 64 bytes each, ~21x, so 68 holds with a 3x margin *at the
    /// format's own limit* — if a denser construction ever exists, the ratio
    /// assertion here is the tripwire.
    #[test]
    fn the_framed_snappy_ratio_bound_survives_the_worst_expressible_blob() {
        let block = max_expansion_block();
        // The block must be real snappy, or the test proves nothing about
        // what a decoder would actually expand.
        let declared = snap::raw::decompress_len(&block).expect("valid preamble");
        assert_eq!(declared, 32 * 1024);
        let mut out = vec![0u8; declared];
        let written = snap::raw::Decoder::new()
            .decompress(&block, &mut out)
            .expect("the pathological block is legal snappy");
        assert_eq!(written, declared);

        // 64 such blocks in the xerial framing kafka-protocol walks.
        let mut blob = Vec::from(&XERIAL_MAGIC[..]);
        for _ in 0..64 {
            blob.extend_from_slice(&u32::try_from(block.len()).unwrap().to_be_bytes());
            blob.extend_from_slice(&block);
        }
        let expanded = 64 * declared;
        let ratio = expanded / blob.len();
        assert!(
            ratio < SNAPPY_MAX_RATIO,
            "found a framed-snappy blob with ratio {ratio}, past the assumed {SNAPPY_MAX_RATIO}"
        );

        // Above the input gate: refused before a byte is decompressed.
        let mut input = Bytes::from(blob.clone());
        let error = bounded(&mut input, Compression::Snappy, 64 * 1024)
            .expect_err("a 96 KiB blob cannot promise to fit 64 KiB");
        assert!(format!("{error}").contains("could exceed"), "{error}");

        // Below it: decompresses fully and lands under the output limit,
        // which is the property the input-side bound exists to guarantee.
        let limit = blob.len() * SNAPPY_MAX_RATIO;
        let mut input = Bytes::from(blob);
        let out = bounded(&mut input, Compression::Snappy, limit).expect("passes the gate");
        assert_eq!(out.len(), expanded);
        assert!(out.len() <= limit);
    }

    #[test]
    fn a_batch_exactly_at_the_limit_is_accepted() {
        // Off-by-one matters: rejecting a batch that fits is a scan that stops
        // working at a size boundary nobody can see.
        let payload = vec![7u8; 1000];
        let mut compressed = Bytes::from(compress_for_test(&payload, Compression::Gzip));
        let out = bounded(&mut compressed, Compression::Gzip, 1000).expect("fits exactly");
        assert_eq!(out.len(), 1000);
    }

    #[test]
    fn one_byte_over_the_limit_is_refused() {
        let payload = vec![7u8; 1001];
        let mut compressed = Bytes::from(compress_for_test(&payload, Compression::Gzip));
        assert!(bounded(&mut compressed, Compression::Gzip, 1000).is_err());
    }
}
