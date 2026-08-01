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
        Compression::Snappy => {
            let max_input = limit / SNAPPY_MAX_RATIO;
            if compressed.len() > max_input {
                anyhow::bail!(
                    "snappy batch of {} compressed bytes could exceed the {limit} byte \
                     decompression limit",
                    compressed.len()
                );
            }
            Snappy::decompress(compressed, |buf| Ok(buf.clone()))
        }
    }
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

/// A decompressor closure for `decode_with_custom_compression`.
pub(crate) fn decompressor(
    limit: usize,
) -> impl Fn(&mut Bytes, Compression) -> anyhow::Result<Bytes> {
    move |compressed, compression| bounded(compressed, compression, limit)
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
            // Raw snappy, which `kafka-protocol`'s decoder accepts as its
            // non-xerial fallback. Producing xerial framing here would be
            // reimplementing the very thing this module refuses to
            // reimplement.
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
    fn a_snappy_bomb_is_refused_on_its_compressed_size() {
        // Snappy is bounded on input rather than output, so this asserts the
        // input cap fires *before* any decompression is attempted — which is
        // what makes it a bound at all.
        let mut compressed = bomb(Compression::Snappy);
        let before = compressed.len();
        let error = bounded(&mut compressed, Compression::Snappy, 1024)
            .expect_err("a batch this large cannot fit in 1 KiB");
        assert!(format!("{error}").contains("compressed bytes"), "{error}");
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
