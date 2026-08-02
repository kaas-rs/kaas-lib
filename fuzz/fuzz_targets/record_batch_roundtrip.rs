//! M19: what we encode, the tolerant decoder must read back unchanged.
//!
//! The other target ([`record_batch`]) fuzzes the decoder against bytes nobody
//! designed, and its pass condition is "no panic". This one is stricter and
//! asks a different question: for every record set the *encoder* accepts, does
//! the decoder return exactly what went in?
//!
//! That matters because the two halves are written against the same reading of
//! the same spec. A mutual misunderstanding — a varint width, a header count,
//! a null-versus-empty distinction — encodes and decodes consistently, passes
//! every round-trip test we would think to write by hand, and is wrong on the
//! wire. Fuzzing the pair does not fix that (only the interop suite does), but
//! it does catch the cases where the pair disagrees *with itself* on inputs no
//! hand-written test covers: empty keys, absent values, headers with null
//! values, non-UTF-8 bytes everywhere, and record counts that straddle a
//! varint boundary.
//!
//! ```sh
//! cargo +nightly fuzz run record_batch_roundtrip -- -max_total_time=300
//! ```

#![no_main]

use libfuzzer_sys::fuzz_target;

/// A record the fuzzer can build, kept deliberately close to the wire's own
/// degrees of freedom: every field that is nullable on the wire is `Option`
/// here, so the tombstone-versus-empty distinction is in the search space
/// rather than assumed away.
#[derive(Debug, arbitrary::Arbitrary)]
struct FuzzRecord {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<(String, Option<Vec<u8>>)>,
    timestamp: i64,
}

#[derive(Debug, arbitrary::Arbitrary)]
struct Input {
    records: Vec<FuzzRecord>,
    codec: u8,
}

fuzz_target!(|input: Input| {
    if input.records.is_empty() || input.records.len() > 512 {
        return;
    }

    let compression = match input.codec % 5 {
        0 => kafka_produce::Compression::None,
        1 => kafka_produce::Compression::Gzip,
        2 => kafka_produce::Compression::Snappy,
        3 => kafka_produce::Compression::Lz4,
        _ => kafka_produce::Compression::Zstd,
    };

    let built: Vec<kafka_produce::ProducerRecord> = input
        .records
        .iter()
        .map(|record| {
            let mut out = kafka_produce::ProducerRecord::new("t")
                // Clamped rather than passed through: a timestamp near i64::MAX
                // overflows the batch header's delta arithmetic, which is a
                // property of the format and not a bug in either half.
                .timestamp(record.timestamp.clamp(0, 1 << 50));
            if let Some(key) = &record.key {
                out = out.key(bytes::Bytes::from(key.clone()));
            }
            if let Some(value) = &record.value {
                out = out.value(bytes::Bytes::from(value.clone()));
            }
            for (name, value) in &record.headers {
                out = match value {
                    Some(value) => out.header(name.clone(), bytes::Bytes::from(value.clone())),
                    None => out.null_header(name.clone()),
                };
            }
            out
        })
        .collect();

    let Ok(encoded) = kafka_produce::encode_for_fuzzing(&built, compression) else {
        // An encoder refusal is a legitimate answer — the pass condition is
        // that it refuses rather than emitting bytes the decoder then chokes
        // on.
        return;
    };

    let decoded = kafka_read::decode_records(
        "t",
        0,
        encoded,
        &kafka_read::DecodeOptions {
            max_decompressed_bytes: 64 * 1024 * 1024,
            visibility: kafka_read::Visibility::All,
        },
    );

    let mut read = Vec::new();
    for outcome in decoded.outcomes {
        match outcome {
            kafka_read::RecordOutcome::Ok(record) => read.push(record),
            // The whole point: bytes *we* wrote must never come back as
            // undecodable. The tolerant path exists for other people's data.
            kafka_read::RecordOutcome::Malformed { offset, reason, .. } => {
                panic!("we encoded offset {offset} and could not decode it: {reason}")
            }
        }
    }

    assert_eq!(
        read.len(),
        built.len(),
        "encoded {} records and decoded {}",
        built.len(),
        read.len()
    );

    for (wrote, got) in built.iter().zip(read.iter()) {
        assert_eq!(got.key, wrote.key, "key changed in the round trip");
        assert_eq!(
            got.value, wrote.value,
            "value changed; a tombstone must not become an empty value"
        );
        assert_eq!(
            got.headers.len(),
            wrote.headers.len(),
            "header count changed"
        );
        for (had, has) in wrote.headers.iter().zip(got.headers.iter()) {
            assert_eq!(&has.0, &had.0, "header name changed");
            assert_eq!(&has.1, &had.1, "header value changed");
        }
    }
});
