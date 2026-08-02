//! M19: what the encoder writes, the decoder must read back unchanged.
//!
//! `cargo test -p kafka-produce --test roundtrip`
//!
//! No Docker and no broker: this is the encoder in this crate against the
//! tolerant decoder in `kafka-read`, which is the pair the fuzz target
//! `record_batch_roundtrip` drives with random input. Having it here as well
//! means the property runs on every `cargo xtask ci` rather than only in the
//! nightly fuzz job — CI has no `cargo-fuzz`, and neither does this machine.
//!
//! What this does **not** prove is agreement with Java. Both halves are
//! written against the same reading of the same spec, so a mutual
//! misunderstanding round-trips perfectly and is still wrong on the wire. Only
//! the interop suite settles that. What this catches is the pair disagreeing
//! with *itself* on inputs nobody would think to write by hand.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use bytes::Bytes;
use kafka_produce::{Compression, ProducerRecord, encode_for_fuzzing};
use kafka_read::{DecodeOptions, RecordOutcome, Visibility, decode_records};

fn round_trip(records: &[ProducerRecord], compression: Compression) -> Vec<kafka_read::Record> {
    let encoded = encode_for_fuzzing(records, compression).expect("encode");
    let decoded = decode_records(
        "t",
        0,
        encoded,
        &DecodeOptions {
            max_decompressed_bytes: 64 * 1024 * 1024,
            visibility: Visibility::All,
        },
    );

    decoded
        .outcomes
        .into_iter()
        .map(|outcome| match outcome {
            RecordOutcome::Ok(record) => record,
            RecordOutcome::Malformed { offset, reason, .. } => {
                panic!("we encoded offset {offset} and could not decode it: {reason}")
            }
        })
        .collect()
}

const CODECS: [Compression; 5] = [
    Compression::None,
    Compression::Gzip,
    Compression::Snappy,
    Compression::Lz4,
    Compression::Zstd,
];

/// The distinctions that are one byte apart on the wire and change what a
/// compacted topic does.
#[test]
fn null_and_empty_stay_distinct_through_every_codec() {
    let records = vec![
        ProducerRecord::new("t").key("k1"),
        ProducerRecord::new("t").key("k2").value(Bytes::new()),
        ProducerRecord::new("t").value("no key"),
        ProducerRecord::new("t").key(Bytes::new()).value("v"),
    ];

    for codec in CODECS {
        let read = round_trip(&records, codec);
        assert_eq!(read.len(), 4, "{codec:?}");
        assert_eq!(read[0].value, None, "{codec:?}: a tombstone became a value");
        assert_eq!(
            read[1].value,
            Some(Bytes::new()),
            "{codec:?}: an empty value became a tombstone"
        );
        assert_eq!(read[2].key, None, "{codec:?}: an absent key gained a value");
        assert_eq!(
            read[3].key,
            Some(Bytes::new()),
            "{codec:?}: an empty key became absent"
        );
    }
}

/// Headers keep their order, their null values and their empty names.
#[test]
fn headers_keep_order_nulls_and_empty_names() {
    let records = vec![
        ProducerRecord::new("t")
            .value("v")
            .header("trace", "a")
            .null_header("gone")
            .header("", "empty name"),
    ];

    for codec in CODECS {
        let read = round_trip(&records, codec);
        let headers = &read[0].headers;
        assert_eq!(headers.len(), 3, "{codec:?}");
        assert_eq!(
            headers[0],
            ("trace".to_owned(), Some("a".into())),
            "{codec:?}"
        );
        assert_eq!(headers[1], ("gone".to_owned(), None), "{codec:?}");
        assert_eq!(
            headers[2],
            (String::new(), Some("empty name".into())),
            "{codec:?}"
        );
    }
}

/// **A duplicate header name is silently dropped on write, and this library
/// cannot currently avoid it.**
///
/// Found by this file rather than by any broker. `ProducerRecord::headers` is
/// a `Vec` on the documented grounds that Kafka headers are an ordered list
/// which may repeat a name — but `kafka_protocol::records::Record::headers` is
/// an `IndexMap`, so a duplicate cannot be *represented* on the way to the
/// encoder, let alone written. The second value wins and the first disappears
/// with no error.
///
/// That makes this an upstream limitation, not something to work around here:
/// writing the batch by hand to dodge it would mean hand-rolling the record
/// format, which CLAUDE.md rules out. It is asserted rather than left as a
/// surprise so that the day upstream changes the field to a list, this test
/// fails and tells us the capability arrived.
///
/// The read path is unaffected — `kafka-read` returns duplicates faithfully,
/// so records a Java producer wrote are read correctly. Only *writing* them is
/// impossible.
#[test]
fn a_duplicate_header_name_is_dropped_and_that_is_an_upstream_limit() {
    let records = vec![
        ProducerRecord::new("t")
            .value("v")
            .header("trace", "first")
            .header("trace", "second"),
    ];

    let read = round_trip(&records, Compression::None);
    assert_eq!(
        read[0].headers.len(),
        1,
        "upstream gained duplicate-header support: make the encoder use it and          restore the round-trip assertion"
    );
    assert_eq!(
        read[0].headers[0],
        ("trace".to_owned(), Some("second".into())),
        "the last value wins, which is IndexMap's insertion semantics"
    );
}

/// Record counts either side of a varint width boundary.
#[test]
fn batches_survive_the_varint_boundaries() {
    for count in [1usize, 2, 63, 64, 65, 127, 128, 129, 1000] {
        let records: Vec<ProducerRecord> = (0..count)
            .map(|i| {
                ProducerRecord::new("t")
                    .key(format!("k{i}"))
                    .value(format!("v{i}"))
            })
            .collect();

        for codec in CODECS {
            let read = round_trip(&records, codec);
            assert_eq!(read.len(), count, "{codec:?} lost records at {count}");
            for (i, record) in read.iter().enumerate() {
                assert_eq!(
                    record.value.as_deref(),
                    Some(format!("v{i}").as_bytes()),
                    "{codec:?}: record {i} of {count} changed"
                );
                assert_eq!(
                    record.offset,
                    i64::try_from(i).unwrap(),
                    "{codec:?}: relative offsets are wrong at {count}"
                );
            }
        }
    }
}

/// A key or value is arbitrary bytes; treating either as a string is a bug
/// that only shows on data somebody else wrote.
#[test]
fn arbitrary_bytes_survive_unchanged() {
    let nasty: Vec<u8> = (0u8..=255).collect();
    let records = vec![
        ProducerRecord::new("t")
            .key(Bytes::from(nasty.clone()))
            .value(Bytes::from(nasty.clone()))
            .header("h", Bytes::from(nasty.clone())),
    ];

    for codec in CODECS {
        let read = round_trip(&records, codec);
        assert_eq!(read[0].key.as_deref(), Some(nasty.as_slice()), "{codec:?}");
        assert_eq!(
            read[0].value.as_deref(),
            Some(nasty.as_slice()),
            "{codec:?}"
        );
        assert_eq!(
            read[0].headers[0].1.as_deref(),
            Some(nasty.as_slice()),
            "{codec:?}"
        );
    }
}

/// A large batch exercises the compression path rather than the trivial one,
/// and the decompression bound has to let a legitimate batch through.
#[test]
fn a_large_batch_compresses_and_returns_intact() {
    let big = Bytes::from(vec![b'z'; 256 * 1024]);
    let records: Vec<ProducerRecord> = (0..8)
        .map(|i| {
            ProducerRecord::new("t")
                .key(format!("k{i}"))
                .value(big.clone())
        })
        .collect();

    for codec in CODECS {
        let read = round_trip(&records, codec);
        assert_eq!(read.len(), 8, "{codec:?}");
        assert_eq!(read[3].value, Some(big.clone()), "{codec:?}");
    }
}
