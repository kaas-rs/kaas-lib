//! Rule 2, made executable.
//!
//! "A malformed record from one topic must not kill a server hosting other
//! clusters" is a claim about behaviour, and the only way to hold a tolerant
//! decoder to it is to hand it bytes nobody designed. The pass condition is
//! simply: no panic. Any input at all — truncated headers, absurd lengths,
//! compressed payloads that are not, offsets that overflow — must come back as
//! records or as `Malformed`, never as an abort.
//!
//! ```sh
//! cargo +nightly fuzz run record_batch -- -max_total_time=300
//! ```

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let options = kafka_read::DecodeOptions {
        // Small on purpose: the bound has to hold under fuzzing too, and a
        // 64 MiB ceiling would let the fuzzer spend its whole budget waiting
        // on allocations instead of finding panics.
        max_decompressed_bytes: 1024 * 1024,
        visibility: kafka_read::Visibility::All,
    };

    let decoded = kafka_read::decode_records(
        "fuzz",
        0,
        bytes::Bytes::copy_from_slice(data),
        &options,
    );

    // Touch every field, so a panic hiding in an accessor counts as a finding
    // rather than as dead code the optimiser removed.
    for outcome in &decoded.outcomes {
        let _ = outcome.offset();
        let _ = outcome.is_malformed();
        if let Some(record) = outcome.record() {
            let _ = record.is_tombstone();
            let _ = record.payload_len();
        }
    }
});
