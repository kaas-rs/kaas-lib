//! Choosing a partition: murmur2 by key, sticky without one.
//!
//! Both halves have a failure mode that looks like success.
//!
//! A murmur2 that is *nearly* Java's still returns a partition, still round
//! trips, and still passes any test written only against ourselves — it just
//! puts a key somewhere a Java consumer of the same topic does not expect, and
//! silently breaks every co-partitioned join downstream. So the byte-exactness
//! assertion lives in the interop crate against `rdkafka`, not here: this
//! module's own tests check the properties, and a genuinely different
//! implementation checks the arithmetic.
//!
//! A null-key partitioner that round-robins *per record* is the other one. It
//! is correct, spreads perfectly, and destroys throughput, because it produces
//! a one-record batch per partition and there is nothing left for the
//! accumulator to batch. KIP-480's sticky partitioner is what this implements.

use std::collections::HashMap;
use std::sync::Mutex;

/// Java's `Utils.murmur2`, which is what every Kafka client's default
/// partitioner hashes with.
///
/// Kept bit-identical to the Java original, including its use of wrapping
/// 32-bit arithmetic and a logical right shift. The signed return type is the
/// original's too — callers want [`partition_for_key`] rather than this, but a
/// hash function that reports a different number from every other client's is
/// worth being able to compare directly.
pub fn murmur2(data: &[u8]) -> i32 {
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;
    const SEED: u32 = 0x9747_b28c;

    // `seed ^ length` in the original. A slice longer than u32::MAX cannot be
    // produced to Kafka under any configuration, so saturating here is
    // unreachable rather than lossy.
    let length = u32::try_from(data.len()).unwrap_or(u32::MAX);
    let mut h = SEED ^ length;

    for chunk in data.chunks_exact(4) {
        // `chunks_exact` yields exactly four bytes, so the fallback is dead —
        // it exists because `try_into` on a slice cannot know that.
        let mut k = u32::from_le_bytes(chunk.try_into().unwrap_or([0; 4]));
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    // The original is a `switch` whose cases fall through, so a 3-byte tail
    // applies all three shifts and every non-empty tail ends with `h *= m`.
    let tail = data.chunks_exact(4).remainder();
    if let Some(byte) = tail.get(2) {
        h ^= u32::from(*byte) << 16;
    }
    if let Some(byte) = tail.get(1) {
        h ^= u32::from(*byte) << 8;
    }
    if let Some(byte) = tail.first() {
        h ^= u32::from(*byte);
        h = h.wrapping_mul(M);
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;

    // Reinterpret rather than cast: the workspace denies `as` conversions, and
    // this is the one place the sign of the bit pattern is the point.
    i32::from_ne_bytes(h.to_ne_bytes())
}

/// The partition a keyed record belongs to, exactly as Java's default
/// partitioner computes it.
///
/// `toPositive(murmur2(key)) % numPartitions`, where `toPositive` masks the
/// sign bit rather than taking an absolute value — the two differ for
/// `i32::MIN`, and the mask is the one Kafka uses.
///
/// Note the modulus is the topic's **total** partition count, not the count of
/// currently available ones. That is deliberate and matches Java: a key must
/// land in the same partition whether or not a leader is momentarily missing,
/// or a co-partitioned join stops being co-partitioned during a failover.
pub fn partition_for_key(key: &[u8], partition_count: i32) -> i32 {
    let positive = u32::from_ne_bytes(murmur2(key).to_ne_bytes()) & 0x7fff_ffff;
    let count = u32::try_from(partition_count).unwrap_or(1).max(1);
    i32::try_from(positive % count).unwrap_or(0)
}

/// Picks a partition for records that did not name one.
///
/// Keyed records hash. Unkeyed records **stick**: one partition per topic,
/// reused until [`Partitioner::rotate`] is called, which is what M13's
/// accumulator will do when it closes a batch.
#[derive(Debug, Default)]
pub struct Partitioner {
    sticky: Mutex<HashMap<String, i32>>,
}

impl Partitioner {
    /// A partitioner with no topic stuck yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The partition for a record, or `None` when the topic has none.
    pub fn assign(&self, topic: &str, key: Option<&[u8]>, partition_count: i32) -> Option<i32> {
        if partition_count <= 0 {
            return None;
        }
        match key {
            Some(key) => Some(partition_for_key(key, partition_count)),
            None => Some(self.sticky_for(topic, partition_count)),
        }
    }

    /// Release the topic's stuck partition, so the next unkeyed record picks a
    /// new one.
    ///
    /// The replacement is drawn afresh rather than stepped, so it may be the
    /// same partition again. That is a property of the original too: sticky is
    /// about batching, not about a guaranteed rotation, and forcing a change
    /// would reintroduce the round-robin this exists to avoid.
    pub fn rotate(&self, topic: &str) {
        let mut sticky = self.lock();
        sticky.remove(topic);
    }

    fn sticky_for(&self, topic: &str, partition_count: i32) -> i32 {
        use rand::Rng;

        let mut sticky = self.lock();
        let chosen = sticky
            .entry(topic.to_owned())
            .or_insert_with(|| rand::rng().random_range(0..partition_count));

        // A topic can gain partitions under us; it can never lose them. Clamp
        // anyway rather than trusting that, because the alternative is
        // producing to a partition that does not exist.
        if *chosen >= partition_count {
            *chosen = partition_count.saturating_sub(1).max(0);
        }
        *chosen
    }

    /// A poisoned map holds cached partition choices and nothing else, so
    /// recovering from the panic of an unrelated caller is strictly better
    /// than propagating it. Rule 2 forbids the alternative anyway.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, i32>> {
        match self.sticky.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur2_is_deterministic_and_key_sensitive() {
        assert_eq!(murmur2(b"customer-7"), murmur2(b"customer-7"));
        assert_ne!(murmur2(b"customer-7"), murmur2(b"customer-8"));
    }

    #[test]
    fn murmur2_handles_every_tail_length() {
        // The fall-through `switch` is the easiest part to get wrong, and a
        // wrong tail only shows up for keys whose length is not a multiple of
        // four. Exercise all four residues and assert they stay distinct.
        let hashes: Vec<i32> = ["", "a", "ab", "abc", "abcd", "abcde"]
            .iter()
            .map(|key| murmur2(key.as_bytes()))
            .collect();

        let mut unique = hashes.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), hashes.len(), "tail handling collapsed keys");
    }

    #[test]
    fn an_empty_key_still_hashes() {
        // Distinct from *no* key, which sticks instead of hashing.
        let partition = partition_for_key(b"", 6);
        assert!((0..6).contains(&partition));
    }

    #[test]
    fn a_key_always_lands_in_range_even_for_a_negative_hash() {
        // `toPositive` masks the sign bit; an absolute value would overflow on
        // i32::MIN and a plain modulo of a negative would return a negative
        // partition. Sweep enough keys to hit negative hashes.
        for i in 0..2_000 {
            let key = format!("key-{i}");
            let partition = partition_for_key(key.as_bytes(), 6);
            assert!(
                (0..6).contains(&partition),
                "key {key} produced partition {partition}"
            );
        }
    }

    #[test]
    fn keys_spread_across_partitions() {
        let mut seen = [0_u32; 6];
        for i in 0..6_000 {
            let key = format!("key-{i}");
            let partition = partition_for_key(key.as_bytes(), 6);
            if let Some(slot) = seen.get_mut(usize::try_from(partition).unwrap_or(0)) {
                *slot += 1;
            }
        }
        assert!(
            seen.iter().all(|count| *count > 500),
            "murmur2 spread badly: {seen:?}"
        );
    }

    #[test]
    fn one_partition_takes_everything() {
        for i in 0..100 {
            assert_eq!(partition_for_key(format!("k{i}").as_bytes(), 1), 0);
        }
    }

    #[test]
    fn an_unkeyed_record_sticks_until_rotated() {
        let partitioner = Partitioner::new();
        let first = partitioner.assign("orders", None, 12).unwrap();

        for _ in 0..100 {
            assert_eq!(
                partitioner.assign("orders", None, 12),
                Some(first),
                "sticky partitioner round-robined, which destroys batching"
            );
        }

        partitioner.rotate("orders");
        // The redraw may legitimately land on the same partition; what must
        // hold is that it is still a real one.
        let after = partitioner.assign("orders", None, 12).unwrap();
        assert!((0..12).contains(&after));
    }

    #[test]
    fn stickiness_is_per_topic() {
        let partitioner = Partitioner::new();
        let orders = partitioner.assign("orders", None, 12);
        let shipments = partitioner.assign("shipments", None, 12);
        assert_eq!(partitioner.assign("orders", None, 12), orders);
        assert_eq!(partitioner.assign("shipments", None, 12), shipments);
    }

    #[test]
    fn a_keyed_record_ignores_the_sticky_choice() {
        let partitioner = Partitioner::new();
        let stuck = partitioner.assign("orders", None, 12).unwrap();
        // Find a key that hashes somewhere else, and assert the partitioner
        // sends it there rather than to the stuck partition.
        let elsewhere = (0..1_000)
            .map(|i| format!("key-{i}"))
            .find(|key| partition_for_key(key.as_bytes(), 12) != stuck)
            .expect("1000 keys should not all hash to one of 12 partitions");

        assert_eq!(
            partitioner.assign("orders", Some(elsewhere.as_bytes()), 12),
            Some(partition_for_key(elsewhere.as_bytes(), 12))
        );
    }

    #[test]
    fn a_topic_with_no_partitions_gets_no_answer() {
        let partitioner = Partitioner::new();
        assert_eq!(partitioner.assign("orders", None, 0), None);
        assert_eq!(partitioner.assign("orders", Some(b"k"), 0), None);
    }

    #[test]
    fn a_stale_sticky_choice_is_clamped_into_range() {
        let partitioner = Partitioner::new();
        // Stick to a partition on a wide topic, then ask about a narrow one.
        // Partition counts only grow in Kafka, so this is defensive rather
        // than reachable — but producing to a partition that does not exist is
        // the wrong way to find that out.
        let _ = partitioner.assign("orders", None, 64);
        let narrowed = partitioner.assign("orders", None, 2).unwrap();
        assert!((0..2).contains(&narrowed));
    }
}
