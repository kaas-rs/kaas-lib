//! The backward scan — "last N messages".
//!
//! The most-used view in any Kafka UI, and not a forward read with a different
//! starting point. Reading forward from `latest - N` is wrong on any topic
//! where records are not one offset apart, which is every compacted topic and
//! every topic that has had `DeleteRecords` run against it.
//!
//! # How it works
//!
//! `ListOffsets(LATEST)` per partition, then walk backwards in bounded chunks:
//! read `[end - step, end)`, keep what came out, and if it was not enough,
//! move `end` back and go again. Each chunk is a normal forward fetch, because
//! Kafka has no backward read — the walk is in the *planning*, not in the wire
//! protocol.
//!
//! # What makes it easy to get wrong
//!
//! * **Batch boundaries do not align to the step.** A fetch from `end - step`
//!   starts at whatever batch contains that offset, so a chunk routinely
//!   returns records before the window. They are filtered, not trusted.
//! * **Compacted topics have offset gaps.** Asking for the last 500 records of
//!   a partition whose offsets run 0, 7, 91, 4001 means the offset arithmetic
//!   over-estimates every time. A step that assumes one record per offset
//!   walks back a handful of records per round trip and re-reads the whole
//!   partition — the naive implementation this design exists to avoid. So the
//!   step *grows* when a chunk yields fewer records than its offset span
//!   suggested.
//! * **The loop must terminate.** It stops at the partition's log start, and
//!   the step only ever grows, so a partition with a thousand-fold offset gap
//!   converges rather than crawling.

use std::collections::VecDeque;

use kafka_conn::Result;
use kafka_meta::Cluster;

use crate::batch::{DecodeOptions, Visibility, decode_partition};
use crate::fetch::{FetchTarget, fetch};
use crate::record::{Record, RecordOutcome};
use crate::scan::RecordFilter;

/// How to read the tail of a partition.
#[derive(Debug, Clone)]
pub struct TailSpec {
    /// Topic.
    pub topic: String,
    /// Partitions, or `None` for every partition.
    pub partitions: Option<Vec<i32>>,
    /// How many records to return, in total across partitions.
    pub limit: usize,
    /// Filter applied after decoding.
    pub filter: Option<RecordFilter>,
    /// Whether aborted-transaction records are visible.
    pub visibility: Visibility,
    /// Per-partition byte budget for one chunk.
    pub partition_max_bytes: i32,
    /// Whole-response byte budget.
    pub fetch_max_bytes: i32,
    /// Ceiling on a single batch's decompressed size.
    pub max_decompressed_bytes: usize,
    /// How many offsets to step back on the first chunk.
    ///
    /// Grows automatically when a chunk under-delivers, which is what keeps a
    /// compacted topic from taking one round trip per record.
    pub initial_step: i64,
    /// Never step back more than this in one chunk, so a single fetch stays
    /// bounded.
    pub max_step: i64,
}

impl TailSpec {
    /// The last `limit` records of a topic.
    pub fn new(topic: impl Into<String>, limit: usize) -> Self {
        Self {
            topic: topic.into(),
            partitions: None,
            limit,
            filter: None,
            visibility: Visibility::default(),
            partition_max_bytes: 1024 * 1024,
            fetch_max_bytes: 8 * 1024 * 1024,
            max_decompressed_bytes: 64 * 1024 * 1024,
            initial_step: 0,
            max_step: 100_000,
        }
    }

    /// Restrict to specific partitions.
    pub fn with_partitions(mut self, partitions: impl IntoIterator<Item = i32>) -> Self {
        self.partitions = Some(partitions.into_iter().collect());
        self
    }

    /// Filter records after decoding.
    pub fn with_filter(mut self, filter: RecordFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Show or hide aborted-transaction records.
    pub fn with_visibility(mut self, visibility: Visibility) -> Self {
        self.visibility = visibility;
        self
    }

    fn decode_options(&self) -> DecodeOptions {
        DecodeOptions {
            max_decompressed_bytes: self.max_decompressed_bytes,
            visibility: self.visibility,
        }
    }

    /// The first step size, derived from the limit when not set explicitly.
    fn first_step(&self, partitions: usize) -> i64 {
        if self.initial_step > 0 {
            return self.initial_step;
        }
        let per_partition = self.limit.div_ceil(partitions.max(1));
        // A little headroom, because a chunk that lands one record short costs
        // a whole extra round trip.
        i64::try_from(per_partition.saturating_mul(2).max(64)).unwrap_or(1024)
    }
}

/// The tail of one partition.
#[derive(Debug, Clone)]
pub struct PartitionTail {
    /// Partition.
    pub partition: i32,
    /// Records, oldest first.
    pub records: Vec<Record>,
    /// Batches that would not decode.
    pub malformed: usize,
    /// How many fetches this partition cost, for the byte-budget assertions
    /// that make this milestone verifiable.
    pub fetches: usize,
}

/// Read the last `limit` records of a topic.
///
/// Returns a `Vec` rather than a stream, deliberately: the whole point of the
/// call is that the caller asked for a bounded number of records, and the
/// bound is enforced before anything is returned.
pub async fn tail(cluster: &Cluster, spec: &TailSpec) -> Result<Vec<PartitionTail>> {
    let snapshot = cluster.refresh_topics(&[spec.topic.as_str()]).await?;
    let topic = snapshot.topic(&spec.topic).ok_or_else(|| {
        kafka_conn::Error::from_code(
            kafka_conn::ErrorCode::UnknownTopicOrPartition,
            Some(spec.topic.clone()),
        )
    })?;

    let wanted: Vec<i32> = match &spec.partitions {
        Some(partitions) => partitions.clone(),
        None => topic.partitions.iter().map(|p| p.partition).collect(),
    };
    if wanted.is_empty() {
        return Ok(Vec::new());
    }

    // Spread the limit across partitions. A UI asking for "the last 500" of a
    // six-partition topic wants roughly the newest 500 overall, and reading
    // 500 from each would fetch six times what was asked for.
    let per_partition = spec.limit.div_ceil(wanted.len());
    let first_step = spec.first_step(wanted.len());

    let mut out = Vec::with_capacity(wanted.len());
    for partition in wanted {
        let leader = cluster.leader_for(&spec.topic, partition).await?;
        out.push(
            tail_partition(cluster, spec, partition, leader, per_partition, first_step).await?,
        );
    }
    Ok(out)
}

async fn tail_partition(
    cluster: &Cluster,
    spec: &TailSpec,
    partition: i32,
    leader: i32,
    limit: usize,
    first_step: i64,
) -> Result<PartitionTail> {
    let (log_start, log_end) =
        crate::offsets::bounds(cluster, &spec.topic, partition, leader).await?;

    let mut collected: VecDeque<Record> = VecDeque::new();
    let mut malformed = 0usize;
    let mut fetches = 0usize;
    // The exclusive upper bound of the window we still need.
    let mut window_end = log_end;
    let mut step = first_step.clamp(1, spec.max_step);

    while collected.len() < limit && window_end > log_start {
        let window_start = window_end.saturating_sub(step).max(log_start);

        let (records, bad, reads) =
            read_window(cluster, spec, partition, leader, window_start, window_end).await?;
        fetches += reads;
        malformed += bad;

        let yielded = records.len();
        // Prepend: we are walking backwards, and the caller wants log order.
        for record in records.into_iter().rev() {
            collected.push_front(record);
            if collected.len() >= limit {
                break;
            }
        }

        // Grow the step when the window under-delivered, which is what a
        // compacted topic looks like: a thousand offsets holding fifty
        // records. Without this the walk crawls and ends up reading the whole
        // partition, which is exactly the naive behaviour this design avoids.
        let span = window_end.saturating_sub(window_start).max(1);
        let density = i64::try_from(yielded).unwrap_or(i64::MAX);
        if density < span / 2 && yielded < limit {
            let scale = if density == 0 {
                8
            } else {
                (span / density.max(1)).clamp(2, 8)
            };
            step = step.saturating_mul(scale).clamp(1, spec.max_step);
        }

        window_end = window_start;
    }

    // Trim from the front: we may have collected more than asked for when a
    // chunk straddled the boundary.
    while collected.len() > limit {
        collected.pop_front();
    }

    Ok(PartitionTail {
        partition,
        records: collected.into_iter().collect(),
        malformed,
        fetches,
    })
}

/// Read `[start, end)` of a partition, following batch boundaries.
async fn read_window(
    cluster: &Cluster,
    spec: &TailSpec,
    partition: i32,
    leader: i32,
    start: i64,
    end: i64,
) -> Result<(Vec<Record>, usize, usize)> {
    let mut records = Vec::new();
    let mut malformed = 0usize;
    let mut fetches = 0usize;
    let mut offset = start;

    while offset < end {
        let fetched = fetch(
            cluster,
            leader,
            &spec.topic,
            &[FetchTarget {
                partition,
                offset,
                max_bytes: spec.partition_max_bytes,
            }],
            // No waiting: the data is already there, and a backward scan that
            // blocks for new records is a backward scan that hangs on an idle
            // topic.
            0,
            spec.fetch_max_bytes,
            spec.visibility,
        )
        .await?;
        fetches += 1;

        let Some(data) = fetched.into_iter().find(|p| p.partition == partition) else {
            break;
        };
        let decoded = decode_partition(
            &spec.topic,
            partition,
            data.records,
            &data.aborted,
            &spec.decode_options(),
        );

        if decoded.outcomes.is_empty() {
            // Nothing decodable in this fetch. Either the window is entirely
            // control batches, or every offset in it was compacted away.
            // Either way there is nothing more to get here.
            break;
        }

        let mut last_offset = offset;
        for outcome in decoded.outcomes {
            last_offset = outcome.offset();
            match outcome {
                // The fetch started at whatever batch contains `offset`, so
                // records before the window are normal and are dropped rather
                // than trusted.
                RecordOutcome::Ok(record) if record.offset >= start && record.offset < end => {
                    if spec
                        .filter
                        .as_ref()
                        .is_none_or(|filter| filter.matches(&record))
                    {
                        records.push(record);
                    }
                }
                RecordOutcome::Ok(_) => {}
                RecordOutcome::Malformed { .. } => malformed += 1,
            }
        }

        let next = last_offset.saturating_add(1);
        if next <= offset {
            // No forward progress: a partition whose offsets do not advance
            // would otherwise loop forever.
            break;
        }
        offset = next;
    }

    Ok((records, malformed, fetches))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_step_defaults_to_twice_the_per_partition_limit() {
        let spec = TailSpec::new("orders", 500);
        // 500 across 6 partitions is 84 each, doubled for headroom.
        assert_eq!(spec.first_step(6), 168);
        // And never absurdly small, which would cost a round trip per record.
        assert_eq!(TailSpec::new("orders", 1).first_step(1), 64);
    }

    #[test]
    fn an_explicit_step_wins() {
        let mut spec = TailSpec::new("orders", 500);
        spec.initial_step = 4096;
        assert_eq!(spec.first_step(6), 4096);
    }

    /// The compaction case, as arithmetic.
    ///
    /// A partition whose offsets run 0..100_000 but holds only 500 live
    /// records: each window of `step` offsets yields roughly `step / 200`
    /// records. A fixed step would need 200 times as many round trips as a
    /// growing one.
    #[test]
    fn the_step_grows_when_a_window_under_delivers() {
        let max_step = 100_000i64;
        let mut step = 1_000i64;
        let mut windows = 0;
        let mut collected = 0i64;
        let limit = 500i64;
        let mut end = 100_000i64;

        while collected < limit && end > 0 && windows < 100 {
            let start = (end - step).max(0);
            let span = (end - start).max(1);
            // One live record per 200 offsets.
            let yielded = span / 200;
            collected += yielded;
            windows += 1;

            if yielded < span / 2 && collected < limit {
                let scale = if yielded == 0 {
                    8
                } else {
                    (span / yielded.max(1)).clamp(2, 8)
                };
                step = step.saturating_mul(scale).clamp(1, max_step);
            }
            end = start;
        }

        assert!(
            collected >= limit,
            "collected {collected} in {windows} windows"
        );
        assert!(
            windows < 20,
            "took {windows} windows; a fixed step would take ~100"
        );
    }

    #[test]
    fn the_walk_terminates_at_the_log_start() {
        // A partition that has been fully compacted away: the loop must stop
        // rather than stepping below the log start forever.
        let log_start = 90_000i64;
        let mut end = 100_000i64;
        let step = 1_000i64;
        let mut iterations = 0;
        while end > log_start && iterations < 1_000 {
            end = end.saturating_sub(step).max(log_start);
            iterations += 1;
        }
        assert_eq!(end, log_start);
        assert_eq!(iterations, 10);
    }

    #[test]
    fn a_window_never_steps_past_the_log_start() {
        let log_start = 50i64;
        let window_end = 60i64;
        let step = 1_000i64;
        let window_start = window_end.saturating_sub(step).max(log_start);
        assert_eq!(window_start, log_start);
    }
}
