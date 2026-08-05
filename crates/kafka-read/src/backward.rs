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
//!
//! # Anchoring
//!
//! The walk does not have to start at the log end. [`TailAnchor`] moves it to
//! an explicit offset or a wall-clock instant, which is what "the 500 records
//! ending at offset N" and "what did this topic look like at 14:30" need. The
//! anchor only decides where `window_end` starts; everything above — the
//! growing step, the batch-boundary filtering, the termination guard — is the
//! same code, because an arbitrary anchor faces the same offset gaps the log
//! end does.

use std::collections::VecDeque;

use kafka_conn::Result;
use kafka_meta::{Cluster, TopicId};

use crate::batch::{DecodeOptions, Visibility, decode_partition};
use crate::fetch::{FetchTarget, fetch};
use crate::record::{Record, RecordOutcome};
use crate::scan::RecordFilter;

/// Where a backward walk starts.
///
/// Both explicit anchors are **inclusive upper bounds**: `Offset(16733)` yields
/// 16733 and the records before it, and `Timestamp(t)` yields the last record
/// written at or before `t`. An anchor beyond a partition's log end is not an
/// error — that partition simply yields its own tail — and one below its log
/// start yields nothing, which is a result rather than a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TailAnchor {
    /// The current end of the log. The original behaviour, and the default.
    #[default]
    LogEnd,
    /// The same explicit offset in every partition.
    Offset(i64),
    /// The last record at or before a wall-clock time, in epoch milliseconds.
    Timestamp(i64),
}

/// How to read the tail of a partition.
#[derive(Debug, Clone)]
pub struct TailSpec {
    /// Topic.
    pub topic: String,
    /// Partitions, or `None` for every partition.
    pub partitions: Option<Vec<i32>>,
    /// Where the walk starts. [`TailAnchor::LogEnd`] unless set.
    pub anchor: TailAnchor,
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
            anchor: TailAnchor::LogEnd,
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
    #[must_use]
    pub fn partitions(mut self, partitions: impl IntoIterator<Item = i32>) -> Self {
        self.partitions = Some(partitions.into_iter().collect());
        self
    }

    /// Walk backwards from somewhere other than the log end.
    ///
    /// The convergence behaviour is unchanged — an arbitrary anchor needs the
    /// step to grow across offset gaps for exactly the same reason the log end
    /// does.
    #[must_use]
    pub fn ending_at(mut self, anchor: TailAnchor) -> Self {
        self.anchor = anchor;
        self
    }

    /// Filter records after decoding.
    #[must_use]
    pub fn filter(mut self, filter: RecordFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Show or hide aborted-transaction records.
    #[must_use]
    pub fn visibility(mut self, visibility: Visibility) -> Self {
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
    let topic_id = topic.topic_id;

    let mut out = Vec::with_capacity(wanted.len());
    for partition in wanted {
        let leader = cluster.leader_for(&spec.topic, partition).await?;
        out.push(
            tail_partition(
                cluster,
                spec,
                partition,
                leader,
                topic_id,
                per_partition,
                first_step,
            )
            .await?,
        );
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
async fn tail_partition(
    cluster: &Cluster,
    spec: &TailSpec,
    partition: i32,
    leader: i32,
    topic_id: TopicId,
    limit: usize,
    first_step: i64,
) -> Result<PartitionTail> {
    let (log_start, log_end) =
        crate::offsets::bounds(cluster, &spec.topic, partition, leader).await?;

    let mut collected: VecDeque<Record> = VecDeque::new();
    let mut malformed = 0usize;
    let mut fetches = 0usize;
    // The exclusive upper bound of the window we still need.
    let mut window_end = resolve_anchor(
        cluster,
        &spec.topic,
        partition,
        leader,
        spec.anchor,
        log_start,
        log_end,
    )
    .await?;
    let mut step = first_step.clamp(1, spec.max_step);

    while collected.len() < limit && window_end > log_start {
        let window_start = window_end.saturating_sub(step).max(log_start);

        let (records, bad, reads) = read_window(
            cluster,
            spec,
            partition,
            leader,
            topic_id,
            window_start,
            window_end,
        )
        .await?;
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

/// The exclusive upper bound the walk starts from, for one partition.
///
/// Every anchor resolves to an offset in `[log_start, log_end]`, so the walk
/// itself is identical whatever it was anchored at. A [`TailAnchor::Timestamp`]
/// costs one `ListOffsets` — the same RPC [`crate::StartPosition::Timestamp`]
/// already uses, so there is no new api key to negotiate.
async fn resolve_anchor(
    cluster: &Cluster,
    topic: &str,
    partition: i32,
    leader: i32,
    anchor: TailAnchor,
    log_start: i64,
    log_end: i64,
) -> Result<i64> {
    match anchor {
        TailAnchor::LogEnd => Ok(log_end),
        TailAnchor::Offset(offset) => Ok(clamp_bound(offset.saturating_add(1), log_start, log_end)),
        TailAnchor::Timestamp(millis) => {
            // `ListOffsets` answers "the first offset at or *after* this
            // instant", and the anchor is inclusive — so ask about the
            // millisecond after it. A record written exactly at `millis` is
            // then inside the window rather than one short of it.
            //
            // The clamp to zero is not defensive tidying: negative values are
            // sentinels on the wire, -1 meaning LATEST and -2 EARLIEST, so a
            // pre-epoch timestamp reaching the broker unclamped would silently
            // become a completely different query.
            let probe = timestamp_probe(millis);
            match crate::offsets::at_timestamp(cluster, topic, partition, leader, probe).await? {
                Some(offset) => Ok(clamp_bound(offset, log_start, log_end)),
                // Nothing was written after the instant, so the whole
                // partition is at or before it.
                None => Ok(log_end),
            }
        }
    }
}

/// The instant to ask `ListOffsets` about for an inclusive timestamp anchor.
fn timestamp_probe(millis: i64) -> i64 {
    millis.saturating_add(1).max(0)
}

/// Hold an exclusive upper bound inside what the partition actually retains.
///
/// A bound above the log end becomes the log end — a partition that has not
/// reached the requested offset yields its own tail rather than an error. A
/// bound below the log start becomes the log start, which makes the walk's
/// `window_end > log_start` guard false and yields an empty tail.
fn clamp_bound(bound: i64, log_start: i64, log_end: i64) -> i64 {
    bound.clamp(log_start.min(log_end), log_end)
}

/// Read `[start, end)` of a partition, following batch boundaries.
#[allow(clippy::too_many_arguments)]
async fn read_window(
    cluster: &Cluster,
    spec: &TailSpec,
    partition: i32,
    leader: i32,
    topic_id: TopicId,
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
            topic_id,
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
            // `last_offset`, not `offset` — a malformed batch spans a range,
            // and stepping to its base + 1 lands back inside it. The guard
            // below keeps that from looping, but only by abandoning the rest
            // of the window; stepping past the whole batch reads it instead.
            last_offset = outcome.last_offset();
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
    fn the_default_anchor_is_the_log_end() {
        // Rule: additive. Every caller written before anchors existed must
        // keep reading the end of the log without saying so.
        assert_eq!(TailSpec::new("orders", 500).anchor, TailAnchor::LogEnd);
        assert_eq!(TailAnchor::default(), TailAnchor::LogEnd);
    }

    #[test]
    fn an_offset_anchor_is_an_inclusive_upper_bound() {
        // `Offset(16733)` yields 16733 and the records before it, so the
        // exclusive bound handed to the walk is one past it.
        assert_eq!(clamp_bound(16_733 + 1, 0, 100_000), 16_734);
    }

    #[test]
    fn an_anchor_above_the_log_end_yields_the_partitions_own_tail() {
        // Not an error: partitions of one topic are at different offsets, and
        // asking for "ending at 900" across all of them must not fail the ones
        // that have not got there yet.
        assert_eq!(clamp_bound(901, 100, 500), 500);
    }

    #[test]
    fn an_anchor_below_the_log_start_yields_nothing() {
        // The walk's guard is `window_end > log_start`, so clamping to the log
        // start is what makes an expired anchor an empty result.
        let log_start = 12_000i64;
        let bound = clamp_bound(43, log_start, 16_733);
        assert_eq!(bound, log_start);
        assert!(!(bound > log_start), "the walk must not run");
    }

    #[test]
    fn an_empty_partition_clamps_to_itself() {
        // log_start == log_end is the empty partition, and `clamp` panics if
        // its bounds cross — which they would here without the `min`.
        assert_eq!(clamp_bound(5, 7, 7), 7);
        assert_eq!(clamp_bound(9, 7, 7), 7);
    }

    #[test]
    fn a_timestamp_anchor_asks_about_the_millisecond_after() {
        // `ListOffsets` answers "first offset at or after", and the anchor is
        // inclusive, so a record written exactly at the instant must be inside
        // the window rather than one short of it.
        assert_eq!(timestamp_probe(1_754_040_945_671), 1_754_040_945_672);
    }

    #[test]
    fn a_timestamp_probe_never_reaches_the_wire_negative() {
        // -1 is LATEST and -2 is EARLIEST on this request. A pre-epoch
        // timestamp arriving unclamped would not fail — it would quietly
        // answer a different question.
        assert_eq!(timestamp_probe(-1), 0);
        assert_eq!(timestamp_probe(-5_000), 0);
        assert_eq!(timestamp_probe(i64::MAX), i64::MAX);
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
