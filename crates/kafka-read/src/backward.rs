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
    /// The partition's log start offset, as measured before the walk.
    ///
    /// The walk pays for these bounds anyway; returning them saves a caller a
    /// second `ListOffsets` for "how much does this partition hold".
    pub log_start: i64,
    /// The partition's log end offset (exclusive), as measured before the walk.
    pub log_end: i64,
    /// `true` when the oldest record returned is the oldest the partition
    /// retains below the anchor: the walk reached the log start and nothing
    /// older was set aside. `false` means records remain below `records[0]`,
    /// so a further page exists and can be anchored at `records[0].offset - 1`.
    pub reached_log_start: bool,
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
    let topic_id = topic.topic_id;

    // Phase 1: bounds and anchors, before any budget is divided. Whether a
    // partition holds anything below its anchor is decidable without a single
    // fetch — `bounds` was being paid for per partition anyway — and the
    // divisor must not count the ones that hold nothing, or a topic with idle
    // partitions returns a fraction of what was asked for.
    let mut walks = Vec::with_capacity(wanted.len());
    for partition in wanted {
        let leader = cluster.leader_for(&spec.topic, partition).await?;
        let (log_start, log_end) =
            crate::offsets::bounds(cluster, &spec.topic, partition, leader).await?;
        let window_end = resolve_anchor(
            cluster,
            &spec.topic,
            partition,
            leader,
            spec.anchor,
            log_start,
            log_end,
        )
        .await?;
        walks.push(Walk {
            partition,
            leader,
            log_start,
            log_end,
            window_end,
            step: 1,
            collected: VecDeque::new(),
            malformed: 0,
            fetches: 0,
            target: 0,
            trimmed: false,
        });
    }

    let holding = walks.iter().filter(|walk| walk.has_more()).count();
    let first_step = spec.first_step(holding.max(1)).clamp(1, spec.max_step);
    for walk in &mut walks {
        walk.step = first_step;
    }

    // Phase 2: spread the limit across the partitions that hold anything, and
    // top up. Each round divides what is still owed among the walks that can
    // still yield — offsets left below their window, or records already in
    // hand past their target, since a chunk that over-delivered is spare
    // capacity too. A walk that cannot fill its grant leaves the divisor, so
    // the next round hands its share to the ones that can; every round either
    // satisfies the limit or retires a walk, so this terminates within one
    // round per partition — and `limit` means "up to this many, if the topic
    // has them" rather than "a per-partition ration".
    //
    // Only `min(collected, target)` counts towards the limit. Chunks are kept
    // whole (see `run_until`), and counting the overshoot would let one
    // partition's chunk spend another partition's share — a four-partition
    // tail of 400 must be [100, 100, 100, 100], not [200, 200, 0, 0], which
    // is what the release gate measured when the overshoot counted.
    loop {
        let kept: usize = walks.iter().map(Walk::kept).sum();
        if kept >= spec.limit {
            break;
        }
        let open: Vec<usize> = walks
            .iter()
            .enumerate()
            .filter(|(_, walk)| walk.can_yield_more())
            .map(|(index, _)| index)
            .collect();
        if open.is_empty() {
            break;
        }
        let share = (spec.limit - kept).div_ceil(open.len());
        for index in open {
            let kept: usize = walks.iter().map(Walk::kept).sum();
            let deficit = spec.limit.saturating_sub(kept);
            if deficit == 0 {
                break;
            }
            let Some(walk) = walks.get_mut(index) else {
                continue;
            };
            let target = walk.target.saturating_add(share.min(deficit));
            walk.target = target;
            walk.run_until(cluster, spec, topic_id, target).await?;
        }
    }

    // Enforce each walk's share before returning: a chunk that straddled a
    // batch boundary over-collected, and what it over-collected is the oldest
    // of that partition's records.
    for walk in &mut walks {
        walk.trim_to_target();
    }

    Ok(walks.into_iter().map(Walk::finish).collect())
}

/// One partition's backward walk, resumable so [`tail`] can raise its target
/// when another partition runs out of records.
struct Walk {
    partition: i32,
    leader: i32,
    log_start: i64,
    log_end: i64,
    /// The exclusive upper bound of the window still to be read.
    window_end: i64,
    step: i64,
    collected: VecDeque<Record>,
    malformed: usize,
    fetches: usize,
    /// This walk's share of the topic-wide limit, granted in rounds.
    target: usize,
    /// Whether records older than `collected` were dropped to hold the limit.
    trimmed: bool,
}

impl Walk {
    /// Whether offsets remain below the window — the walk can yield more.
    fn has_more(&self) -> bool {
        self.window_end > self.log_start
    }

    /// Records that count towards the topic-wide limit: what is in hand,
    /// capped at what this walk was asked for.
    fn kept(&self) -> usize {
        self.collected.len().min(self.target)
    }

    /// Whether granting this walk a larger target could yield more records —
    /// offsets remain below the window, or a chunk already over-delivered
    /// past the current target and the spare is in hand.
    fn can_yield_more(&self) -> bool {
        self.has_more() || self.collected.len() > self.target
    }

    /// Drop over-collection past the target — the oldest records, since the
    /// walk collects backwards and a chunk's overshoot lands at the front.
    fn trim_to_target(&mut self) {
        while self.collected.len() > self.target {
            self.collected.pop_front();
            self.trimmed = true;
        }
    }

    /// Walk backwards until `target` records are held or the log start is hit.
    ///
    /// Everything a chunk yields is kept, even past the target: the walk may
    /// be resumed with a higher target, and a record discarded mid-chunk would
    /// be skipped over on resume — `window_end` has already moved below it.
    /// The overshoot is bounded by one chunk and trimmed by the caller.
    async fn run_until(
        &mut self,
        cluster: &Cluster,
        spec: &TailSpec,
        topic_id: TopicId,
        target: usize,
    ) -> Result<()> {
        while self.collected.len() < target && self.window_end > self.log_start {
            let window_start = self
                .window_end
                .saturating_sub(self.step)
                .max(self.log_start);

            let (records, bad, reads) = read_window(
                cluster,
                spec,
                self.partition,
                self.leader,
                topic_id,
                window_start,
                self.window_end,
            )
            .await?;
            self.fetches += reads;
            self.malformed += bad;

            let yielded = records.len();
            // Prepend: we are walking backwards, and the caller wants log
            // order.
            for record in records.into_iter().rev() {
                self.collected.push_front(record);
            }

            // Grow the step when the window under-delivered, which is what a
            // compacted topic looks like: a thousand offsets holding fifty
            // records. Without this the walk crawls and ends up reading the
            // whole partition, which is exactly the naive behaviour this
            // design avoids.
            let span = self.window_end.saturating_sub(window_start).max(1);
            let density = i64::try_from(yielded).unwrap_or(i64::MAX);
            if density < span / 2 && self.collected.len() < target {
                let scale = if density == 0 {
                    8
                } else {
                    (span / density.max(1)).clamp(2, 8)
                };
                self.step = self.step.saturating_mul(scale).clamp(1, spec.max_step);
            }

            self.window_end = window_start;
        }
        Ok(())
    }

    fn finish(self) -> PartitionTail {
        PartitionTail {
            partition: self.partition,
            // Nothing below the oldest record returned: the walk examined
            // down to the log start, and the trim did not set anything older
            // aside on the way out.
            reached_log_start: !self.has_more() && !self.trimmed,
            records: self.collected.into_iter().collect(),
            malformed: self.malformed,
            fetches: self.fetches,
            log_start: self.log_start,
            log_end: self.log_end,
        }
    }
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

    /// The allocation rounds of [`tail`]'s phase 2, as arithmetic: each round
    /// divides what is still owed among the partitions that still hold
    /// records, and a partition that runs dry leaves the divisor.
    fn allocate(capacities: &[usize], limit: usize) -> Vec<usize> {
        let mut granted = vec![0usize; capacities.len()];
        let mut total = 0usize;
        while total < limit {
            let open: Vec<usize> = (0..capacities.len())
                .filter(|&i| granted[i] < capacities[i])
                .collect();
            if open.is_empty() {
                break;
            }
            let share = (limit - total).div_ceil(open.len());
            for i in open {
                let take = share.min(capacities[i] - granted[i]).min(limit - total);
                granted[i] += take;
                total += take;
                if total >= limit {
                    break;
                }
            }
        }
        granted
    }

    /// The shape that motivated the change: `kaas-canary-v1`, three partitions
    /// of which two hold nothing, read with the UI's default limit of 500.
    /// Dividing by three spends two thirds of the budget on nothing and
    /// returns 167; dividing by what holds records returns all 500.
    #[test]
    fn the_limit_is_not_spent_on_partitions_that_hold_nothing() {
        assert_eq!(allocate(&[0, 0, 89_478], 500), [0, 0, 500]);
    }

    #[test]
    fn a_partition_that_runs_dry_hands_its_share_to_the_rest() {
        // 500 across [3, big, big]: the dry partition yields its 3, and the
        // other two split the rest rather than stopping at ⌈500/3⌉ each.
        let granted = allocate(&[3, 100_000, 100_000], 500);
        assert_eq!(granted[0], 3);
        assert_eq!(granted.iter().sum::<usize>(), 500);
        assert!(granted[1] >= 167 && granted[2] >= 167, "{granted:?}");
    }

    #[test]
    fn a_topic_that_holds_less_than_the_limit_returns_everything_it_has() {
        assert_eq!(allocate(&[10, 0, 20], 500), [10, 0, 20]);
    }

    #[test]
    fn an_even_topic_still_splits_evenly() {
        // The `kperf-bench` shape: every partition holds plenty, and the
        // limit divides as it always did — except the total is now the limit
        // itself, not ⌈500/16⌉ × 16 = 512 with the caller left to truncate.
        // ⌈500/16⌉ rounding shorts only the last partition in the round.
        let granted = allocate(&[10_000; 16], 500);
        assert_eq!(granted.iter().sum::<usize>(), 500);
        assert!(
            granted.iter().filter(|&&g| g == 32).count() >= 15,
            "{granted:?}"
        );
    }

    fn walk_at(window_end: i64, log_start: i64, trimmed: bool) -> Walk {
        Walk {
            partition: 0,
            leader: 1,
            log_start,
            log_end: 100,
            window_end,
            step: 64,
            collected: VecDeque::new(),
            malformed: 0,
            fetches: 0,
            target: 0,
            trimmed,
        }
    }

    fn record_at(offset: i64) -> Record {
        Record {
            topic: "orders".to_owned(),
            partition: 0,
            offset,
            timestamp: offset,
            timestamp_type: crate::record::TimestampType::Creation,
            key: None,
            value: None,
            headers: Vec::new(),
            producer_id: None,
            transactional: false,
            leader_epoch: None,
        }
    }

    /// The release-gate regression: a chunk that over-delivers keeps its
    /// records, but they must not count towards the topic-wide limit, and the
    /// trim must drop the oldest of them.
    #[test]
    fn overshoot_is_kept_whole_but_counts_and_returns_only_the_target() {
        let mut walk = walk_at(50, 10, false);
        walk.target = 2;
        for offset in [7, 8, 9] {
            walk.collected.push_back(record_at(offset));
        }
        assert_eq!(walk.kept(), 2, "overshoot must not spend another's share");
        assert!(
            walk.can_yield_more(),
            "the spare record is capacity for a later round"
        );

        walk.trim_to_target();
        assert_eq!(
            walk.collected
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![8, 9],
            "the oldest record is the one dropped"
        );
        assert!(walk.trimmed);
        assert!(
            !walk.finish().reached_log_start,
            "a trimmed walk never claims the bottom"
        );
    }

    #[test]
    fn a_walk_that_hit_the_log_start_says_so() {
        // `collected.len() >= target` and `window_end <= log_start` were both
        // known to the loop and neither was reported — this is the field that
        // lets a caller compute "is there more below" without a second
        // ListOffsets.
        assert!(walk_at(10, 10, false).finish().reached_log_start);
        assert!(!walk_at(50, 10, false).finish().reached_log_start);
    }

    #[test]
    fn a_trimmed_walk_never_claims_to_be_the_bottom() {
        // The trim drops the oldest records of the fullest partition, so the
        // records below the oldest returned exist even though the walk itself
        // examined down to the log start.
        assert!(!walk_at(10, 10, true).finish().reached_log_start);
    }

    #[test]
    fn the_bounds_ride_along_on_the_result() {
        let tail = walk_at(10, 10, false).finish();
        assert_eq!((tail.log_start, tail.log_end), (10, 100));
    }
}
