//! The forward scan.
//!
//! Returns a `Stream`, never a `Vec`. A UI browsing a partition with a hundred
//! million records must not decide how much memory to use by how much data the
//! user happened to ask about, and a scan that materialises its results has
//! already lost that argument before the first record is decoded.
//!
//! # Memory
//!
//! Bounded by [`ScanSpec::max_buffered_records`] *regardless of partition
//! count*. Interleaving across partitions needs some lookahead, and the naive
//! implementation keeps one fetch's worth per partition — which on a
//! thousand-partition topic is a thousand times the budget anyone intended.
//!
//! # Ordering
//!
//! Within a partition, exact log order, always.
//!
//! Across partitions, records come out in timestamp order whenever the buffer
//! holds at least one record from every partition still being read. That is the
//! usual case. When the buffer cap forces an emit before every partition is
//! represented, ordering degrades gracefully: the emitted record is the
//! earliest among those buffered, so the reorder is bounded by the span of the
//! buffer rather than by the length of the topic.
//! [`ScanProgress::reorder_window`] reports how far that bound stretched — and
//! stays `0` on a single-partition scan, where there is no merge to degrade —
//! so a UI can say "approximately ordered, within N" rather than quietly
//! lying.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;
use kafka_conn::{Error, Result};
use kafka_meta::{Cluster, TopicId};

use crate::batch::{DecodeOptions, Visibility, decode_partition};
use crate::fetch::{FetchTarget, fetch};
use crate::record::{DecodeError, Record, RecordOutcome};

/// Where a scan starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartPosition {
    /// The first offset the log still holds.
    Earliest,
    /// The end of the log — a scan that will only see new records.
    Latest,
    /// An explicit offset, the same one in every partition.
    Offset(i64),
    /// The first record at or after a wall-clock timestamp, in epoch
    /// milliseconds.
    Timestamp(i64),
}

/// A predicate applied to each record before it is emitted.
///
/// Applied client-side, after decoding: Kafka has no server-side filtering, so
/// a filter reduces what a UI renders rather than what the cluster sends.
#[derive(Clone)]
pub enum RecordFilter {
    /// The key contains this byte sequence.
    KeyContains(Bytes),
    /// The value contains this byte sequence.
    ValueContains(Bytes),
    /// A header with this name is present.
    HasHeader(String),
    /// Only tombstones.
    TombstonesOnly,
    /// Anything the caller likes.
    Custom(Arc<dyn Fn(&Record) -> bool + Send + Sync>),
}

impl std::fmt::Debug for RecordFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordFilter::KeyContains(needle) => {
                write!(f, "KeyContains({} bytes)", needle.len())
            }
            RecordFilter::ValueContains(needle) => {
                write!(f, "ValueContains({} bytes)", needle.len())
            }
            RecordFilter::HasHeader(name) => write!(f, "HasHeader({name})"),
            RecordFilter::TombstonesOnly => f.write_str("TombstonesOnly"),
            RecordFilter::Custom(_) => f.write_str("Custom(..)"),
        }
    }
}

impl RecordFilter {
    /// Whether a record passes.
    pub fn matches(&self, record: &Record) -> bool {
        match self {
            RecordFilter::KeyContains(needle) => {
                record.key.as_ref().is_some_and(|k| contains(k, needle))
            }
            RecordFilter::ValueContains(needle) => {
                record.value.as_ref().is_some_and(|v| contains(v, needle))
            }
            RecordFilter::HasHeader(name) => record.headers.iter().any(|(key, _)| key == name),
            RecordFilter::TombstonesOnly => record.is_tombstone(),
            RecordFilter::Custom(predicate) => predicate(record),
        }
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// What to scan.
#[derive(Debug, Clone)]
pub struct ScanSpec {
    /// Topic.
    pub topic: String,
    /// Partitions, or `None` for every partition.
    pub partitions: Option<Vec<i32>>,
    /// Where to start.
    pub from: StartPosition,
    /// Stop after this many *emitted* records, or `None` to reach the end.
    pub limit: Option<usize>,
    /// Whether to keep waiting at the end of the log instead of finishing.
    ///
    /// `false` — the default — is a *browse*: the scan plans against the log
    /// end as it stood when it started, and [`ScanEvent::Done`] means the
    /// window is read. `true` is a *tail*: reaching the end is not an ending,
    /// the fetch long-polls for what has not been written yet, and the stream
    /// only finishes when the caller drops it or a `limit` is reached.
    pub follow: bool,
    /// Filter applied after decoding.
    pub filter: Option<RecordFilter>,
    /// Whether aborted-transaction records are visible.
    pub visibility: Visibility,
    /// Per-partition byte budget for a single fetch.
    pub partition_max_bytes: i32,
    /// Whole-response byte budget for a single fetch.
    pub fetch_max_bytes: i32,
    /// How long a fetch may wait for data.
    pub max_wait_ms: i32,
    /// The scan's memory ceiling, in decoded records held at once.
    pub max_buffered_records: usize,
    /// Ceiling on a single batch's decompressed size.
    pub max_decompressed_bytes: usize,
}

impl ScanSpec {
    /// Scan a whole topic from the beginning.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            partitions: None,
            from: StartPosition::Earliest,
            limit: None,
            follow: false,
            filter: None,
            visibility: Visibility::default(),
            partition_max_bytes: 1024 * 1024,
            fetch_max_bytes: 8 * 1024 * 1024,
            max_wait_ms: 500,
            max_buffered_records: 10_000,
            max_decompressed_bytes: 64 * 1024 * 1024,
        }
    }

    /// Restrict to specific partitions.
    #[must_use]
    pub fn partitions(mut self, partitions: impl IntoIterator<Item = i32>) -> Self {
        self.partitions = Some(partitions.into_iter().collect());
        self
    }

    /// Start somewhere other than the beginning.
    #[must_use]
    pub fn from(mut self, from: StartPosition) -> Self {
        self.from = from;
        self
    }

    /// Stop after `limit` records.
    #[must_use]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Keep the scan open at the end of the log, waiting for new records.
    ///
    /// Turns [`StartPosition::Latest`] from a scan of nothing into a tail. It
    /// is still not a consumer: there is no group, no membership and no commit
    /// — the stream ends when it is dropped.
    #[must_use]
    pub fn following(mut self) -> Self {
        self.follow = true;
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
}

/// How far a scan has got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanProgress {
    /// Records emitted so far, after filtering.
    pub records_emitted: u64,
    /// Records decoded so far, before filtering.
    pub records_scanned: u64,
    /// Batches that would not decode.
    pub malformed_batches: u64,
    /// Offsets consumed across every partition.
    pub offsets_consumed: i64,
    /// Offsets the scan set out to consume, from the initial offset ranges.
    ///
    /// Compaction and retention mean this is an upper bound, not an exact
    /// count — which is why a UI should render it as a proportion rather than
    /// as "N of M records".
    pub offsets_total: i64,
    /// Partitions still being read.
    ///
    /// Forced to zero on the final event — it counts what is *left*, so it is
    /// how a caller watches partitions finish, and it is not the merge's
    /// width. For the width, which a caller needs exactly when this is zero,
    /// see [`ScanProgress::partitions_planned`].
    pub partitions_active: usize,
    /// Partitions the scan set out to read — the widest the merge has been.
    /// Never changes over the life of the scan.
    pub partitions_planned: usize,
    /// Roughly how far apart, in records, two records from *different*
    /// partitions may have been emitted relative to timestamp order.
    ///
    /// `0` means cross-partition timestamp order held throughout — including,
    /// always, when the merge is one partition wide: within a partition the
    /// order is exact whatever the buffer did. Non-zero means the buffer
    /// ceiling forced an emit before every partition was represented, and the
    /// reorder is bounded by this many records rather than by the length of
    /// the topic.
    pub reorder_window: usize,
}

impl ScanProgress {
    /// Completion as a fraction in `0.0..=1.0`, or `None` when the total is
    /// not yet known.
    pub fn fraction(&self) -> Option<f64> {
        if self.offsets_total <= 0 {
            return None;
        }
        let consumed = u32::try_from(self.offsets_consumed.max(0)).unwrap_or(u32::MAX);
        let total = u32::try_from(self.offsets_total.max(1)).unwrap_or(u32::MAX);
        Some((f64::from(consumed) / f64::from(total)).clamp(0.0, 1.0))
    }
}

/// Why a partition's scan starts somewhere other than where it was asked to.
///
/// Both substitutions are the right behaviour for browsing and quietly wrong
/// for verification — "did my record land at 900001" answered from offset
/// 12000 looks like it worked. The scan still substitutes, because a partial
/// browse beats an error; this says so, per partition, so the caller can tell
/// "I read from where you asked" apart from "I read from somewhere else".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartSubstitution {
    /// [`StartPosition::Offset`] named an offset the partition no longer
    /// retains; the scan starts at the log start instead.
    OffsetBelowLogStart {
        /// The offset that was asked for.
        requested: i64,
        /// The first offset the partition still holds, where the scan starts.
        log_start: i64,
    },
    /// [`StartPosition::Offset`] named an offset the partition has not
    /// reached; the scan starts at the log end instead.
    OffsetBeyondLogEnd {
        /// The offset that was asked for.
        requested: i64,
        /// The partition's log end, where the scan starts.
        log_end: i64,
    },
    /// [`StartPosition::Timestamp`] resolved to no offset on this partition —
    /// nothing was written at or after the instant, or the broker holds no
    /// timestamp index — and the scan starts at the log end. Without this an
    /// empty window is indistinguishable from "nothing has been written
    /// since then".
    TimestampUnresolved {
        /// The instant that was asked about, in epoch milliseconds.
        requested: i64,
        /// The partition's log end, where the scan starts.
        log_end: i64,
    },
}

/// What a scan emits.
#[derive(Debug, Clone)]
pub enum ScanEvent {
    /// Where a partition's scan actually starts — one per partition, before
    /// any record. `substituted` is `None` when the requested
    /// [`StartPosition`] was honoured exactly; for a timestamp start,
    /// `start_offset` is what the instant resolved to, so a caller does not
    /// need its own `ListOffsets` to find out.
    PartitionStarted {
        /// Partition.
        partition: i32,
        /// The first offset the scan will read.
        start_offset: i64,
        /// The substitution that was made, when the requested position could
        /// not be honoured.
        substituted: Option<StartSubstitution>,
    },
    /// A decoded record.
    Record(Record),
    /// A batch that would not decode. The scan continues.
    Malformed {
        /// Topic.
        topic: String,
        /// Partition.
        partition: i32,
        /// First offset the batch claimed.
        offset: i64,
        /// Last offset the batch claimed, when its header said.
        last_offset: Option<i64>,
        /// The raw batch.
        raw: Bytes,
        /// Why.
        reason: DecodeError,
    },
    /// Periodic progress, so a UI can render a bar rather than a spinner.
    Progress(ScanProgress),
    /// A partition reached its end offset.
    PartitionComplete {
        /// Partition.
        partition: i32,
        /// The last offset read.
        last_offset: i64,
    },
    /// The scan finished. Always the last event.
    Done(ScanProgress),
}

/// Whether a partition with an empty buffer is worth fetching for right now.
///
/// * `behind` — it has offsets it has not read. Always worth a fetch; this is
///   the only rule a browse needs.
/// * following and `idle` — nothing at all is buffered, so the only way to
///   make progress is to long-poll the log end.
///
/// The case this exists to exclude is a *tail with records already buffered*:
/// polling every partition sitting at its log end before emitting them costs
/// one `max_wait_ms` per record, which on a topic with one busy partition and
/// fifteen quiet ones is a couple of records a second.
fn should_fetch(behind: bool, follow: bool, idle: bool) -> bool {
    behind || (follow && idle)
}

/// One partition's position in a scan.
#[derive(Debug)]
struct PartitionCursor {
    partition: i32,
    leader: i32,
    next_offset: i64,
    end_offset: i64,
    start_offset: i64,
    /// How the start was substituted, when it was; drained into the
    /// [`ScanEvent::PartitionStarted`] queued before the first record.
    substituted: Option<StartSubstitution>,
    buffered: VecDeque<RecordOutcome>,
    finished: bool,
}

impl PartitionCursor {
    fn exhausted(&self) -> bool {
        self.finished || self.next_offset >= self.end_offset
    }

    fn head_timestamp(&self) -> Option<i64> {
        self.buffered.front().map(|outcome| match outcome {
            RecordOutcome::Ok(record) => record.timestamp,
            // A batch that would not decode still has an offset, and its
            // header timestamp is unavailable. Sorting it by i64::MIN emits it
            // as soon as it is seen, which keeps a decode failure adjacent to
            // where it happened.
            RecordOutcome::Malformed { .. } => i64::MIN,
        })
    }
}

/// How far apart two records from different partitions may be when the buffer
/// ceiling forces an emit: the buffer budget spread over the merge's width.
/// `0` when the merge is one partition wide — there is no cross-partition
/// order to degrade.
fn reorder_window(budget: usize, merging: usize) -> usize {
    if merging <= 1 { 0 } else { budget / merging }
}

/// Where an explicit-offset start actually lands in `[earliest, latest]`, and
/// whether that is a substitution the caller should hear about.
fn resolve_offset_start(
    requested: i64,
    earliest: i64,
    latest: i64,
) -> (i64, Option<StartSubstitution>) {
    if requested < earliest {
        (
            earliest,
            Some(StartSubstitution::OffsetBelowLogStart {
                requested,
                log_start: earliest,
            }),
        )
    } else if requested > latest {
        (
            latest,
            Some(StartSubstitution::OffsetBeyondLogEnd {
                requested,
                log_end: latest,
            }),
        )
    } else {
        (requested, None)
    }
}

/// The state a scan carries between polls.
struct ScanState {
    cluster: Cluster,
    spec: ScanSpec,
    /// From v13 `Fetch` names topics by id, not by name.
    topic_id: TopicId,
    cursors: Vec<PartitionCursor>,
    progress: ScanProgress,
    pending: VecDeque<ScanEvent>,
    buffered_total: usize,
    done: bool,
    /// Emit progress every this many decoded records.
    progress_every: u64,
    next_progress_at: u64,
}

/// Start a forward scan.
///
/// The returned stream does its own work as it is polled — there is no
/// background task — so dropping it stops the scan immediately and releases
/// every buffer. That is the leak property M11 tests for, and it comes from the
/// shape rather than from cleanup code.
pub async fn scan(
    cluster: &Cluster,
    spec: ScanSpec,
) -> Result<impl Stream<Item = Result<ScanEvent>> + Send> {
    let (mut cursors, topic_id) = plan(cluster, &spec).await?;
    let offsets_total = cursors
        .iter()
        .map(|c| c.end_offset.saturating_sub(c.start_offset).max(0))
        .sum();

    // Every partition announces where it actually starts before any record is
    // emitted, so a caller sees a substitution — or a timestamp's resolution
    // — before it has to interpret an empty window.
    let mut pending = VecDeque::with_capacity(cursors.len());
    for cursor in &mut cursors {
        pending.push_back(ScanEvent::PartitionStarted {
            partition: cursor.partition,
            start_offset: cursor.start_offset,
            substituted: cursor.substituted.take(),
        });
    }

    let state = ScanState {
        cluster: cluster.clone(),
        progress: ScanProgress {
            records_emitted: 0,
            records_scanned: 0,
            malformed_batches: 0,
            offsets_consumed: 0,
            offsets_total,
            partitions_active: cursors.len(),
            partitions_planned: cursors.len(),
            reorder_window: 0,
        },
        cursors,
        spec,
        topic_id,
        pending,
        buffered_total: 0,
        done: false,
        progress_every: 1_000,
        next_progress_at: 1_000,
    };

    Ok(futures::stream::unfold(state, |mut state| async move {
        state.next_event().await.map(|event| (event, state))
    }))
}

/// Work out where each partition starts and ends.
async fn plan(cluster: &Cluster, spec: &ScanSpec) -> Result<(Vec<PartitionCursor>, TopicId)> {
    let snapshot = cluster.refresh_topics(&[spec.topic.as_str()]).await?;
    let topic = snapshot.topic(&spec.topic).ok_or_else(|| {
        Error::from_code(
            kafka_conn::ErrorCode::UnknownTopicOrPartition,
            Some(spec.topic.clone()),
        )
    })?;
    if let Some(code) = topic.error {
        return Err(Error::from_code(code, Some(spec.topic.clone())));
    }

    let wanted: Vec<i32> = match &spec.partitions {
        Some(partitions) => partitions.clone(),
        None => topic.partitions.iter().map(|p| p.partition).collect(),
    };

    let topic_id = topic.topic_id;
    let mut cursors = Vec::new();
    for partition in wanted {
        let leader = cluster.leader_for(&spec.topic, partition).await?;
        let (earliest, latest) =
            crate::offsets::bounds(cluster, &spec.topic, partition, leader).await?;

        // Resolve the start, and remember when it is a substitution rather
        // than an honouring: the facts are in hand at exactly this point —
        // `bounds` was just paid for, and `at_timestamp` said `None` rather
        // than an offset — and dropping them forces every caller to buy them
        // again to interpret an empty window.
        let (start, substituted) = match spec.from {
            StartPosition::Earliest => (earliest, None),
            StartPosition::Latest => (latest, None),
            StartPosition::Offset(offset) => resolve_offset_start(offset, earliest, latest),
            StartPosition::Timestamp(timestamp) => {
                match crate::offsets::at_timestamp(
                    cluster,
                    &spec.topic,
                    partition,
                    leader,
                    timestamp,
                )
                .await?
                {
                    Some(offset) => (offset, None),
                    None => (
                        latest,
                        Some(StartSubstitution::TimestampUnresolved {
                            requested: timestamp,
                            log_end: latest,
                        }),
                    ),
                }
            }
        };

        cursors.push(PartitionCursor {
            partition,
            leader,
            next_offset: start,
            end_offset: latest,
            start_offset: start,
            substituted,
            buffered: VecDeque::new(),
            // A partition that starts at its own log end has nothing to read
            // — unless the scan is following, in which case that is precisely
            // where it is supposed to sit and wait.
            finished: start >= latest && !spec.follow,
        });
    }
    Ok((cursors, topic_id))
}

impl ScanState {
    /// Produce the next event, or `None` when the scan is over.
    async fn next_event(&mut self) -> Option<Result<ScanEvent>> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Some(Ok(event));
            }
            if self.done {
                return None;
            }

            if self.limit_reached() {
                return Some(Ok(self.finish()));
            }

            // Refill any partition that is out of buffered records and not yet
            // at its end, unless doing so would break the memory ceiling.
            match self.refill().await {
                Ok(true) => {}
                Ok(false) => {}
                Err(error) => {
                    self.done = true;
                    return Some(Err(error));
                }
            }

            match self.emit_next() {
                Some(event) => return Some(Ok(event)),
                None if self.all_exhausted() => return Some(Ok(self.finish())),
                // Nothing buffered and something still to read: loop and fetch
                // again.
                None => continue,
            }
        }
    }

    fn limit_reached(&self) -> bool {
        self.spec.limit.is_some_and(|limit| {
            self.progress.records_emitted >= u64::try_from(limit).unwrap_or(u64::MAX)
        })
    }

    fn all_exhausted(&self) -> bool {
        // A following scan is never finished. "Every partition is at its log
        // end" is the steady state of a tail, not the end of one, and the
        // stream ends by being dropped.
        !self.spec.follow
            && self
                .cursors
                .iter()
                .all(|cursor| cursor.exhausted() && cursor.buffered.is_empty())
    }

    /// Fetch for partitions that need it. Returns whether anything was read.
    async fn refill(&mut self) -> Result<bool> {
        if self.buffered_total >= self.spec.max_buffered_records {
            // At the ceiling. Emit from what we have rather than growing.
            self.note_ordering_degraded();
            return Ok(false);
        }

        // Group the partitions that need data by leader, so one broker gets one
        // request rather than one per partition.
        //
        // A partition sitting at its log end is only worth polling when there
        // is nothing else to emit. Polling it every round would make a tail
        // pay one `max_wait_ms` long-poll on every idle partition per record
        // — sixteen partitions and one busy one is two records a second, which
        // reads as a hung UI rather than a slow one.
        let idle = self.buffered_total == 0;
        let mut by_leader: HashMap<i32, Vec<FetchTarget>> = HashMap::new();
        for cursor in &self.cursors {
            if !cursor.buffered.is_empty() {
                continue;
            }
            if !should_fetch(!cursor.exhausted(), self.spec.follow, idle) {
                continue;
            }
            by_leader
                .entry(cursor.leader)
                .or_default()
                .push(FetchTarget {
                    partition: cursor.partition,
                    offset: cursor.next_offset,
                    max_bytes: self.spec.partition_max_bytes,
                });
        }
        if by_leader.is_empty() {
            return Ok(false);
        }

        let mut read_anything = false;
        for (leader, targets) in by_leader {
            let fetched = fetch(
                &self.cluster,
                leader,
                &self.spec.topic,
                self.topic_id,
                &targets,
                self.spec.max_wait_ms,
                self.spec.fetch_max_bytes,
                self.spec.visibility,
            )
            .await?;

            for partition in fetched {
                let Some(cursor) = self
                    .cursors
                    .iter_mut()
                    .find(|c| c.partition == partition.partition)
                else {
                    continue;
                };

                // Under read_committed the log's usable end is the last stable
                // offset, not the high watermark: everything past it is inside
                // an open transaction whose outcome is not decided.
                let end = match self.spec.visibility {
                    Visibility::All => partition.high_watermark,
                    Visibility::CommittedOnly => partition.last_stable_offset,
                };
                if end >= 0 {
                    cursor.end_offset = cursor.end_offset.max(end).min(end.max(cursor.end_offset));
                }

                // A partition whose start has been deleted out from under us.
                if partition.log_start_offset > cursor.next_offset {
                    cursor.next_offset = partition.log_start_offset;
                }

                let decoded = decode_partition(
                    &self.spec.topic,
                    partition.partition,
                    partition.records,
                    &partition.aborted,
                    &self.spec.decode_options(),
                );

                if decoded.outcomes.is_empty() {
                    // No progress from this partition. Either it is caught up,
                    // or every record in the fetch was a control batch or an
                    // aborted transaction — in which case the offsets still
                    // advanced and we must not re-request them forever.
                    if decoded.control_batches_skipped > 0 || decoded.aborted_records_skipped > 0 {
                        cursor.next_offset = cursor.next_offset.saturating_add(1);
                    } else if !decoded.truncated_tail {
                        // The broker had nothing for us right now. For a
                        // browse that is the end of the partition; for a tail
                        // it is an idle moment, and latching `finished` here
                        // would retire the partition the first time nobody
                        // wrote to it.
                        cursor.finished = !self.spec.follow;
                    } else {
                        // A single batch larger than the per-partition budget.
                        // Growing the budget for this one fetch is the only way
                        // past it; without this the scan stalls forever on a
                        // record it can never fit.
                        return Err(Error::Unsupported(format!(
                            "{}-{} has a batch larger than partition_max_bytes ({} bytes); \
                             raise ScanSpec::partition_max_bytes",
                            self.spec.topic, partition.partition, self.spec.partition_max_bytes
                        )));
                    }
                    continue;
                }

                read_anything = true;
                // `last_offset`, not `offset`: a malformed *batch* covers every
                // offset from its base to its last, so resuming from base + 1
                // lands back inside the same batch. The broker returns the
                // batch containing that offset, it fails to decode again, and
                // the scan re-reads it forever — emitting a Malformed event
                // each time, which reads as a very slow scan rather than a
                // stuck one.
                let last = decoded
                    .outcomes
                    .last()
                    .map(RecordOutcome::last_offset)
                    .unwrap_or(cursor.next_offset);
                // And never move backwards or stand still. When a batch header
                // is too damaged to report its last offset there is nothing to
                // compute a safe skip from, so forward progress has to be
                // asserted rather than derived.
                cursor.next_offset = last
                    .saturating_add(1)
                    .max(cursor.next_offset.saturating_add(1));

                let bad = decoded
                    .outcomes
                    .iter()
                    .filter(|outcome| outcome.is_malformed())
                    .count();
                self.progress.malformed_batches += u64::try_from(bad).unwrap_or(0);

                // A fetch begins at whatever *batch* contains the requested
                // offset, so the first one routinely hands back records from
                // before the scan's start. They are dropped rather than
                // emitted — "scan from offset N" that answers with N-59 is
                // wrong, and the backward walk has always filtered here.
                //
                // A malformed batch is kept whatever its base: the batch
                // containing the start offset covers it, and its whole reason
                // for existing is to say where the damage is.
                let start = cursor.start_offset;
                let kept: Vec<RecordOutcome> = decoded
                    .outcomes
                    .into_iter()
                    .filter(|outcome| match outcome {
                        RecordOutcome::Ok(record) => record.offset >= start,
                        RecordOutcome::Malformed { .. } => true,
                    })
                    .collect();

                self.buffered_total += kept.len();
                cursor.buffered.extend(kept);
            }
        }
        Ok(read_anything)
    }

    /// Emit the earliest buffered record, if it is safe to do so.
    fn emit_next(&mut self) -> Option<ScanEvent> {
        // Ordering is only exact while every unfinished partition is
        // represented in the buffer. When it is not, we either wait (by
        // returning None so the caller fetches) or — at the memory ceiling —
        // emit anyway and say so.
        let waiting_on_empty_partition = self
            .cursors
            .iter()
            .any(|cursor| !cursor.exhausted() && cursor.buffered.is_empty());
        let at_ceiling = self.buffered_total >= self.spec.max_buffered_records;
        if waiting_on_empty_partition && !at_ceiling {
            return None;
        }
        if waiting_on_empty_partition && at_ceiling {
            self.note_ordering_degraded();
        }

        let choice = self
            .cursors
            .iter()
            .enumerate()
            .filter_map(|(index, cursor)| cursor.head_timestamp().map(|ts| (index, ts)))
            .min_by_key(|(_, timestamp)| *timestamp)
            .map(|(index, _)| index)?;

        let (partition, outcome, exhausted, next_offset) = {
            let cursor = self.cursors.get_mut(choice)?;
            let outcome = cursor.buffered.pop_front()?;
            (
                cursor.partition,
                outcome,
                cursor.exhausted() && cursor.buffered.is_empty(),
                cursor.next_offset,
            )
        };
        self.buffered_total = self.buffered_total.saturating_sub(1);
        self.progress.records_scanned += 1;
        self.progress.offsets_consumed += 1;

        // Not while following: a partition draining to its head is a tail
        // catching up, and reporting it complete every time the buffer empties
        // would emit the event once per record on a quiet topic.
        if exhausted && !self.spec.follow {
            self.progress.partitions_active = self
                .cursors
                .iter()
                .filter(|c| !(c.exhausted() && c.buffered.is_empty()))
                .count();
            self.pending.push_back(ScanEvent::PartitionComplete {
                partition,
                last_offset: next_offset.saturating_sub(1),
            });
        }

        if self.progress.records_scanned >= self.next_progress_at {
            self.next_progress_at += self.progress_every;
            self.pending
                .push_back(ScanEvent::Progress(self.progress.clone()));
        }

        match outcome {
            RecordOutcome::Ok(record) => {
                if self
                    .spec
                    .filter
                    .as_ref()
                    .is_some_and(|filter| !filter.matches(&record))
                {
                    // Filtered out: not an event, but the scan still advanced.
                    return self.pending.pop_front();
                }
                self.progress.records_emitted += 1;
                Some(ScanEvent::Record(record))
            }
            RecordOutcome::Malformed {
                offset,
                last_offset,
                raw,
                reason,
            } => Some(ScanEvent::Malformed {
                topic: self.spec.topic.clone(),
                partition,
                offset,
                last_offset,
                raw,
                reason,
            }),
        }
    }

    /// Record that the buffer ceiling forced an emit before every partition
    /// was represented — but only when there is a merge to degrade.
    ///
    /// Within one partition the order is exact, always, so a single-partition
    /// scan that is simply big must not raise a caveat about a guarantee that
    /// still holds. When it did happen, what a caller wants to render is the
    /// magnitude — "records may be up to N apart" — and N is the buffer
    /// budget spread over the merge's width, both of which are this scan's
    /// own numbers. The widest window ever reached is kept: the caveat
    /// describes the whole scan, not the moment the last record was emitted.
    fn note_ordering_degraded(&mut self) {
        let merging = self
            .cursors
            .iter()
            .filter(|c| !(c.exhausted() && c.buffered.is_empty()))
            .count();
        let window = reorder_window(self.spec.max_buffered_records, merging);
        self.progress.reorder_window = self.progress.reorder_window.max(window);
    }

    fn finish(&mut self) -> ScanEvent {
        self.done = true;
        self.progress.partitions_active = 0;
        ScanEvent::Done(self.progress.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(offset: i64, key: &str, value: Option<&str>) -> Record {
        Record {
            topic: "orders".to_owned(),
            partition: 0,
            offset,
            timestamp: offset,
            timestamp_type: crate::record::TimestampType::Creation,
            key: Some(Bytes::from(key.to_owned())),
            value: value.map(|v| Bytes::from(v.to_owned())),
            headers: vec![("trace".to_owned(), None)],
            producer_id: None,
            transactional: false,
            leader_epoch: None,
        }
    }

    #[test]
    fn filters_look_inside_keys_values_and_headers() {
        let record = record(0, "order-42", Some("hello world"));
        assert!(RecordFilter::KeyContains(Bytes::from_static(b"42")).matches(&record));
        assert!(!RecordFilter::KeyContains(Bytes::from_static(b"99")).matches(&record));
        assert!(RecordFilter::ValueContains(Bytes::from_static(b"world")).matches(&record));
        assert!(RecordFilter::HasHeader("trace".to_owned()).matches(&record));
        assert!(!RecordFilter::HasHeader("span".to_owned()).matches(&record));
        assert!(!RecordFilter::TombstonesOnly.matches(&record));
        assert!(RecordFilter::TombstonesOnly.matches(&record_tombstone()));
    }

    fn record_tombstone() -> Record {
        record(1, "order-1", None)
    }

    #[test]
    fn an_empty_needle_matches_anything_with_that_field() {
        let record = record(0, "k", Some("v"));
        assert!(RecordFilter::KeyContains(Bytes::new()).matches(&record));
        // ... but a tombstone has no value to search.
        assert!(!RecordFilter::ValueContains(Bytes::new()).matches(&record_tombstone()));
    }

    #[test]
    fn a_custom_filter_sees_the_whole_record() {
        let filter = RecordFilter::Custom(Arc::new(|record: &Record| record.offset % 2 == 0));
        assert!(filter.matches(&record(0, "k", Some("v"))));
        assert!(!filter.matches(&record(1, "k", Some("v"))));
        // And it renders without leaking a closure address into a log line.
        assert_eq!(format!("{filter:?}"), "Custom(..)");
    }

    #[test]
    fn progress_is_a_fraction_only_once_the_total_is_known() {
        let mut progress = ScanProgress {
            records_emitted: 0,
            records_scanned: 0,
            malformed_batches: 0,
            offsets_consumed: 0,
            offsets_total: 0,
            partitions_active: 1,
            partitions_planned: 1,
            reorder_window: 0,
        };
        assert_eq!(progress.fraction(), None);

        progress.offsets_total = 100;
        progress.offsets_consumed = 25;
        assert_eq!(progress.fraction(), Some(0.25));

        // Compaction means consumed can exceed the estimate; a progress bar
        // past 100% is worse than one that stops there.
        progress.offsets_consumed = 500;
        assert_eq!(progress.fraction(), Some(1.0));
    }

    #[test]
    fn a_malformed_batch_sorts_first_so_it_stays_where_it_happened() {
        let mut cursor = PartitionCursor {
            partition: 0,
            leader: 1,
            next_offset: 0,
            end_offset: 10,
            start_offset: 0,
            substituted: None,
            buffered: VecDeque::new(),
            finished: false,
        };
        assert_eq!(cursor.head_timestamp(), None);

        cursor.buffered.push_back(RecordOutcome::Malformed {
            offset: 5,
            last_offset: None,
            raw: Bytes::new(),
            reason: DecodeError::new("bad"),
        });
        assert_eq!(cursor.head_timestamp(), Some(i64::MIN));
    }

    #[test]
    fn a_cursor_at_its_end_offset_is_exhausted() {
        let cursor = PartitionCursor {
            partition: 0,
            leader: 1,
            next_offset: 10,
            end_offset: 10,
            start_offset: 0,
            substituted: None,
            buffered: VecDeque::new(),
            finished: false,
        };
        assert!(cursor.exhausted());
    }

    #[test]
    fn a_scan_browses_unless_it_is_told_to_follow() {
        // Additive: every caller written before `following` existed keeps
        // getting a bounded browse that ends at the log end it planned for.
        assert!(!ScanSpec::new("orders").follow);
        assert!(ScanSpec::new("orders").following().follow);
    }

    #[test]
    fn a_browse_only_ever_fetches_for_a_partition_that_is_behind() {
        // Unchanged behaviour, asserted so the follow rules cannot quietly
        // widen it: a bounded scan never polls a partition it has read to the
        // end, whatever else is going on.
        assert!(should_fetch(true, false, true));
        assert!(should_fetch(true, false, false));
        assert!(!should_fetch(false, false, true));
        assert!(!should_fetch(false, false, false));
    }

    #[test]
    fn a_tail_polls_the_log_end_only_when_it_has_nothing_left_to_emit() {
        // Both halves matter. Never polling means the tail never sees a new
        // record; always polling means every record costs a long-poll on every
        // idle partition.
        assert!(
            should_fetch(false, true, true),
            "a tail must poll when idle"
        );
        assert!(
            !should_fetch(false, true, false),
            "a tail with records in hand must emit them first"
        );
    }

    #[test]
    fn a_single_partition_merge_has_no_order_to_degrade() {
        // Within one partition the order is exact, always — a scan that is
        // simply bigger than its buffer must not raise a caveat about a
        // guarantee that still holds. This used to fire from the ceiling
        // alone, and kaas-ui carried a suppression (and a test for it) to
        // avoid rendering "approximately ordered" over an exactly-ordered
        // list.
        assert_eq!(reorder_window(10_000, 1), 0);
        assert_eq!(reorder_window(10_000, 0), 0);
    }

    #[test]
    fn the_reorder_window_is_the_budget_spread_over_the_merge() {
        // "Records may be up to N apart", where N is the scan's own numbers —
        // not something every caller reconstructs from the spec it handed in.
        assert_eq!(reorder_window(10_000, 16), 625);
        assert_eq!(reorder_window(10_000, 2), 5_000);
    }

    #[test]
    fn an_in_range_offset_start_is_honoured_and_says_nothing() {
        assert_eq!(resolve_offset_start(500, 100, 1_000), (500, None));
        // The bounds themselves are in range.
        assert_eq!(resolve_offset_start(100, 100, 1_000), (100, None));
        assert_eq!(resolve_offset_start(1_000, 100, 1_000), (1_000, None));
    }

    #[test]
    fn an_expired_offset_start_names_the_substitution() {
        // "Did my record land at 43" answered from offset 12_000 looks like
        // it worked; the substitution is performed for the browse case but
        // must be reported.
        let (start, substituted) = resolve_offset_start(43, 12_000, 16_733);
        assert_eq!(start, 12_000);
        assert_eq!(
            substituted,
            Some(StartSubstitution::OffsetBelowLogStart {
                requested: 43,
                log_start: 12_000,
            })
        );
    }

    #[test]
    fn an_unreached_offset_start_names_the_substitution() {
        let (start, substituted) = resolve_offset_start(900_001, 0, 200);
        assert_eq!(start, 200);
        assert_eq!(
            substituted,
            Some(StartSubstitution::OffsetBeyondLogEnd {
                requested: 900_001,
                log_end: 200,
            })
        );
    }

    #[test]
    fn the_default_spec_bounds_its_own_memory() {
        let spec = ScanSpec::new("orders");
        assert!(spec.max_buffered_records > 0);
        assert!(spec.max_decompressed_bytes > 0);
        assert_eq!(spec.visibility, Visibility::All);
        assert!(spec.partitions.is_none());
    }
}
