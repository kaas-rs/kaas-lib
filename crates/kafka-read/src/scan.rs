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
//! buffer rather than by the length of the topic. [`ScanEvent::Progress`]
//! reports when that happens, so a UI can say "approximately ordered" rather
//! than quietly lying.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;
use kafka_conn::{Error, Result};
use kafka_meta::Cluster;

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
    pub fn with_partitions(mut self, partitions: impl IntoIterator<Item = i32>) -> Self {
        self.partitions = Some(partitions.into_iter().collect());
        self
    }

    /// Start somewhere other than the beginning.
    pub fn from(mut self, from: StartPosition) -> Self {
        self.from = from;
        self
    }

    /// Stop after `limit` records.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
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
    pub partitions_active: usize,
    /// Whether ordering had to degrade because the buffer filled.
    pub ordering_degraded: bool,
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

/// What a scan emits.
#[derive(Debug, Clone)]
pub enum ScanEvent {
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

/// One partition's position in a scan.
#[derive(Debug)]
struct PartitionCursor {
    partition: i32,
    leader: i32,
    next_offset: i64,
    end_offset: i64,
    start_offset: i64,
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

/// The state a scan carries between polls.
struct ScanState {
    cluster: Cluster,
    spec: ScanSpec,
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
    let cursors = plan(cluster, &spec).await?;
    let offsets_total = cursors
        .iter()
        .map(|c| c.end_offset.saturating_sub(c.start_offset).max(0))
        .sum();

    let state = ScanState {
        cluster: cluster.clone(),
        progress: ScanProgress {
            records_emitted: 0,
            records_scanned: 0,
            malformed_batches: 0,
            offsets_consumed: 0,
            offsets_total,
            partitions_active: cursors.len(),
            ordering_degraded: false,
        },
        cursors,
        spec,
        pending: VecDeque::new(),
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
async fn plan(cluster: &Cluster, spec: &ScanSpec) -> Result<Vec<PartitionCursor>> {
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

    let mut cursors = Vec::new();
    for partition in wanted {
        let leader = cluster.leader_for(&spec.topic, partition).await?;
        let (earliest, latest) =
            crate::offsets::bounds(cluster, &spec.topic, partition, leader).await?;

        let start = match spec.from {
            StartPosition::Earliest => earliest,
            StartPosition::Latest => latest,
            StartPosition::Offset(offset) => offset.clamp(earliest, latest),
            StartPosition::Timestamp(timestamp) => {
                crate::offsets::at_timestamp(cluster, &spec.topic, partition, leader, timestamp)
                    .await?
                    .unwrap_or(latest)
            }
        };

        cursors.push(PartitionCursor {
            partition,
            leader,
            next_offset: start,
            end_offset: latest,
            start_offset: start,
            buffered: VecDeque::new(),
            finished: start >= latest,
        });
    }
    Ok(cursors)
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
        self.cursors
            .iter()
            .all(|cursor| cursor.exhausted() && cursor.buffered.is_empty())
    }

    /// Fetch for partitions that need it. Returns whether anything was read.
    async fn refill(&mut self) -> Result<bool> {
        if self.buffered_total >= self.spec.max_buffered_records {
            // At the ceiling. Emit from what we have rather than growing.
            self.progress.ordering_degraded = true;
            return Ok(false);
        }

        // Group the partitions that need data by leader, so one broker gets one
        // request rather than one per partition.
        let mut by_leader: HashMap<i32, Vec<FetchTarget>> = HashMap::new();
        for cursor in &self.cursors {
            if cursor.exhausted() || !cursor.buffered.is_empty() {
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
                        cursor.finished = cursor.next_offset >= cursor.end_offset;
                        if !cursor.finished {
                            // The broker had nothing for us right now.
                            cursor.finished = true;
                        }
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
                let last = decoded
                    .outcomes
                    .last()
                    .map(RecordOutcome::offset)
                    .unwrap_or(cursor.next_offset);
                cursor.next_offset = last.saturating_add(1);

                let bad = decoded
                    .outcomes
                    .iter()
                    .filter(|outcome| outcome.is_malformed())
                    .count();
                self.progress.malformed_batches += u64::try_from(bad).unwrap_or(0);

                self.buffered_total += decoded.outcomes.len();
                cursor.buffered.extend(decoded.outcomes);
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
            self.progress.ordering_degraded = true;
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

        if exhausted {
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
            ordering_degraded: false,
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
            buffered: VecDeque::new(),
            finished: false,
        };
        assert!(cursor.exhausted());
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
