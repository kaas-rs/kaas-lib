//! Batching: many records into one request, with bounded memory.
//!
//! # Why an actor rather than a shared lock
//!
//! Every piece of batching state — the open batch per partition, the closed
//! ones queued behind it, and which partitions have a request on the wire —
//! lives in one task and is touched by nothing else. Callers reach it through a
//! channel. That is what makes rule 5 tractable: dropping a [`crate::Producer`]
//! send future drops a `oneshot::Receiver` and nothing more, so a cancelled
//! caller cannot leave a half-updated batch behind for the next one to trip
//! over. The record it already enqueued is still produced; only the result is
//! discarded.
//!
//! # Ordering, and the in-flight rule that protects it
//!
//! **At most one batch per partition is on the wire at a time.** Different
//! partitions proceed concurrently — ordering is a per-partition property — but
//! within one partition the next batch is not sent until the previous one has
//! been answered.
//!
//! This is the clamp PLAN.md M14 describes, landed here because M13 is what
//! introduces retry. `ConnectionConfig::max_in_flight` defaults to 5, and the
//! moment a rejected batch is re-sent while a later batch for the same
//! partition is already in flight, the log's order stops matching the caller's
//! — with no error and no log line. Doing it per *partition* rather than per
//! connection keeps the ordering guarantee while still letting six partitions
//! on one broker fill six batches concurrently.
//!
//! M14 relaxes this to the broker's five in-flight sequence windows once
//! records carry sequence numbers that let the broker restore order itself.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use kafka_conn::{Error, ErrorCode, Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::Instant;

use crate::config::ProducerConfig;
use crate::dispatch::{Dispatcher, Outbound};
use crate::encode::encode_batch;
use crate::record::{ProducerRecord, RecordMetadata};

/// How many commands may queue before `send` waits.
///
/// Backpressure proper is the memory semaphore, which counts bytes; this only
/// stops an unbounded number of *notifications* piling up in front of an actor
/// that is busy.
const COMMAND_QUEUE: usize = 1024;

/// Bytes of v2 record framing to charge a record on top of its own payload.
///
/// The batch header (61 bytes) amortises across the batch; this is the
/// per-record part: length varint, attributes, timestamp and offset deltas, the
/// key and value length varints, and the header count. It is an estimate used
/// for accounting only — the encoder decides the real size — and it errs high
/// so a `batch.size` is never overshot by the estimate being optimistic.
const RECORD_OVERHEAD: usize = 32;

/// Per-header accounting overhead: two length varints.
const HEADER_OVERHEAD: usize = 8;

/// A record waiting for the wire, with the caller's delivery channel.
#[derive(Debug)]
struct Queued {
    record: ProducerRecord,
    respond: oneshot::Sender<Result<RecordMetadata>>,
    /// Released when this record is resolved, which is what makes the buffer
    /// bound real rather than advisory.
    _permit: OwnedSemaphorePermit,
}

impl Queued {
    /// Hand the caller their result. A dropped receiver is not an error — it is
    /// a caller who stopped caring, which cancel safety entitles them to do.
    fn resolve(self, outcome: Result<RecordMetadata>) {
        let _ = self.respond.send(outcome);
    }
}

/// One partition's batch, open or closed.
#[derive(Debug)]
struct Batch {
    records: Vec<Queued>,
    bytes: usize,
    opened_at: Instant,
}

impl Batch {
    fn new(now: Instant) -> Self {
        Self {
            records: Vec::new(),
            bytes: 0,
            opened_at: now,
        }
    }
}

/// What the actor knows about one partition.
#[derive(Debug, Default)]
struct PartitionState {
    /// The batch currently accepting records.
    open: Option<Batch>,
    /// Batches closed and waiting for the wire, oldest first.
    pending: VecDeque<Batch>,
    /// Whether a request carrying this partition is on the wire.
    in_flight: bool,
}

impl PartitionState {
    fn idle(&self) -> bool {
        self.open.is_none() && self.pending.is_empty() && !self.in_flight
    }
}

/// A batch handed to a send task.
#[derive(Debug)]
struct Ready {
    topic: String,
    partition: i32,
    records: Vec<Queued>,
}

#[derive(Debug)]
enum Command {
    Append {
        topic: String,
        partition: i32,
        queued: Queued,
    },
    Flush(oneshot::Sender<()>),
}

/// Partitions a send task has finished with.
#[derive(Debug)]
struct Completed {
    partitions: Vec<(String, i32)>,
}

/// The caller-side handle to the accumulator task.
#[derive(Debug)]
pub(crate) struct Accumulator {
    commands: mpsc::Sender<Command>,
    memory: Arc<Semaphore>,
    /// The largest record this producer will accept, in accounted bytes.
    record_limit: usize,
}

impl Accumulator {
    /// Spawn the accumulator task and return a handle to it.
    pub(crate) fn spawn(dispatcher: Dispatcher, config: ProducerConfig) -> Self {
        let (commands, command_rx) = mpsc::channel(COMMAND_QUEUE);
        let memory = Arc::new(Semaphore::new(config.buffer_memory_permits()));

        // A record can never be delivered if it exceeds the whole buffer, so
        // the limit is the smaller of the two bounds. Without this, a record
        // between `buffer_memory` and `max_request_size` would wait on a
        // permit that can never be granted, which presents as a hang rather
        // than as the error it is.
        let record_limit = config.max_request_size.min(config.buffer_memory);

        let actor = Actor::new(dispatcher, config, Arc::clone(&memory));
        tokio::spawn(actor.run(command_rx));

        Self {
            commands,
            memory,
            record_limit,
        }
    }

    /// Enqueue a record, and hand back the channel its result arrives on.
    ///
    /// Waits when the buffer is full: that wait *is* the backpressure, and it
    /// is what stops a slow broker from turning into an OOM.
    pub(crate) async fn append(
        &self,
        topic: String,
        partition: i32,
        record: ProducerRecord,
    ) -> Result<oneshot::Receiver<Result<RecordMetadata>>> {
        let size = accounted_size(&record);
        if size > self.record_limit {
            // Rule 4 in the write direction, at the earliest point it can be
            // applied: this record fails, and the ones batched beside it are
            // never touched because it never joins them.
            return Err(Error::from_code(
                ErrorCode::MessageTooLarge,
                Some(format!(
                    "{topic}: the record is {size} bytes, over the {} byte limit",
                    self.record_limit
                )),
            ));
        }

        let permit = Arc::clone(&self.memory)
            .acquire_many_owned(permits_for(size))
            .await
            .map_err(|_| producer_gone())?;

        let (respond, receiver) = oneshot::channel();
        self.commands
            .send(Command::Append {
                topic,
                partition,
                queued: Queued {
                    record,
                    respond,
                    _permit: permit,
                },
            })
            .await
            .map_err(|_| producer_gone())?;

        Ok(receiver)
    }

    /// Send every buffered record and wait until all of them are resolved.
    pub(crate) async fn flush(&self) -> Result<()> {
        let (respond, receiver) = oneshot::channel();
        self.commands
            .send(Command::Flush(respond))
            .await
            .map_err(|_| producer_gone())?;
        receiver.await.map_err(|_| producer_gone())
    }
}

/// The task that owns every batch.
struct Actor {
    dispatcher: Dispatcher,
    config: ProducerConfig,
    memory: Arc<Semaphore>,
    partitions: HashMap<(String, i32), PartitionState>,
    flush_waiters: Vec<oneshot::Sender<()>>,
    completions_tx: mpsc::Sender<Completed>,
    completions_rx: mpsc::Receiver<Completed>,
}

impl Actor {
    fn new(dispatcher: Dispatcher, config: ProducerConfig, memory: Arc<Semaphore>) -> Self {
        let (completions_tx, completions_rx) = mpsc::channel(COMMAND_QUEUE);
        Self {
            dispatcher,
            config,
            memory,
            partitions: HashMap::new(),
            flush_waiters: Vec::new(),
            completions_tx,
            completions_rx,
        }
    }

    async fn run(mut self, mut commands: mpsc::Receiver<Command>) {
        let mut closing = false;

        loop {
            let deadline = self.next_deadline();

            tokio::select! {
                command = commands.recv(), if !closing => match command {
                    Some(command) => self.handle(command),
                    // Every handle is gone. Drain what is buffered rather than
                    // dropping it: the records were accepted, and a caller who
                    // dropped the producer without flushing still expects the
                    // writes it was told were accepted to be attempted.
                    None => {
                        closing = true;
                        self.close_all();
                    }
                },
                completed = self.completions_rx.recv() => {
                    if let Some(completed) = completed {
                        for key in completed.partitions {
                            if let Some(state) = self.partitions.get_mut(&key) {
                                state.in_flight = false;
                            }
                        }
                    }
                }
                () = sleep_until(deadline) => self.close_expired(),
            }

            self.dispatch_ready();
            self.settle_flushes();
            self.forget_idle();

            if closing && self.is_idle() {
                break;
            }
        }
    }

    fn handle(&mut self, command: Command) {
        match command {
            Command::Append {
                topic,
                partition,
                queued,
            } => self.append(topic, partition, queued),
            Command::Flush(respond) => {
                // Close everything open so the flush does not wait out a linger
                // that has nothing left to gain by expiring.
                self.close_all();
                self.flush_waiters.push(respond);
            }
        }
    }

    fn append(&mut self, topic: String, partition: i32, queued: Queued) {
        let size = accounted_size(&queued.record);
        let batch_size = self.config.batch_size;
        let max_request = self.config.max_request_size;
        let now = Instant::now();

        let state = self
            .partitions
            .entry((topic, partition))
            .or_default();

        // Close the open batch when this record would push it past either
        // bound. A record that is itself over `batch_size` then lands in a
        // batch of its own, which is the only way it can be sent at all.
        let would_overflow = state.open.as_ref().is_some_and(|open| {
            let would_be = open.bytes.saturating_add(size);
            !open.records.is_empty() && (would_be > batch_size || would_be > max_request)
        });
        if would_overflow && let Some(full) = state.open.take() {
            state.pending.push_back(full);
        }

        let open = state.open.get_or_insert_with(|| Batch::new(now));
        open.bytes = open.bytes.saturating_add(size);
        open.records.push(queued);

        // A batch that has reached the target does not wait for its linger.
        let reached_target = open.bytes >= batch_size;
        if reached_target && let Some(full) = state.open.take() {
            state.pending.push_back(full);
        }
    }

    /// The earliest moment an open batch's linger expires.
    fn next_deadline(&self) -> Option<Instant> {
        let linger = self.config.linger;
        self.partitions
            .values()
            .filter_map(|state| state.open.as_ref())
            .map(|open| open.opened_at + linger)
            .min()
    }

    fn close_expired(&mut self) {
        let linger = self.config.linger;
        let now = Instant::now();
        for state in self.partitions.values_mut() {
            let expired = state
                .open
                .as_ref()
                .is_some_and(|open| open.opened_at + linger <= now);
            if expired && let Some(batch) = state.open.take() {
                state.pending.push_back(batch);
            }
        }
    }

    fn close_all(&mut self) {
        for state in self.partitions.values_mut() {
            if let Some(batch) = state.open.take() {
                state.pending.push_back(batch);
            }
        }
    }

    /// Hand every partition that is free and has work to a send task.
    ///
    /// Grouped by the leader the current snapshot names, so partitions sharing
    /// a broker travel in one request. The snapshot may be stale; the
    /// dispatcher re-resolves each partition itself, so a wrong guess here
    /// costs an extra request rather than a wrong answer.
    fn dispatch_ready(&mut self) {
        let snapshot = self.dispatcher.cluster().snapshot();
        let mut groups: HashMap<Option<i32>, Vec<Ready>> = HashMap::new();

        for ((topic, partition), state) in self.partitions.iter_mut() {
            if state.in_flight {
                continue;
            }
            let Some(batch) = state.pending.pop_front() else {
                continue;
            };
            state.in_flight = true;

            let leader = snapshot
                .topic(topic)
                .and_then(|info| info.partition(*partition))
                .and_then(|info| info.leader);

            groups.entry(leader).or_default().push(Ready {
                topic: topic.clone(),
                partition: *partition,
                records: batch.records,
            });
        }

        for (_, group) in groups {
            let dispatcher = self.dispatcher.clone();
            let compression = self.config.compression;
            let completions = self.completions_tx.clone();
            tokio::spawn(async move {
                let partitions = send_group(dispatcher, compression, group).await;
                let _ = completions.send(Completed { partitions }).await;
            });
        }
    }

    fn settle_flushes(&mut self) {
        if self.flush_waiters.is_empty() || !self.is_idle() {
            return;
        }
        for waiter in self.flush_waiters.drain(..) {
            let _ = waiter.send(());
        }
    }

    /// Drop the bookkeeping for partitions with nothing outstanding, so a
    /// long-lived producer over many topics does not grow a map entry per
    /// partition it has ever touched.
    fn forget_idle(&mut self) {
        self.partitions.retain(|_, state| !state.idle());
    }

    fn is_idle(&self) -> bool {
        self.partitions.values().all(PartitionState::idle)
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        // Nothing should reach here with records still buffered — `run` only
        // breaks when idle — but a task that is cancelled outright can. Close
        // the semaphore so any caller waiting on a permit gets an error instead
        // of waiting for an actor that no longer exists.
        self.memory.close();
    }
}

/// Encode a group of batches, send them, and resolve every record.
///
/// Returns the partitions it was responsible for, so the actor can let their
/// next batch go.
async fn send_group(
    dispatcher: Dispatcher,
    compression: crate::config::Compression,
    group: Vec<Ready>,
) -> Vec<(String, i32)> {
    let now = now_millis();
    let mut partitions = Vec::with_capacity(group.len());
    let mut outbound = Vec::with_capacity(group.len());
    let mut records: HashMap<(String, i32), Vec<Queued>> = HashMap::new();

    for ready in group {
        let key = (ready.topic.clone(), ready.partition);
        partitions.push(key.clone());

        match encode_batch(
            &ready
                .records
                .iter()
                .map(|queued| queued.record.clone())
                .collect::<Vec<_>>(),
            compression,
            now,
        ) {
            Ok(encoded) => {
                outbound.push(Outbound {
                    topic: ready.topic,
                    partition: ready.partition,
                    encoded,
                });
                records.insert(key, ready.records);
            }
            Err(error) => {
                // An unencodable batch fails its own records and no others.
                for queued in ready.records {
                    queued.resolve(Err(error.clone()));
                }
            }
        }
    }

    if outbound.is_empty() {
        return partitions;
    }

    for (key, outcome) in dispatcher.dispatch(outbound).await {
        let Some(queued) = records.remove(&key) else {
            continue;
        };
        let (topic, partition) = key;
        match outcome {
            Ok(ack) => {
                for (index, record) in queued.into_iter().enumerate() {
                    let offset = i64::try_from(index)
                        .map(|index| ack.base_offset.saturating_add(index))
                        .unwrap_or(ack.base_offset);
                    record.resolve(Ok(RecordMetadata {
                        topic: topic.clone(),
                        partition,
                        offset,
                        timestamp: ack.log_append_time_ms,
                    }));
                }
            }
            Err(error) => {
                for record in queued {
                    record.resolve(Err(error.clone()));
                }
            }
        }
    }

    // Anything the dispatcher did not answer for — it always answers for every
    // batch it was given, so this is defence rather than an expected path.
    for (_, queued) in records {
        for record in queued {
            record.resolve(Err(producer_gone()));
        }
    }

    partitions
}

/// Wait until `deadline`, or forever when there is nothing to wait for.
async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// What one record costs the buffer.
///
/// An estimate, deliberately on the high side: the accounting decides when a
/// batch closes and when a caller waits, and over-charging costs a slightly
/// smaller batch while under-charging costs an oversized request the broker
/// rejects.
fn accounted_size(record: &ProducerRecord) -> usize {
    let key = record.key.as_ref().map_or(0, bytes::Bytes::len);
    let value = record.value.as_ref().map_or(0, bytes::Bytes::len);
    let headers: usize = record
        .headers
        .iter()
        .map(|(name, value)| {
            name.len()
                .saturating_add(value.as_ref().map_or(0, bytes::Bytes::len))
                .saturating_add(HEADER_OVERHEAD)
        })
        .sum();

    RECORD_OVERHEAD
        .saturating_add(key)
        .saturating_add(value)
        .saturating_add(headers)
}

/// Semaphore permits are counted in `u32`; a size past that is clamped rather
/// than wrapped, which would otherwise charge a huge record almost nothing.
fn permits_for(size: usize) -> u32 {
    u32::try_from(size).unwrap_or(u32::MAX)
}

/// Wall clock in the milliseconds a record timestamp wants.
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since| i64::try_from(since.as_millis()).ok())
        .unwrap_or(0)
}

/// Every producer clone was dropped, or its task stopped, with the record still
/// outstanding.
///
/// [`Error::ConnectionClosed`] rather than a new variant because it is the same
/// situation from the caller's side — the channel the answer was going to
/// arrive on is gone, and whether the record was written is unknowable.
pub(crate) fn producer_gone() -> Error {
    Error::ConnectionClosed {
        peer: "the producer".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn record(value_len: usize) -> ProducerRecord {
        ProducerRecord::new("t").value(Bytes::from(vec![b'x'; value_len]))
    }

    #[test]
    fn accounting_charges_payload_plus_framing() {
        let empty = accounted_size(&ProducerRecord::new("t"));
        assert_eq!(empty, RECORD_OVERHEAD);

        let with_value = accounted_size(&record(100));
        assert_eq!(with_value, RECORD_OVERHEAD + 100);

        let with_header = accounted_size(&ProducerRecord::new("t").header("k", "vv"));
        assert_eq!(with_header, RECORD_OVERHEAD + 1 + 2 + HEADER_OVERHEAD);
    }

    /// A tombstone's null value is not charged as if it were data, and a null
    /// header value is charged only for its name.
    #[test]
    fn a_null_value_costs_nothing_to_buffer() {
        let tombstone = accounted_size(&ProducerRecord::new("t").key("k"));
        assert_eq!(tombstone, RECORD_OVERHEAD + 1);

        let null_header = accounted_size(&ProducerRecord::new("t").null_header("h"));
        assert_eq!(null_header, RECORD_OVERHEAD + 1 + HEADER_OVERHEAD);
    }

    /// The permit count must not wrap: a record accounted above `u32::MAX`
    /// would otherwise take a tiny number of permits and defeat the bound.
    #[test]
    fn permits_saturate_rather_than_wrapping() {
        assert_eq!(permits_for(0), 0);
        assert_eq!(permits_for(4096), 4096);
        assert_eq!(permits_for(usize::MAX), u32::MAX);
    }

    #[test]
    fn a_batch_closes_when_it_reaches_the_target_size() {
        let mut state = PartitionState::default();
        let now = Instant::now();
        let mut batch = Batch::new(now);
        batch.bytes = 900;
        state.open = Some(batch);
        assert!(state.pending.is_empty());
        assert!(!state.idle(), "an open batch is outstanding work");
    }

    #[test]
    fn a_partition_with_nothing_outstanding_is_idle() {
        let state = PartitionState::default();
        assert!(state.idle());

        let busy = PartitionState {
            in_flight: true,
            ..PartitionState::default()
        };
        assert!(!busy.idle(), "a request on the wire is outstanding work");
    }
}
