//! A manually-assigned consumer: an unbounded stream over assigned partitions.
//!
//! The shape is deliberately different from `kafka-read`'s `scan`. A scan is
//! bounded and reports progress, because a UI is drawing a page and wants a
//! progress bar rather than a spinner. A consumer runs until it is told to
//! stop, and its interesting operations — `seek`, `pause`, `resume` — are all
//! about *changing its mind mid-stream*, which a bounded scan never does.
//!
//! Both sit on the same fetcher and the same tolerant decoder. Neither is a
//! special case of the other.

use std::collections::{HashMap, HashSet};

use kafka_conn::{Error, Result};
use kafka_meta::{Cluster, TopicId};
use kafka_read::{DecodeOptions, Record, RecordOutcome, Visibility};

use crate::fetcher::{BrokerFetcher, Limits};
use crate::offsets::{self, CommittedOffset};
use crate::session::PartitionState;

/// How a [`Consumer`] behaves.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// How long a fetch may wait for `min_bytes` before answering empty.
    pub max_wait_ms: i32,
    /// Ceiling on one fetch response.
    pub max_bytes: i32,
    /// Ceiling on one partition's share of a fetch response.
    pub partition_max_bytes: i32,
    /// Whether records from aborted transactions are visible.
    pub visibility: Visibility,
    /// Ceiling on a single batch's decompressed size.
    pub max_decompressed_bytes: usize,
    /// The group whose committed offsets `commit` and `committed` use.
    ///
    /// A manually-assigned consumer is **not a member** of this group; it only
    /// borrows the group's offset storage. See [`crate::offsets`].
    pub group_id: Option<String>,
}

impl ConsumerConfig {
    /// Kafka's own defaults, where it has one.
    pub fn new() -> Self {
        Self {
            max_wait_ms: 500,
            max_bytes: 50 * 1024 * 1024,
            partition_max_bytes: 1024 * 1024,
            visibility: Visibility::CommittedOnly,
            max_decompressed_bytes: 64 * 1024 * 1024,
            group_id: None,
        }
    }

    /// Store offsets under this group id.
    #[must_use]
    pub fn group_id(mut self, id: impl Into<String>) -> Self {
        self.group_id = Some(id.into());
        self
    }

    /// Whether aborted-transaction records are visible.
    #[must_use]
    pub fn visibility(mut self, visibility: Visibility) -> Self {
        self.visibility = visibility;
        self
    }

    /// How long a fetch may wait before answering empty.
    #[must_use]
    pub fn max_wait_ms(mut self, ms: i32) -> Self {
        self.max_wait_ms = ms;
        self
    }
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Where to start reading a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
    /// The log's start.
    Earliest,
    /// The log's end: only records written from now on.
    Latest,
    /// An explicit offset.
    Offset(i64),
}

/// One assigned partition's state.
#[derive(Debug)]
struct Assigned {
    /// The offset of the next record to read.
    position: i64,
    /// Whether fetching is suspended for this partition.
    paused: bool,
    /// The last high watermark the broker reported.
    high_watermark: i64,
}

/// A consumer over an explicit set of partitions.
///
/// No group membership: the assignment is whatever the caller says it is, and
/// nothing rebalances it. Group membership is M17 and M18.
#[derive(Debug)]
pub struct Consumer {
    cluster: Cluster,
    config: ConsumerConfig,
    assignment: HashMap<(String, i32), Assigned>,
    topic_ids: HashMap<String, TopicId>,
    fetchers: HashMap<i32, BrokerFetcher>,
    /// Decoded records not yet handed to the caller, in log order.
    buffered: std::collections::VecDeque<Record>,
}

impl Consumer {
    /// Wrap an existing cluster handle.
    pub fn new(cluster: Cluster, config: ConsumerConfig) -> Self {
        Self {
            cluster,
            config,
            assignment: HashMap::new(),
            topic_ids: HashMap::new(),
            fetchers: HashMap::new(),
            buffered: std::collections::VecDeque::new(),
        }
    }

    /// Connect to a cluster and consume from it.
    pub async fn connect(
        bootstrap: impl IntoIterator<Item = impl Into<String>>,
        cluster_config: kafka_meta::ClusterConfig,
        config: ConsumerConfig,
    ) -> Result<Self> {
        Ok(Self::new(
            Cluster::connect(bootstrap, cluster_config).await?,
            config,
        ))
    }

    /// The underlying cluster handle.
    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    /// Assign an explicit set of partitions, replacing any previous
    /// assignment.
    ///
    /// Partitions that leave the assignment are forgotten in the next fetch,
    /// which is what stops the broker holding session state for partitions
    /// nobody is reading.
    pub async fn assign(
        &mut self,
        partitions: impl IntoIterator<Item = (String, i32)>,
        from: Position,
    ) -> Result<()> {
        let wanted: Vec<(String, i32)> = partitions.into_iter().collect();
        let topics: HashSet<String> = wanted.iter().map(|(topic, _)| topic.clone()).collect();

        let names: Vec<&str> = topics.iter().map(String::as_str).collect();
        let snapshot = self.cluster.refresh_topics(&names).await?;
        for topic in &topics {
            let info = snapshot.topic(topic).ok_or_else(|| {
                Error::from_code(
                    kafka_conn::ErrorCode::UnknownTopicOrPartition,
                    Some(topic.clone()),
                )
            })?;
            self.topic_ids.insert(topic.clone(), info.topic_id);
        }

        let starts = self.resolve(&wanted, from).await?;

        // Rebuild rather than merge: `assign` replaces, and a partition that
        // is re-assigned should keep the position we just resolved for it.
        self.assignment = wanted
            .into_iter()
            .map(|key| {
                let position = starts.get(&key).copied().unwrap_or(0);
                (
                    key,
                    Assigned {
                        position,
                        paused: false,
                        high_watermark: -1,
                    },
                )
            })
            .collect();
        self.buffered.clear();
        Ok(())
    }

    /// The current assignment.
    pub fn assignment(&self) -> Vec<(String, i32)> {
        let mut keys: Vec<(String, i32)> = self.assignment.keys().cloned().collect();
        keys.sort();
        keys
    }

    /// Move one partition's read position.
    ///
    /// Takes effect on the next fetch, and discards anything already buffered
    /// for that partition — a seek that still delivered the old records would
    /// not be a seek.
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        let key = (topic.to_owned(), partition);
        let assigned = self
            .assignment
            .get_mut(&key)
            .ok_or_else(|| Error::InvalidRequest(format!("{topic}-{partition} is not assigned")))?;
        assigned.position = offset;
        self.buffered
            .retain(|record| !(record.topic == topic && record.partition == partition));
        Ok(())
    }

    /// Stop fetching a partition without giving it up.
    ///
    /// The partition stays in the assignment and keeps its position, so
    /// `resume` continues from where it stopped rather than re-resolving.
    pub fn pause(&mut self, topic: &str, partition: i32) {
        if let Some(assigned) = self.assignment.get_mut(&(topic.to_owned(), partition)) {
            assigned.paused = true;
        }
    }

    /// Start fetching a paused partition again.
    pub fn resume(&mut self, topic: &str, partition: i32) {
        if let Some(assigned) = self.assignment.get_mut(&(topic.to_owned(), partition)) {
            assigned.paused = false;
        }
    }

    /// Whether a partition is paused.
    pub fn is_paused(&self, topic: &str, partition: i32) -> bool {
        self.assignment
            .get(&(topic.to_owned(), partition))
            .is_some_and(|assigned| assigned.paused)
    }

    /// The offset of the next record this consumer will read.
    pub fn position(&self, topic: &str, partition: i32) -> Option<i64> {
        self.assignment
            .get(&(topic.to_owned(), partition))
            .map(|assigned| assigned.position)
    }

    /// How far this consumer is behind the log end, per partition.
    ///
    /// `None` until a fetch has reported a high watermark for it.
    pub fn lag(&self, topic: &str, partition: i32) -> Option<i64> {
        self.assignment
            .get(&(topic.to_owned(), partition))
            .filter(|assigned| assigned.high_watermark >= 0)
            .map(|assigned| (assigned.high_watermark - assigned.position).max(0))
    }

    /// Read the next batch of records, fetching if nothing is buffered.
    ///
    /// Returns an empty vector when the fetch timed out with nothing new,
    /// which is a normal outcome and not an error: a consumer at the log end
    /// is caught up, not broken.
    ///
    /// # Cancel safety
    ///
    /// Dropping this future may discard a fetch that was in flight. It never
    /// advances a position for records the caller did not receive, so the
    /// worst case is re-fetching the same records.
    pub async fn poll(&mut self) -> Result<Vec<Record>> {
        if let Some(ready) = self.take_buffered() {
            return Ok(ready);
        }
        self.fetch_once().await?;
        Ok(self.take_buffered().unwrap_or_default())
    }

    fn take_buffered(&mut self) -> Option<Vec<Record>> {
        if self.buffered.is_empty() {
            return None;
        }
        Some(self.buffered.drain(..).collect())
    }

    /// One fetch round: group the active assignment by leader and ask each
    /// broker once.
    async fn fetch_once(&mut self) -> Result<()> {
        let mut by_leader: HashMap<i32, HashMap<(String, i32), PartitionState>> = HashMap::new();

        for (key, assigned) in &self.assignment {
            if assigned.paused {
                continue;
            }
            let leader = match self.cluster.leader_for(&key.0, key.1).await {
                Ok(leader) => leader,
                Err(error) => {
                    // One partition without a leader must not stop the other
                    // eleven; the next round re-resolves it.
                    tracing::debug!(topic = %key.0, partition = key.1, %error, "no leader yet");
                    continue;
                }
            };
            by_leader.entry(leader).or_default().insert(
                key.clone(),
                PartitionState {
                    offset: assigned.position,
                    max_bytes: self.config.partition_max_bytes,
                },
            );
        }

        // A broker with nothing assigned still needs one request if it holds a
        // session, so the session's forgotten list can drain. Once that is
        // done, drop the fetcher.
        let idle: Vec<i32> = self
            .fetchers
            .iter()
            .filter(|(leader, fetcher)| {
                !by_leader.contains_key(*leader) && fetcher.session_id() == 0
            })
            .map(|(leader, _)| *leader)
            .collect();
        for leader in idle {
            self.fetchers.remove(&leader);
        }
        for leader in self.fetchers.keys().copied().collect::<Vec<_>>() {
            by_leader.entry(leader).or_default();
        }

        let options = DecodeOptions {
            max_decompressed_bytes: self.config.max_decompressed_bytes,
            visibility: self.config.visibility,
        };

        for (leader, wanted) in by_leader {
            let fetcher = self.fetchers.entry(leader).or_default();
            let fetched = fetcher
                .fetch(
                    &self.cluster,
                    leader,
                    &wanted,
                    &self.topic_ids,
                    Limits {
                        max_wait_ms: self.config.max_wait_ms,
                        max_bytes: self.config.max_bytes,
                        visibility: self.config.visibility,
                    },
                )
                .await?;

            for partition in fetched {
                let key = (partition.topic.clone(), partition.partition);
                let Some(assigned) = self.assignment.get_mut(&key) else {
                    continue;
                };

                if let Some(error) = partition.error {
                    // Per-partition, and not fatal: a leader that just moved
                    // is resolved again on the next round.
                    tracing::debug!(topic = %key.0, partition = key.1, %error, "partition error");
                    if error.needs_metadata_refresh() {
                        self.cluster.invalidate();
                    }
                    continue;
                }

                assigned.high_watermark = partition.high_watermark;
                if partition.records.is_empty() {
                    continue;
                }

                let decoded = kafka_read::decode_records_with_aborted(
                    &partition.topic,
                    partition.partition,
                    partition.records,
                    &partition.aborted,
                    &options,
                );

                for outcome in decoded.outcomes {
                    match outcome {
                        RecordOutcome::Ok(record) => {
                            // Records before the requested offset are normal:
                            // a fetch returns whole batches, and the batch
                            // holding our offset may start before it.
                            if record.offset < assigned.position {
                                continue;
                            }
                            assigned.position = record.offset.saturating_add(1);
                            self.buffered.push_back(record);
                        }
                        RecordOutcome::Malformed { offset, .. } => {
                            // One corrupt record must not stall the partition:
                            // step past it rather than re-reading it forever.
                            assigned.position = assigned.position.max(offset.saturating_add(1));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Commit the current positions under the configured group id.
    pub async fn commit(&self) -> Result<Vec<((String, i32), Result<()>)>> {
        let group = self.group()?;
        let offsets: HashMap<(String, i32), CommittedOffset> = self
            .assignment
            .iter()
            .map(|(key, assigned)| {
                (
                    key.clone(),
                    CommittedOffset {
                        offset: assigned.position,
                        metadata: None,
                    },
                )
            })
            .collect();
        offsets::commit(&self.cluster, group, &offsets).await
    }

    /// Read the group's committed positions for the current assignment.
    pub async fn committed(&self) -> Result<HashMap<(String, i32), CommittedOffset>> {
        let group = self.group()?;
        let keys: Vec<(String, i32)> = self.assignment.keys().cloned().collect();
        offsets::fetch(&self.cluster, group, &keys).await
    }

    /// Seek every assigned partition to its committed position, where one
    /// exists. Partitions with nothing committed keep their current position.
    pub async fn seek_to_committed(&mut self) -> Result<()> {
        let committed = self.committed().await?;
        for (key, offset) in committed {
            if let Some(assigned) = self.assignment.get_mut(&key) {
                assigned.position = offset.offset;
            }
        }
        self.buffered.clear();
        Ok(())
    }

    fn group(&self) -> Result<&str> {
        self.config.group_id.as_deref().ok_or_else(|| {
            Error::InvalidRequest(
                "this consumer has no group id; set one with ConsumerConfig::group_id".to_owned(),
            )
        })
    }

    /// Turn a [`Position`] into a starting offset per partition.
    async fn resolve(
        &self,
        partitions: &[(String, i32)],
        from: Position,
    ) -> Result<HashMap<(String, i32), i64>> {
        let mut out = HashMap::new();
        match from {
            Position::Offset(offset) => {
                for key in partitions {
                    out.insert(key.clone(), offset);
                }
            }
            Position::Earliest | Position::Latest => {
                for key in partitions {
                    // One `ListOffsets` gives both ends, so `Earliest` and
                    // `Latest` cost the same round trip.
                    let (earliest, latest) =
                        kafka_read::partition_bounds(&self.cluster, &key.0, key.1).await?;
                    out.insert(
                        key.clone(),
                        if matches!(from, Position::Earliest) {
                            earliest
                        } else {
                            latest
                        },
                    );
                }
            }
        }
        Ok(out)
    }
}
