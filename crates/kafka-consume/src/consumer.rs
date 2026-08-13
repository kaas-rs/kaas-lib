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
use kafka_meta::{Cluster, ConsumerGroupMetadata, TopicId};
use kafka_read::{DecodeOptions, Record, RecordOutcome, Visibility};

use crate::classic::Assignor;
use crate::fetcher::{BrokerFetcher, Limits};
use crate::offsets::{self, CommittedOffset};
use crate::rebalance::{self, Listener, Pending, RebalanceListener, RevokedPartition};
use crate::session::PartitionState;

/// The rebalance budget a member advertises, matching Java's and librdkafka's
/// `max.poll.interval.ms`.
///
/// Both send exactly this number in exactly this field — librdkafka passes
/// `max_poll_interval_ms` straight into its `ConsumerGroupHeartbeat` builder —
/// so a smaller default is not a conservative choice but a client-specific one,
/// and it fences callers whose per-batch work is slower than ours happens to
/// allow.
const DEFAULT_REBALANCE_TIMEOUT_MS: i32 = 300_000;

/// The session timeout a classic member advertises, matching Java's and
/// librdkafka's `session.timeout.ms`.
///
/// It was 20s here for as long as `JoinGroup` had to answer inside the
/// connection's 30s `request_timeout`; the join now carries its own deadline,
/// so the constraint that shrank it is gone.
const DEFAULT_SESSION_TIMEOUT_MS: i32 = 45_000;

/// Reject a classic member whose session outlives its rebalance budget.
///
/// Java pairs a 300s `max.poll.interval.ms` with a 45s `session.timeout.ms` and
/// is not tested the other way round. Inverted, a member that is legitimately
/// mid-rebalance — heartbeats pause while `JoinGroup` blocks — outlives its own
/// session and is evicted for being slow at something the coordinator asked it
/// to be slow at.
fn check_timeout_ordering(config: &ConsumerConfig) -> Result<()> {
    if config.session_timeout_ms > config.rebalance_timeout_ms {
        return Err(Error::InvalidRequest(format!(
            "session_timeout_ms ({}) must not exceed rebalance_timeout_ms ({})",
            config.session_timeout_ms, config.rebalance_timeout_ms
        )));
    }
    Ok(())
}

/// Which group-membership protocol a subscribing consumer speaks.
///
/// Kafka 4.x brokers serve two: KIP-848 (`ConsumerGroupHeartbeat`, the
/// broker computes assignments) and the classic
/// `JoinGroup`/`SyncGroup`/`Heartbeat` dance. Which one a broker offers is
/// discoverable from `ApiVersions` — and nothing else about the caller's code
/// needs to change with the answer, which is what [`NegotiatedConsumer`]
/// exists to exploit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GroupProtocol {
    /// Probe at subscribe time: KIP-848 when the broker serves the GA
    /// heartbeat (v1+, Kafka 4.0), the classic protocol otherwise —
    /// including against 3.7–3.9 brokers, which advertise only the preview's
    /// v0 and ship the protocol disabled. Mirrors the Java client, for which
    /// `group.protocol=consumer` also requires 4.0+ brokers.
    #[default]
    Auto,
    /// KIP-848, and an error on a broker that does not serve it.
    Consumer,
    /// The classic protocol, unconditionally — for mixed groups whose other
    /// members are pinned to it.
    Classic,
}

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
    /// How long a member may take to complete a rebalance before the
    /// coordinator gives up on it.
    ///
    /// This is the group-membership counterpart of Java's
    /// `max.poll.interval.ms`, and the same wire field: a member that has not
    /// re-joined and acknowledged its new assignment within this budget is
    /// dropped from the group. Since a rebalance only advances when `poll` is
    /// called, it is in practice a ceiling on how long a caller may spend
    /// between polls — writing each batch to a database, say — before the
    /// group moves on without it.
    ///
    /// Defaults to Java's and librdkafka's 300s. Sent on both protocols:
    /// `JoinGroup.rebalance_timeout_ms` on the classic one and
    /// `ConsumerGroupHeartbeat.rebalance_timeout_ms` under KIP-848.
    ///
    /// On the classic protocol `JoinGroup` *blocks* for up to this long, so a
    /// value above the connection's `request_timeout` is honoured by handing
    /// the join its own deadline rather than by widening the timeout every
    /// other RPC uses.
    pub rebalance_timeout_ms: i32,
    /// How long the coordinator waits for a heartbeat before evicting a member.
    ///
    /// For a static member (see `instance_id`) this is also how long its
    /// `group.instance.id` stays claimed after the process dies, which is how
    /// long a restart is refused with `UNRELEASED_INSTANCE_ID`.
    ///
    /// **Classic protocol only.** KIP-848 moved this to the broker: there is no
    /// field for it in `ConsumerGroupHeartbeat`, and the coordinator applies
    /// `group.consumer.session.timeout.ms` — 45s by default and floored at 45s
    /// by `group.consumer.min.session.timeout.ms` on a stock 4.x broker,
    /// alterable per group through `IncrementalAlterConfigs` on a `GROUP`
    /// config resource. [`GroupConsumer`] therefore ignores this field, exactly
    /// as librdkafka refuses `session.timeout.ms` under
    /// `group.protocol=consumer`.
    ///
    /// Defaults to Java's and librdkafka's 45s. Must not exceed
    /// [`ConsumerConfig::rebalance_timeout_ms`]; Kafka is not tested with the
    /// two inverted, and [`ClassicConsumer::subscribe`] rejects it.
    pub session_timeout_ms: i32,
    /// Which group protocol [`NegotiatedConsumer::subscribe`] uses.
    ///
    /// [`GroupProtocol::Auto`] — the default — asks the broker. Ignored by
    /// [`GroupConsumer`] and [`ClassicConsumer`], whose types *are* the
    /// choice.
    pub group_protocol: GroupProtocol,
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
            rebalance_timeout_ms: DEFAULT_REBALANCE_TIMEOUT_MS,
            session_timeout_ms: DEFAULT_SESSION_TIMEOUT_MS,
            group_protocol: GroupProtocol::Auto,
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

    /// How long a member may take to complete a rebalance.
    ///
    /// See [`ConsumerConfig::rebalance_timeout_ms`]. Raise it when a caller
    /// does slow work per batch; lower it to have the group notice a departed
    /// member sooner.
    #[must_use]
    pub fn with_rebalance_timeout_ms(mut self, ms: i32) -> Self {
        self.rebalance_timeout_ms = ms;
        self
    }

    /// How long the coordinator waits for a heartbeat before evicting a member.
    ///
    /// Classic protocol only — see [`ConsumerConfig::session_timeout_ms`] for
    /// why KIP-848 has no client-side equivalent.
    #[must_use]
    pub fn with_session_timeout_ms(mut self, ms: i32) -> Self {
        self.session_timeout_ms = ms;
        self
    }

    /// Which group protocol [`NegotiatedConsumer::subscribe`] uses.
    #[must_use]
    pub fn with_group_protocol(mut self, protocol: GroupProtocol) -> Self {
        self.group_protocol = protocol;
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

    /// Resolve and remember topic ids without changing the assignment.
    ///
    /// A group member needs these before its first heartbeat: the broker's
    /// assignment names topics by uuid, and one we cannot name is one we
    /// cannot act on.
    pub(crate) async fn learn_topics(&mut self, topics: &[String]) -> Result<()> {
        let names: Vec<&str> = topics.iter().map(String::as_str).collect();
        let snapshot = self.cluster.refresh_topics(&names).await?;
        for topic in topics {
            if let Some(info) = snapshot.topic(topic) {
                self.topic_ids.insert(topic.clone(), info.topic_id);
            }
        }
        Ok(())
    }

    pub(crate) fn topic_ids(&self) -> &HashMap<String, TopicId> {
        &self.topic_ids
    }

    /// Replace the assignment, keeping the position of partitions that stay.
    ///
    /// Distinct from [`Consumer::assign`], which resolves fresh positions: a
    /// rebalance that re-resolved a partition this member already held would
    /// re-read or skip records it had already accounted for.
    pub(crate) async fn reassign(&mut self, partitions: Vec<(String, i32)>) -> Result<()> {
        let wanted: HashSet<(String, i32)> = partitions.iter().cloned().collect();
        self.assignment.retain(|key, _| wanted.contains(key));

        let fresh: Vec<(String, i32)> = partitions
            .into_iter()
            .filter(|key| !self.assignment.contains_key(key))
            .collect();
        if !fresh.is_empty() {
            let starts = self.committed_or_earliest(&fresh).await?;
            for key in fresh {
                let position = starts.get(&key).copied().unwrap_or(0);
                self.assignment.insert(
                    key,
                    Assigned {
                        position,
                        paused: false,
                        high_watermark: -1,
                    },
                );
            }
        }

        // Records buffered for a partition we no longer own must not be
        // delivered: somebody else owns it now.
        self.buffered
            .retain(|record| wanted.contains(&(record.topic.clone(), record.partition)));
        Ok(())
    }

    /// Where a newly-gained partition starts: its committed position where the
    /// group has one, the log start otherwise.
    async fn committed_or_earliest(
        &self,
        partitions: &[(String, i32)],
    ) -> Result<HashMap<(String, i32), i64>> {
        let mut out = HashMap::new();
        if let Ok(group) = self.group() {
            for (key, committed) in offsets::fetch(&self.cluster, group, partitions).await? {
                out.insert(key, committed.offset);
            }
        }
        for key in partitions {
            if !out.contains_key(key) {
                let (earliest, _) =
                    kafka_read::partition_bounds(&self.cluster, &key.0, key.1).await?;
                out.insert(key.clone(), earliest);
            }
        }
        Ok(out)
    }

    /// The current assignment.
    pub fn assignment(&self) -> Vec<(String, i32)> {
        let mut keys: Vec<(String, i32)> = self.assignment.keys().cloned().collect();
        keys.sort();
        keys
    }

    /// Pair partitions with the positions they currently hold, for a listener.
    ///
    /// Read *before* anything is revoked, because the position is the whole
    /// point: a listener checkpointing its own state needs the offset this
    /// member would have read next, and after the reassignment that number is
    /// gone.
    pub(crate) fn revoked_positions(&self, keys: &[(String, i32)]) -> Vec<RevokedPartition> {
        keys.iter()
            .map(|(topic, partition)| RevokedPartition {
                topic: topic.clone(),
                partition: *partition,
                position: self
                    .assignment
                    .get(&(topic.clone(), *partition))
                    .map_or(-1, |assigned| assigned.position),
            })
            .collect()
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

    /// Every assigned partition's read position, in the shape a transactional
    /// offset commit wants.
    ///
    /// Hand this straight to `Producer::send_offsets_to_transaction`. The
    /// values are the offset of the **next** record to read, which is what a
    /// committed offset means — not the offset of the last record `poll`
    /// returned.
    pub fn positions(&self) -> Vec<((String, i32), i64)> {
        self.assignment
            .iter()
            .map(|(key, assigned)| (key.clone(), assigned.position))
            .collect()
    }

    /// This consumer's identity for a transactional offset commit (KIP-447).
    ///
    /// A manually-assigned consumer is not a member of anything, so this is the
    /// non-member form: an empty member id and the `-1` sentinel, honoured by
    /// the coordinator only while the group has no members. It still needs a
    /// group id, because there is no such thing as committing an offset outside
    /// a group.
    pub fn group_metadata(&self) -> Result<ConsumerGroupMetadata> {
        Ok(ConsumerGroupMetadata::new(self.group()?))
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
    ///
    /// Anonymously — this is the standalone consumer's commit, and the broker
    /// honours the anonymous form only while the group has no members. A
    /// group member's commit goes through [`Consumer::commit_as`] with its
    /// membership, because a live group refuses the anonymous form with
    /// `UNKNOWN_MEMBER_ID` on every partition.
    pub async fn commit(&self) -> Result<Vec<((String, i32), Result<()>)>> {
        self.commit_as(None).await
    }

    /// Commit the current positions, under a membership when there is one.
    pub(crate) async fn commit_as(
        &self,
        member: Option<offsets::CommitAs<'_>>,
    ) -> Result<Vec<((String, i32), Result<()>)>> {
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
        offsets::commit(&self.cluster, group, member, &offsets).await
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

/// The `ConsumerGroupHeartbeat` version floor of the KIP-848 protocol this
/// implementation speaks.
///
/// v0 is the 3.7–3.9 *preview*: brokers of that whole line advertise key 68
/// at `0-0` while shipping the protocol disabled by default — measured
/// against `apache/kafka:3.7.2`, which advertises 57 api keys with key 68 at
/// exactly `0-0` — so advertisement alone does not mean a group can form.
/// The GA protocol is v1+ (Kafka 4.0), and the Java client draws the same
/// line ("the consumer group protocol requires brokers 4.0 or newer").
/// Keying membership on the floor is what makes "advertised" mean "usable".
const GA_HEARTBEAT_FLOOR: i16 = 1;

/// Require that this cluster serves the GA (v1+) `ConsumerGroupHeartbeat`,
/// with a typed error naming both sides when it does not.
///
/// Free where it matters: the broker's table is already in hand on every
/// pooled connection, so no extra round trip.
async fn require_ga_heartbeat(cluster: &Cluster) -> Result<()> {
    let connection = cluster.pool().any().await?;
    let row = connection
        .versions()
        .get(kafka_conn::ApiKey::ConsumerGroupHeartbeat)
        .copied();
    match row.and_then(|entry| entry.negotiated()) {
        Some(version) if version >= GA_HEARTBEAT_FLOOR => Ok(()),
        _ => Err(Error::UnsupportedApi {
            api_key: kafka_conn::ApiKey::ConsumerGroupHeartbeat,
            broker: row.map(|entry| entry.broker.into()),
            // What this implementation requires, not the codec's full range:
            // a `0-0` broker overlaps the codec and still cannot form a group.
            ours: kafka_conn::our_range(kafka_conn::ApiKey::ConsumerGroupHeartbeat)
                .map(|range| (GA_HEARTBEAT_FLOOR.max(range.min), range.max)),
        }),
    }
}

/// A consumer that joins a KIP-848 group and lets the broker assign it
/// partitions.
///
/// Wraps [`Consumer`] rather than replacing it: the fetch path, the sessions
/// and the decoding are identical, and the only thing membership changes is
/// *where the assignment comes from*. That is also why the manual mode
/// survives — it is not a degraded group consumer, it is the same engine with
/// the assignment supplied by the caller.
pub struct GroupConsumer {
    inner: Consumer,
    membership: crate::group::Membership,
    /// Whether to commit owned positions before giving a partition up.
    auto_commit: bool,
    /// The caller's rebalance hook, if it registered one.
    listener: Listener,
    /// A rebalance computed but not yet carried out.
    ///
    /// Held here rather than run inline inside `beat` so a cancelled `poll`
    /// cannot skip the callback — see [`crate::rebalance`].
    pending: Option<Pending>,
}

// Hand-written because a `dyn RebalanceListener` is not `Debug`, and requiring
// it of every caller's listener is a worse trade than one impl here.
impl std::fmt::Debug for GroupConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupConsumer")
            .field("inner", &self.inner)
            .field("membership", &self.membership)
            .field("auto_commit", &self.auto_commit)
            .field("listener", &self.listener.is_some())
            .field("pending", &self.pending)
            .finish()
    }
}

impl GroupConsumer {
    /// Join `group_id` and subscribe to `topics`.
    ///
    /// The client generates its own member id (KIP-848 inverts the classic
    /// protocol here) and the **broker** computes the assignment; nothing is
    /// owned until the first heartbeat comes back with one.
    pub async fn subscribe(
        cluster: Cluster,
        mut config: ConsumerConfig,
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        // A broker that cannot serve the GA heartbeat can never admit this
        // member. One `UnsupportedApi` here, where the caller made the
        // choice, beats the same error from every subsequent `poll` — where a
        // retry loop reads it as a transient fault and spins forever.
        // [`NegotiatedConsumer::subscribe`] makes the downgrade automatic.
        require_ga_heartbeat(&cluster).await?;

        let group_id = group_id.into();
        let subscription: Vec<String> = topics.into_iter().map(Into::into).collect();
        config.group_id = Some(group_id.clone());

        let rebalance_timeout_ms = config.rebalance_timeout_ms;
        let mut inner = Consumer::new(cluster, config);
        // Resolve topic ids up front: the broker's assignment names topics by
        // uuid, and an assignment we cannot name is an assignment we cannot
        // act on.
        inner.learn_topics(&subscription).await?;

        Ok(Self {
            inner,
            membership: crate::group::Membership::new(
                group_id,
                subscription,
                None,
                rebalance_timeout_ms,
            ),
            auto_commit: true,
            listener: None,
            pending: None,
        })
    }

    /// Join as a **static** member, so a restart inside the session timeout
    /// does not trigger a rebalance.
    #[must_use]
    pub fn instance_id(mut self, instance_id: impl Into<String>) -> Self {
        self.membership.set_instance_id(instance_id.into());
        self
    }

    /// Whether to commit owned positions before revoking a partition.
    #[must_use]
    pub fn auto_commit(mut self, enabled: bool) -> Self {
        self.auto_commit = enabled;
        self
    }

    /// Register a hook that fires around every rebalance.
    ///
    /// `on_revoke` runs while this member still owns the partitions and before
    /// auto-commit, which is the only moment a caller can flush its own
    /// per-partition state safely. See [`crate::rebalance`] for the ordering,
    /// the at-least-once delivery, and what an error does.
    #[must_use]
    pub fn on_rebalance(mut self, listener: impl RebalanceListener + 'static) -> Self {
        self.listener = Some(Box::new(listener));
        self
    }

    /// The partitions this member currently owns.
    pub fn assignment(&self) -> Vec<(String, i32)> {
        self.inner.assignment()
    }

    /// This member's id.
    ///
    /// Generated by the client, not the broker — KIP-848 inverts the classic
    /// protocol here, and an empty one is rejected outright.
    pub fn member_id(&self) -> &str {
        self.membership.member_id()
    }

    /// The underlying cluster handle.
    pub fn cluster(&self) -> &Cluster {
        self.inner.cluster()
    }

    /// Heartbeat if due, reconcile any new assignment, then read.
    ///
    /// # The ordering that matters
    ///
    /// A rebalance is handled as **revoke, then acknowledge**, across two
    /// beats:
    ///
    /// ```text
    /// listener.on_revoke  →  auto-commit  →  drop the partitions  →  ack
    /// ```
    ///
    /// The caller's hook runs first, while this member still owns everything;
    /// the offsets follow, so a committed offset never runs ahead of data the
    /// caller has written; and only then does the next heartbeat tell the
    /// broker what is now owned. Acknowledging first would mean two consumers
    /// holding the same partition at once, which delivers every record twice
    /// and reports nothing.
    ///
    /// # Cancel safety
    ///
    /// A rebalance that has been computed but not finished is held on the
    /// consumer, so dropping this future mid-callback does not skip it — the
    /// next `poll` picks it up, still ahead of the acknowledging beat. The cost
    /// is that `on_revoke` may run twice for the same partitions; see
    /// [`crate::rebalance`].
    pub async fn poll(&mut self) -> Result<Vec<Record>> {
        // Anything a cancelled poll left half-done, before beating again: the
        // broker is waiting on our acknowledgement and must not get one for an
        // assignment the caller has not been told about.
        self.settle().await?;

        if self.membership.beat_due() {
            let topic_ids = self.inner.topic_ids().clone();
            let outcome = self
                .membership
                .beat(self.inner.cluster(), &topic_ids)
                .await?;

            if outcome.changed {
                // Positions are read here, before anything is dropped — after
                // the reassignment they are gone.
                self.pending = Some(Pending {
                    revoked: self.inner.revoked_positions(&outcome.revoked),
                    gained: outcome.gained,
                });
                self.settle().await?;
            }
        }
        self.inner.poll().await
    }

    /// Carry out a pending rebalance: tell the caller, commit, then move.
    async fn settle(&mut self) -> Result<()> {
        let Some(pending) = self.pending.as_ref() else {
            return Ok(());
        };
        if pending.is_empty() {
            self.pending = None;
            return Ok(());
        }

        let revoked = pending.revoked.clone();
        let gained = pending.gained.clone();

        if !revoked.is_empty() {
            // The caller flushes first, then the offsets move. Cancelling here
            // leaves `pending` in place, so the next poll runs it again rather
            // than acknowledging an assignment nobody was told about.
            rebalance::revoke(&mut self.listener, revoked).await;
            if self.auto_commit {
                let _ = self.inner.commit_as(Some(self.commit_identity())).await;
            }
        }

        let owned: Vec<(String, i32)> = self.membership.owned().iter().cloned().collect();
        self.inner.reassign(owned).await?;
        self.pending = None;

        if !gained.is_empty() {
            rebalance::assign(&mut self.listener, gained).await;
        }
        Ok(())
    }

    /// Commit the current positions.
    ///
    /// As this member: the commit carries the member id and epoch the
    /// coordinator knows us by. The anonymous form the standalone consumer
    /// uses is refused with `UNKNOWN_MEMBER_ID` the moment a group has
    /// members — this group visibly has at least one.
    pub async fn commit(&self) -> Result<Vec<((String, i32), Result<()>)>> {
        self.inner.commit_as(Some(self.commit_identity())).await
    }

    /// How far behind the log end a partition is.
    pub fn lag(&self, topic: &str, partition: i32) -> Option<i64> {
        self.inner.lag(topic, partition)
    }

    /// Every owned partition's read position, for a transactional commit.
    pub fn positions(&self) -> Vec<((String, i32), i64)> {
        self.inner.positions()
    }

    /// The group's committed positions for the partitions this member owns.
    ///
    /// The read half of [`GroupConsumer::commit`], and the way to see what a
    /// transactional commit stored: offsets written inside a transaction stay
    /// invisible here until that transaction commits.
    pub async fn committed(&self) -> Result<HashMap<(String, i32), CommittedOffset>> {
        self.inner.committed().await
    }

    /// This member's identity for a transactional offset commit (KIP-447).
    ///
    /// Carries the member id and the **member epoch** the coordinator knows us
    /// by, exactly as [`GroupConsumer::commit`] does — a live group refuses the
    /// non-member form. Read it fresh for each transaction rather than caching
    /// it: the epoch moves with every rebalance, and a stale one is refused
    /// with `STALE_MEMBER_EPOCH`, which is the coordinator correctly stopping a
    /// member from committing partitions it no longer owns.
    pub fn group_metadata(&self) -> Result<ConsumerGroupMetadata> {
        Ok(ConsumerGroupMetadata::new(self.inner.group()?)
            .with_member_id(self.membership.member_id())
            .with_generation(self.membership.member_epoch())
            .with_maybe_instance_id(self.membership.instance_id().map(str::to_owned)))
    }

    /// Leave the group, releasing the assignment — or parking it, for a static
    /// member.
    ///
    /// Counts as a revocation: the listener gets the whole assignment before
    /// the offsets are committed and the member departs, on the same argument
    /// as a rebalance. A caller with unflushed state does not care *why* it is
    /// losing a partition.
    pub async fn leave(&mut self) -> Result<()> {
        let held = self.inner.assignment();
        if !held.is_empty() {
            let revoked = self.inner.revoked_positions(&held);
            rebalance::revoke(&mut self.listener, revoked).await;
        }
        if self.auto_commit {
            let _ = self.inner.commit_as(Some(self.commit_identity())).await;
        }
        self.membership.leave(self.inner.cluster()).await
    }

    /// What this member commits as: the id and epoch from the live
    /// membership, plus the instance id if it is static.
    fn commit_identity(&self) -> offsets::CommitAs<'_> {
        offsets::CommitAs {
            member_id: self.membership.member_id(),
            epoch: self.membership.member_epoch(),
            instance_id: self.membership.instance_id(),
        }
    }
}

/// A consumer that joins a **classic** group.
///
/// Only for brokers older than 4.0, or mixed groups with Java clients pinned
/// to `group.protocol=classic`. [`GroupConsumer`] is the default on a 4.x
/// cluster and is strictly less work — see [`crate::classic`] for which
/// assignors this implements and, deliberately, which it does not.
pub struct ClassicConsumer {
    inner: Consumer,
    membership: crate::classic::ClassicMembership,
    subscription: Vec<String>,
    auto_commit: bool,
    rejoin: bool,
    /// The caller's rebalance hook, if it registered one.
    listener: Listener,
    /// Partitions a detected rebalance is about to take, not yet handled.
    ///
    /// The classic protocol revokes **eagerly**: a rebalance takes the whole
    /// assignment, not a subset, so this is either everything or nothing.
    pending_revoke: Option<Vec<RevokedPartition>>,
}

// Same reason as `GroupConsumer`: a caller's listener should not have to be
// `Debug` for this struct to be.
impl std::fmt::Debug for ClassicConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClassicConsumer")
            .field("inner", &self.inner)
            .field("membership", &self.membership)
            .field("subscription", &self.subscription)
            .field("auto_commit", &self.auto_commit)
            .field("rejoin", &self.rejoin)
            .field("listener", &self.listener.is_some())
            .field("pending_revoke", &self.pending_revoke)
            .finish()
    }
}

impl ClassicConsumer {
    /// Join `group_id` under the classic protocol.
    ///
    /// # Every member needs its own `Cluster`
    ///
    /// Not a style preference — a hard requirement of this protocol. See
    /// [`crate::classic`] for the full reasoning; in short, `JoinGroup` blocks
    /// on the coordinator and a Kafka broker will not read a second request
    /// from a socket until it has answered the first, so two members of one
    /// group sharing a connection deadlock and present as a plain timeout.
    ///
    /// [`GroupConsumer`] has no such constraint.
    ///
    /// # Errors
    ///
    /// Rejects a [`ConsumerConfig`] whose `session_timeout_ms` exceeds its
    /// `rebalance_timeout_ms`. The coordinator would accept the pair and then
    /// evict a member that is doing exactly what it was told to — waiting out a
    /// rebalance it still has budget for — so this is caught here rather than
    /// diagnosed from an eviction later.
    pub async fn subscribe(
        cluster: Cluster,
        mut config: ConsumerConfig,
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let group_id = group_id.into();
        let subscription: Vec<String> = topics.into_iter().map(Into::into).collect();
        config.group_id = Some(group_id.clone());
        check_timeout_ordering(&config)?;
        let (session_timeout_ms, rebalance_timeout_ms) =
            (config.session_timeout_ms, config.rebalance_timeout_ms);

        let mut inner = Consumer::new(cluster, config);
        inner.learn_topics(&subscription).await?;

        Ok(Self {
            inner,
            membership: crate::classic::ClassicMembership::new(
                group_id,
                subscription.clone(),
                None,
                session_timeout_ms,
                rebalance_timeout_ms,
            ),
            subscription,
            auto_commit: true,
            rejoin: true,
            listener: None,
            pending_revoke: None,
        })
    }

    /// Register a hook that fires around every rebalance.
    ///
    /// Same contract as [`GroupConsumer::on_rebalance`], with one protocol
    /// difference worth knowing: the classic protocol revokes **eagerly**, so
    /// every rebalance hands `on_revoke` the *whole* assignment rather than
    /// just the partitions that end up moving. That is what
    /// `RangeAssignor`-style rebalancing does — it is not this client rounding
    /// up.
    #[must_use]
    pub fn on_rebalance(mut self, listener: impl RebalanceListener + 'static) -> Self {
        self.listener = Some(Box::new(listener));
        self
    }

    /// Whether to commit owned positions before revoking a partition.
    #[must_use]
    pub fn auto_commit(mut self, enabled: bool) -> Self {
        self.auto_commit = enabled;
        self
    }

    /// Join as a **static** member (KIP-345), so a restart inside the session
    /// timeout does not trigger a rebalance.
    ///
    /// [`ConsumerConfig::session_timeout_ms`] is therefore also how long this
    /// `instance_id` stays claimed after the process dies: a restart inside
    /// that window is refused with `UNRELEASED_INSTANCE_ID` until the previous
    /// session lapses.
    #[must_use]
    pub fn instance_id(mut self, instance_id: impl Into<String>) -> Self {
        let assignors = self.membership.assignors().to_vec();
        self.membership = crate::classic::ClassicMembership::new(
            self.membership.group_id().to_owned(),
            self.subscription.clone(),
            Some(instance_id.into()),
            self.inner.config.session_timeout_ms,
            self.inner.config.rebalance_timeout_ms,
        );
        self.membership.set_assignors(assignors);
        self
    }

    /// Advertise these assignors, in this order of preference.
    ///
    /// Defaults to `[Range, RoundRobin, CooperativeSticky]`, which matches
    /// Java's own first choice and therefore settles a mixed group on `range`
    /// without a tie-break. Put [`Assignor::CooperativeSticky`] first to ask
    /// for incremental rebalancing — a rebalance that moves only the partitions
    /// that have to move, rather than stopping every member.
    ///
    /// Order is a vote, not a demand: the coordinator intersects every member's
    /// list, each member votes for the first of its own that survived, and the
    /// most-voted protocol wins. Advertising exactly one assignor is how you
    /// force the issue, at the cost of failing to join any group that does not
    /// share it — `INCONSISTENT_GROUP_PROTOCOL`, at join time, loudly.
    ///
    /// An empty list is ignored: a member with no assignors cannot join
    /// anything.
    #[must_use]
    pub fn assignors(mut self, assignors: impl IntoIterator<Item = Assignor>) -> Self {
        let assignors: Vec<Assignor> = assignors.into_iter().collect();
        if !assignors.is_empty() {
            self.membership.set_assignors(assignors);
        }
        self
    }

    /// The partitions this member currently owns.
    pub fn assignment(&self) -> Vec<(String, i32)> {
        self.inner.assignment()
    }

    /// This member's id, as the coordinator issued it.
    ///
    /// Empty until the first join — the classic protocol has the *broker*
    /// assign it, which is the opposite of KIP-848.
    pub fn member_id(&self) -> &str {
        self.membership.member_id()
    }

    /// Whether this member was elected group leader and computed the
    /// assignment for everyone.
    pub fn is_leader(&self) -> bool {
        self.membership.is_leader()
    }

    /// Every owned partition's read position, for a transactional commit.
    pub fn positions(&self) -> Vec<((String, i32), i64)> {
        self.inner.positions()
    }

    /// The group's committed positions for the partitions this member owns.
    pub async fn committed(&self) -> Result<HashMap<(String, i32), CommittedOffset>> {
        self.inner.committed().await
    }

    /// This member's identity for a transactional offset commit (KIP-447).
    ///
    /// The classic protocol's `generation_id` fills the same field KIP-848
    /// calls `member_epoch`, so this is the same type
    /// [`GroupConsumer::group_metadata`] returns rather than a parallel one —
    /// see [`ConsumerGroupMetadata`] for why that is not flattening the group
    /// kinds. Read it fresh for each transaction: a generation that has moved
    /// on is refused with `ILLEGAL_GENERATION`.
    pub fn group_metadata(&self) -> Result<ConsumerGroupMetadata> {
        Ok(ConsumerGroupMetadata::new(self.inner.group()?)
            .with_member_id(self.membership.member_id())
            .with_generation(self.membership.generation_id())
            .with_maybe_instance_id(self.membership.instance_id().map(str::to_owned)))
    }

    /// The underlying cluster handle.
    pub fn cluster(&self) -> &Cluster {
        self.inner.cluster()
    }

    /// Re-join if needed, heartbeat, then read.
    ///
    /// # Cancel safety
    ///
    /// A detected rebalance is remembered, so dropping this future before the
    /// listener has run does not skip it — the next `poll` runs it before
    /// re-joining. See [`crate::rebalance`].
    pub async fn poll(&mut self) -> Result<Vec<Record>> {
        // Held over from a cancelled poll, or set by the heartbeat below. Runs
        // before the re-join, which is what makes it a *pre*-revocation hook.
        self.settle().await;

        // Drive the group machinery to quiescence *within* this poll rather
        // than one protocol step per call. One step per poll deadlocked two
        // members driven from a single task — `tokio::join!(a.poll(),
        // b.poll())`, the shape every pairwise test uses: `b` blocks inside
        // JoinGroup waiting for `a` to re-join, but `a` only flagged `rejoin`
        // and will not act on it until its *next* poll, which cannot start
        // until `b`'s returns. The coordinator ends that stand-off at the
        // rebalance timeout by evicting whoever never re-joined, and the
        // evicted member then surfaced `UNKNOWN_MEMBER_ID` as a hard error.
        //
        // The bound covers the longest ordinary dance — detect via heartbeat,
        // join + sync, the KIP-429 second round, and a KIP-394 handshake —
        // with room for one surprise. Running out is not an error: the flags
        // survive, the caller polls again.
        for _ in 0..6 {
            if !self.rejoin {
                if !self.heartbeat_says_rejoin().await? {
                    break;
                }
                continue;
            }
            let sizes = self.partition_counts().await?;
            let held = self.inner.assignment();
            match self
                .membership
                .join(self.inner.cluster(), &sizes, &held)
                .await
            {
                Ok(assigned) => {
                    let cooperative = self.membership.is_cooperative();

                    // Under a cooperative protocol this member *kept* its
                    // partitions across the join, so the sync's answer is the
                    // first thing that says which ones moved: anything held but
                    // not assigned is revoked here, before the reassignment.
                    //
                    // Under an eager one there is nothing to work out. The
                    // heartbeat that detected the rebalance already announced
                    // the whole assignment as revoked and committed it;
                    // re-deriving a subset here would fire the listener a
                    // second time for partitions it has already been told
                    // about.
                    let lost: Vec<(String, i32)> = if cooperative {
                        let target: HashSet<(String, i32)> = assigned.iter().cloned().collect();
                        held.iter()
                            .filter(|key| !target.contains(*key))
                            .cloned()
                            .collect()
                    } else {
                        Vec::new()
                    };
                    if !lost.is_empty() {
                        self.pending_revoke = Some(self.inner.revoked_positions(&lost));
                        self.settle().await;
                    }

                    // Symmetrically: a cooperative member gained only what it
                    // did not already hold, an eager one gained everything —
                    // it owned nothing a moment ago.
                    let gained: Vec<(String, i32)> = if cooperative {
                        assigned
                            .iter()
                            .filter(|key| !held.contains(key))
                            .cloned()
                            .collect()
                    } else {
                        assigned.clone()
                    };
                    self.inner.reassign(assigned).await?;

                    // KIP-429's second round. The partitions this member just
                    // gave up are unowned now, so a re-join hands them to
                    // whoever is meant to have them. Settling here instead
                    // would strand them until the next unrelated rebalance.
                    self.rejoin = cooperative && !lost.is_empty();

                    if !gained.is_empty() {
                        rebalance::assign(&mut self.listener, gained).await;
                    }
                }
                Err(error) => match error.code() {
                    // KIP-394: the coordinator refuses a first join and hands
                    // back the id to use. That is the handshake, not a
                    // failure — the next cycle joins with the granted id.
                    Some(kafka_conn::ErrorCode::MemberIdRequired) => {}
                    // This member was evicted — it missed a rebalance or its
                    // session lapsed, and the coordinator no longer knows the
                    // id it is presenting. Re-presenting it can only fail the
                    // same way, so do what the Java client does: drop the
                    // identity, treat everything held as revoked, and go
                    // through the door as a new member. The commit inside
                    // `settle` is refused for an evicted member and ignored —
                    // redelivery of the uncommitted tail is exactly
                    // at-least-once.
                    Some(
                        kafka_conn::ErrorCode::UnknownMemberId
                        | kafka_conn::ErrorCode::IllegalGeneration,
                    ) => {
                        self.membership.forget();
                        let held = self.inner.assignment();
                        if !held.is_empty() {
                            self.pending_revoke = Some(self.inner.revoked_positions(&held));
                            self.settle().await;
                        }
                        // And actually let go: an evicted member that keeps
                        // its assignment keeps *fetching* partitions somebody
                        // else now owns, and re-advertises them as `owned` to
                        // the sticky assignor when it rejoins — which is how
                        // two members end up claiming the same twelve
                        // partitions. A new member owns nothing.
                        self.inner.reassign(Vec::new()).await?;
                    }
                    // The group moved on mid-sync; joining again is the
                    // protocol's answer, not an error.
                    Some(kafka_conn::ErrorCode::RebalanceInProgress) => {}
                    _ => return Err(error),
                },
            }
        }

        if self.rejoin {
            // Out of budget mid-dance. The flags survive, so the next poll
            // picks up exactly where this one stopped.
            return Ok(Vec::new());
        }
        self.inner.poll().await
    }

    /// One heartbeat; `true` means the group is rebalancing and this member
    /// must re-join.
    ///
    /// What happens to the assignment in the meantime is the difference
    /// between the two rebalance styles. An **eager** member gives everything
    /// up right now, before it re-joins, so the whole assignment goes to the
    /// listener and the auto-commit follows it. A **cooperative** member
    /// holds on to everything and finds out from the sync which partitions
    /// actually moved — revoking here would throw away the stickiness that is
    /// the entire point.
    async fn heartbeat_says_rejoin(&mut self) -> Result<bool> {
        if !self.membership.heartbeat(self.inner.cluster()).await? {
            return Ok(false);
        }
        self.rejoin = true;
        if !self.membership.is_cooperative() {
            let held = self.inner.assignment();
            self.pending_revoke = Some(self.inner.revoked_positions(&held));
            self.settle().await;
        }
        Ok(true)
    }

    /// Tell the listener what is being revoked, then commit it.
    ///
    /// `pending_revoke` is cleared only once both have finished, so a poll
    /// cancelled part-way through leaves the work to be redone rather than
    /// dropped.
    async fn settle(&mut self) {
        let Some(revoked) = self.pending_revoke.clone() else {
            return;
        };
        if !revoked.is_empty() {
            rebalance::revoke(&mut self.listener, revoked).await;
            if self.auto_commit {
                let _ = self.inner.commit_as(Some(self.commit_identity())).await;
            }
        }
        self.pending_revoke = None;
    }

    /// Commit the current positions.
    ///
    /// As this member: the coordinator-issued member id and the current
    /// generation. The anonymous form is refused with `UNKNOWN_MEMBER_ID`
    /// while the group has members — see [`GroupConsumer::commit`].
    pub async fn commit(&self) -> Result<Vec<((String, i32), Result<()>)>> {
        self.inner.commit_as(Some(self.commit_identity())).await
    }

    /// Leave the group. A static member deliberately does **not** leave.
    ///
    /// Counts as a revocation, for the same reason it does on
    /// [`GroupConsumer::leave`]: a caller with unflushed state does not care
    /// why it is losing a partition.
    pub async fn leave(&mut self) -> Result<()> {
        let held = self.inner.assignment();
        if !held.is_empty() {
            let revoked = self.inner.revoked_positions(&held);
            rebalance::revoke(&mut self.listener, revoked).await;
        }
        if self.auto_commit {
            let _ = self.inner.commit_as(Some(self.commit_identity())).await;
        }
        self.membership.leave(self.inner.cluster()).await
    }

    /// What this member commits as. The classic protocol spells the epoch
    /// `generation_id`; the wire field is the same one KIP-848 uses for the
    /// member epoch.
    fn commit_identity(&self) -> offsets::CommitAs<'_> {
        offsets::CommitAs {
            member_id: self.membership.member_id(),
            epoch: self.membership.generation_id(),
            instance_id: self.membership.instance_id(),
        }
    }

    /// How many partitions each subscribed topic has, which the leader needs
    /// to size the assignment.
    async fn partition_counts(&self) -> Result<std::collections::BTreeMap<String, i32>> {
        let names: Vec<&str> = self.subscription.iter().map(String::as_str).collect();
        let snapshot = self.inner.cluster().refresh_topics(&names).await?;
        let mut out = std::collections::BTreeMap::new();
        for topic in &self.subscription {
            if let Some(info) = snapshot.topic(topic) {
                out.insert(
                    topic.clone(),
                    i32::try_from(info.partitions.len()).unwrap_or(0),
                );
            }
        }
        Ok(out)
    }
}

/// A group consumer whose protocol was chosen at subscribe time.
///
/// [`GroupConsumer`] and [`ClassicConsumer`] are deliberately distinct types
/// — different wire protocols, different constraints — but which one a broker
/// serves is the broker's fact, not the caller's, and it is advertised in
/// `ApiVersions`. This wrapper asks, picks, and then delegates: the caller
/// gets "the right consumer for this broker" without knowing its protocol
/// support up front, and without a retry loop spinning on `UnsupportedApi`
/// against a pre-KIP-848 broker.
///
/// The variants are public: a caller that needs a protocol-specific method —
/// [`ClassicConsumer::is_leader`], say — can match and reach it.
#[derive(Debug)]
pub enum NegotiatedConsumer {
    /// KIP-848 — the broker advertises `ConsumerGroupHeartbeat`.
    Group(GroupConsumer),
    /// The classic `JoinGroup`/`SyncGroup`/`Heartbeat` path.
    Classic(ClassicConsumer),
}

impl NegotiatedConsumer {
    /// Join `group_id` and subscribe to `topics`, speaking whichever protocol
    /// [`ConsumerConfig::group_protocol`] names — and under
    /// [`GroupProtocol::Auto`], whichever this broker serves: KIP-848 where
    /// the GA heartbeat (v1+) is negotiable, the classic path otherwise.
    ///
    /// The probe is free: the broker's `ApiVersions` table is already in hand
    /// on every pooled connection, so `Auto` costs no extra round trip.
    ///
    /// Note the classic protocol's constraint survives negotiation: when the
    /// choice lands on [`ClassicConsumer`], every member of the group needs
    /// its own [`Cluster`] — see [`ClassicConsumer::subscribe`].
    pub async fn subscribe(
        cluster: Cluster,
        config: ConsumerConfig,
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let choice = match config.group_protocol {
            GroupProtocol::Consumer => GroupProtocol::Consumer,
            GroupProtocol::Classic => GroupProtocol::Classic,
            GroupProtocol::Auto => {
                match require_ga_heartbeat(&cluster).await {
                    Ok(()) => GroupProtocol::Consumer,
                    // The downgrade this type exists for — the broker does
                    // not serve the GA heartbeat, whether by not advertising
                    // key 68 at all (kaas, pre-3.7) or by advertising only
                    // the 3.7–3.9 preview's v0, which ships disabled and
                    // cannot admit this member either way. Any other failure
                    // — no broker reachable, say — is a real error and stays
                    // one.
                    Err(Error::UnsupportedApi { .. }) => GroupProtocol::Classic,
                    Err(error) => return Err(error),
                }
            }
        };
        match choice {
            GroupProtocol::Classic => Ok(Self::Classic(
                ClassicConsumer::subscribe(cluster, config, group_id, topics).await?,
            )),
            _ => Ok(Self::Group(
                GroupConsumer::subscribe(cluster, config, group_id, topics).await?,
            )),
        }
    }

    /// Which protocol the subscribe settled on. Never [`GroupProtocol::Auto`].
    pub fn protocol(&self) -> GroupProtocol {
        match self {
            Self::Group(_) => GroupProtocol::Consumer,
            Self::Classic(_) => GroupProtocol::Classic,
        }
    }

    /// Join as a **static** member. See [`GroupConsumer::instance_id`].
    #[must_use]
    pub fn with_instance_id(self, instance_id: impl Into<String>) -> Self {
        match self {
            Self::Group(consumer) => Self::Group(consumer.instance_id(instance_id)),
            Self::Classic(consumer) => Self::Classic(consumer.instance_id(instance_id)),
        }
    }

    /// Whether to commit owned positions before revoking a partition.
    #[must_use]
    pub fn with_auto_commit(self, enabled: bool) -> Self {
        match self {
            Self::Group(consumer) => Self::Group(consumer.auto_commit(enabled)),
            Self::Classic(consumer) => Self::Classic(consumer.auto_commit(enabled)),
        }
    }

    /// Register a hook that fires around every rebalance.
    ///
    /// Same contract as [`GroupConsumer::on_rebalance`]; when the choice
    /// landed on the classic protocol, every rebalance revokes eagerly — see
    /// [`ClassicConsumer::on_rebalance`].
    #[must_use]
    pub fn with_rebalance_listener(self, listener: impl RebalanceListener + 'static) -> Self {
        match self {
            Self::Group(consumer) => Self::Group(consumer.on_rebalance(listener)),
            Self::Classic(consumer) => Self::Classic(consumer.on_rebalance(listener)),
        }
    }

    /// Heartbeat, rebalance when told to, fetch, decode.
    pub async fn poll(&mut self) -> Result<Vec<Record>> {
        match self {
            Self::Group(consumer) => consumer.poll().await,
            Self::Classic(consumer) => consumer.poll().await,
        }
    }

    /// Commit the current positions as this member.
    pub async fn commit(&self) -> Result<Vec<((String, i32), Result<()>)>> {
        match self {
            Self::Group(consumer) => consumer.commit().await,
            Self::Classic(consumer) => consumer.commit().await,
        }
    }

    /// The group's committed offsets for the current assignment.
    pub async fn committed(&self) -> Result<HashMap<(String, i32), CommittedOffset>> {
        match self {
            Self::Group(consumer) => consumer.committed().await,
            Self::Classic(consumer) => consumer.committed().await,
        }
    }

    /// Leave the group. A static member deliberately does **not** leave.
    pub async fn leave(&mut self) -> Result<()> {
        match self {
            Self::Group(consumer) => consumer.leave().await,
            Self::Classic(consumer) => consumer.leave().await,
        }
    }

    /// The partitions this member currently owns.
    pub fn assignment(&self) -> Vec<(String, i32)> {
        match self {
            Self::Group(consumer) => consumer.assignment(),
            Self::Classic(consumer) => consumer.assignment(),
        }
    }

    /// The member id the coordinator knows this member by.
    pub fn member_id(&self) -> &str {
        match self {
            Self::Group(consumer) => consumer.member_id(),
            Self::Classic(consumer) => consumer.member_id(),
        }
    }

    /// The next offset to be read, per owned partition.
    pub fn positions(&self) -> Vec<((String, i32), i64)> {
        match self {
            Self::Group(consumer) => consumer.positions(),
            Self::Classic(consumer) => consumer.positions(),
        }
    }

    /// The identity a transactional producer needs for KIP-447 offset
    /// commits.
    pub fn group_metadata(&self) -> Result<ConsumerGroupMetadata> {
        match self {
            Self::Group(consumer) => consumer.group_metadata(),
            Self::Classic(consumer) => consumer.group_metadata(),
        }
    }

    /// The cluster handle this consumer reads through.
    pub fn cluster(&self) -> &Cluster {
        match self {
            Self::Group(consumer) => consumer.cluster(),
            Self::Classic(consumer) => consumer.cluster(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Matching Java and librdkafka is the point: these are the same numbers in
    /// the same fields, and a client-specific default is a client-specific
    /// eviction.
    #[test]
    fn the_group_timeouts_default_to_javas() {
        let config = ConsumerConfig::new();
        assert_eq!(config.rebalance_timeout_ms, 300_000);
        assert_eq!(config.session_timeout_ms, 45_000);
    }

    #[test]
    fn the_group_timeouts_are_settable() {
        let config = ConsumerConfig::new()
            .with_rebalance_timeout_ms(60_000)
            .with_session_timeout_ms(10_000);
        assert_eq!(config.rebalance_timeout_ms, 60_000);
        assert_eq!(config.session_timeout_ms, 10_000);
    }

    /// `rebalance >= session`, matching Java. Equal is allowed; inverted is the
    /// pair that gets a member evicted mid-rebalance.
    #[test]
    fn a_session_outliving_its_rebalance_budget_is_rejected() {
        let inverted = ConsumerConfig::new()
            .with_rebalance_timeout_ms(10_000)
            .with_session_timeout_ms(45_000);
        let error = check_timeout_ordering(&inverted).unwrap_err();
        assert!(
            matches!(error, Error::InvalidRequest(ref message) if message.contains("45000")),
            "the message must name the offending pair, got {error}"
        );

        let equal = ConsumerConfig::new()
            .with_rebalance_timeout_ms(45_000)
            .with_session_timeout_ms(45_000);
        assert!(check_timeout_ordering(&equal).is_ok());
        assert!(check_timeout_ordering(&ConsumerConfig::new()).is_ok());
    }

    /// `Auto` is the default because the whole point of negotiation is that a
    /// caller should not have to know a broker's protocol support up front.
    #[test]
    fn the_group_protocol_defaults_to_auto_and_is_settable() {
        assert_eq!(ConsumerConfig::new().group_protocol, GroupProtocol::Auto);
        assert_eq!(GroupProtocol::default(), GroupProtocol::Auto);
        let pinned = ConsumerConfig::new().with_group_protocol(GroupProtocol::Classic);
        assert_eq!(pinned.group_protocol, GroupProtocol::Classic);
    }
}
