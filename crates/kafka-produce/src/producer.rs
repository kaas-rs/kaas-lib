//! The producer: resolve a partition, encode a batch, send it to the leader.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_conn::protocol::messages::{ProduceRequest, ProduceResponse, TopicName};
use kafka_conn::{Error, ErrorCode, Result};
use kafka_meta::{Cluster, RetryPolicy, TopicId};

use crate::config::ProducerConfig;
use crate::encode::encode_batch;
use crate::partition::Partitioner;
use crate::record::{ProducerRecord, RecordMetadata};

/// The first `Produce` version that identifies topics by uuid rather than name.
///
/// The same transition `Fetch` made at its own v13, and the same failure if it
/// is missed: the broker sees a nil id and answers `UNKNOWN_TOPIC_ID`, which
/// reads like a deleted topic rather than a client asking the wrong question.
const PRODUCE_TOPIC_ID_VERSION: i16 = 13;

/// Why one attempt failed, which decides whether another one is allowed.
///
/// The distinction is the producer's central safety property, so it is a type
/// rather than a boolean buried in a match arm — a new failure path has to say
/// which kind it is before it will compile.
#[derive(Debug)]
enum Attempt {
    /// The broker answered, and said no. Nothing was appended.
    Rejected(Error),
    /// The request was in flight and we do not know what became of it.
    Ambiguous(Error),
}

/// Writes records to a cluster.
///
/// Cheap to clone; every clone shares the metadata cache, the connection pool
/// and the sticky partitioner's state. Sharing the partitioner is deliberate —
/// two clones producing to one topic should fill the same partition's batch,
/// not two.
#[derive(Debug, Clone)]
pub struct Producer {
    cluster: Cluster,
    config: ProducerConfig,
    partitioner: Arc<Partitioner>,
}

impl Producer {
    /// Wrap an existing cluster handle.
    pub fn new(cluster: Cluster, config: ProducerConfig) -> Self {
        Self {
            cluster,
            config,
            partitioner: Arc::new(Partitioner::new()),
        }
    }

    /// Connect to a cluster and produce to it.
    pub async fn connect(
        bootstrap: impl IntoIterator<Item = impl Into<String>>,
        cluster_config: kafka_meta::ClusterConfig,
        config: ProducerConfig,
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

    /// The configuration this producer was built with.
    pub fn config(&self) -> &ProducerConfig {
        &self.config
    }

    /// The partitioner, for callers that want to rotate the sticky choice.
    pub fn partitioner(&self) -> &Arc<Partitioner> {
        &self.partitioner
    }

    /// Write one record and wait for the broker to acknowledge it.
    ///
    /// # Which failures are retried, and why the distinction is the whole point
    ///
    /// This does **not** go through [`Cluster::send_to_leader`], whose retry
    /// loop treats every retriable error alike. A write cannot afford that,
    /// because two kinds of failure look similar and differ completely in what
    /// they permit:
    ///
    /// * **The broker rejected the request.** A response arrived carrying an
    ///   error code — `NOT_LEADER_OR_FOLLOWER` after a leader moved, say. The
    ///   record was definitively *not* appended, so re-sending it cannot
    ///   duplicate anything. These are retried, after refreshing the metadata
    ///   that made us ask the wrong broker.
    /// * **The outcome is unknown.** A timeout, or a connection that died with
    ///   the request in flight. The record may well have been written and the
    ///   acknowledgement lost. Re-sending it here is how a non-idempotent
    ///   producer silently duplicates records, so these are surfaced to the
    ///   caller, who knows whether a duplicate is worse than a gap. M14's
    ///   sequence numbers are what makes retrying these safe, and this
    ///   restriction lifts there rather than being loosened by hand.
    ///
    /// Collapsing the two is a bug in either direction: retry everything and
    /// you duplicate on every timeout; retry nothing and an ordinary leader
    /// election becomes a delivery failure. The second is what a live run
    /// against a second broker implementation caught.
    ///
    /// # Cancel safety
    ///
    /// Dropping this future either completes the request and discards the
    /// response or closes the connection, per rule 5; it never leaves a
    /// half-read response behind. What it cannot tell you is whether the
    /// record was written — that is inherent to cancelling a write, not a
    /// property of this implementation.
    pub async fn send(&self, record: ProducerRecord) -> Result<RecordMetadata> {
        let topic = record.topic.clone();
        let partition = self.resolve_partition(&record).await?;

        // Encoded once and reused across attempts: the bytes do not depend on
        // where the record is routed, and re-encoding would re-read the clock
        // and give a retried record a different timestamp from the one the
        // caller was originally told about.
        let batch = encode_batch(
            std::slice::from_ref(&record),
            self.config.compression,
            now_millis(),
        )?;

        let mut attempt: u32 = 1;
        loop {
            match self.attempt(&topic, partition, batch.clone()).await {
                Ok(metadata) => return Ok(metadata),
                Err(Attempt::Ambiguous(error)) => {
                    // Unknown outcome. Do not re-send; see above.
                    return Err(error);
                }
                Err(Attempt::Rejected(error)) => {
                    if error.needs_metadata_refresh() {
                        self.cluster.invalidate();
                    }
                    if !may_retry(&error, attempt, &self.config.retry) {
                        return Err(error);
                    }
                    // Backoff before re-resolving, not just before re-sending.
                    // The point of the pause is to give the cluster time to
                    // agree on the new leader; retrying instantly re-reads the
                    // same stale metadata and fails the same way.
                    let delay = self.config.retry.delay(attempt.saturating_add(1));
                    tracing::debug!(
                        %topic, partition, attempt, ?delay, %error,
                        "produce rejected; refreshing metadata and retrying"
                    );
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    }

    /// One routing-and-send attempt.
    ///
    /// Resolves the leader *inside* the attempt, so a retry that follows a
    /// metadata invalidation actually re-asks rather than re-using the
    /// snapshot that sent us to the wrong broker.
    async fn attempt(
        &self,
        topic: &str,
        partition: i32,
        batch: bytes::Bytes,
    ) -> std::result::Result<RecordMetadata, Attempt> {
        let topic_id = self.topic_id(topic).await.map_err(Attempt::Rejected)?;
        let version = self
            .cluster
            .negotiated_for::<ProduceRequest>()
            .await
            .map_err(Attempt::Rejected)?;
        let request = self
            .build_request(topic, topic_id, partition, batch, version)
            .map_err(Attempt::Rejected)?;

        // Routing failures are rejections: nothing was sent, so retrying after
        // a refresh is free.
        let leader = self
            .cluster
            .leader_for(topic, partition)
            .await
            .map_err(Attempt::Rejected)?;
        let connection = self
            .cluster
            .pool()
            .get(leader)
            .await
            .map_err(Attempt::Rejected)?;

        tracing::debug!(%topic, partition, leader, version, "producing");

        // From here the request is on the wire and any failure is ambiguous.
        let response = connection.send(request).await.map_err(Attempt::Ambiguous)?;

        self.read_response(response, topic, partition)
            .map_err(Attempt::Rejected)
    }

    /// Which partition a record belongs to.
    async fn resolve_partition(&self, record: &ProducerRecord) -> Result<i32> {
        let partition_count = self.partition_count(&record.topic).await?;

        match record.partition {
            Some(explicit) => {
                if explicit < 0 || explicit >= partition_count {
                    return Err(Error::InvalidRequest(format!(
                        "{}: partition {explicit} does not exist; the topic has {partition_count}",
                        record.topic
                    )));
                }
                Ok(explicit)
            }
            None => self
                .partitioner
                .assign(
                    &record.topic,
                    record.key.as_ref().map(|key| key.as_ref()),
                    partition_count,
                )
                .ok_or_else(|| {
                    Error::InvalidRequest(format!("{}: topic has no partitions", record.topic))
                }),
        }
    }

    /// How many partitions a topic has, refreshing metadata if we have never
    /// seen it.
    async fn partition_count(&self, topic: &str) -> Result<i32> {
        if let Some(info) = self.cluster.snapshot().topic(topic) {
            return i32::try_from(info.partitions.len()).map_err(|_| {
                Error::InvalidRequest(format!("{topic}: implausible partition count"))
            });
        }

        let refreshed = self.cluster.refresh_topics(&[topic]).await?;
        let info = refreshed.topic(topic).ok_or_else(|| {
            Error::from_code(ErrorCode::UnknownTopicOrPartition, Some(topic.to_owned()))
        })?;
        i32::try_from(info.partitions.len())
            .map_err(|_| Error::InvalidRequest(format!("{topic}: implausible partition count")))
    }

    /// The topic's uuid, as the metadata cache knows it.
    async fn topic_id(&self, topic: &str) -> Result<TopicId> {
        if let Some(info) = self.cluster.snapshot().topic(topic) {
            return Ok(info.topic_id);
        }
        let refreshed = self.cluster.refresh_topics(&[topic]).await?;
        refreshed
            .topic(topic)
            .map(|info| info.topic_id)
            .ok_or_else(|| {
                Error::from_code(ErrorCode::UnknownTopicOrPartition, Some(topic.to_owned()))
            })
    }

    fn build_request(
        &self,
        topic: &str,
        topic_id: TopicId,
        partition: i32,
        batch: bytes::Bytes,
        version: i16,
    ) -> Result<ProduceRequest> {
        let partition_data = PartitionProduceData::default()
            .with_index(partition)
            .with_records(Some(batch));

        let mut topic_data = TopicProduceData::default().with_partition_data(vec![partition_data]);

        // Set exactly the field this version has. The codec rejects a field
        // outside its own version range rather than ignoring it, so "set both
        // and let the encoder pick" is an encode error, not belt and braces.
        if version >= PRODUCE_TOPIC_ID_VERSION {
            if topic_id.is_zero() {
                return Err(Error::InvalidRequest(format!(
                    "{topic}: Produce v{version} identifies topics by id, and no topic id is known"
                )));
            }
            topic_data = topic_data.with_topic_id(uuid::Uuid::from_bytes(*topic_id.as_bytes()));
        } else {
            topic_data = topic_data.with_name(TopicName(StrBytes::from_string(topic.to_owned())));
        }

        Ok(ProduceRequest::default()
            .with_acks(self.config.acks.wire())
            .with_timeout_ms(self.config.delivery_timeout_ms())
            .with_topic_data(vec![topic_data]))
    }

    /// Pull the one partition's result out of the response.
    fn read_response(
        &self,
        response: ProduceResponse,
        topic: &str,
        partition: i32,
    ) -> Result<RecordMetadata> {
        let result = response
            .responses
            .into_iter()
            .flat_map(|topic_response| topic_response.partition_responses)
            .find(|partition_response| partition_response.index == partition)
            .ok_or_else(|| {
                Error::InvalidRequest(format!(
                    "{topic}-{partition}: the broker answered a produce for a partition we did not send"
                ))
            })?;

        if let Some(code) = ErrorCode::from_code(result.error_code) {
            // Refreshing and retrying is `send`'s job — a response carrying an
            // error code means the record was not appended, which is what
            // makes another attempt safe.
            return Err(Error::from_code(
                code,
                result.error_message.map(|m| m.to_string()),
            ));
        }

        Ok(RecordMetadata {
            topic: topic.to_owned(),
            partition,
            offset: result.base_offset,
            // -1 is the protocol's "not reported", which is the normal answer
            // on a CreateTime topic. Passing it through as a timestamp would
            // date every record to a millisecond before the epoch.
            timestamp: Some(result.log_append_time_ms).filter(|ts| *ts >= 0),
        })
    }
}

/// Whether a **rejection** earns another attempt.
///
/// Only ever consulted for [`Attempt::Rejected`]; an ambiguous failure returns
/// before reaching it. Split out so the rule is testable without a broker,
/// because the case that motivated it — a leader election between our metadata
/// and our request — is not reproducible in a single-broker fixture.
fn may_retry(error: &Error, attempt: u32, policy: &RetryPolicy) -> bool {
    error.retriable() && policy.should_retry(attempt)
}

/// Wall clock in the milliseconds a record timestamp wants.
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since| i64::try_from(since.as_millis()).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Found by a live run against a second broker implementation, where a
    /// partition's leader had moved between our metadata and our request.
    ///
    /// The producer refused to retry *anything*, on the reasoning that a
    /// retried write can duplicate. That reasoning is right for an ambiguous
    /// failure and wrong for a rejection: a response carrying an error code
    /// proves the record was never appended. The result was that an ordinary
    /// leader election surfaced as a hard delivery failure.
    ///
    /// A single-broker fixture cannot produce this — its one broker is the
    /// leader of everything — which is why it is asserted here as a table
    /// rather than only in the acceptance suite.
    #[test]
    fn a_rejection_that_a_metadata_refresh_would_fix_is_retried() {
        let policy = RetryPolicy::default();
        for code in [
            ErrorCode::NotLeaderOrFollower,
            ErrorCode::LeaderNotAvailable,
            ErrorCode::UnknownTopicOrPartition,
        ] {
            let error = Error::from_code(code, None);
            assert!(
                may_retry(&error, 1, &policy),
                "{code:?} is a rejection a refresh fixes; it must be retried"
            );
            assert!(
                error.needs_metadata_refresh(),
                "{code:?} must invalidate the snapshot that misrouted us"
            );
        }
    }

    #[test]
    fn a_rejection_no_retry_can_fix_is_surfaced_immediately() {
        let policy = RetryPolicy::default();
        for code in [
            ErrorCode::RecordListTooLarge,
            ErrorCode::InvalidTopicException,
            ErrorCode::TopicAuthorizationFailed,
        ] {
            let error = Error::from_code(code, None);
            assert!(
                !may_retry(&error, 1, &policy),
                "{code:?} will fail identically next time; retrying only delays the error"
            );
        }
    }

    #[test]
    fn retries_are_bounded() {
        let policy = RetryPolicy {
            max_attempts: 3,
            ..RetryPolicy::default()
        };
        let error = Error::from_code(ErrorCode::NotLeaderOrFollower, None);
        assert!(may_retry(&error, 1, &policy));
        assert!(may_retry(&error, 2, &policy));
        assert!(
            !may_retry(&error, 3, &policy),
            "attempt 3 of 3 is the last one"
        );
        assert!(
            !may_retry(&error, 1, &RetryPolicy::none()),
            "a no-retry policy means exactly one attempt"
        );
    }

    /// The other half of the rule, and the one that costs data if it breaks.
    ///
    /// `may_retry` is never consulted for an ambiguous failure — `send`
    /// returns before reaching it. This asserts the classification that makes
    /// that safe: a timeout is retriable *as a read*, so anything that routed
    /// produce failures through the generic retriable check would re-send on
    /// timeout and duplicate the record with no error anywhere.
    #[test]
    fn a_timeout_is_retriable_in_general_which_is_exactly_why_produce_must_not_use_that() {
        let timeout = Error::Timeout {
            api_key: kafka_conn::ApiKey::Produce,
            elapsed: std::time::Duration::from_secs(1),
        };
        assert!(
            timeout.retriable(),
            "if this ever becomes false the comment above is stale, but the \
             producer must still not re-send on it"
        );
    }

    #[test]
    fn the_clock_is_a_plausible_millisecond_timestamp() {
        // Guards the conversion, not the clock: a truncating cast here would
        // stamp every record with a wrapped or zero timestamp, and a UI
        // sorting by time would silently disagree with the log.
        let now = now_millis();
        assert!(now > 1_700_000_000_000, "timestamp is not in milliseconds");
    }
}
