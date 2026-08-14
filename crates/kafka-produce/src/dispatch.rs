//! One `Produce` round trip: build the request, send it to each leader, and
//! report a result per partition.
//!
//! Split out of `producer.rs` at M13. The accumulator sends *groups* of batches
//! — one batch per partition, several partitions per broker — and M12's
//! single-partition path turned out to be the degenerate case of that rather
//! than a different operation. Grouping is why the request count scales with
//! broker count instead of partition count.

use std::collections::HashMap;
use tokio::time::Instant;

use bytes::Bytes;
use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_conn::protocol::messages::{ProduceRequest, ProduceResponse, TopicName, TransactionalId};
use kafka_conn::{Error, ErrorCode, Result};
use kafka_meta::{Cluster, RetryPolicy, TopicId};

use crate::config::ProducerConfig;

/// The first `Produce` version that identifies topics by uuid rather than name.
///
/// The same transition `Fetch` made at its own v13, and the same failure if it
/// is missed: the broker sees a nil id and answers `UNKNOWN_TOPIC_ID`, which
/// reads like a deleted topic rather than a client asking the wrong question.
const PRODUCE_TOPIC_ID_VERSION: i16 = 13;

/// One partition's encoded batch, waiting for the wire.
#[derive(Debug)]
pub(crate) struct Outbound {
    /// Topic the batch belongs to.
    pub topic: String,
    /// Partition index within that topic.
    pub partition: i32,
    /// The encoded v2 record batch.
    pub encoded: Bytes,
    /// When this batch's most impatient record gives up (#21).
    ///
    /// Bounds the retry loop below and the broker-side `timeout_ms` above:
    /// there is no sense asking a leader to spend thirty seconds collecting
    /// acknowledgements for records that will be failed in five.
    pub deadline: Instant,
}

impl Outbound {
    /// The key this batch is tracked and resolved by.
    fn key(&self) -> (String, i32) {
        (self.topic.clone(), self.partition)
    }
}

/// What the broker said about one partition's batch.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Ack {
    /// Offset the batch's first record landed at.
    pub base_offset: i64,
    /// Broker-assigned append time, or `None` on a `CreateTime` topic.
    pub log_append_time_ms: Option<i64>,
}

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

impl Attempt {
    fn into_error(self) -> Error {
        match self {
            Attempt::Rejected(error) | Attempt::Ambiguous(error) => error,
        }
    }

    /// Whether another attempt is allowed.
    ///
    /// A rejection may always be repeated: a response carrying an error code
    /// proves the records were never appended.
    ///
    /// An ambiguous failure may be repeated **only under idempotence**. The
    /// records may already be in the log, and without a producer id and
    /// sequence the broker cannot tell a re-send from a new batch — so
    /// retrying is how a duplicate is written. With them it recognises the
    /// batch and answers with the original offsets, which is the entire point
    /// of M14 and the reason a leader election stops being a delivery failure.
    ///
    /// The budget depends on what failed. An error that says the *partition
    /// leader moved* — `NOT_LEADER_OR_FOLLOWER`, a connection refused by a
    /// broker that just died — is bounded by
    /// [`RetryPolicy::coordinator_timeout`], not by the attempt count: an
    /// election is a cluster-side event whose duration has nothing to do with
    /// our backoff curve, and five attempts expire ~1.5s into one. That is
    /// how "ride out a leader restart" lost 1,408 of 20,000 records on a
    /// loaded runner — every retry dialled the old leader before the new one
    /// existed, and the attempts ran out. Everything else stays
    /// attempt-bounded.
    fn retriable(
        &self,
        attempt: u32,
        started: Instant,
        policy: &RetryPolicy,
        idempotent: bool,
        deadline: Instant,
    ) -> bool {
        let error = match self {
            Attempt::Rejected(error) | Attempt::Ambiguous(error) => error,
        };
        // The delivery deadline outranks both budgets, in both directions: it
        // stops a leader-election retry that would outlive the records it is
        // for, and — since it is usually the longer of the two — it is what
        // lets a producer ride out an outage the attempt count alone gave up
        // on. Expiring here returns the *attempt's* error rather than a
        // timeout, so an ambiguous send stays ambiguous (#21).
        if Instant::now() >= deadline {
            return false;
        }
        let budget_left = if error.needs_metadata_refresh() {
            started.elapsed() < policy.coordinator_timeout
        } else {
            policy.should_retry(attempt)
        };
        match self {
            Attempt::Rejected(_) => error.retriable() && budget_left,
            Attempt::Ambiguous(_) => idempotent && error.retriable() && budget_left,
        }
    }
}

/// Sends encoded batches and reports one result per partition.
#[derive(Debug, Clone)]
pub(crate) struct Dispatcher {
    cluster: Cluster,
    config: ProducerConfig,
}

impl Dispatcher {
    pub(crate) fn new(cluster: Cluster, config: ProducerConfig) -> Self {
        Self { cluster, config }
    }

    pub(crate) fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    /// Send every batch, retrying the ones the broker *rejected*, and return a
    /// result for each.
    ///
    /// Never returns fewer results than it was given batches: a partition that
    /// could not be routed, was rejected past its retry budget, or ended
    /// ambiguous still gets an entry. That is CLAUDE.md rule 4 in the write
    /// direction — one partition whose leader is mid-election must not decide
    /// the fate of the five batches that travelled with it.
    ///
    /// # Cancel safety
    ///
    /// Dropping this future abandons the in-flight requests the same way
    /// dropping a single `Connection::send` does: the connection either
    /// completes and discards the response or closes. Callers that need the
    /// records resolved regardless run this inside a task rather than inline,
    /// which is what the accumulator does.
    pub(crate) async fn dispatch(
        &self,
        batches: Vec<Outbound>,
    ) -> Vec<((String, i32), Result<Ack>)> {
        let mut resolved: Vec<((String, i32), Result<Ack>)> = Vec::with_capacity(batches.len());
        let mut pending = batches;
        let mut attempt: u32 = 1;
        let started = Instant::now();

        while !pending.is_empty() {
            let mut retry: Vec<Outbound> = Vec::new();
            let mut refresh = false;

            for (key, outcome, outbound) in self.round(std::mem::take(&mut pending)).await {
                match outcome {
                    Ok(ack) => resolved.push((key, Ok(ack))),
                    Err(failure) => {
                        let error = match &failure {
                            Attempt::Rejected(error) | Attempt::Ambiguous(error) => error,
                        };
                        if error.needs_metadata_refresh() {
                            refresh = true;
                        }
                        let may_retry = failure.retriable(
                            attempt,
                            started,
                            &self.config.retry,
                            self.config.idempotent,
                            outbound
                                .as_ref()
                                .map(|outbound| outbound.deadline)
                                .unwrap_or_else(Instant::now),
                        );
                        match (may_retry, outbound) {
                            (true, Some(outbound)) => retry.push(outbound),
                            (_, _) => resolved.push((key, Err(failure.into_error()))),
                        }
                    }
                }
            }

            if retry.is_empty() {
                break;
            }
            if refresh {
                self.cluster.invalidate();
            }

            // Backoff before re-resolving, not just before re-sending. The
            // point of the pause is to give the cluster time to agree on the
            // new leader; retrying instantly re-reads the same stale metadata
            // and fails the same way.
            let delay = self.config.retry.delay(attempt.saturating_add(1));
            tracing::debug!(
                batches = retry.len(),
                attempt,
                ?delay,
                "produce rejected; refreshing metadata and retrying"
            );
            if !delay.is_zero() {
                // Never sleep past the nearest deadline: waking to send a
                // request whose records have already expired wastes the
                // cluster's time and the caller's.
                let nearest = retry.iter().map(|outbound| outbound.deadline).min();
                match nearest {
                    Some(nearest) => {
                        tokio::time::sleep_until(nearest.min(Instant::now() + delay)).await
                    }
                    None => tokio::time::sleep(delay).await,
                }
            }
            attempt = attempt.saturating_add(1);
            pending = retry;
        }

        resolved
    }

    /// One pass: route every batch to its leader, send one request per leader,
    /// and report what came back.
    ///
    /// The `Outbound` is handed back alongside a failure so the caller can put
    /// it in the retry set without re-encoding it. Re-encoding would re-read
    /// the clock and give a retried record a different timestamp from the one
    /// its caller was told about.
    #[allow(clippy::type_complexity)]
    async fn round(
        &self,
        batches: Vec<Outbound>,
    ) -> Vec<(
        (String, i32),
        std::result::Result<Ack, Attempt>,
        Option<Outbound>,
    )> {
        let mut groups: HashMap<i32, Vec<Outbound>> = HashMap::new();
        let mut results = Vec::new();

        for outbound in batches {
            match self
                .cluster
                .leader_for(&outbound.topic, outbound.partition)
                .await
            {
                Ok(leader) => groups.entry(leader).or_default().push(outbound),
                // Routing failed, so nothing was sent — a rejection, and
                // retrying after a refresh is free.
                Err(error) => {
                    let key = outbound.key();
                    results.push((key, Err(Attempt::Rejected(error)), Some(outbound)));
                }
            }
        }

        let sends = groups
            .into_iter()
            .map(|(leader, group)| self.send_to(leader, group));
        for group_results in futures::future::join_all(sends).await {
            results.extend(group_results);
        }

        results
    }

    /// Send one request carrying every batch bound for one broker.
    #[allow(clippy::type_complexity)]
    async fn send_to(
        &self,
        leader: i32,
        group: Vec<Outbound>,
    ) -> Vec<(
        (String, i32),
        std::result::Result<Ack, Attempt>,
        Option<Outbound>,
    )> {
        // A failure before the request exists applies to the whole group and
        // is a rejection: nothing reached the wire.
        macro_rules! reject_group {
            ($error:expr) => {{
                let error = $error;
                return group
                    .into_iter()
                    .map(|outbound| {
                        let key = outbound.key();
                        (key, Err(Attempt::Rejected(error.clone())), Some(outbound))
                    })
                    .collect();
            }};
        }

        let version = match self.cluster.negotiated_for::<ProduceRequest>().await {
            Ok(version) => version,
            Err(error) => reject_group!(error),
        };

        let request = match self.build_request(&group, version).await {
            Ok(request) => request,
            Err(error) => reject_group!(error),
        };

        let connection = match self.cluster.pool().get(leader).await {
            Ok(connection) => connection,
            Err(error) => reject_group!(error),
        };

        tracing::debug!(
            leader,
            version,
            partitions = group.len(),
            "producing a grouped batch"
        );

        // From here the request is on the wire and any failure is ambiguous.
        match connection.send(request).await {
            Ok(response) => read_response(response, group),
            Err(error) => group
                .into_iter()
                .map(|outbound| {
                    let key = outbound.key();
                    // Unknown outcome: the records may well have been written
                    // and the acknowledgement lost. The batch is handed back so
                    // an *idempotent* producer can re-send it — the broker
                    // deduplicates on its sequence. `Attempt::retriable` is
                    // what refuses this for a non-idempotent one, and it is the
                    // only thing standing between a timeout and a duplicate.
                    (key, Err(Attempt::Ambiguous(error.clone())), Some(outbound))
                })
                .collect(),
        }
    }

    /// Build one request covering every batch in the group.
    async fn build_request(&self, group: &[Outbound], version: i16) -> Result<ProduceRequest> {
        let mut by_topic: HashMap<String, Vec<PartitionProduceData>> = HashMap::new();
        for outbound in group {
            by_topic.entry(outbound.topic.clone()).or_default().push(
                PartitionProduceData::default()
                    .with_index(outbound.partition)
                    .with_records(Some(outbound.encoded.clone())),
            );
        }

        let mut topic_data = Vec::with_capacity(by_topic.len());
        for (topic, partitions) in by_topic {
            let mut data = TopicProduceData::default().with_partition_data(partitions);

            // Set exactly the field this version has. The codec rejects a field
            // outside its own version range rather than ignoring it, so "set
            // both and let the encoder pick" is an encode error, not belt and
            // braces.
            if version >= PRODUCE_TOPIC_ID_VERSION {
                let topic_id = self.topic_id(&topic).await?;
                if topic_id.is_zero() {
                    return Err(Error::InvalidRequest(format!(
                        "{topic}: Produce v{version} identifies topics by id, and no topic id is known"
                    )));
                }
                data = data.with_topic_id(uuid::Uuid::from_bytes(*topic_id.as_bytes()));
            } else {
                data = data.with_name(TopicName(StrBytes::from_string(topic)));
            }
            topic_data.push(data);
        }

        // The broker's own budget for collecting acknowledgements is what
        // remains of the nearest record's delivery deadline, never more than
        // the configured timeout (#21): a leader holding a request open past
        // the moment its records expire is working for nobody.
        let remaining = group
            .iter()
            .map(|outbound| outbound.deadline)
            .min()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
            .unwrap_or(self.config.delivery_timeout);
        let timeout_ms = i32::try_from(remaining.as_millis())
            .unwrap_or(i32::MAX)
            .min(self.config.delivery_timeout_ms())
            .max(0);

        let mut request = ProduceRequest::default()
            .with_acks(self.config.acks.wire())
            .with_timeout_ms(timeout_ms)
            .with_topic_data(topic_data);

        // A transactional produce must name its transaction. Without this the
        // broker sees an ordinary idempotent batch carrying the transactional
        // bit, which is not a combination it will accept.
        if let Some(id) = &self.config.transactional_id {
            request = request
                .with_transactional_id(Some(TransactionalId(StrBytes::from_string(id.clone()))));
        }

        Ok(request)
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
}

/// Pull one result per partition out of the response.
///
/// A partition the broker did not answer for is an ambiguous failure rather
/// than a rejection: the request reached the broker, so we cannot claim the
/// records were not appended.
#[allow(clippy::type_complexity)]
fn read_response(
    response: ProduceResponse,
    group: Vec<Outbound>,
) -> Vec<(
    (String, i32),
    std::result::Result<Ack, Attempt>,
    Option<Outbound>,
)> {
    let mut answers: HashMap<i32, (i16, Option<String>, i64, i64)> = HashMap::new();
    for topic_response in response.responses {
        for partition in topic_response.partition_responses {
            answers.insert(
                partition.index,
                (
                    partition.error_code,
                    partition.error_message.map(|message| message.to_string()),
                    partition.base_offset,
                    partition.log_append_time_ms,
                ),
            );
        }
    }

    group
        .into_iter()
        .map(|outbound| {
            let key = outbound.key();
            match answers.get(&outbound.partition) {
                Some((code, message, base_offset, log_append_time_ms)) => {
                    if let Some(code) = ErrorCode::from_code(*code) {
                        // A response carrying an error code means the records
                        // were not appended, which is what makes another
                        // attempt safe.
                        let error = Error::from_code(code, message.clone());
                        (key, Err(Attempt::Rejected(error)), Some(outbound))
                    } else {
                        (
                            key,
                            Ok(Ack {
                                base_offset: *base_offset,
                                // -1 is the protocol's "not reported", which is
                                // the normal answer on a CreateTime topic.
                                // Passing it through as a timestamp would date
                                // every record to a millisecond before the
                                // epoch.
                                log_append_time_ms: Some(*log_append_time_ms).filter(|ts| *ts >= 0),
                            }),
                            None,
                        )
                    }
                }
                None => {
                    let error = Error::InvalidRequest(format!(
                        "{}-{}: the broker did not answer a partition we sent",
                        outbound.topic, outbound.partition
                    ));
                    (key, Err(Attempt::Ambiguous(error)), Some(outbound))
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn outbound(topic: &str, partition: i32) -> Outbound {
        Outbound {
            topic: topic.to_owned(),
            partition,
            encoded: Bytes::from_static(b"batch"),
            deadline: Instant::now() + Duration::from_secs(30),
        }
    }

    /// A deadline far enough out that it is never what a test is measuring.
    fn open_ended() -> Instant {
        Instant::now() + Duration::from_secs(300)
    }

    /// The rule that costs data if it breaks: an ambiguous failure is never
    /// handed back for retry, at any retry setting.
    #[test]
    fn an_ambiguous_failure_is_never_retriable() {
        let policy = RetryPolicy::default();
        let timeout = Error::Timeout {
            api_key: kafka_conn::ApiKey::Produce,
            elapsed: std::time::Duration::from_secs(1),
        };
        assert!(
            timeout.retriable(),
            "a timeout is retriable as a read, which is exactly why produce \
             must not route through that check"
        );
        assert!(
            !Attempt::Ambiguous(timeout.clone()).retriable(
                1,
                Instant::now(),
                &policy,
                false,
                open_ended()
            ),
            "without a producer id, re-sending an unknown outcome duplicates"
        );
        assert!(
            Attempt::Ambiguous(timeout).retriable(1, Instant::now(), &policy, true, open_ended()),
            "with one, the broker deduplicates the re-send — that is M14"
        );
    }

    /// #21: the delivery deadline outranks both retry budgets. An expired
    /// batch stops retrying even where the attempt budget and the coordinator
    /// deadline would both allow another go.
    #[test]
    fn an_expired_deadline_stops_a_retry_the_budgets_would_allow() {
        let policy = RetryPolicy::default();
        let refresh = Error::from_code(ErrorCode::NotLeaderOrFollower, None);
        assert!(
            Attempt::Rejected(refresh.clone()).retriable(
                1,
                Instant::now(),
                &policy,
                false,
                open_ended()
            ),
            "with budget left this is the retry that rides out an election"
        );
        assert!(
            !Attempt::Rejected(refresh).retriable(
                1,
                Instant::now(),
                &policy,
                false,
                Instant::now()
            ),
            "past the deadline there is nobody left to deliver to"
        );
    }

    /// And the expiry must not launder an ambiguous outcome into a clean
    /// failure: a non-idempotent producer's duplicate-safety rests on the
    /// caller being told the write *may* have landed.
    #[test]
    fn expiry_preserves_the_ambiguous_verdict() {
        let policy = RetryPolicy::default();
        let error = Error::Timeout {
            api_key: kafka_conn::ApiKey::Produce,
            elapsed: Duration::from_secs(1),
        };
        let attempt = Attempt::Ambiguous(error);
        assert!(!attempt.retriable(1, Instant::now(), &policy, true, Instant::now()));
        assert!(
            matches!(attempt, Attempt::Ambiguous(_)),
            "the verdict the caller is handed still says the outcome is unknown"
        );
    }

    #[test]
    fn a_rejection_a_refresh_would_fix_is_retriable() {
        let policy = RetryPolicy::default();
        for code in [
            ErrorCode::NotLeaderOrFollower,
            ErrorCode::LeaderNotAvailable,
            ErrorCode::UnknownTopicOrPartition,
        ] {
            let error = Error::from_code(code, None);
            assert!(error.needs_metadata_refresh(), "{code:?} must refresh");
            assert!(
                Attempt::Rejected(error).retriable(1, Instant::now(), &policy, false, open_ended()),
                "{code:?}"
            );
        }
    }

    /// The regression from the leader-restart acceptance test: a leader
    /// election outlives the attempt budget, so an error that a metadata
    /// refresh would fix is bounded by *time*, not by attempts — and an
    /// election still in progress at the deadline finally fails.
    #[test]
    fn a_leader_handover_is_bounded_by_time_not_attempts() {
        let policy = RetryPolicy::default();
        let refresh = Error::from_code(ErrorCode::NotLeaderOrFollower, None);
        assert!(
            Attempt::Rejected(refresh.clone()).retriable(
                policy.max_attempts + 100,
                Instant::now(),
                &policy,
                false,
                open_ended()
            ),
            "attempts must not expire a retry the coordinator_timeout still allows"
        );
        // A zeroed budget, rather than an `Instant` wound backwards — the
        // subtraction panics on a machine that booted more recently than the
        // budget is long, and CI runners are exactly such machines.
        let expired = RetryPolicy {
            coordinator_timeout: Duration::ZERO,
            ..RetryPolicy::default()
        };
        assert!(
            !Attempt::Rejected(refresh).retriable(1, Instant::now(), &expired, false, open_ended()),
            "and the deadline is still a deadline"
        );

        // An attempt-bounded error stays attempt-bounded.
        let plain = Error::from_code(ErrorCode::CorruptMessage, None);
        assert!(!plain.needs_metadata_refresh());
        assert!(
            !Attempt::Rejected(plain).retriable(
                policy.max_attempts,
                Instant::now(),
                &policy,
                false,
                open_ended()
            ),
            "a non-handover error must not inherit the longer budget"
        );
    }

    #[test]
    fn a_rejection_no_retry_can_fix_is_not() {
        let policy = RetryPolicy::default();
        for code in [
            ErrorCode::RecordListTooLarge,
            ErrorCode::InvalidTopicException,
            ErrorCode::TopicAuthorizationFailed,
        ] {
            let error = Error::from_code(code, None);
            assert!(
                !Attempt::Rejected(error).retriable(1, Instant::now(), &policy, true, open_ended()),
                "{code:?}"
            );
        }
    }

    /// Rule 4 in the write direction, at the layer that decides it: one
    /// partition's error code must not touch the partitions that travelled
    /// with it in the same request.
    #[test]
    fn one_partitions_rejection_leaves_its_group_mates_acknowledged() {
        use kafka_conn::protocol::messages::produce_response::{
            PartitionProduceResponse, TopicProduceResponse,
        };

        let response = ProduceResponse::default().with_responses(vec![
            TopicProduceResponse::default().with_partition_responses(vec![
                PartitionProduceResponse::default()
                    .with_index(0)
                    .with_base_offset(100)
                    .with_log_append_time_ms(-1),
                PartitionProduceResponse::default()
                    .with_index(1)
                    .with_error_code(ErrorCode::MessageTooLarge.code()),
                PartitionProduceResponse::default()
                    .with_index(2)
                    .with_base_offset(200)
                    .with_log_append_time_ms(-1),
            ]),
        ]);

        let results = read_response(
            response,
            vec![outbound("t", 0), outbound("t", 1), outbound("t", 2)],
        );

        assert_eq!(results.len(), 3);
        let by_partition: HashMap<i32, &std::result::Result<Ack, Attempt>> = results
            .iter()
            .map(|((_, partition), outcome, _)| (*partition, outcome))
            .collect();

        assert!(matches!(by_partition.get(&0), Some(Ok(ack)) if ack.base_offset == 100));
        assert!(matches!(
            by_partition.get(&1),
            Some(Err(Attempt::Rejected(_)))
        ));
        assert!(matches!(by_partition.get(&2), Some(Ok(ack)) if ack.base_offset == 200));
    }

    /// A `CreateTime` topic answers -1, and that is not a timestamp.
    #[test]
    fn an_unreported_append_time_is_none_not_a_pre_epoch_instant() {
        use kafka_conn::protocol::messages::produce_response::{
            PartitionProduceResponse, TopicProduceResponse,
        };

        let response = ProduceResponse::default().with_responses(vec![
            TopicProduceResponse::default().with_partition_responses(vec![
                PartitionProduceResponse::default()
                    .with_index(0)
                    .with_base_offset(0)
                    .with_log_append_time_ms(-1),
            ]),
        ]);

        let results = read_response(response, vec![outbound("t", 0)]);
        let (_, outcome, _) = results.into_iter().next().expect("one result");
        match outcome {
            Ok(ack) => assert_eq!(ack.log_append_time_ms, None),
            Err(other) => panic!("expected an ack, got {other:?}"),
        }
    }

    /// A partition missing from the response is ambiguous, not a rejection —
    /// the request reached the broker, so we cannot claim nothing was written.
    #[test]
    fn a_partition_the_broker_ignored_is_ambiguous() {
        let response = ProduceResponse::default();
        let results = read_response(response, vec![outbound("t", 7)]);
        let (_, outcome, retry) = results.into_iter().next().expect("one result");
        assert!(matches!(outcome, Err(Attempt::Ambiguous(_))));

        // The batch *is* handed back, because an idempotent producer can
        // safely re-send it. Whether it actually is re-sent is
        // `Attempt::retriable`'s decision, and that is where the
        // non-idempotent refusal lives — see
        // `an_ambiguous_failure_is_never_retriable`.
        assert!(
            retry.is_some(),
            "an ambiguous batch must still be available to an idempotent retry"
        );
    }
}
