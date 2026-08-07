//! Transactions: a fenced producer id, a set of enrolled partitions, and an
//! atomic end.
//!
//! # The three rules that are easy to get wrong
//!
//! 1. **`AddPartitionsToTxn` must precede the first produce to each
//!    partition.** The coordinator has to know which partitions the
//!    transaction touches before it can write markers to them. Skipping it
//!    gets the produce rejected in a way that reads like a permissions
//!    problem, not a protocol-order problem.
//! 2. **`AddPartitionsToTxn` is version-shaped, and the client ceiling is v3.**
//!    v4 (KIP-890) replaced the flat request with a `transactions` array for
//!    brokers batching several transactions into one call. The clamp lives on
//!    the `Rpc` impl in `kafka-conn` so no call site has to remember it.
//! 3. **`PRODUCER_FENCED` is terminal.** A second producer sharing our
//!    transactional id has bumped the epoch, and every later request of ours
//!    will fail the same way. Retrying is an infinite loop; the only correct
//!    response is to stop and say so.

use std::collections::HashSet;
use std::sync::Mutex;
use std::time::Duration;

use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use kafka_conn::protocol::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use kafka_conn::protocol::messages::{
    AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, EndTxnRequest, GroupId,
    InitProducerIdRequest, TopicName, TransactionalId, TxnOffsetCommitRequest,
};
use kafka_conn::{Error, ErrorCode, Result};
use kafka_meta::{Cluster, ConsumerGroupMetadata, CoordinatorKind};

use crate::idempotence::ProducerIdentity;

/// How long the coordinator may leave our transaction open before aborting it
/// itself. Kafka's own client default.
const TRANSACTION_TIMEOUT_MS: i32 = 60_000;

/// How many times to re-ask when the coordinator is not ready.
const COORDINATOR_ATTEMPTS: u32 = 12;

/// How long to wait between those attempts.
const COORDINATOR_BACKOFF: Duration = Duration::from_millis(500);

/// Whether an error code means "ask the coordinator again", rather than
/// "this failed".
///
/// `CONCURRENT_TRANSACTIONS` is in here for a different reason from the other
/// three: the coordinator is not *unavailable*, it is still finalising the
/// previous transaction for this id. Committing and immediately beginning
/// again — which is what any transactional loop does — hits it routinely, and
/// treating it as fatal makes the second transaction fail on a healthy
/// cluster. Java retries it the same way.
///
/// # Why this retry lives here and not in `Cluster::dispatch`
///
/// `dispatch` already retries coordinator errors and invalidates the cached
/// coordinator — but only for errors it can *see*, and it sees only transport
/// failures. A broker that answers `NOT_COORDINATOR` does so **inside a
/// successful response body**, which reaches `dispatch` as `Ok(response)` and
/// sails straight through the retry loop to the caller that reads
/// `error_code`. That caller is this file.
///
/// The case that made it necessary is not exotic: `__transaction_state` is
/// created lazily on the first `FindCoordinator` for a transactional id, so
/// the very first `InitProducerId` against a cluster that has never run a
/// transaction races the topic's own creation. Found on the first live run.
fn coordinator_not_ready(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::NotCoordinator
            | ErrorCode::CoordinatorNotAvailable
            | ErrorCode::CoordinatorLoadInProgress
            | ErrorCode::ConcurrentTransactions
    )
}

/// A producer's transactional state.
///
/// Behind a `Mutex` rather than owned by the accumulator task because both
/// sides need it: the caller drives `begin`/`commit`/`abort`, and the actor
/// reads the identity to stamp batches and records which partitions it has
/// already enrolled.
#[derive(Debug)]
pub(crate) struct Transactions {
    transactional_id: String,
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    identity: Option<ProducerIdentity>,
    open: bool,
    enrolled: HashSet<(String, i32)>,
    /// Once set, every operation fails with the same error. A fenced producer
    /// cannot be un-fenced; the caller has to build a new one.
    fenced: bool,
}

impl Transactions {
    pub(crate) fn new(transactional_id: String) -> Self {
        Self {
            transactional_id,
            state: Mutex::new(State::default()),
        }
    }

    /// A poisoned lock means a panic happened while holding it. Rule 2 forbids
    /// unwrapping, and there is a real answer here: the producer's state can no
    /// longer be trusted, which is what fencing already means.
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state.lock().map_err(|_| {
            Error::InvalidRequest(
                "the transactional state is poisoned; build a new producer".to_owned(),
            )
        })
    }

    pub(crate) fn identity(&self) -> Option<ProducerIdentity> {
        self.lock().ok().and_then(|state| state.identity)
    }

    pub(crate) fn is_open(&self) -> bool {
        self.lock().is_ok_and(|state| state.open)
    }

    fn check_live(state: &State) -> Result<()> {
        if state.fenced {
            return Err(Error::from_code(
                ErrorCode::ProducerFenced,
                Some(
                    "another producer with this transactional id has taken over; \
                     this one is permanently fenced"
                        .to_owned(),
                ),
            ));
        }
        Ok(())
    }

    /// Note a fencing error so every later call fails the same way rather than
    /// retrying into a wall.
    fn note(&self, error: &Error) {
        let fencing = matches!(
            error.code(),
            Some(ErrorCode::ProducerFenced | ErrorCode::InvalidProducerEpoch)
        );
        if fencing && let Ok(mut state) = self.lock() {
            state.fenced = true;
            state.open = false;
        }
    }

    /// One transaction-coordinator round trip, re-asked while it is not ready.
    ///
    /// `code_of` pulls the decisive error code out of the response, because
    /// where it lives differs per api — `AddPartitionsToTxn` v3 reports per
    /// partition and leaves the top level at zero.
    async fn call<R, F>(&self, cluster: &Cluster, request: R, code_of: F) -> Result<R::Response>
    where
        R: kafka_conn::Rpc + Clone,
        F: Fn(&R::Response) -> i16,
    {
        self.call_coordinator(
            cluster,
            CoordinatorKind::Transaction,
            &self.transactional_id.clone(),
            request,
            code_of,
        )
        .await
    }

    /// The same round trip against whichever coordinator owns the request.
    ///
    /// Split out for KIP-447, which is the one operation that touches *both*:
    /// `AddOffsetsToTxn` goes to the transaction coordinator and
    /// `TxnOffsetCommit` to the group's, inside one transaction. Both hops want
    /// the same re-ask, because "the coordinator moved" is the same answer from
    /// either.
    async fn call_coordinator<R, F>(
        &self,
        cluster: &Cluster,
        kind: CoordinatorKind,
        key: &str,
        request: R,
        code_of: F,
    ) -> Result<R::Response>
    where
        R: kafka_conn::Rpc + Clone,
        F: Fn(&R::Response) -> i16,
    {
        let mut attempt = 1;
        loop {
            let response = cluster
                .send_to_coordinator(kind, key, request.clone())
                .await?;

            let Some(code) = ErrorCode::from_code(code_of(&response)) else {
                return Ok(response);
            };

            if coordinator_not_ready(code) && attempt < COORDINATOR_ATTEMPTS {
                // Drop the cached coordinator only when it is the *coordinator*
                // that is wrong. `CONCURRENT_TRANSACTIONS` comes from the right
                // broker, which is merely busy; re-resolving it would throw
                // away a correct answer and ask the same node again anyway.
                if code != ErrorCode::ConcurrentTransactions {
                    cluster.invalidate_coordinator(kind, key);
                }
                tracing::debug!(
                    %code,
                    attempt,
                    ?kind,
                    "the coordinator is not ready; re-asking"
                );
                tokio::time::sleep(COORDINATOR_BACKOFF).await;
                attempt += 1;
                continue;
            }

            let error = Error::from_code(code, None);
            self.note(&error);
            return Err(error);
        }
    }

    /// Claim the transactional producer id, fencing any previous holder.
    ///
    /// This is where a *previous* instance of this producer gets fenced: the
    /// coordinator bumps the epoch, and the old one's next request fails. That
    /// is the intended behaviour of a transactional id, not a side effect.
    pub(crate) async fn init(&self, cluster: &Cluster) -> Result<()> {
        {
            let state = self.lock()?;
            Self::check_live(&state)?;
        }

        let request = InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId(StrBytes::from_string(
                self.transactional_id.clone(),
            ))))
            .with_transaction_timeout_ms(TRANSACTION_TIMEOUT_MS);

        let response = self
            .call(cluster, request, |response| response.error_code)
            .await?;

        let mut state = self.lock()?;
        state.identity = Some(ProducerIdentity {
            producer_id: response.producer_id.0,
            producer_epoch: response.producer_epoch,
        });
        state.enrolled.clear();
        state.open = false;
        Ok(())
    }

    /// Open a transaction.
    ///
    /// Local only: the protocol has no "begin" request. The coordinator learns
    /// a transaction exists from the first `AddPartitionsToTxn`.
    pub(crate) fn begin(&self) -> Result<()> {
        let mut state = self.lock()?;
        Self::check_live(&state)?;
        if state.identity.is_none() {
            return Err(Error::InvalidRequest(
                "init_transactions must be called before begin_transaction".to_owned(),
            ));
        }
        if state.open {
            return Err(Error::InvalidRequest(
                "a transaction is already open; commit or abort it first".to_owned(),
            ));
        }
        state.open = true;
        state.enrolled.clear();
        Ok(())
    }

    /// Which of these partitions the coordinator has not been told about yet.
    pub(crate) fn unenrolled(&self, partitions: &[(String, i32)]) -> Vec<(String, i32)> {
        let Ok(state) = self.lock() else {
            return Vec::new();
        };
        if !state.open {
            return Vec::new();
        }
        partitions
            .iter()
            .filter(|key| !state.enrolled.contains(*key))
            .cloned()
            .collect()
    }

    /// Tell the coordinator which partitions this transaction touches.
    ///
    /// Must complete before the first produce to each of them.
    pub(crate) async fn enrol(
        &self,
        cluster: &Cluster,
        partitions: &[(String, i32)],
    ) -> Result<()> {
        if partitions.is_empty() {
            return Ok(());
        }

        let identity = {
            let state = self.lock()?;
            Self::check_live(&state)?;
            state.identity.ok_or_else(|| {
                Error::InvalidRequest("no transactional producer id has been claimed".to_owned())
            })?
        };

        let mut by_topic: std::collections::HashMap<String, Vec<i32>> =
            std::collections::HashMap::new();
        for (topic, partition) in partitions {
            by_topic.entry(topic.clone()).or_default().push(*partition);
        }

        // The v3-and-below shape. `Rpc::CLIENT_MAX_VERSION` keeps the
        // negotiated version at 3 or below, so these are the fields that get
        // encoded; setting the v4 `transactions` array here would be a field
        // outside the negotiated version and the codec would refuse it.
        let request = AddPartitionsToTxnRequest::default()
            .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_string(
                self.transactional_id.clone(),
            )))
            .with_v3_and_below_producer_id(kafka_conn::protocol::messages::ProducerId(
                identity.producer_id,
            ))
            .with_v3_and_below_producer_epoch(identity.producer_epoch)
            .with_v3_and_below_topics(
                by_topic
                    .into_iter()
                    .map(|(topic, partitions)| {
                        AddPartitionsToTxnTopic::default()
                            .with_name(TopicName(StrBytes::from_string(topic)))
                            .with_partitions(partitions)
                    })
                    .collect(),
            );

        let response = self
            .call(cluster, request, |response| {
                // v3 and below report per-partition codes and leave the
                // top-level one at zero, so the retry decision has to look at
                // the partitions.
                response
                    .results_by_topic_v3_and_below
                    .iter()
                    .flat_map(|topic| &topic.results_by_partition)
                    .map(|partition| partition.partition_error_code)
                    .find(|code| *code != 0)
                    .unwrap_or(0)
            })
            .await?;

        // Per-partition results, so one partition's failure is reported as
        // that partition's — rule 4 applies here too.
        for topic in response.results_by_topic_v3_and_below {
            for partition in topic.results_by_partition {
                if let Some(code) = ErrorCode::from_code(partition.partition_error_code) {
                    let error = Error::from_code(
                        code,
                        Some(format!(
                            "{}-{}: could not be added to the transaction",
                            topic.name.0, partition.partition_index
                        )),
                    );
                    self.note(&error);
                    return Err(error);
                }
            }
        }

        let mut state = self.lock()?;
        for key in partitions {
            state.enrolled.insert(key.clone());
        }
        Ok(())
    }

    /// KIP-447: put a consumer's offsets inside this transaction.
    ///
    /// Two hops, two coordinators, in this order and no other:
    ///
    /// 1. **`AddOffsetsToTxn`** to the *transaction* coordinator, which enrols
    ///    the `__consumer_offsets` partition backing this group in the
    ///    transaction. Without it the coordinator does not know to write a
    ///    marker there, and the offsets commit outside the transaction — which
    ///    looks like it worked and abandons exactly-once at the first abort.
    /// 2. **`TxnOffsetCommit`** to the *group* coordinator, which stores the
    ///    offsets pending that marker. They stay invisible to an ordinary
    ///    `OffsetFetch` until the transaction commits.
    ///
    /// The offsets are the position of the *next* record to read, which is what
    /// [`crate::Producer::send_offsets_to_transaction`] is documented to take
    /// and what a consumer's own `commit` stores.
    pub(crate) async fn send_offsets(
        &self,
        cluster: &Cluster,
        group: &ConsumerGroupMetadata,
        offsets: &[((String, i32), i64)],
    ) -> Result<()> {
        let identity = {
            let state = self.lock()?;
            Self::check_live(&state)?;
            if !state.open {
                return Err(Error::InvalidRequest(
                    "no transaction is open; offsets can only be sent inside one".to_owned(),
                ));
            }
            state.identity.ok_or_else(|| {
                Error::InvalidRequest("no transactional producer id has been claimed".to_owned())
            })?
        };

        let group_id = GroupId(StrBytes::from_string(group.group_id.clone()));

        let add = AddOffsetsToTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_string(
                self.transactional_id.clone(),
            )))
            .with_producer_id(kafka_conn::protocol::messages::ProducerId(
                identity.producer_id,
            ))
            .with_producer_epoch(identity.producer_epoch)
            .with_group_id(group_id.clone());
        self.call(cluster, add, |response| response.error_code)
            .await?;

        let mut by_topic: std::collections::HashMap<String, Vec<TxnOffsetCommitRequestPartition>> =
            std::collections::HashMap::new();
        for ((topic, partition), offset) in offsets {
            by_topic.entry(topic.clone()).or_default().push(
                TxnOffsetCommitRequestPartition::default()
                    .with_partition_index(*partition)
                    .with_committed_offset(*offset),
            );
        }

        // `member_id`, `generation_id` and `group_instance_id` encode only at
        // v3+; below that the codec refuses anything but their defaults, which
        // is precisely the non-member form. So a standalone consumer's commit
        // encodes at any version and a member's needs a broker from 2.5 on.
        let commit = TxnOffsetCommitRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_string(
                self.transactional_id.clone(),
            )))
            .with_group_id(group_id)
            .with_producer_id(kafka_conn::protocol::messages::ProducerId(
                identity.producer_id,
            ))
            .with_producer_epoch(identity.producer_epoch)
            .with_generation_id(group.generation)
            .with_member_id(StrBytes::from_string(group.member_id.clone()))
            .with_group_instance_id(group.instance_id.clone().map(StrBytes::from_string))
            .with_topics(
                by_topic
                    .into_iter()
                    .map(|(topic, partitions)| {
                        TxnOffsetCommitRequestTopic::default()
                            .with_name(TopicName(StrBytes::from_string(topic)))
                            .with_partitions(partitions)
                    })
                    .collect(),
            );

        let response = self
            .call_coordinator(
                cluster,
                CoordinatorKind::Group,
                &group.group_id,
                commit,
                |response| {
                    // Per partition, like `AddPartitionsToTxn` v3: there is no
                    // top-level code to read.
                    response
                        .topics
                        .iter()
                        .flat_map(|topic| &topic.partitions)
                        .map(|partition| partition.error_code)
                        .find(|code| *code != 0)
                        .unwrap_or(0)
                },
            )
            .await?;

        // Rule 4 says a multi-resource call reports per resource. This one
        // deliberately does not, and the reason is what the API is for: these
        // offsets are one *atomic* unit with the records this transaction
        // wrote. A caller handed "eleven of twelve committed" has no useful
        // move — committing publishes a split state, and the only correct
        // response is the abort it would have to work out for itself. So a
        // single partition's refusal fails the call, naming the partition.
        for topic in &response.topics {
            for partition in &topic.partitions {
                if let Some(code) = ErrorCode::from_code(partition.error_code) {
                    let error = Error::from_code(
                        code,
                        Some(format!(
                            "{}-{}: offset could not be committed in the transaction; \
                             abort it rather than committing a partial one",
                            topic.name.0, partition.partition_index
                        )),
                    );
                    self.note(&error);
                    return Err(error);
                }
            }
        }

        Ok(())
    }

    /// Commit or abort, atomically for every enrolled partition.
    pub(crate) async fn end(&self, cluster: &Cluster, commit: bool) -> Result<()> {
        let identity = {
            let state = self.lock()?;
            Self::check_live(&state)?;
            if !state.open {
                return Err(Error::InvalidRequest("no transaction is open".to_owned()));
            }
            state.identity.ok_or_else(|| {
                Error::InvalidRequest("no transactional producer id has been claimed".to_owned())
            })?
        };

        let request = EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_string(
                self.transactional_id.clone(),
            )))
            .with_producer_id(kafka_conn::protocol::messages::ProducerId(
                identity.producer_id,
            ))
            .with_producer_epoch(identity.producer_epoch)
            .with_committed(commit);

        let response = self
            .call(cluster, request, |response| response.error_code)
            .await?;

        let mut state = self.lock()?;

        // KIP-890 (transaction.version 2): `EndTxn` v5 answers with the
        // producer id and epoch to use for the *next* transaction, because the
        // coordinator bumps the epoch as it finalises this one. Keeping the old
        // epoch makes the next transaction fail with `PRODUCER_FENCED` — the
        // producer fenced by its own previous incarnation, which reads like a
        // second instance racing us and is not.
        //
        // Older brokers leave both at -1, so the guard is what keeps this from
        // clobbering a valid identity with a sentinel.
        if response.producer_id.0 >= 0 && response.producer_epoch >= 0 {
            state.identity = Some(ProducerIdentity {
                producer_id: response.producer_id.0,
                producer_epoch: response.producer_epoch,
            });
        }

        state.open = false;
        state.enrolled.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn txn() -> Transactions {
        Transactions::new("t-1".to_owned())
    }

    #[test]
    fn begin_needs_an_id_first() {
        let txn = txn();
        let error = txn.begin().expect_err("no id claimed yet");
        assert!(error.to_string().contains("init_transactions"));
    }

    #[test]
    fn a_transaction_cannot_be_opened_twice() {
        let txn = txn();
        txn.lock().unwrap().identity = Some(ProducerIdentity {
            producer_id: 7,
            producer_epoch: 0,
        });
        txn.begin().expect("opens");
        let error = txn.begin().expect_err("already open");
        assert!(error.to_string().contains("already open"));
    }

    /// A fenced producer must not become un-fenced by anything short of being
    /// thrown away, because the epoch it holds is permanently stale.
    #[test]
    fn fencing_is_terminal_for_every_later_operation() {
        let txn = txn();
        txn.lock().unwrap().identity = Some(ProducerIdentity {
            producer_id: 7,
            producer_epoch: 0,
        });
        txn.begin().expect("opens");

        txn.note(&Error::from_code(ErrorCode::ProducerFenced, None));

        let error = txn.begin().expect_err("fenced");
        assert_eq!(error.code(), Some(ErrorCode::ProducerFenced));
        assert!(!txn.is_open(), "fencing closes the open transaction");
    }

    /// `INVALID_PRODUCER_EPOCH` is the same situation wearing a different code:
    /// somebody else bumped our epoch.
    #[test]
    fn a_bumped_epoch_fences_too() {
        let txn = txn();
        txn.note(&Error::from_code(ErrorCode::InvalidProducerEpoch, None));
        assert_eq!(
            txn.begin().expect_err("fenced").code(),
            Some(ErrorCode::ProducerFenced)
        );
    }

    #[test]
    fn enrolment_is_remembered_so_a_partition_is_added_once() {
        let txn = txn();
        txn.lock().unwrap().identity = Some(ProducerIdentity {
            producer_id: 7,
            producer_epoch: 0,
        });
        txn.begin().expect("opens");

        let partitions = vec![("a".to_owned(), 0), ("a".to_owned(), 1)];
        assert_eq!(txn.unenrolled(&partitions).len(), 2);

        txn.lock().unwrap().enrolled.insert(("a".to_owned(), 0));
        assert_eq!(txn.unenrolled(&partitions), vec![("a".to_owned(), 1)]);
    }

    /// Outside a transaction nothing is enrolled, so a producer that never
    /// called `begin` does not send `AddPartitionsToTxn` for its writes.
    #[test]
    fn nothing_is_enrolled_while_no_transaction_is_open() {
        let txn = txn();
        assert!(txn.unenrolled(&[("a".to_owned(), 0)]).is_empty());
    }

    #[test]
    fn beginning_clears_the_previous_transactions_enrolment() {
        let txn = txn();
        txn.lock().unwrap().identity = Some(ProducerIdentity {
            producer_id: 7,
            producer_epoch: 0,
        });
        txn.begin().expect("opens");
        txn.lock().unwrap().enrolled.insert(("a".to_owned(), 0));
        txn.lock().unwrap().open = false;

        txn.begin().expect("reopens");
        assert_eq!(
            txn.unenrolled(&[("a".to_owned(), 0)]).len(),
            1,
            "a new transaction must re-add every partition it touches"
        );
    }
}
