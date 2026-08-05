//! The metadata cache and the RPC dispatcher.
//!
//! Everything above this crate sends requests through [`Cluster`], which knows
//! four things the connection layer does not: which broker a request belongs
//! to, what the cluster currently looks like, which errors mean "your view is
//! stale", and how long to wait before trying again.
//!
//! The snapshot lives behind an `ArcSwap`. Readers take an `Arc` and never
//! block, never wait on a refresh in progress, and never observe a partially
//! updated cluster — a UI rendering a topic list while a refresh lands gets the
//! old list or the new one, not a mixture.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use arc_swap::ArcSwap;
use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_conn::protocol::messages::{FindCoordinatorRequest, MetadataRequest, TopicName};
use kafka_conn::{ApiKey, Connection, ConnectionConfig, Error, ErrorCode, Result, Rpc};

use crate::pool::BrokerPool;
use crate::retry::RetryPolicy;
use crate::routing::{BrokerSelector, CoordinatorKind, Routing, routing};
use crate::snapshot::{BrokerInfo, MetadataSnapshot, PartitionInfo, TopicId, TopicInfo};

/// How to build a [`Cluster`].
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Per-connection settings.
    pub connection: ConnectionConfig,
    /// Retry behaviour for routed requests.
    pub retry: RetryPolicy,
    /// How often the background task refreshes metadata.
    ///
    /// Kafka's own client default is five minutes. A UI wants fresher than
    /// that, and metadata for a large cluster is not cheap, so thirty seconds
    /// is the compromise — with on-demand invalidation doing the real work.
    pub refresh_interval: Duration,
    /// Refresh before answering when the snapshot is older than this.
    pub max_staleness: Duration,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            retry: RetryPolicy::default(),
            refresh_interval: Duration::from_secs(30),
            max_staleness: Duration::from_secs(300),
        }
    }
}

/// A connected Kafka cluster: metadata, routing, connections and retries.
#[derive(Debug, Clone)]
pub struct Cluster {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    pool: BrokerPool,
    config: ClusterConfig,
    snapshot: ArcSwap<MetadataSnapshot>,
    coordinators: Mutex<HashMap<(CoordinatorKind, String), i32>>,
}

impl Cluster {
    /// Connect and fetch the first metadata snapshot.
    pub async fn connect(
        bootstrap: impl IntoIterator<Item = impl Into<String>>,
        config: ClusterConfig,
    ) -> Result<Self> {
        let pool = BrokerPool::new(bootstrap, config.connection.clone(), config.retry);
        let cluster = Cluster {
            inner: Arc::new(Inner {
                pool,
                config,
                snapshot: ArcSwap::from_pointee(MetadataSnapshot::empty()),
                coordinators: Mutex::new(HashMap::new()),
            }),
        };
        cluster.refresh().await?;
        cluster.spawn_refresh_task();
        Ok(cluster)
    }

    /// The current snapshot. Never blocks.
    pub fn snapshot(&self) -> Arc<MetadataSnapshot> {
        self.inner.snapshot.load_full()
    }

    /// The underlying connection pool.
    pub fn pool(&self) -> &BrokerPool {
        &self.inner.pool
    }

    /// The version a connection would send a specific request at.
    ///
    /// Exposed because several requests change *shape* with the version rather
    /// than merely gaining fields — `Fetch` names its topics by string up to
    /// v12 and by uuid from v13 — and the codec rejects a field set outside
    /// its own range rather than ignoring it.
    pub async fn negotiated_for<R: Rpc>(&self) -> Result<i16> {
        self.inner.pool.any().await?.negotiated_for::<R>()
    }

    /// Fetch metadata for the whole cluster and install it.
    pub async fn refresh(&self) -> Result<Arc<MetadataSnapshot>> {
        let connection = self.inner.pool.any().await?;
        let response = connection.send(all_topics_request(&connection)).await?;
        let snapshot = Arc::new(decode_metadata(response));
        self.install(snapshot.clone());
        Ok(snapshot)
    }

    /// Fetch metadata for specific topics and merge it in.
    ///
    /// Cheaper than a full refresh by orders of magnitude on a large cluster,
    /// and the only sane thing to do when the trigger was one partition's
    /// leader moving.
    pub async fn refresh_topics(&self, topics: &[&str]) -> Result<Arc<MetadataSnapshot>> {
        if topics.is_empty() {
            return Ok(self.snapshot());
        }
        let connection = self.inner.pool.any().await?;
        let response = connection.send(topics_request(topics)).await?;
        let fresh = decode_metadata(response);
        let merged = Arc::new(self.snapshot().with_topics_merged(fresh.topics().to_vec()));
        self.install(merged.clone());
        Ok(merged)
    }

    /// Refresh only if the snapshot has gone stale.
    pub async fn refresh_if_stale(&self) -> Result<Arc<MetadataSnapshot>> {
        let snapshot = self.snapshot();
        if snapshot.age() < self.inner.config.max_staleness && !snapshot.brokers().is_empty() {
            return Ok(snapshot);
        }
        self.refresh().await
    }

    /// The leader of a partition, refreshing if the snapshot does not know.
    pub async fn leader_for(&self, topic: &str, partition: i32) -> Result<i32> {
        if let Some(leader) = self.snapshot().leader_for(topic, partition) {
            return Ok(leader);
        }
        let snapshot = self.refresh_topics(&[topic]).await?;
        snapshot.leader_for(topic, partition).ok_or_else(|| {
            match snapshot.topic(topic).and_then(|t| t.error) {
                Some(code) => Error::from_code(code, Some(format!("topic {topic}"))),
                None => Error::from_code(
                    ErrorCode::LeaderNotAvailable,
                    Some(format!("{topic}-{partition}")),
                ),
            }
        })
    }

    /// The coordinator for a group, cached.
    pub async fn coordinator_for(&self, group: &str) -> Result<i32> {
        self.coordinator(CoordinatorKind::Group, group).await
    }

    /// The coordinator for a group or transactional id, cached.
    ///
    /// Retried on the retriable codes like every other routed call. This one
    /// is easy to miss because it is not a `send_*` and so never went through
    /// [`Cluster::dispatch`] — but `COORDINATOR_NOT_AVAILABLE` is exactly what
    /// a *fresh* cluster returns, because `__consumer_offsets` is created
    /// lazily on first use and has no leader for a moment afterwards. Without
    /// a retry the first group lookup against a new cluster is a hard error
    /// for a condition that clears itself.
    ///
    /// "About a second", this used to say, and the attempt budget was sized
    /// for that. On a three-node cluster with 50 offset partitions to elect
    /// leaders for it is not about a second, and every KIP-848 acceptance
    /// test failed on `NOT_COORDINATOR` the first time the suite ran
    /// somewhere slower than a laptop. So the wait is bounded by
    /// [`RetryPolicy::coordinator_timeout`] here exactly as it is in
    /// `dispatch`: this is the same event, one round trip earlier.
    pub async fn coordinator(&self, kind: CoordinatorKind, key: &str) -> Result<i32> {
        let policy = self.inner.config.retry;
        let started = std::time::Instant::now();
        let mut attempt = 1;
        loop {
            let delay = policy.delay(attempt);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }

            let error = match self.coordinator_once(kind, key).await {
                Ok(node) => return Ok(node),
                Err(error) => error,
            };

            let budget_left = if error.needs_coordinator_refresh() {
                started.elapsed() < policy.coordinator_timeout
            } else {
                policy.should_retry(attempt)
            };

            if !error.retriable() || !budget_left {
                return Err(error);
            }
            tracing::debug!(?kind, key, attempt, %error, "retrying FindCoordinator");
            attempt = attempt.saturating_add(1);
        }
    }

    /// One `FindCoordinator` round trip, cache included.
    async fn coordinator_once(&self, kind: CoordinatorKind, key: &str) -> Result<i32> {
        let cache_key = (kind, key.to_owned());
        if let Some(node) = self
            .inner
            .coordinators
            .lock()
            .ok()
            .and_then(|map| map.get(&cache_key).copied())
        {
            return Ok(node);
        }

        let connection = self.inner.pool.any().await?;
        // `key` is versions 0-3 and `coordinator_keys` is 4+, and the codec
        // *rejects* a field set outside its own version range rather than
        // ignoring it. Setting both to cover the range looks like belt and
        // braces and is an encode failure on every modern broker — which takes
        // down every coordinator-routed RPC with it.
        let version = connection.negotiated_for::<FindCoordinatorRequest>()?;
        let request = FindCoordinatorRequest::default().with_key_type(kind.key_type());
        let request = if version >= 4 {
            request.with_coordinator_keys(vec![StrBytes::from_string(key.to_owned())])
        } else {
            request.with_key(StrBytes::from_string(key.to_owned()))
        };
        let response = connection.send(request).await?;

        // v4+ moved the answer into a `coordinators` array and left the
        // top-level fields empty; older versions do the opposite. Reading only
        // one of the two shapes yields "coordinator 0", which is a real broker
        // id and therefore a bug that looks like it works.
        let (node_id, error_code, message) = match response.coordinators.first() {
            Some(coordinator) => (
                coordinator.node_id.0,
                coordinator.error_code,
                coordinator.error_message.as_ref().map(|m| m.to_string()),
            ),
            None => (
                response.node_id.0,
                response.error_code,
                response.error_message.as_ref().map(|m| m.to_string()),
            ),
        };

        if let Some(code) = ErrorCode::from_code(error_code) {
            return Err(Error::from_code(code, message));
        }
        if node_id < 0 {
            return Err(Error::from_code(
                ErrorCode::CoordinatorNotAvailable,
                Some(key.to_owned()),
            ));
        }

        if let Ok(mut map) = self.inner.coordinators.lock() {
            map.insert(cache_key, node_id);
        }
        Ok(node_id)
    }

    /// The active controller.
    pub async fn controller(&self) -> Result<i32> {
        if let Some(id) = self.snapshot().controller_id() {
            return Ok(id);
        }
        self.refresh()
            .await?
            .controller_id()
            .ok_or_else(|| Error::from_code(ErrorCode::NotController, None))
    }

    /// Forget a cached coordinator.
    pub fn invalidate_coordinator(&self, kind: CoordinatorKind, key: &str) {
        if let Ok(mut map) = self.inner.coordinators.lock() {
            map.remove(&(kind, key.to_owned()));
        }
    }

    /// Discard the snapshot, forcing the next access to refetch.
    pub fn invalidate(&self) {
        self.install(Arc::new(MetadataSnapshot::empty()));
    }

    /// Send a request to any broker.
    pub async fn send_any<R: Rpc + Clone>(&self, request: R) -> Result<R::Response> {
        self.dispatch(Target::Any, request).await
    }

    /// Send a request to the controller.
    pub async fn send_to_controller<R: Rpc + Clone>(&self, request: R) -> Result<R::Response> {
        self.dispatch(Target::Controller, request).await
    }

    /// Send a request to one named broker.
    pub async fn send_to_node<R: Rpc + Clone>(
        &self,
        node_id: i32,
        request: R,
    ) -> Result<R::Response> {
        self.dispatch(Target::Node(node_id), request).await
    }

    /// Send a request to a group or transaction coordinator.
    pub async fn send_to_coordinator<R: Rpc + Clone>(
        &self,
        kind: CoordinatorKind,
        key: &str,
        request: R,
    ) -> Result<R::Response> {
        self.dispatch(Target::Coordinator(kind, key.to_owned()), request)
            .await
    }

    /// Send a request to a partition's leader.
    pub async fn send_to_leader<R: Rpc + Clone>(
        &self,
        topic: &str,
        partition: i32,
        request: R,
    ) -> Result<R::Response> {
        self.dispatch(Target::Leader(topic.to_owned(), partition), request)
            .await
    }

    /// Send a request to wherever [`routing`] says it belongs.
    ///
    /// Only usable for the `Any` and `Controller` classes; coordinator- and
    /// broker-routed requests need a key the api key alone does not carry, so
    /// asking for them here is a caller error rather than a guess.
    pub async fn send_routed<R: Rpc + Clone>(&self, request: R) -> Result<R::Response> {
        match routing(R::API_KEY) {
            Routing::Any => self.send_any(request).await,
            Routing::Controller => self.send_to_controller(request).await,
            Routing::Coordinator(kind) => Err(Error::InvalidRequest(format!(
                "{} is routed to a {kind:?} coordinator; use send_to_coordinator",
                R::API_KEY
            ))),
            Routing::Specific(BrokerSelector::Caller) => Err(Error::InvalidRequest(format!(
                "{} is routed to one broker; use send_to_node",
                R::API_KEY
            ))),
            Routing::Specific(BrokerSelector::PartitionLeader) => {
                Err(Error::InvalidRequest(format!(
                    "{} is routed to a partition leader; use send_to_leader",
                    R::API_KEY
                )))
            }
        }
    }

    /// The retry loop: resolve a broker, send, and decide what a failure means.
    async fn dispatch<R: Rpc + Clone>(&self, target: Target, request: R) -> Result<R::Response> {
        let policy = self.inner.config.retry;
        let started = std::time::Instant::now();
        let mut attempt = 1;
        loop {
            let delay = policy.delay(attempt);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }

            let outcome = self.attempt(&target, request.clone()).await;
            let error = match outcome {
                Ok(response) => return Ok(response),
                Err(error) => error,
            };

            // Two independent axes, and both have to be acted on: a stale
            // leader and a moved coordinator are different caches, and
            // refreshing the wrong one leaves the retry pointed at the same
            // wrong broker.
            if error.needs_metadata_refresh() {
                self.on_stale_metadata(&target).await;
            }
            let coordinator_moved =
                error.needs_coordinator_refresh() && matches!(&target, Target::Coordinator(..));
            if coordinator_moved && let Target::Coordinator(kind, key) = &target {
                self.invalidate_coordinator(*kind, key);
            }

            // A moved or still-loading coordinator is bounded by time, not by
            // attempts. The attempt count is tuned for "this broker answered
            // badly"; a coordinator handover is a cluster-side event whose
            // duration has nothing to do with our backoff curve, and five
            // attempts expire ~1.5s into an election that routinely takes
            // longer. See `RetryPolicy::coordinator_timeout`.
            let budget_left = if coordinator_moved {
                started.elapsed() < policy.coordinator_timeout
            } else {
                policy.should_retry(attempt)
            };

            if !error.retriable() || !budget_left {
                return Err(error);
            }
            tracing::debug!(api = %R::API_KEY, attempt, %error, "retrying");
            attempt = attempt.saturating_add(1);
        }
    }

    async fn attempt<R: Rpc + Clone>(&self, target: &Target, request: R) -> Result<R::Response> {
        let connection = self.resolve(target).await?;
        connection.send(request).await
    }

    async fn resolve(&self, target: &Target) -> Result<Connection> {
        match target {
            Target::Any => self.inner.pool.any().await,
            Target::Node(node_id) => self.inner.pool.get(*node_id).await,
            Target::Controller => {
                let controller = self.controller().await?;
                self.inner.pool.get(controller).await
            }
            Target::Coordinator(kind, key) => {
                let node = self.coordinator(*kind, key).await?;
                self.inner.pool.get(node).await
            }
            Target::Leader(topic, partition) => {
                let leader = self.leader_for(topic, *partition).await?;
                self.inner.pool.get(leader).await
            }
        }
    }

    async fn on_stale_metadata(&self, target: &Target) {
        let refreshed = match target {
            Target::Leader(topic, _) => self.refresh_topics(&[topic.as_str()]).await.map(|_| ()),
            _ => self.refresh().await.map(|_| ()),
        };
        if let Err(error) = refreshed {
            tracing::debug!(%error, "metadata refresh after a stale-view error failed");
        }
    }

    fn install(&self, snapshot: Arc<MetadataSnapshot>) {
        self.inner.pool.learn_addresses(
            snapshot
                .brokers()
                .iter()
                .map(|broker| (broker.node_id, broker.address())),
        );
        self.inner.snapshot.store(snapshot);
    }

    /// Refresh in the background, and stop when the last `Cluster` is dropped.
    fn spawn_refresh_task(&self) {
        let weak = Arc::downgrade(&self.inner);
        let interval = self.inner.config.refresh_interval;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(inner) = Weak::upgrade(&weak) else {
                    return;
                };
                let cluster = Cluster { inner };
                if let Err(error) = cluster.refresh().await {
                    tracing::debug!(%error, "background metadata refresh failed");
                }
            }
        });
    }
}

/// Where a request is going.
#[derive(Debug, Clone)]
enum Target {
    Any,
    Controller,
    Node(i32),
    Coordinator(CoordinatorKind, String),
    Leader(String, i32),
}

/// A metadata request for every topic.
///
/// The null-versus-empty distinction is version dependent: from v1 a null topic
/// list means "everything", while at v0 an *empty* list meant that. Getting it
/// backwards asks a modern broker for no topics at all and yields a snapshot
/// that quietly has none.
fn all_topics_request(connection: &Connection) -> MetadataRequest {
    let version = connection.negotiated_version(ApiKey::Metadata).unwrap_or(1);
    let topics = if version >= 1 { None } else { Some(Vec::new()) };
    base_metadata_request().with_topics(topics)
}

fn topics_request(topics: &[&str]) -> MetadataRequest {
    base_metadata_request().with_topics(Some(
        topics
            .iter()
            .map(|name| {
                MetadataRequestTopic::default()
                    .with_name(Some(TopicName(StrBytes::from_string((*name).to_owned()))))
            })
            .collect(),
    ))
}

/// Every metadata request in this workspace goes through here.
///
/// `MetadataRequest::default()` sets `allow_auto_topic_creation: true`, because
/// that is the schema default and the crate honours it. On a cluster with
/// `auto.create.topics.enable=true` that turns a typo in a UI search box into a
/// created topic. There is no legitimate case for `true` in this codebase, so
/// the only constructor turns it off and there is a unit test to keep it that
/// way.
fn base_metadata_request() -> MetadataRequest {
    MetadataRequest::default().with_allow_auto_topic_creation(false)
}

/// Convert a metadata response into our own types.
fn decode_metadata(response: kafka_conn::protocol::messages::MetadataResponse) -> MetadataSnapshot {
    let brokers = response
        .brokers
        .into_iter()
        .map(|broker| BrokerInfo {
            node_id: broker.node_id.0,
            host: broker.host.to_string(),
            port: broker.port,
            rack: broker.rack.map(|r| r.to_string()),
        })
        .collect();

    let topics = response
        .topics
        .into_iter()
        .map(|topic| TopicInfo {
            name: topic.name.map(|n| n.0.to_string()).unwrap_or_default(),
            topic_id: TopicId::from_bytes(topic.topic_id.into_bytes()),
            internal: topic.is_internal,
            partitions: topic
                .partitions
                .into_iter()
                .map(|partition| PartitionInfo {
                    partition: partition.partition_index,
                    // -1 is the protocol's "no leader"; keep that out of the
                    // domain type entirely.
                    leader: Some(partition.leader_id.0).filter(|id| *id >= 0),
                    leader_epoch: partition.leader_epoch,
                    replicas: partition.replica_nodes.iter().map(|id| id.0).collect(),
                    isr: partition.isr_nodes.iter().map(|id| id.0).collect(),
                    offline_replicas: partition.offline_replicas.iter().map(|id| id.0).collect(),
                    error: ErrorCode::from_code(partition.error_code),
                })
                .collect(),
            error: ErrorCode::from_code(topic.error_code),
        })
        .collect();

    MetadataSnapshot::new(
        brokers,
        topics,
        Some(response.controller_id.0).filter(|id| *id >= 0),
        response.cluster_id.map(|id| id.to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// M4 makes this a required assertion, and it is worth saying why: this is
    /// a one-word regression with a destructive blast radius, and nothing about
    /// the resulting behaviour looks wrong from the client side.
    #[test]
    fn metadata_requests_never_allow_auto_topic_creation() {
        assert!(!base_metadata_request().allow_auto_topic_creation);
        assert!(!topics_request(&["orders"]).allow_auto_topic_creation);
    }

    #[test]
    fn the_crates_default_is_the_dangerous_one() {
        // If this ever starts failing, the trap has been fixed upstream and
        // the guard above can relax. Until then it is load-bearing.
        assert!(MetadataRequest::default().allow_auto_topic_creation);
    }

    #[test]
    fn a_targeted_request_names_its_topics() {
        let request = topics_request(&["orders", "events"]);
        let names: Vec<String> = request
            .topics
            .unwrap_or_default()
            .into_iter()
            .filter_map(|t| t.name.map(|n| n.0.to_string()))
            .collect();
        assert_eq!(names, vec!["orders".to_owned(), "events".to_owned()]);
    }

    #[test]
    fn send_routed_refuses_the_classes_it_cannot_resolve() {
        // Compile-time proof that the routing table is consulted; the runtime
        // check is exercised in the integration suite.
        assert_eq!(
            routing(ApiKey::OffsetFetch),
            Routing::Coordinator(CoordinatorKind::Group)
        );
        assert_eq!(
            routing(ApiKey::DescribeLogDirs),
            Routing::Specific(BrokerSelector::Caller)
        );
    }
}
