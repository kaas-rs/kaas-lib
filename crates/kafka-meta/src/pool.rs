//! The broker connection pool.
//!
//! One connection per broker, opened lazily, reopened with capped jittered
//! backoff, and — when every known broker is unreachable — re-resolved from the
//! bootstrap addresses. That last part matters more than it looks: a cluster
//! that rolls every broker onto new addresses is a normal Kubernetes event, and
//! a pool that only knows the addresses from its last successful metadata fetch
//! never recovers from it.
//!
//! Connecting happens under a per-endpoint async mutex rather than a global
//! one, so a slow handshake to a dead broker does not stall connections to
//! healthy ones, and twenty concurrent callers for the same broker open one
//! socket rather than twenty.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use kafka_conn::{Connection, ConnectionConfig, Error, Result};

use crate::retry::RetryPolicy;

/// A broker to connect to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Endpoint {
    /// A broker known by id, whose address comes from metadata.
    Node(i32),
    /// A bootstrap address, used before metadata exists and when nothing else
    /// is reachable.
    Bootstrap(String),
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Endpoint::Node(id) => write!(f, "broker {id}"),
            Endpoint::Bootstrap(addr) => write!(f, "bootstrap {addr}"),
        }
    }
}

/// Per-endpoint connection state, guarded by its own lock.
#[derive(Debug, Default)]
struct SlotState {
    connection: Option<Connection>,
    consecutive_failures: u32,
    next_attempt: Option<Instant>,
}

#[derive(Debug, Default)]
struct Slot {
    state: tokio::sync::Mutex<SlotState>,
}

/// Broker connections, keyed by endpoint.
#[derive(Debug)]
pub struct BrokerPool {
    config: ConnectionConfig,
    bootstrap: Vec<String>,
    retry: RetryPolicy,
    slots: Mutex<HashMap<Endpoint, Arc<Slot>>>,
    addresses: Mutex<HashMap<i32, String>>,
}

impl BrokerPool {
    /// Build a pool over a set of bootstrap addresses.
    pub fn new(
        bootstrap: impl IntoIterator<Item = impl Into<String>>,
        config: ConnectionConfig,
        retry: RetryPolicy,
    ) -> Self {
        Self {
            config,
            bootstrap: bootstrap.into_iter().map(Into::into).collect(),
            retry,
            slots: Mutex::new(HashMap::new()),
            addresses: Mutex::new(HashMap::new()),
        }
    }

    /// The bootstrap addresses this pool was built with.
    pub fn bootstrap(&self) -> &[String] {
        &self.bootstrap
    }

    /// The connection configuration, for callers that need to open their own.
    pub fn config(&self) -> &ConnectionConfig {
        &self.config
    }

    /// Learn broker addresses from a metadata snapshot.
    pub fn learn_addresses(&self, addresses: impl IntoIterator<Item = (i32, String)>) {
        if let Ok(mut map) = self.addresses.lock() {
            for (node_id, address) in addresses {
                map.insert(node_id, address);
            }
        }
    }

    /// A connection to one broker.
    pub async fn get(&self, node_id: i32) -> Result<Connection> {
        let address = self
            .addresses
            .lock()
            .ok()
            .and_then(|map| map.get(&node_id).cloned())
            .ok_or_else(|| {
                // A client-side conclusion wearing the code the broker would
                // have used, so the routing retry table needs no special
                // case. This is *stale metadata*, not a bad request: on a
                // booting or re-electing cluster, topic metadata can name a
                // leader whose broker entry has not registered yet, and the
                // gap closes on the next refresh. `InvalidRequest` here made
                // that one transient response a terminal delivery failure —
                // non-retriable, no refresh — which is how a codec round-trip
                // test lost a batch to a broker that was seconds from
                // existing.
                Error::from_code(
                    kafka_conn::ErrorCode::LeaderNotAvailable,
                    Some(format!(
                        "no known address for broker {node_id}; \
                         the metadata naming it is stale"
                    )),
                )
            })?;
        self.connect(Endpoint::Node(node_id), &address, Some(node_id))
            .await
    }

    /// Any usable connection.
    ///
    /// Prefers brokers we already know about, and falls back to the bootstrap
    /// addresses when none of them answer.
    pub async fn any(&self) -> Result<Connection> {
        let known: Vec<(i32, String)> = self
            .addresses
            .lock()
            .map(|map| map.iter().map(|(k, v)| (*k, v.clone())).collect())
            .unwrap_or_default();

        let mut last_error = None;
        for (node_id, address) in known {
            match self
                .connect(Endpoint::Node(node_id), &address, Some(node_id))
                .await
            {
                Ok(connection) => return Ok(connection),
                Err(error) => last_error = Some(error),
            }
        }

        match self.bootstrap_connection().await {
            Ok(connection) => Ok(connection),
            Err(error) => Err(last_error.unwrap_or(error)),
        }
    }

    /// A connection to one of the bootstrap addresses.
    pub async fn bootstrap_connection(&self) -> Result<Connection> {
        let mut last_error = None;
        for address in &self.bootstrap {
            match self
                .connect(Endpoint::Bootstrap(address.clone()), address, None)
                .await
            {
                Ok(connection) => return Ok(connection),
                Err(error) => {
                    tracing::debug!(%address, %error, "bootstrap address unreachable");
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            Error::InvalidRequest("no bootstrap addresses were configured".to_owned())
        }))
    }

    /// Drop a connection, so the next caller opens a fresh one.
    pub async fn evict(&self, node_id: i32) {
        let slot = self.slot(&Endpoint::Node(node_id));
        if let Some(slot) = slot {
            let mut state = slot.state.lock().await;
            if let Some(connection) = state.connection.take() {
                connection.close();
            }
        }
    }

    /// Close everything.
    pub async fn close(&self) {
        let slots: Vec<Arc<Slot>> = self
            .slots
            .lock()
            .map(|map| map.values().cloned().collect())
            .unwrap_or_default();
        for slot in slots {
            let mut state = slot.state.lock().await;
            if let Some(connection) = state.connection.take() {
                connection.close();
            }
        }
    }

    /// How many live connections the pool holds to brokers known by id.
    ///
    /// Distinct from [`BrokerPool::live_connections`], which also counts the
    /// bootstrap socket. That socket is real and deliberately kept — it is the
    /// fallback when every known broker becomes unreachable — so a caller
    /// asking "did we open one connection per broker?" wants this, and gets a
    /// confusing off-by-one from the other.
    pub async fn live_node_connections(&self) -> usize {
        let slots: Vec<(Endpoint, Arc<Slot>)> = self
            .slots
            .lock()
            .map(|map| {
                map.iter()
                    .filter(|(endpoint, _)| matches!(endpoint, Endpoint::Node(_)))
                    .map(|(endpoint, slot)| (endpoint.clone(), Arc::clone(slot)))
                    .collect()
            })
            .unwrap_or_default();
        let mut count = 0;
        for (_, slot) in slots {
            let state = slot.state.lock().await;
            if state.connection.as_ref().is_some_and(|c| !c.is_closed()) {
                count += 1;
            }
        }
        count
    }

    /// How many live connections the pool is holding, bootstrap included.
    pub async fn live_connections(&self) -> usize {
        let slots: Vec<Arc<Slot>> = self
            .slots
            .lock()
            .map(|map| map.values().cloned().collect())
            .unwrap_or_default();
        let mut count = 0;
        for slot in slots {
            let state = slot.state.lock().await;
            if state.connection.as_ref().is_some_and(|c| !c.is_closed()) {
                count += 1;
            }
        }
        count
    }

    fn slot(&self, endpoint: &Endpoint) -> Option<Arc<Slot>> {
        self.slots.lock().ok()?.get(endpoint).cloned()
    }

    fn slot_or_create(&self, endpoint: &Endpoint) -> Result<Arc<Slot>> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| Error::InvalidRequest("connection pool lock poisoned".to_owned()))?;
        Ok(slots.entry(endpoint.clone()).or_default().clone())
    }

    async fn connect(
        &self,
        endpoint: Endpoint,
        address: &str,
        node_id: Option<i32>,
    ) -> Result<Connection> {
        let slot = self.slot_or_create(&endpoint)?;
        let mut state = slot.state.lock().await;

        if let Some(connection) = &state.connection
            && !connection.is_closed()
        {
            return Ok(connection.clone());
        }
        state.connection = None;

        // Backoff is per endpoint, so a broker that is down does not get
        // hammered by every caller that happens to want it.
        if let Some(next) = state.next_attempt
            && Instant::now() < next
        {
            return Err(Error::ConnectionClosed {
                peer: address.to_owned(),
            });
        }

        match Connection::connect_as(address, node_id, self.config.clone()).await {
            Ok(connection) => {
                state.connection = Some(connection.clone());
                state.consecutive_failures = 0;
                state.next_attempt = None;
                Ok(connection)
            }
            Err(error) => {
                state.consecutive_failures = state.consecutive_failures.saturating_add(1);
                let delay = self
                    .retry
                    .delay(state.consecutive_failures.saturating_add(1));
                state.next_attempt = Some(Instant::now() + delay);
                tracing::debug!(
                    %endpoint,
                    %address,
                    failures = state.consecutive_failures,
                    ?delay,
                    %error,
                    "connection attempt failed"
                );
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool() -> BrokerPool {
        BrokerPool::new(
            ["127.0.0.1:1"],
            ConnectionConfig::new().with_connect_timeout(std::time::Duration::from_millis(200)),
            RetryPolicy::default(),
        )
    }

    /// An id the address map has never heard of fails fast — and fails as
    /// *stale metadata*, because that is what it is: topic metadata named a
    /// leader whose broker entry has not arrived yet. Classified
    /// `InvalidRequest` once, which is non-retriable and refreshes nothing,
    /// so a single mid-boot metadata response turned into a terminal
    /// delivery failure.
    #[tokio::test]
    async fn an_unknown_broker_id_is_stale_metadata_not_a_bad_request() {
        let err = pool().get(42).await.unwrap_err();
        assert_eq!(err.code(), Some(kafka_conn::ErrorCode::LeaderNotAvailable));
        assert!(err.retriable(), "a refresh can fix this: {err:?}");
        assert!(err.needs_metadata_refresh(), "{err:?}");
    }

    #[tokio::test]
    async fn addresses_are_learned_from_metadata() {
        let pool = pool();
        pool.learn_addresses([(1, "127.0.0.1:1".to_owned())]);
        // Still fails — nothing is listening — but it now fails at connect
        // rather than at address resolution.
        let err = pool.get(1).await.unwrap_err();
        assert!(!matches!(err, Error::InvalidRequest(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_failed_endpoint_backs_off_rather_than_retrying_immediately() {
        let pool = pool();
        pool.learn_addresses([(1, "127.0.0.1:1".to_owned())]);

        let first = std::time::Instant::now();
        assert!(pool.get(1).await.is_err());
        let first_took = first.elapsed();

        // The second attempt is refused from the backoff window rather than
        // spending another connect timeout on a broker we just saw fail.
        let second = std::time::Instant::now();
        assert!(pool.get(1).await.is_err());
        assert!(
            second.elapsed() < first_took,
            "second attempt took {:?}, first took {first_took:?}",
            second.elapsed()
        );
    }

    #[tokio::test]
    async fn with_no_bootstrap_addresses_the_error_says_so() {
        let pool = BrokerPool::new(
            Vec::<String>::new(),
            ConnectionConfig::new(),
            RetryPolicy::default(),
        );
        let err = pool.bootstrap_connection().await.unwrap_err();
        assert!(format!("{err}").contains("bootstrap"), "{err}");
    }

    #[tokio::test]
    async fn closing_an_empty_pool_is_fine() {
        let pool = pool();
        pool.close().await;
        assert_eq!(pool.live_connections().await, 0);
    }
}
