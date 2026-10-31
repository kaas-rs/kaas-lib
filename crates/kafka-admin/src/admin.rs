//! The admin client.

use kafka_conn::{ApiKey, Result};
use kafka_meta::{Cluster, ClusterConfig};

/// Admin RPCs over a routed, retrying, pooled connection.
///
/// Cheap to clone; every clone shares the metadata cache and connections.
#[derive(Debug, Clone)]
pub struct Admin {
    cluster: Cluster,
}

impl Admin {
    /// Wrap an existing cluster handle.
    pub fn new(cluster: Cluster) -> Self {
        Self { cluster }
    }

    /// Connect to a cluster.
    pub async fn connect(
        bootstrap: impl IntoIterator<Item = impl Into<String>>,
        config: ClusterConfig,
    ) -> Result<Self> {
        Ok(Self::new(Cluster::connect(bootstrap, config).await?))
    }

    /// Connect a client that refuses every mutating api key.
    ///
    /// The gate is enforced on `ApiKey` inside the connection layer, not over
    /// this crate's method surface, so a new admin method added tomorrow is
    /// covered without anyone remembering to cover it.
    pub async fn connect_read_only(
        bootstrap: impl IntoIterator<Item = impl Into<String>>,
        mut config: ClusterConfig,
    ) -> Result<Self> {
        config.connection = config.connection.with_read_only();
        Self::connect(bootstrap, config).await
    }

    /// Connect a client whose connections admit only these mutating keys.
    ///
    /// The ceiling sits where [`connect_read_only`](Self::connect_read_only)'s
    /// does — in the connection, before the socket — and refuses every
    /// mutation it does not name, including ones a future release adds. Build
    /// it from [`ops`](crate::ops): `connect_with_mutations(bootstrap, config,
    /// ops::RESET_OFFSETS.iter().copied())`. An empty set is read-only.
    pub async fn connect_with_mutations(
        bootstrap: impl IntoIterator<Item = impl Into<String>>,
        mut config: ClusterConfig,
        keys: impl IntoIterator<Item = ApiKey>,
    ) -> Result<Self> {
        config.connection = config.connection.with_allowed_mutations(keys);
        Self::connect(bootstrap, config).await
    }

    /// A handle over the same pool that may send only these mutating keys.
    ///
    /// For least privilege per operation: a caller about to reset a group's
    /// offsets takes `admin.scoped(ops::RESET_OFFSETS.iter().copied())` and
    /// that handle cannot delete a topic, whatever the code around it does.
    /// Narrows and never widens — the connection's ceiling still applies
    /// beneath it, and scoping a scoped handle intersects. See
    /// [`Cluster::scoped`](kafka_meta::Cluster::scoped).
    #[must_use]
    pub fn scoped(&self, keys: impl IntoIterator<Item = ApiKey>) -> Self {
        Self::new(self.cluster.scoped(keys))
    }

    /// The underlying cluster handle.
    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    /// The per-request timeout, in the milliseconds admin RPCs want.
    pub(crate) fn request_timeout_ms(&self) -> i32 {
        i32::try_from(self.cluster.pool().config().request_timeout.as_millis()).unwrap_or(i32::MAX)
    }

    /// Whether the cluster offers an api key we can speak.
    ///
    /// Used for the `DescribeTopicPartitions`-or-`Metadata` decision, where
    /// "the broker is too old" and "our schemas are too old" both have to fall
    /// back to the same place.
    pub(crate) async fn supports(&self, api_key: ApiKey) -> bool {
        match self.cluster.pool().any().await {
            Ok(connection) => connection.versions().supports(api_key),
            Err(_) => false,
        }
    }

    /// The version a connection would use for an api key.
    pub(crate) async fn negotiated_version(&self, api_key: ApiKey) -> Option<i16> {
        self.cluster
            .pool()
            .any()
            .await
            .ok()
            .and_then(|connection| connection.negotiated_version(api_key))
    }

    /// The version a connection would send a specific request at.
    ///
    /// Needed wherever a request's *shape* changes with the version — the
    /// codec rejects a field set outside its own version range rather than
    /// ignoring it, so "set both the old and the new field" is an encode
    /// failure rather than a compatibility trick.
    pub(crate) async fn negotiated_for<R: kafka_conn::Rpc>(&self) -> Result<i16> {
        self.cluster.pool().any().await?.negotiated_for::<R>()
    }
}
