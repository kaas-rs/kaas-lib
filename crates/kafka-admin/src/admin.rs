//! The admin client.

use kafka_conn::{ApiKey, ConnectionConfig, Result};
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
        config.connection = ConnectionConfig {
            read_only: true,
            ..config.connection
        };
        Self::connect(bootstrap, config).await
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
}
