//! How to open a connection.

use std::sync::Arc;
use std::time::Duration;

use crate::codec::DEFAULT_MAX_FRAME_BYTES;
use crate::sasl::SaslConfig;
use crate::tls::TlsConfig;

/// Everything a connection needs to know before it opens a socket.
///
/// Cheap to clone — the pool hands the same config to every broker.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Sent in every request header; shows up in broker request logs and
    /// quota attribution, so it is worth setting to something recognisable.
    pub client_id: Option<String>,
    /// Reported in `ApiVersions`, which is how a broker's metrics learn what
    /// clients are talking to it.
    pub client_software_name: String,
    /// Version reported alongside the name.
    pub client_software_version: String,
    /// How long to wait for the TCP connect and handshake.
    pub connect_timeout: Duration,
    /// Default deadline for a request that does not carry its own.
    pub request_timeout: Duration,
    /// How many requests may be outstanding on one socket.
    ///
    /// Kafka's own default is 5. Higher trades head-of-line blocking for
    /// memory; the broker processes a connection's requests in order either
    /// way, so this is about pipelining, not parallelism.
    pub max_in_flight: usize,
    /// Reject frames larger than this rather than allocating for them.
    pub max_frame_bytes: usize,
    /// Refuse every mutating api key before opening a socket.
    ///
    /// Enforced in `Connection::send` against [`crate::ApiKey::is_mutating`],
    /// not over the admin method surface, so a new admin method cannot bypass
    /// it by construction.
    pub read_only: bool,
    /// TLS settings, or `None` for a plaintext socket.
    pub tls: Option<Arc<TlsConfig>>,
    /// SASL settings, or `None` for an unauthenticated connection.
    pub sasl: Option<Arc<SaslConfig>>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            client_id: Some("kaas-lib".to_owned()),
            client_software_name: "kaas-lib".to_owned(),
            client_software_version: env!("CARGO_PKG_VERSION").to_owned(),
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            max_in_flight: 5,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            read_only: false,
            tls: None,
            sasl: None,
        }
    }
}

impl ConnectionConfig {
    /// Default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the client id sent in request headers.
    #[must_use]
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Set the default per-request deadline.
    #[must_use]
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set the connect and handshake deadline.
    #[must_use]
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the in-flight ceiling.
    #[must_use]
    pub fn with_max_in_flight(mut self, max: usize) -> Self {
        self.max_in_flight = max.max(1);
        self
    }

    /// Refuse mutating requests.
    #[must_use]
    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    /// Connect over TLS.
    #[must_use]
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(Arc::new(tls));
        self
    }

    /// Authenticate with SASL.
    #[must_use]
    pub fn with_sasl(mut self, sasl: SaslConfig) -> Self {
        self.sasl = Some(Arc::new(sasl));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_not_read_only_and_pipeline_five() {
        let cfg = ConnectionConfig::new();
        assert!(!cfg.read_only);
        assert_eq!(cfg.max_in_flight, 5);
    }

    #[test]
    fn in_flight_can_never_be_zero() {
        // Zero permits is a deadlock, not a configuration.
        assert_eq!(
            ConnectionConfig::new().with_max_in_flight(0).max_in_flight,
            1
        );
    }
}
