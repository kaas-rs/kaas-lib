//! Framing, correlation, version negotiation, TLS, SASL and the connection
//! actor — everything between a TCP socket and a typed Kafka request.
//!
//! # What this crate owns
//!
//! * [`ApiKey`] and [`ErrorCode`], our own versions of the two protocol
//!   vocabularies that would otherwise leak everywhere. Both carry an
//!   `Unknown` variant, because `kafka-protocol` 0.17 ships Kafka 4.0 schemas
//!   and the brokers we target are newer than that.
//! * [`Connection`], one socket with concurrent, correlated, deadline-bounded
//!   request/response.
//! * The read-only gate, enforced on [`ApiKey::is_mutating`] inside
//!   [`Connection::send`] rather than over an admin method surface.
//!
//! # The one deliberate exception to rule 1
//!
//! [`Connection::send`] is generic over `kafka_protocol::protocol::Request`.
//! This crate is the wire boundary, and a parallel request trait here would
//! convert protocol types into protocol types for no gain. Everything built on
//! top of this crate is held to the rule without exception: no `kafka_protocol`
//! type may appear in the public API of `kafka-meta`, `kafka-admin` or
//! `kafka-read`.
//!
//! ```no_run
//! # async fn example() -> kafka_conn::Result<()> {
//! use kafka_conn::{ApiKey, Connection, ConnectionConfig};
//!
//! let conn = Connection::connect("localhost:9092", ConnectionConfig::new()).await?;
//! for entry in conn.versions().entries() {
//!     println!("{} broker={:?} ours={:?}", entry.api_key, entry.broker, entry.ours);
//! }
//! assert!(conn.versions().supports(ApiKey::Metadata));
//! # Ok(())
//! # }
//! ```

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

mod api_key;
mod codec;
mod config;
mod conn;
mod error;
mod error_code;
mod rpc;
mod sasl;
mod scram;
mod stats;
mod tls;
mod transport;
mod versions;

pub use api_key::ApiKey;
pub use codec::DEFAULT_MAX_FRAME_BYTES;
pub use config::ConnectionConfig;
pub use conn::Connection;
pub use error::{Error, Result};
pub use error_code::{ErrorCode, KNOWN_ERROR_CODES};
pub use rpc::Rpc;
pub use sasl::{SaslConfig, SaslMechanism};
pub use scram::ScramHash;
pub use stats::{ConnectionStats, StatsSnapshot};
pub use tls::{ClientCertificate, TlsConfig, TrustAnchors};
pub use transport::Transport;
pub use versions::{ApiVersions, BrokerApiVersion, VersionRange, our_range};

/// The codec, re-exported.
///
/// Crates above this one should reach for `kafka_conn::protocol` rather than
/// depending on `kafka-protocol` directly, so the version is pinned in exactly
/// one manifest and an upstream bump is a single coordinated change. Note that
/// re-exporting these types is *not* licence to put them in a public
/// signature — see the crate docs.
pub mod protocol {
    pub use kafka_protocol::protocol::{
        Decodable, Encodable, HeaderVersion, Message, Request, StrBytes,
    };
    pub use kafka_protocol::{compression, indexmap, messages, records};
}
