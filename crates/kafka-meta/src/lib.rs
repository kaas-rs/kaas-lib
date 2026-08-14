//! Metadata, routing, connection pooling and the error taxonomy.
//!
//! This is the layer that knows what a cluster looks like. Everything above it
//! — admin RPCs, the read path — sends through [`Cluster`], which resolves the
//! right broker, retries on the errors that mean "your view is stale", and
//! keeps an immutable snapshot readers can take without blocking.
//!
//! # The two tables
//!
//! [`routing`] and the error taxonomy are first-class artifacts, each in one
//! file, because both encode knowledge that is otherwise scattered into
//! individual call sites and then quietly diverges.
//!
//! The error table lives in `kafka-conn` — [`ErrorCode`], [`Error`] — and is
//! re-exported here so the two sit together at the layer that acts on them.
//! It has to be defined down there because every crate in the workspace,
//! including the connection layer itself, needs to classify a broker's answer,
//! and a workspace with two error types would push a `From` conversion into
//! every call site.
//!
//! ```no_run
//! # async fn example() -> kafka_meta::Result<()> {
//! use kafka_meta::{Cluster, ClusterConfig};
//!
//! let cluster = Cluster::connect(["localhost:9092"], ClusterConfig::default()).await?;
//! let snapshot = cluster.snapshot();
//! println!(
//!     "{} brokers, fetched {:?} ago",
//!     snapshot.brokers().len(),
//!     snapshot.age()
//! );
//!
//! let leader = cluster.leader_for("orders", 0).await?;
//! let coordinator = cluster.coordinator_for("my-group").await?;
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

mod cluster;
mod group;
mod pool;
mod retry;
mod routing;
mod snapshot;

pub use cluster::{Cluster, ClusterConfig};
pub use group::ConsumerGroupMetadata;
pub use pool::{BrokerPool, Endpoint};
pub use retry::{RetryPolicy, Verdict, reask};
pub use routing::{BrokerSelector, CoordinatorKind, Routing, routing};
pub use snapshot::{BrokerInfo, MetadataSnapshot, PartitionInfo, TopicId, TopicInfo};

/// The error taxonomy, re-exported.
///
/// One type across the workspace: see the crate docs for why it is defined a
/// layer down.
pub use kafka_conn::{ApiKey, Error, ErrorCode, KNOWN_ERROR_CODES, Result};
