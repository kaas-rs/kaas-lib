//! Broker fixtures for the acceptance suite.
//!
//! Every milestone in PLAN.md is verified against a real broker in a
//! container — there are no mocked brokers in this workspace, by rule. This
//! crate is what makes that affordable.
//!
//! ```no_run
//! # async fn example() -> testkit::Result<()> {
//! use testkit::{BrokerConfig, Cluster, SaslMechanism, Security};
//!
//! // The common case.
//! let broker = testkit::single_broker().await?;
//! let addr = broker.bootstrap_csv();
//!
//! // A three-node cluster, for anything replica-aware.
//! let cluster = testkit::cluster(3).await?;
//!
//! // Or a configured one.
//! let sasl = testkit::single_broker_with(
//!     BrokerConfig::new()
//!         .with_security(Security::SaslPlaintext)
//!         .with_mechanism(SaslMechanism::Plain)
//!         .with_user("alice", "alice-pw"),
//! )
//! .await?;
//! # Ok(())
//! # }
//! ```
//!
//! Tests should take [`&dyn Cluster`](Cluster) rather than [`KafkaCluster`]:
//! see the module docs on [`harness`] for why that seam matters.

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

pub mod config;
pub mod harness;

mod certs;
mod error;
mod image;
mod kafka;
mod token;

pub use config::{
    BrokerConfig, ClientAuth, DEFAULT_IMAGE, DEFAULT_TAG, INTERNAL_BOOTSTRAP, SaslMechanism,
    SaslUser, Security,
};
pub use error::{Error, Result};
pub use harness::{Cluster, ExecOutput, ExternalCluster, exec_ok};
pub use kafka::{KafkaCluster, cluster, cluster_with, single_broker, single_broker_with};
/// The attribute every integration test wears: `#[tokio::test]` +
/// `#[ignore = "needs Docker"]` plus a hard two-minute deadline on the whole
/// test, container boot included. `cargo xtask` refuses a hand-written
/// `#[ignore]` in workspace test sources, so this is the only door into the
/// integration job — which is what makes the deadline a property of the job
/// rather than a convention.
pub use testkit_macros::integration_test;
pub use token::unsecured_jws;
