//! The write path: encode a record batch, route it to the partition leader,
//! and report where it landed.
//!
//! This is the first half of lifting kaas-lib past its admin-first scope. The
//! read path answers "show me what is in this partition"; this one answers
//! "put this there, and tell me the truth about whether it worked".
//!
//! ```no_run
//! # async fn example(cluster: kafka_meta::Cluster) -> kafka_produce::Result<()> {
//! use kafka_produce::{Producer, ProducerConfig, ProducerRecord};
//!
//! let producer = Producer::new(cluster, ProducerConfig::new());
//! let meta = producer
//!     .send(
//!         ProducerRecord::new("orders")
//!             .key("customer-7")
//!             .value("{\"total\":42}")
//!             .header("content-type", "application/json"),
//!     )
//!     .await?;
//! println!("landed at {}:{}", meta.partition, meta.offset);
//! # Ok(())
//! # }
//! ```
//!
//! # `acks=0` is not offered, and that is a decision rather than an omission
//!
//! PLAN.md M12 requires this to be settled before any encoder code exists,
//! because the failure mode is silent: `acks=0` is a request the broker sends
//! **no response to at all**. [`kafka_conn::Connection`] correlates every
//! in-flight request on a `HashMap<i32, oneshot::Sender<_>>`, so an `acks=0`
//! produce would register a waiter nothing ever resolves — and every
//! *successful* write would surface to the caller as a timeout.
//!
//! The two ways out were a fire-and-forget path on the connection that drops
//! the correlation entry at send time, or refusing the mode. This crate
//! refuses it, and refuses it at the type level: [`Acks`] has no `None`
//! variant, so the unsupported state cannot be constructed rather than being
//! constructed and rejected. Three reasons, in order of weight:
//!
//! 1. A second send path punches a hole in the connection actor's invariant
//!    that every in-flight request has a waiter, and would have to be held to
//!    rule 5 (cancel safety) independently and forever.
//! 2. `acks=0` gives the caller no delivery signal whatsoever. A library whose
//!    stated contract is that partial failure is a *result* should not ship a
//!    mode whose entire character is discarding results.
//! 3. It is incompatible with idempotence (M14), which needs the response to
//!    advance its per-partition sequence numbers. Offering the mode now would
//!    mean withdrawing it there.
//!
//! What the mode actually buys — not waiting on the leader — is what the
//! accumulator in M13 provides safely, by batching rather than by throwing the
//! acknowledgement away.
//!
//! # What this milestone is, and is not
//!
//! M12 is one record on the wire, acked, and readable back. There is no
//! accumulator yet: [`Producer::send`] encodes a single-record batch and awaits
//! its response, so throughput is one round trip per record. M13 adds batching
//! behind the same signature. Idempotence and transactions are M14 and M15;
//! until M14 lands, a produce is **not** retried, because retrying without
//! sequence numbers is how a duplicate is written.

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

mod config;
mod encode;
mod partition;
mod producer;
mod record;

pub use config::{Acks, Compression, ProducerConfig};
pub use partition::{Partitioner, murmur2, partition_for_key};
pub use producer::Producer;
pub use record::{ProducerRecord, RecordMetadata};

pub use kafka_conn::{Error, Result};
pub use kafka_meta::{Cluster, ClusterConfig};
