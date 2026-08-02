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
//! # Batching, and how to get it
//!
//! Records are buffered per partition and sent together. [`Producer::send`]
//! accepts one record and waits for it, which means a loop of `send().await`
//! keeps exactly one record in flight and batches nothing. To get the
//! throughput, use [`Producer::enqueue`], which returns as soon as the record
//! is buffered, and await the [`Delivery`] handles together:
//!
//! ```no_run
//! # async fn example(producer: &kafka_produce::Producer) -> kafka_produce::Result<()> {
//! # use kafka_produce::ProducerRecord;
//! let mut pending = Vec::new();
//! for i in 0..10_000 {
//!     pending.push(producer.enqueue(ProducerRecord::new("t").value(format!("{i}"))).await?);
//! }
//! for delivery in pending {
//!     delivery.await?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! `linger` defaults to zero and that is not a reason to raise it: a partition
//! holds one batch on the wire at a time, so records arriving during a round
//! trip accumulate into the next batch on their own. Batching scales with load
//! rather than with the setting.
//!
//! # What this milestone is, and is not
//!
//! M13 is batching, bounded buffer memory and per-record delivery futures.
//! Idempotence and transactions are M14 and M15; until M14 lands a produce is
//! retried **only** when the broker rejected it, because re-sending a request
//! whose outcome is unknown is how a duplicate is written.
//!
//! One consequence of that is visible in the configuration: at most one batch
//! per partition is in flight, because retrying a rejected batch while a later
//! one is already on the wire reorders the log with no error anywhere. See
//! [`accumulator`](crate) — M14 is what lifts it.

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

mod accumulator;
mod config;
mod dispatch;
mod encode;
mod partition;
mod producer;
mod record;

pub use config::{Acks, Compression, ProducerConfig};
pub use partition::{Partitioner, murmur2, partition_for_key};
pub use producer::{Delivery, Producer};
pub use record::{ProducerRecord, RecordMetadata};

pub use kafka_conn::{Error, Result};
pub use kafka_meta::{Cluster, ClusterConfig};
