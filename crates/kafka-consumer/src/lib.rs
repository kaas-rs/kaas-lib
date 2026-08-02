//! The consume path: incremental fetch sessions, a streaming fetcher, and
//! offsets for a consumer that is not a group member.
//!
//! # What this is, and what M17/M18 add
//!
//! A [`Consumer`] here reads an **explicitly assigned** set of partitions.
//! Nothing rebalances it, nothing heartbeats, and no broker knows it exists as
//! a member of anything. That is a complete and useful mode — it is how you
//! pin a reader to a partition, or run a single-instance tail — and it is also
//! the substrate the group protocols sit on. Group membership is M17 (KIP-848)
//! and M18 (classic).
//!
//! It does borrow a group's *offset storage*: set
//! [`ConsumerConfig::group_id`] and [`Consumer::commit`] stores positions
//! under it using the non-member sentinel. Borrowing the storage is not
//! joining the group.
//!
//! # Why this is not `kafka-read::scan`
//!
//! `scan` is bounded and reports progress, because a UI is drawing a page. A
//! consumer runs until told to stop, and its interesting operations —
//! [`Consumer::seek`], [`Consumer::pause`], [`Consumer::resume`] — are about
//! changing its mind mid-stream, which a bounded scan never does.
//!
//! They share the fetcher's shape and the tolerant decoder, and neither is a
//! special case of the other. `scan` and `tail` keep the **legacy** fetch
//! sentinel (`session_id = 0, session_epoch = -1`) deliberately: they are
//! one-shot, and a session would leave broker state behind for a client that
//! is not coming back. See [`session`](crate) for the incremental rules.

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

mod classic;
mod consumer;
mod fetcher;
mod group;
mod offsets;
mod session;

pub use consumer::{ClassicConsumer, Consumer, ConsumerConfig, GroupConsumer, Position};
pub use offsets::CommittedOffset;

pub use kafka_conn::{Error, Result};
pub use kafka_meta::{Cluster, ClusterConfig};
pub use kafka_read::{Record, RecordOutcome, Visibility};
