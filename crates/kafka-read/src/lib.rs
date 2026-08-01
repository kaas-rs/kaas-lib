//! The read path: browse a topic forwards, or read its tail.
//!
//! Shaped for a UI rather than for a consumer group. There is no rebalance, no
//! commit, no membership — the question is "show me what is in this partition",
//! and the answer is a bounded stream that never materialises a `Vec` and never
//! grows with the size of the topic.
//!
//! # Tolerant decoding
//!
//! The point of the whole design. One batch that will not decode does not fail
//! the scan; it becomes a [`ScanEvent::Malformed`] carrying the raw bytes and
//! the offsets it covered, and the scan continues. See [`RecordOutcome`] for
//! why the granularity is a batch rather than a record, decided deliberately
//! rather than discovered.
//!
//! Equally important is what is *not* reported as corruption: a trailing batch
//! cut short by `max_bytes`, a control batch, and — unless asked otherwise —
//! records from aborted transactions. See the `batch` module docs.
//!
//! ```no_run
//! # async fn example(cluster: &kafka_meta::Cluster) -> kafka_read::Result<()> {
//! use futures::StreamExt;
//! use kafka_read::{ScanEvent, ScanSpec, StartPosition, TailSpec};
//!
//! // Browse forwards.
//! let mut stream = Box::pin(
//!     kafka_read::scan(cluster, ScanSpec::new("orders").from(StartPosition::Earliest)).await?,
//! );
//! while let Some(event) = stream.next().await {
//!     match event? {
//!         ScanEvent::Record(record) => println!("{}: {:?}", record.offset, record.value),
//!         ScanEvent::Progress(progress) => println!("{:?}", progress.fraction()),
//!         ScanEvent::Malformed { offset, reason, .. } => {
//!             println!("offset {offset} did not decode: {reason}")
//!         }
//!         _ => {}
//!     }
//! }
//!
//! // Or read the tail — the most-used view in any Kafka UI.
//! let tails = kafka_read::tail(cluster, &TailSpec::new("orders", 500)).await?;
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

mod backward;
mod batch;
mod decompress;
mod fetch;
mod offsets;
mod record;
mod scan;

pub use backward::{PartitionTail, TailSpec, tail};
pub use batch::{AbortedTransaction, DecodeOptions, DecodedPartition, Visibility, decode_records};
pub use record::{DecodeError, Record, RecordOutcome, TimestampType};
pub use scan::{RecordFilter, ScanEvent, ScanProgress, ScanSpec, StartPosition, scan};

pub use kafka_conn::{Error, Result};
pub use kafka_meta::{Cluster, ClusterConfig};
