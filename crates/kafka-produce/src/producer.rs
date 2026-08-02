//! The producer: resolve a partition, buffer the record, and report where it
//! landed.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use kafka_conn::{Error, ErrorCode, Result};
use tokio::sync::{OnceCell, oneshot};

use crate::accumulator::Accumulator;
use crate::config::ProducerConfig;
use crate::dispatch::Dispatcher;
use crate::partition::Partitioner;
use crate::record::{ProducerRecord, RecordMetadata};

/// A record's outcome, once the broker has answered for its batch.
///
/// Returned by [`Producer::enqueue`] for callers that want many records in
/// flight at once. Dropping it is allowed and does not cancel the write — the
/// record has already been accepted for delivery, and only the result is
/// discarded.
#[derive(Debug)]
pub struct Delivery(oneshot::Receiver<Result<RecordMetadata>>);

impl Future for Delivery {
    type Output = Result<RecordMetadata>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            // A dropped sender means the accumulator went away with the record
            // unresolved, which is the same observable situation as a closed
            // connection.
            .map(|received| received.unwrap_or_else(|_| Err(crate::accumulator::producer_gone())))
    }
}

/// Writes records to a cluster.
///
/// Cheap to clone; every clone shares the metadata cache, the connection pool,
/// the sticky partitioner's state and the accumulator. Sharing the accumulator
/// is what makes batching work across clones — two clones producing to one
/// partition fill the same batch, not two.
#[derive(Debug, Clone)]
pub struct Producer {
    cluster: kafka_meta::Cluster,
    config: ProducerConfig,
    partitioner: Arc<Partitioner>,
    /// Spawned on first use rather than in [`Producer::new`], which is not
    /// async and therefore cannot assume a runtime exists to spawn onto.
    accumulator: Arc<OnceCell<Accumulator>>,
}

impl Producer {
    /// Wrap an existing cluster handle.
    pub fn new(cluster: kafka_meta::Cluster, config: ProducerConfig) -> Self {
        Self {
            cluster,
            config,
            partitioner: Arc::new(Partitioner::new()),
            accumulator: Arc::new(OnceCell::new()),
        }
    }

    /// Connect to a cluster and produce to it.
    ///
    /// Clamps the connections it opens to the number of in-flight requests the
    /// producer's guarantees permit: one without idempotence, five with it.
    /// See [`Producer::max_in_flight`].
    pub async fn connect(
        bootstrap: impl IntoIterator<Item = impl Into<String>>,
        mut cluster_config: kafka_meta::ClusterConfig,
        config: ProducerConfig,
    ) -> Result<Self> {
        cluster_config.connection = cluster_config
            .connection
            .with_max_in_flight(config.max_in_flight());
        Ok(Self::new(
            kafka_meta::Cluster::connect(bootstrap, cluster_config).await?,
            config,
        ))
    }

    /// How many requests this producer's connections may have in flight.
    ///
    /// One without idempotence, five with it — the broker tracks exactly five
    /// in-flight sequence windows per partition, so five is a ceiling rather
    /// than a tuning knob.
    ///
    /// [`Producer::connect`] applies this to the connections it opens.
    /// [`Producer::new`] cannot: the cluster handed to it is shared with
    /// whatever else is using it, and clamping a pool the admin and read paths
    /// also use would slow them down to protect a guarantee they do not need.
    /// That is safe here because ordering does not rest on this number — the
    /// accumulator holds at most one batch per partition on the wire, so a
    /// re-sent batch can never overtake a later one regardless. The clamp is
    /// defence for the connection layer, not the mechanism.
    pub fn max_in_flight(&self) -> usize {
        self.cluster.pool().config().max_in_flight
    }

    /// The underlying cluster handle.
    pub fn cluster(&self) -> &kafka_meta::Cluster {
        &self.cluster
    }

    /// The configuration this producer was built with.
    pub fn config(&self) -> &ProducerConfig {
        &self.config
    }

    /// The partitioner, for callers that want to rotate the sticky choice.
    pub fn partitioner(&self) -> &Arc<Partitioner> {
        &self.partitioner
    }

    /// Write one record and wait for the broker to acknowledge it.
    ///
    /// Equivalent to awaiting [`Producer::enqueue`]. A caller writing many
    /// records should use `enqueue` and await the handles together — awaiting
    /// each `send` in turn allows only one record in flight at a time, which
    /// batches nothing.
    ///
    /// # Which failures are retried, and why the distinction is the whole point
    ///
    /// Two kinds of failure look similar and differ completely in what they
    /// permit:
    ///
    /// * **The broker rejected the batch.** A response arrived carrying an
    ///   error code — `NOT_LEADER_OR_FOLLOWER` after a leader moved, say. The
    ///   records were definitively *not* appended, so re-sending cannot
    ///   duplicate anything. These are retried, after refreshing the metadata
    ///   that made us ask the wrong broker.
    /// * **The outcome is unknown.** A timeout, or a connection that died with
    ///   the request in flight. The records may well have been written and the
    ///   acknowledgement lost. Whether this is retried depends entirely on
    ///   [`ProducerConfig::idempotent`]: with a producer id the broker
    ///   recognises the re-sent batch and answers with the original offsets, so
    ///   it is safe; without one, re-sending is how a duplicate is written, so
    ///   the error is surfaced to the caller, who knows whether a duplicate is
    ///   worse than a gap.
    ///
    /// Collapsing the two is a bug in either direction: retry everything
    /// without sequence numbers and you duplicate on every timeout; retry
    /// nothing and an ordinary leader election becomes a delivery failure.
    ///
    /// # Cancel safety
    ///
    /// Dropping this future does **not** cancel the write. Once the record has
    /// been accepted into the accumulator it will be sent; dropping only
    /// discards the result. Dropping while still waiting for buffer space does
    /// cancel it, and in that case the record was never accepted.
    pub async fn send(&self, record: ProducerRecord) -> Result<RecordMetadata> {
        self.enqueue(record).await?.await
    }

    /// Accept a record for delivery and hand back a handle to its outcome.
    ///
    /// Returns as soon as the record is buffered, which is what lets a caller
    /// keep many records in flight and is how batching earns its throughput.
    /// The returned [`Delivery`] resolves when the broker has answered for the
    /// batch the record travelled in.
    ///
    /// Waits when the buffer is full — that wait is the backpressure.
    pub async fn enqueue(&self, record: ProducerRecord) -> Result<Delivery> {
        let topic = record.topic.clone();
        let partition = self.resolve_partition(&record).await?;
        let receiver = self
            .accumulator()
            .await
            .append(topic, partition, record)
            .await?;
        Ok(Delivery(receiver))
    }

    /// Send every buffered record and wait for all of them to be acknowledged.
    ///
    /// Returns once the accumulator is empty and nothing is on the wire. Errors
    /// belong to the individual records and are reported through their own
    /// [`Delivery`]; this reports only that the flush itself could not be
    /// carried out.
    pub async fn flush(&self) -> Result<()> {
        self.accumulator().await.flush().await
    }

    /// The accumulator, spawning it on first use.
    async fn accumulator(&self) -> &Accumulator {
        self.accumulator
            .get_or_init(|| async {
                Accumulator::spawn(
                    Dispatcher::new(self.cluster.clone(), self.config.clone()),
                    self.config.clone(),
                )
            })
            .await
    }

    /// Which partition a record belongs to.
    async fn resolve_partition(&self, record: &ProducerRecord) -> Result<i32> {
        let partition_count = self.partition_count(&record.topic).await?;

        match record.partition {
            Some(explicit) => {
                if explicit < 0 || explicit >= partition_count {
                    return Err(Error::InvalidRequest(format!(
                        "{}: partition {explicit} does not exist; the topic has {partition_count}",
                        record.topic
                    )));
                }
                Ok(explicit)
            }
            None => self
                .partitioner
                .assign(
                    &record.topic,
                    record.key.as_ref().map(|key| key.as_ref()),
                    partition_count,
                )
                .ok_or_else(|| {
                    Error::InvalidRequest(format!("{}: topic has no partitions", record.topic))
                }),
        }
    }

    /// How many partitions a topic has, refreshing metadata if we have never
    /// seen it.
    async fn partition_count(&self, topic: &str) -> Result<i32> {
        if let Some(info) = self.cluster.snapshot().topic(topic) {
            return i32::try_from(info.partitions.len()).map_err(|_| {
                Error::InvalidRequest(format!("{topic}: implausible partition count"))
            });
        }

        let refreshed = self.cluster.refresh_topics(&[topic]).await?;
        let info = refreshed.topic(topic).ok_or_else(|| {
            Error::from_code(ErrorCode::UnknownTopicOrPartition, Some(topic.to_owned()))
        })?;
        i32::try_from(info.partitions.len())
            .map_err(|_| Error::InvalidRequest(format!("{topic}: implausible partition count")))
    }
}
