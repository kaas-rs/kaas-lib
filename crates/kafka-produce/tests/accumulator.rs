//! M13 acceptance: the accumulator.
//!
//! `cargo test -p kafka-produce --test accumulator -- --ignored`
//!
//! The load-bearing assertion here is not the record count — M12 already
//! proved a record survives the round trip. It is the *request* count. A
//! producer with a broken accumulator delivers every record, in order, and
//! passes every correctness check while sending one request per record. That
//! is a throughput bug wearing a correctness result, and only counting
//! requests catches it.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_conn::StatsSnapshot;
use kafka_produce::{Compression, Producer, ProducerConfig, ProducerRecord};
use kafka_read::{Cluster, ScanEvent, ScanSpec, StartPosition};
use testkit::{Cluster as _, KafkaCluster};

async fn setup(topic: &str, partitions: i32) -> (KafkaCluster, Cluster, Admin) {
    let fixture = testkit::cluster(3).await.expect("cluster");
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .expect("admin");
    admin
        .create_topics([NewTopic::new(topic, partitions, 3)])
        .await
        .expect("topic");
    await_topic(&admin, topic).await;
    let cluster = admin.cluster().clone();
    (fixture, cluster, admin)
}

/// Creation is asynchronous on the broker; produce before the leader is
/// elected and the failure is `LEADER_NOT_AVAILABLE`, which reads like a bug
/// in the producer.
async fn await_topic(admin: &Admin, topic: &str) {
    for _ in 0..50 {
        let described = admin.describe_topics([topic.to_owned()]).await;
        if let Ok(results) = described
            && results.iter().any(|(_, result)| result.is_ok())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("{topic} never became describable");
}

/// Read a whole topic back through `kafka-read`.
async fn read_topic(cluster: &Cluster, topic: &str, partitions: i32) -> Vec<kafka_read::Record> {
    let spec = ScanSpec::new(topic)
        .partitions(0..partitions)
        .from(StartPosition::Earliest);
    let mut stream = Box::pin(kafka_read::scan(cluster, spec).await.expect("scan"));

    let mut records = Vec::new();
    while let Some(event) = stream.next().await {
        match event.expect("scan event") {
            ScanEvent::Record(record) => records.push(record),
            ScanEvent::Malformed { offset, reason, .. } => {
                panic!("we wrote offset {offset} and could not read it: {reason}")
            }
            _ => {}
        }
    }
    records
}

/// Traffic across every broker connection in the pool, and how many brokers
/// that sum actually covers.
///
/// Summed rather than per-connection because the accumulator groups batches by
/// leader, so the requests are spread over three sockets by design.
///
/// # Why the broker ids are passed in rather than read from the snapshot
///
/// `Cluster::invalidate` installs an *empty* snapshot rather than marking the
/// existing one stale, and the produce dispatcher invalidates whenever a batch
/// comes back with an error a refresh would fix — routine while a freshly
/// created topic settles its leaders. A helper that reads `snapshot.brokers()`
/// at both ends of the measurement therefore samples three brokers before the
/// run and *zero* after it, and `saturating_sub` floors that delta at zero.
///
/// That is not hypothetical: the live run of this same measurement reported
/// **zero requests for 20,000 delivered records, and passed**. Since counting
/// requests exists to catch a producer that batches nothing, a measurement that
/// can read zero is indistinguishable from a perfect pass. Capturing the ids
/// once makes both ends cover the same set by construction, and `sampled`
/// catches the residual case where the pool declines a connection.
#[derive(Debug, Clone, Copy)]
struct Traffic {
    snapshot: StatsSnapshot,
    sampled: usize,
}

/// The cluster's broker ids, retried until the snapshot actually has some.
///
/// `Cluster::invalidate` installs an empty snapshot, and a produce running
/// concurrently invalidates on any error a refresh would fix. That race can
/// empty the snapshot *between* a refresh fetching and returning it, so even an
/// explicit refresh can hand back zero brokers — observed on one live run in
/// three against a real cluster.
async fn broker_ids(cluster: &Cluster) -> Vec<i32> {
    for _ in 0..10 {
        if let Ok(snapshot) = cluster.refresh().await {
            let ids: Vec<i32> = snapshot
                .brokers()
                .iter()
                .map(|broker| broker.node_id)
                .collect();
            if !ids.is_empty() {
                return ids;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("the cluster reported no brokers after ten refreshes");
}

async fn traffic(cluster: &Cluster, brokers: &[i32]) -> Traffic {
    let mut total = StatsSnapshot::default();
    let mut sampled = 0;
    for node_id in brokers {
        if let Ok(connection) = cluster.pool().get(*node_id).await {
            let stats = connection.stats_snapshot();
            total.bytes_sent += stats.bytes_sent;
            total.bytes_received += stats.bytes_received;
            total.requests_sent += stats.requests_sent;
            total.responses_received += stats.responses_received;
            sampled += 1;
        }
    }
    Traffic {
        snapshot: total,
        sampled,
    }
}

/// The acceptance case: 50k records, every one delivered, in far fewer than
/// 50k requests.
#[testkit::integration_test]
async fn fifty_thousand_records_arrive_in_under_five_hundred_requests() {
    const TOPIC: &str = "accumulator-batching";
    const PARTITIONS: i32 = 6;
    const RECORDS: usize = 50_000;

    let (_fixture, cluster, _admin) = setup(TOPIC, PARTITIONS).await;
    let producer = Producer::new(cluster.clone(), ProducerConfig::new());

    // Captured once and reused for both ends of the measurement — see
    // `Traffic` for why re-reading the snapshot is a trap, and taken before
    // the warmup because a produce can empty the snapshot mid-refresh.
    let brokers = broker_ids(&cluster).await;

    // Warm the pool and the metadata cache before the mark, so the delta is
    // produce traffic rather than the first Metadata round trip per broker.
    producer
        .send(ProducerRecord::new(TOPIC).partition(0).value("warmup"))
        .await
        .expect("warmup");

    let mark = traffic(&cluster, &brokers).await;

    // Enqueue everything before awaiting anything. Awaiting each send in turn
    // would keep exactly one record in flight and batch nothing — which is a
    // real way to use the API and a useless way to test it.
    let mut pending = Vec::with_capacity(RECORDS);
    for i in 0..RECORDS {
        let record = ProducerRecord::new(TOPIC)
            .key(format!("k{i}"))
            .value(format!("v{i}"));
        pending.push(producer.enqueue(record).await.expect("enqueued"));
    }

    let mut delivered = 0;
    for delivery in pending {
        delivery.await.expect("delivered");
        delivered += 1;
    }
    assert_eq!(delivered, RECORDS);

    let after = traffic(&cluster, &brokers).await;
    let used = after.snapshot.since(&mark.snapshot);

    // Check the measurement before believing it — see `Traffic`.
    assert_eq!(
        mark.sampled, after.sampled,
        "request counting is unreliable: the two ends cover different brokers"
    );
    assert!(
        used.requests_sent > 0,
        "request counting is broken: {RECORDS} records were delivered and \
         acknowledged, which cannot have taken zero requests"
    );
    assert!(
        used.requests_sent < 500,
        "batching did not happen: {} requests for {RECORDS} records",
        used.requests_sent
    );

    let read_back = read_topic(&cluster, TOPIC, PARTITIONS).await;
    assert_eq!(
        read_back.len(),
        RECORDS + 1,
        "every record produced must be readable, plus the warmup"
    );

    // And every one of them exactly once: a retry that duplicated would still
    // satisfy a >= assertion.
    let values: HashSet<Vec<u8>> = read_back
        .iter()
        .filter_map(|record| record.value.as_ref().map(|v| v.to_vec()))
        .collect();
    assert_eq!(values.len(), RECORDS + 1, "a value arrived twice");
}

/// Rule 4 in the write direction: the oversized record fails, the batch it
/// would have joined does not.
#[testkit::integration_test]
async fn one_oversized_record_among_a_thousand_fails_alone() {
    const TOPIC: &str = "accumulator-oversized";
    const RECORDS: usize = 1_000;
    const LIMIT: usize = 4 * 1024;

    let (_fixture, cluster, _admin) = setup(TOPIC, 3).await;
    let producer = Producer::new(
        cluster.clone(),
        ProducerConfig::new().max_request_size(LIMIT),
    );

    let mut outcomes = Vec::with_capacity(RECORDS);
    for i in 0..RECORDS {
        // One record, in the middle of the run, that cannot fit.
        let value = if i == RECORDS / 2 {
            Bytes::from(vec![b'x'; LIMIT * 4])
        } else {
            Bytes::from(format!("v{i}"))
        };
        let record = ProducerRecord::new(TOPIC).value(value);

        // An enqueue-time refusal and a broker rejection are the same thing
        // from the caller's side: this record failed and no other did.
        outcomes.push(match producer.enqueue(record).await {
            Ok(delivery) => delivery.await,
            Err(error) => Err(error),
        });
    }

    let failed: Vec<&kafka_conn::Error> = outcomes
        .iter()
        .filter_map(|outcome| outcome.as_ref().err())
        .collect();

    assert_eq!(
        failed.len(),
        1,
        "exactly one record was too large; the other {} must deliver",
        RECORDS - 1
    );
    assert_eq!(
        failed[0].code(),
        Some(kafka_conn::ErrorCode::MessageTooLarge),
        "an oversized record must say so, not fail generically: {}",
        failed[0]
    );
    assert_eq!(
        outcomes.iter().filter(|outcome| outcome.is_ok()).count(),
        RECORDS - 1
    );
}

/// Every codec, batched rather than one record at a time — compression only
/// has anything to work across once there is a batch.
#[testkit::integration_test]
async fn every_codec_round_trips_a_whole_batch() {
    const PARTITIONS: i32 = 3;
    const RECORDS: usize = 2_000;

    for (codec, topic) in [
        (Compression::None, "accumulator-none"),
        (Compression::Gzip, "accumulator-gzip"),
        (Compression::Snappy, "accumulator-snappy"),
        (Compression::Lz4, "accumulator-lz4"),
        (Compression::Zstd, "accumulator-zstd"),
    ] {
        let (_fixture, cluster, _admin) = setup(topic, PARTITIONS).await;
        let producer = Producer::new(cluster.clone(), ProducerConfig::new().compression(codec));

        let mut pending = Vec::with_capacity(RECORDS);
        for i in 0..RECORDS {
            let record = ProducerRecord::new(topic)
                .key(format!("k{i}"))
                .value(format!("value-{i}-{codec:?}"));
            pending.push(producer.enqueue(record).await.expect("enqueued"));
        }
        for delivery in pending {
            delivery.await.expect("delivered");
        }

        let read_back = read_topic(&cluster, topic, PARTITIONS).await;
        assert_eq!(read_back.len(), RECORDS, "{codec:?} lost records");

        let values: HashSet<Vec<u8>> = read_back
            .iter()
            .filter_map(|record| record.value.as_ref().map(|v| v.to_vec()))
            .collect();
        assert_eq!(values.len(), RECORDS, "{codec:?} duplicated a record");
    }
}

/// `flush` is the only way a caller who never awaits a delivery can know their
/// records left the buffer.
#[testkit::integration_test]
async fn flush_waits_for_records_nobody_awaited() {
    const TOPIC: &str = "accumulator-flush";
    const RECORDS: usize = 5_000;

    let (_fixture, cluster, _admin) = setup(TOPIC, 3).await;
    // A linger long enough that nothing would be sent on its own within the
    // test: if flush did not force the open batches out, the read-back is
    // empty rather than merely late.
    let producer = Producer::new(
        cluster.clone(),
        ProducerConfig::new().linger(Duration::from_secs(60)),
    );

    for i in 0..RECORDS {
        // The delivery handle is dropped immediately, which cancel safety
        // explicitly allows and which must not cancel the write.
        drop(
            producer
                .enqueue(ProducerRecord::new(TOPIC).value(format!("v{i}")))
                .await
                .expect("enqueued"),
        );
    }

    producer.flush().await.expect("flushed");

    let read_back = read_topic(&cluster, TOPIC, 3).await;
    assert_eq!(
        read_back.len(),
        RECORDS,
        "flush returned before every record was acknowledged"
    );
}

/// The buffer bound is real: a producer given far less memory than the payload
/// still delivers everything, by making its caller wait.
#[testkit::integration_test]
async fn a_tiny_buffer_applies_backpressure_rather_than_dropping_records() {
    const TOPIC: &str = "accumulator-backpressure";
    const RECORDS: usize = 10_000;

    let (_fixture, cluster, _admin) = setup(TOPIC, 3).await;
    let producer = Producer::new(
        cluster.clone(),
        // 64 KiB of buffer for roughly 400 KiB of records: every enqueue past
        // the first few hundred has to wait for an acknowledgement.
        ProducerConfig::new().buffer_memory(64 * 1024),
    );

    let mut pending = Vec::with_capacity(RECORDS);
    for i in 0..RECORDS {
        pending.push(
            producer
                .enqueue(ProducerRecord::new(TOPIC).value(format!("value-number-{i}")))
                .await
                .expect("enqueued"),
        );
    }
    for delivery in pending {
        delivery.await.expect("delivered");
    }

    let read_back = read_topic(&cluster, TOPIC, 3).await;
    assert_eq!(read_back.len(), RECORDS, "backpressure lost records");
}

/// Ordering is the property the one-batch-per-partition rule exists to
/// protect, and it is checked per partition because that is the only place
/// Kafka promises it.
#[testkit::integration_test]
async fn records_keep_their_order_within_a_partition() {
    const TOPIC: &str = "accumulator-order";
    const RECORDS: usize = 20_000;

    let (_fixture, cluster, _admin) = setup(TOPIC, 1).await;
    let producer = Producer::new(cluster.clone(), ProducerConfig::new());

    let mut pending = Vec::with_capacity(RECORDS);
    for i in 0..RECORDS {
        pending.push(
            producer
                .enqueue(
                    ProducerRecord::new(TOPIC)
                        .partition(0)
                        .value(format!("{i}")),
                )
                .await
                .expect("enqueued"),
        );
    }
    for delivery in pending {
        delivery.await.expect("delivered");
    }

    let read_back = read_topic(&cluster, TOPIC, 1).await;
    assert_eq!(read_back.len(), RECORDS);

    let observed: Vec<usize> = read_back
        .iter()
        .filter_map(|record| record.value.as_ref())
        .filter_map(|value| String::from_utf8_lossy(value).parse().ok())
        .collect();
    let expected: Vec<usize> = (0..RECORDS).collect();
    assert_eq!(
        observed, expected,
        "the log's order does not match the order records were enqueued in"
    );
}
