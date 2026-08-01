//! M9 acceptance: the forward scan.
//!
//! `cargo test -p kafka-read --test forward -- --ignored`
//!
//! In-container shell tools bootstrap `localhost:9093`, the BROKER
//! listener — see `testkit::INTERNAL_BOOTSTRAP`. Port 9092 is advertised
//! as the *host-mapped* port for the test process, so a client inside the
//! container follows metadata to a port nothing is listening on and dies
//! with a bare TimeoutException.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::HashMap;

use futures::StreamExt;
use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_read::{Cluster, ScanEvent, ScanSpec, StartPosition};
use testkit::{Cluster as _, KafkaCluster};

/// Produce `count` records into `topic` with a given codec, using the broker's
/// own console producer so the bytes on disk are what a real Kafka client
/// writes.
async fn produce(fixture: &KafkaCluster, topic: &str, from: u32, count: u32, codec: &str) {
    let command = format!(
        "seq {from} {} | /opt/kafka/bin/kafka-console-producer.sh \
         --bootstrap-server localhost:9093 --topic {topic} \
         --producer-property compression.type={codec} \
         --producer-property batch.size=16384 \
         --producer-property linger.ms=50",
        from + count - 1
    );
    fixture
        .exec(0, vec!["bash".to_owned(), "-c".to_owned(), command])
        .await
        .expect("produced");
}

async fn setup(partitions: i32) -> (KafkaCluster, Cluster, Admin) {
    let fixture = testkit::cluster(3).await.expect("cluster");
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .expect("admin");
    admin
        .create_topics([NewTopic::new("scanned", partitions, 1)])
        .await
        .expect("topic");
    let cluster = admin.cluster().clone();
    (fixture, cluster, admin)
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn ten_thousand_records_across_six_partitions_with_mixed_codecs() {
    let (fixture, cluster, _admin) = setup(6).await;

    // 2000 records per codec, five codecs: 10k records, every compression the
    // protocol supports, all in one topic.
    let mut produced = 0u32;
    for codec in ["none", "gzip", "snappy", "lz4", "zstd"] {
        produce(&fixture, "scanned", produced + 1, 2000, codec).await;
        produced += 2000;
    }

    let mut stream = Box::pin(
        kafka_read::scan(
            &cluster,
            ScanSpec::new("scanned").from(StartPosition::Earliest),
        )
        .await
        .expect("scan starts"),
    );

    let mut count = 0usize;
    let mut per_partition: HashMap<i32, Vec<i64>> = HashMap::new();
    let mut saw_progress = false;
    let mut done = None;

    while let Some(event) = stream.next().await {
        match event.expect("no scan-level failure") {
            ScanEvent::Record(record) => {
                count += 1;
                per_partition
                    .entry(record.partition)
                    .or_default()
                    .push(record.offset);
            }
            ScanEvent::Malformed { offset, reason, .. } => {
                panic!("unexpected malformed batch at {offset}: {reason}")
            }
            ScanEvent::Progress(progress) => {
                saw_progress = true;
                // A progress bar, not a spinner: the fraction has to be a
                // number, not an unknown.
                assert!(progress.fraction().is_some(), "{progress:?}");
            }
            ScanEvent::PartitionComplete { .. } => {}
            ScanEvent::Done(progress) => done = Some(progress),
        }
    }

    assert_eq!(count, 10_000, "exact count across all five codecs");
    assert!(saw_progress, "a 10k-record scan must report progress");

    let done = done.expect("the scan ends with Done");
    assert_eq!(done.records_emitted, 10_000);
    assert_eq!(done.partitions_active, 0);

    // Per-partition ordering is exact, always. This is the assertion that
    // catches an interleave that reorders within a partition.
    assert_eq!(per_partition.len(), 6, "records landed in every partition");
    for (partition, offsets) in &per_partition {
        let mut sorted = offsets.clone();
        sorted.sort_unstable();
        assert_eq!(
            offsets, &sorted,
            "partition {partition} came out of log order"
        );
        assert!(
            offsets.windows(2).all(|w| w[0] < w[1]),
            "partition {partition} repeated an offset"
        );
    }
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_corrupt_batch_yields_malformed_and_the_scan_continues() {
    // Hand-crafted corruption, produced by writing over a record batch in the
    // broker's own log segment. Everything before and after it must still
    // arrive.
    let fixture = testkit::single_broker().await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    admin
        .create_topics([NewTopic::new("corrupt", 1, 1)])
        .await
        .unwrap();

    produce(&fixture, "corrupt", 1, 300, "none").await;
    // Force a roll so the damaged segment is not the active one.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Overwrite bytes in the middle of the segment, past the first batch's
    // header, so the batch framing survives and its contents do not.
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "SEG=$(ls /tmp/kaas-testkit/logs/corrupt-0/*.log | head -1); \
                 SIZE=$(stat -c%s $SEG); \
                 dd if=/dev/urandom of=$SEG bs=1 seek=$((SIZE/2)) count=64 conv=notrunc"
                    .to_owned(),
            ],
        )
        .await
        .expect("segment damaged");

    let cluster = admin.cluster().clone();
    let mut stream = Box::pin(
        kafka_read::scan(&cluster, ScanSpec::new("corrupt"))
            .await
            .expect("scan starts"),
    );

    let mut records = 0usize;
    let mut malformed = 0usize;
    while let Some(event) = stream.next().await {
        match event.expect("a corrupt batch must not fail the scan") {
            ScanEvent::Record(_) => records += 1,
            ScanEvent::Malformed { .. } => malformed += 1,
            _ => {}
        }
    }

    // The whole point: the scan finished, and it told us which part it could
    // not read rather than failing.
    assert!(malformed > 0, "the damage was not reported");
    assert!(records > 0, "no records survived the damaged batch");
    println!("{records} records, {malformed} malformed batches");
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn truncating_a_fetch_mid_batch_produces_no_malformed_events() {
    // The not-a-bug that matters most. A `max_bytes` small enough to cut a
    // batch in half is the normal case for any large fetch; a decoder that
    // reports it claims corruption at the end of every fetch and is worse
    // than useless.
    let fixture = testkit::single_broker().await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    admin
        .create_topics([NewTopic::new("truncated", 1, 1)])
        .await
        .unwrap();
    produce(&fixture, "truncated", 1, 2000, "none").await;

    let cluster = admin.cluster().clone();
    let mut spec = ScanSpec::new("truncated");
    // Small enough that almost every fetch ends mid-batch.
    spec.partition_max_bytes = 512;
    spec.fetch_max_bytes = 512;

    let mut stream = Box::pin(kafka_read::scan(&cluster, spec).await.expect("scan starts"));

    let mut records = 0usize;
    while let Some(event) = stream.next().await {
        match event.expect("truncation is not a failure") {
            ScanEvent::Record(_) => records += 1,
            ScanEvent::Malformed { offset, reason, .. } => {
                panic!("truncation reported as corruption at {offset}: {reason}")
            }
            _ => {}
        }
    }
    assert_eq!(records, 2000, "a tight byte budget must not lose records");
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_limited_scan_stops_early_and_a_filter_narrows_it() {
    let (fixture, cluster, _admin) = setup(1).await;
    produce(&fixture, "scanned", 1, 1000, "none").await;

    let mut stream = Box::pin(
        kafka_read::scan(&cluster, ScanSpec::new("scanned").with_limit(10))
            .await
            .unwrap(),
    );
    let mut records = 0usize;
    while let Some(event) = stream.next().await {
        if matches!(event.unwrap(), ScanEvent::Record(_)) {
            records += 1;
        }
    }
    assert_eq!(records, 10);

    let filtered = kafka_read::scan(
        &cluster,
        ScanSpec::new("scanned").with_filter(kafka_read::RecordFilter::ValueContains(
            bytes::Bytes::from_static(b"999"),
        )),
    )
    .await
    .unwrap();
    let mut stream = Box::pin(filtered);
    let mut matched = 0usize;
    while let Some(event) = stream.next().await {
        if matches!(event.unwrap(), ScanEvent::Record(_)) {
            matched += 1;
        }
    }
    assert!((1..100).contains(&matched), "{matched} records matched 999");
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_scan_from_a_timestamp_skips_what_came_before() {
    let (fixture, cluster, _admin) = setup(1).await;
    produce(&fixture, "scanned", 1, 500, "none").await;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let cutoff = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    produce(&fixture, "scanned", 501, 500, "none").await;

    let mut stream = Box::pin(
        kafka_read::scan(
            &cluster,
            ScanSpec::new("scanned").from(StartPosition::Timestamp(cutoff)),
        )
        .await
        .unwrap(),
    );
    let mut records = 0usize;
    while let Some(event) = stream.next().await {
        if matches!(event.unwrap(), ScanEvent::Record(_)) {
            records += 1;
        }
    }
    assert_eq!(records, 500, "the timestamp cut the scan in half");
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn dropping_a_scan_stops_it() {
    // Cancel safety, at the scan level. The stream does its own work as it is
    // polled, so dropping it must free everything immediately rather than
    // leaving a task filling a channel nobody reads.
    let (fixture, cluster, _admin) = setup(3).await;
    produce(&fixture, "scanned", 1, 5000, "none").await;

    let before = cluster.pool().live_connections().await;
    {
        let mut stream = Box::pin(
            kafka_read::scan(&cluster, ScanSpec::new("scanned"))
                .await
                .unwrap(),
        );
        for _ in 0..5 {
            let _ = stream.next().await;
        }
    }
    // The pool's connections are shared and stay open; what must not happen is
    // a new one per abandoned scan.
    assert_eq!(cluster.pool().live_connections().await, before);
}
