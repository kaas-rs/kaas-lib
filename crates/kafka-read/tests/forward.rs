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
    // Round-robin rather than the default sticky partitioner. Sticky fills one
    // partition per batch, which is correct for throughput and wrong for a
    // fixture: 10k records reached only 3 of 6 partitions and the test failed
    // as "records landed in every partition", blaming the scan for the
    // producer's batching.
    let command = format!(
        "seq {from} {} | /opt/kafka/bin/kafka-console-producer.sh \
         --bootstrap-server localhost:9093 --topic {topic} \
         --producer-property compression.type={codec} \
         --producer-property batch.size=16384 \
         --producer-property linger.ms=50 \
         --producer-property \
         partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner",
        from + count - 1
    );
    fixture
        .exec(0, vec!["bash".to_owned(), "-c".to_owned(), command])
        .await
        .expect("produced");
}

/// The one cluster this binary's tests share.
///
/// Never dropped, because a `static` is not: the containers go with the
/// ephemeral runner pod on CI, and may want `docker container prune` locally.
static SHARED: tokio::sync::OnceCell<KafkaCluster> = tokio::sync::OnceCell::const_new();

async fn shared_fixture() -> &'static KafkaCluster {
    SHARED
        .get_or_init(|| async { testkit::cluster(3).await.expect("cluster") })
        .await
}

/// A topic of this test's own, with the partition count it asked for.
///
/// The name is load-bearing. Every test here created a topic called
/// `scanned`, sized 6, 3 or 1 — harmless while each had its own cluster, and
/// on a shared one the first creation wins and the rest scan a shape they
/// never asked for.
async fn setup(name: &str, partitions: i32) -> (&'static KafkaCluster, Cluster, String) {
    let fixture = shared_fixture().await;
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .expect("admin");
    let topic = format!("scanned-{name}");
    admin
        .create_topics([NewTopic::new(topic.clone(), partitions, 1)])
        .await
        .expect("topic");
    let cluster = admin.cluster().clone();
    (fixture, cluster, topic)
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn ten_thousand_records_across_six_partitions_with_mixed_codecs() {
    let (fixture, cluster, topic) = setup("ten-thousand-recor", 6).await;

    // 2000 records per codec, five codecs: 10k records, every compression the
    // protocol supports, all in one topic.
    let mut produced = 0u32;
    for codec in ["none", "gzip", "snappy", "lz4", "zstd"] {
        produce(fixture, topic.as_str(), produced + 1, 2000, codec).await;
        produced += 2000;
    }

    let mut stream = Box::pin(
        kafka_read::scan(
            &cluster,
            ScanSpec::new(topic.as_str()).from(StartPosition::Earliest),
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

    // Twelve separate producer runs, not one. Each invocation flushes and
    // closes its own producer, so each leaves at least one batch behind — and
    // "one corrupt batch must not fail the scan" is only a claim about
    // *several* batches.
    //
    // A single run of 300 tiny records fits inside one 16 KiB batch, so the
    // damage below landed on the only batch in the log. Everything that could
    // have survived was inside it, and the test failed as "no records survived
    // the damaged batch" while the decoder had done precisely the right thing:
    // reported the batch it could not read, and finished.
    for round in 0..12 {
        produce(&fixture, "corrupt", round * 25 + 1, 25, "none").await;
    }
    // Let the writes reach the segment before measuring its size.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Overwrite bytes halfway through the segment. With a dozen batches in the
    // log that lands inside one of the middle ones, leaving whole batches on
    // either side — which is the arrangement the assertions below describe.
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
    // And the damage was real. A scan that returned all 300 would mean the
    // overwrite missed the log entirely and `malformed` came from somewhere
    // else — which would make the two assertions above pass for no reason.
    assert!(
        records < 300,
        "{records} records came back intact; the segment was not actually damaged"
    );
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
    let (fixture, cluster, topic) = setup("a-limited-scan-sto", 1).await;
    produce(fixture, topic.as_str(), 1, 1000, "none").await;

    let mut stream = Box::pin(
        kafka_read::scan(&cluster, ScanSpec::new(topic.as_str()).limit(10))
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
        ScanSpec::new(topic.as_str()).filter(kafka_read::RecordFilter::ValueContains(
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
    let (fixture, cluster, topic) = setup("a-scan-from-a-time", 1).await;
    produce(fixture, topic.as_str(), 1, 500, "none").await;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let cutoff = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    produce(fixture, topic.as_str(), 501, 500, "none").await;

    let mut stream = Box::pin(
        kafka_read::scan(
            &cluster,
            ScanSpec::new(topic.as_str()).from(StartPosition::Timestamp(cutoff)),
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
async fn a_scan_from_an_offset_never_emits_records_before_it() {
    // A fetch begins at whatever *batch* contains the offset, so the first one
    // routinely hands back records from before it. The walk in `backward` has
    // always filtered them; this asserts the forward path agrees. Without it,
    // "browse from offset 1000037" answers with 1000005 and the reader has no
    // way to tell that from an off-by-N in their own bookkeeping.
    let (fixture, cluster, topic) = setup("a-scan-from-an-off", 1).await;
    produce(fixture, topic.as_str(), 1, 5000, "none").await;

    // Several starts, because whether one lands mid-batch depends on the
    // producer's batching — a single offset can pass by luck.
    for start in [37i64, 991, 1_234, 2_500] {
        let mut stream = Box::pin(
            kafka_read::scan(
                &cluster,
                ScanSpec::new(topic.as_str())
                    .partitions([0])
                    .from(StartPosition::Offset(start))
                    .limit(20),
            )
            .await
            .unwrap(),
        );

        let mut first = None;
        while let Some(event) = stream.next().await {
            if let ScanEvent::Record(record) = event.unwrap() {
                assert!(
                    record.offset >= start,
                    "scan from {start} emitted offset {}",
                    record.offset
                );
                first.get_or_insert(record.offset);
            }
        }
        assert_eq!(
            first,
            Some(start),
            "the first record must be the one asked for, not the next batch"
        );
    }
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_following_scan_waits_at_the_log_end_and_sees_what_arrives_next() {
    // Without `following`, a scan from `Latest` plans against a log end it is
    // already standing on and finishes immediately having emitted nothing —
    // which looks exactly like a working live view of an idle topic, and is
    // not one.
    let (fixture, cluster, topic) = setup("a-following-scan-w", 3).await;
    produce(fixture, topic.as_str(), 1, 100, "none").await;

    let mut stream = Box::pin(
        kafka_read::scan(
            &cluster,
            ScanSpec::new(topic.as_str())
                .from(StartPosition::Latest)
                .following(),
        )
        .await
        .unwrap(),
    );

    // Nothing has been written since the scan opened, so it must be waiting
    // rather than finished.
    let idle = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next()).await;
    assert!(
        idle.is_err(),
        "a following scan must not end on an idle topic, got {:?}",
        idle.map(|event| event.map(|e| e.map(|_| ())))
    );

    produce(fixture, topic.as_str(), 101, 20, "none").await;

    let mut seen = 0;
    while seen < 20 {
        match tokio::time::timeout(std::time::Duration::from_secs(20), stream.next()).await {
            Ok(Some(Ok(ScanEvent::Record(record)))) => {
                assert!(record.offset >= 0);
                seen += 1;
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(error))) => panic!("scan failed: {error}"),
            Ok(None) => panic!("a following scan ended by itself after {seen} records"),
            Err(_) => panic!("only {seen} of 20 records arrived"),
        }
    }
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn following_does_not_stall_behind_an_idle_partition() {
    // The regression this guards: a tail that polls every idle partition
    // before emitting anything pays one `max_wait_ms` per record. On a topic
    // where one partition is busy and the rest are silent — the normal case —
    // that is a couple of records a second, which reads as a hung UI.
    let (fixture, cluster, topic) = setup("following-does-not", 6).await;
    produce(fixture, topic.as_str(), 1, 60, "none").await;

    let mut stream = Box::pin(
        kafka_read::scan(
            &cluster,
            ScanSpec::new(topic.as_str())
                .from(StartPosition::Earliest)
                .following(),
        )
        .await
        .unwrap(),
    );

    let started = std::time::Instant::now();
    let mut seen = 0;
    while seen < 60 {
        match tokio::time::timeout(std::time::Duration::from_secs(30), stream.next()).await {
            Ok(Some(Ok(ScanEvent::Record(_)))) => seen += 1,
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(error))) => panic!("scan failed: {error}"),
            Ok(None) => panic!("a following scan ended by itself"),
            Err(_) => panic!("only {seen} of 60 records arrived"),
        }
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "60 buffered records took {elapsed:?}; the tail is polling idle partitions per record"
    );
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn dropping_a_following_scan_stops_it() {
    // The same cancel-safety property as a bounded scan, on the stream that
    // actually stays open long enough for a leak to matter.
    let (fixture, cluster, topic) = setup("dropping-a-followi", 3).await;
    produce(fixture, topic.as_str(), 1, 500, "none").await;

    for _ in 0..2 {
        let mut stream = Box::pin(
            kafka_read::scan(&cluster, ScanSpec::new(topic.as_str()).following())
                .await
                .unwrap(),
        );
        for _ in 0..5 {
            let _ = stream.next().await;
        }
    }

    let before = cluster.pool().live_connections().await;
    {
        let mut stream = Box::pin(
            kafka_read::scan(&cluster, ScanSpec::new(topic.as_str()).following())
                .await
                .unwrap(),
        );
        for _ in 0..5 {
            let _ = stream.next().await;
        }
    }
    assert_eq!(cluster.pool().live_connections().await, before);
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn dropping_a_scan_stops_it() {
    // Cancel safety, at the scan level. The stream does its own work as it is
    // polled, so dropping it must free everything immediately rather than
    // leaving a task filling a channel nobody reads.
    let (fixture, cluster, topic) = setup("dropping-a-scan-st", 3).await;
    produce(fixture, topic.as_str(), 1, 5000, "none").await;

    // Warm the pool first. A scan legitimately opens a connection to each
    // partition leader, and the pool keeps them — that is reuse working, not a
    // leak. Measuring `before` on a cold pool counted those as growth and
    // failed as 4 against 2. The property is that *repeating* an abandoned
    // scan does not add any more.
    for _ in 0..2 {
        let mut stream = Box::pin(
            kafka_read::scan(&cluster, ScanSpec::new(topic.as_str()))
                .await
                .unwrap(),
        );
        for _ in 0..5 {
            let _ = stream.next().await;
        }
    }

    let before = cluster.pool().live_connections().await;
    {
        let mut stream = Box::pin(
            kafka_read::scan(&cluster, ScanSpec::new(topic.as_str()))
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
