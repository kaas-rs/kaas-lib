//! M11 acceptance: cancelling a thousand scans leaks nothing.
//!
//! `cargo test -p kafka-read --test leak -- --ignored`
//!
//! This is rule 5 and the stream design put together. A scan does its work as
//! it is polled — there is no background task filling a channel — so dropping
//! the stream must free everything it held, immediately, with no cleanup path
//! to forget. A thousand cancellations at random points is the cheapest way to
//! find out whether that is true or merely intended.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::time::Duration;

use futures::StreamExt;
use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_read::{ScanSpec, TailSpec};
use testkit::Cluster as _;

/// Resident set size in kibibytes, from `/proc/self/statm`.
///
/// Linux only, which is where the acceptance suite runs. Returns `None`
/// elsewhere, and the test degrades to asserting on connection count alone
/// rather than failing for the wrong reason.
fn rss_kib() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    // `getconf PAGESIZE` is 4 KiB on every platform this runs on.
    Some(pages * 4)
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_thousand_cancelled_scans_return_to_baseline() {
    let fixture = testkit::single_broker().await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    admin
        .create_topics([NewTopic::new("leaky", 6, 1)])
        .await
        .unwrap();

    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 50000 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9092 --topic leaky \
                 --producer-property batch.size=32768 --producer-property linger.ms=20"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    let cluster = admin.cluster().clone();

    // Warm up: open every connection and let allocators settle, so the
    // baseline is a steady state rather than a cold start.
    for _ in 0..5 {
        let mut stream = Box::pin(
            kafka_read::scan(&cluster, ScanSpec::new("leaky"))
                .await
                .unwrap(),
        );
        for _ in 0..50 {
            let _ = stream.next().await;
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    let baseline_connections = cluster.pool().live_connections().await;
    let baseline_rss = rss_kib();

    // A cheap deterministic sequence: no rand dependency, and a failure is
    // reproducible.
    let mut seed = 0x2545_F491_4F6C_DD1Du64;
    for iteration in 0..1_000u32 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let polls = usize::try_from(seed >> 58).unwrap_or(0);

        let mut stream = Box::pin(
            kafka_read::scan(&cluster, ScanSpec::new("leaky"))
                .await
                .unwrap(),
        );
        for _ in 0..polls {
            if stream.next().await.is_none() {
                break;
            }
        }
        // Cancel mid-flight.
        drop(stream);

        // Every hundredth iteration, cancel a backward scan too — it has a
        // different shape and its own buffers.
        if iteration % 100 == 0 {
            let spec = TailSpec::new("leaky", 50);
            let mut tail = Box::pin(kafka_read::tail(&cluster, &spec));
            let _ = tokio::time::timeout(Duration::from_millis(5), &mut tail).await;
            drop(tail);
        }
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Connections: the pool holds one per broker, and a cancelled scan must
    // never leave an extra one behind.
    let after_connections = cluster.pool().live_connections().await;
    assert_eq!(
        after_connections, baseline_connections,
        "connection count drifted from {baseline_connections} to {after_connections} \
         over a thousand cancelled scans"
    );

    // And the connection still works, which rules out "returned to baseline by
    // dying".
    let mut stream = Box::pin(
        kafka_read::scan(&cluster, ScanSpec::new("leaky").with_limit(10))
            .await
            .unwrap(),
    );
    let mut records = 0;
    while let Some(event) = stream.next().await {
        if matches!(event.unwrap(), kafka_read::ScanEvent::Record(_)) {
            records += 1;
        }
    }
    assert_eq!(records, 10);

    if let (Some(before), Some(after)) = (baseline_rss, rss_kib()) {
        // Allocators do not return everything to the OS, so this is a
        // generous ceiling looking for a *leak* — unbounded growth — rather
        // than for fragmentation.
        let growth = after.saturating_sub(before);
        println!("RSS {before} KiB -> {after} KiB (+{growth} KiB)");
        assert!(
            growth < 128 * 1024,
            "RSS grew by {growth} KiB over a thousand cancelled scans"
        );
    } else {
        println!("RSS unavailable on this platform; asserted on connection count only");
    }
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn cancelling_between_every_single_event_is_safe() {
    // The tightest cancellation schedule: drop after exactly one event, over
    // and over. If any state is left half-updated by a cancellation, this is
    // where it shows.
    let fixture = testkit::single_broker().await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    admin
        .create_topics([NewTopic::new("tight", 3, 1)])
        .await
        .unwrap();
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 5000 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9092 --topic tight"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    let cluster = admin.cluster().clone();
    for _ in 0..200 {
        let mut stream = Box::pin(
            kafka_read::scan(&cluster, ScanSpec::new("tight"))
                .await
                .unwrap(),
        );
        let first = stream.next().await;
        assert!(first.is_some());
        assert!(first.unwrap().is_ok());
    }

    // And a full scan afterwards still returns every record.
    let mut stream = Box::pin(
        kafka_read::scan(&cluster, ScanSpec::new("tight"))
            .await
            .unwrap(),
    );
    let mut records = 0usize;
    while let Some(event) = stream.next().await {
        if matches!(event.unwrap(), kafka_read::ScanEvent::Record(_)) {
            records += 1;
        }
    }
    assert_eq!(records, 5000);
}
