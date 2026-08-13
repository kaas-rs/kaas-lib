//! M10 acceptance: the backward scan.
//!
//! `cargo test -p kafka-read --test backward -- --ignored`
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

use std::time::Duration;

use kafka_admin::{Admin, ClusterConfig, NewTopic, OffsetSpec};
use kafka_read::{Cluster, TailAnchor, TailSpec};
use testkit::{BrokerConfig, Cluster as _, KafkaCluster};

/// Produce with deliberately varied batch sizes, so batch boundaries do not
/// line up with any step the backward walk might choose. A partition whose
/// batches are uniform makes this test pass for the wrong reason.
async fn produce_randomised(fixture: &KafkaCluster, topic: &str, total: u32) {
    let command = format!(
        "for i in $(seq 1 40); do \
           BATCH=$(( (i * 7919) % 400 + 25 )); \
           LINGER=$(( (i * 13) % 30 + 1 )); \
           START=$(( (i - 1) * {per} + 1 )); \
           END=$(( i * {per} )); \
           seq $START $END | /opt/kafka/bin/kafka-console-producer.sh \
             --bootstrap-server localhost:9093 --topic {topic} \
             --producer-property batch.size=$((BATCH * 64)) \
             --producer-property linger.ms=$LINGER; \
         done",
        per = total / 40
    );
    fixture
        .exec(0, vec!["bash".to_owned(), "-c".to_owned(), command])
        .await
        .expect("produced");
}

async fn setup(topic: &str, partitions: i32, config: BrokerConfig) -> (KafkaCluster, Cluster) {
    let fixture = testkit::single_broker_with(config).await.expect("broker");
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .expect("admin");
    admin
        .create_topics([NewTopic::new(topic, partitions, 1)])
        .await
        .expect("topic");
    let cluster = admin.cluster().clone();
    (fixture, cluster)
}

#[testkit::integration_test]
async fn the_last_500_of_100k_records_are_exactly_the_last_500() {
    let (fixture, cluster) = setup("tailed", 1, BrokerConfig::new()).await;
    produce_randomised(&fixture, "tailed", 100_000).await;

    let admin = Admin::new(cluster.clone());
    let latest = admin
        .list_offsets([("tailed".to_owned(), 0)], OffsetSpec::Latest)
        .await
        .unwrap();
    let end = latest[0].1.as_ref().unwrap().offset.unwrap();
    assert!(end >= 100_000, "only {end} records were produced");

    let before = partition_bytes_read(&cluster).await;

    let tails = kafka_read::tail(&cluster, &TailSpec::new("tailed", 500))
        .await
        .expect("tail");
    let after = partition_bytes_read(&cluster).await;

    assert_eq!(tails.len(), 1);
    let records = &tails[0].records;
    assert_eq!(records.len(), 500, "exactly the number asked for");

    // In order, and *the last* 500 — not any 500.
    assert!(
        records.windows(2).all(|w| w[0].offset < w[1].offset),
        "the tail came back out of order"
    );
    assert_eq!(
        records.last().map(|r| r.offset),
        Some(end - 1),
        "the tail must end at the high watermark"
    );
    assert_eq!(
        records.first().map(|r| r.offset),
        Some(end - 500),
        "the tail must start exactly 500 records back"
    );

    // The assertion the milestone exists for. A naive implementation reads the
    // whole partition; this one must read a sliver of it.
    let read = after.saturating_sub(before);
    let partition_size = admin
        .topic_sizes()
        .await
        .unwrap()
        .into_iter()
        .find(|(name, _)| name == "tailed")
        .and_then(|(_, size)| size.ok())
        .map(|size| size.logical_bytes)
        .unwrap_or_default();
    assert!(partition_size > 0, "the topic reported no size");

    let budget = partition_size / 20; // 5%
    assert!(
        i64::try_from(read).unwrap_or(i64::MAX) < budget,
        "read {read} bytes for the last 500 of a {partition_size}-byte partition \
         (budget {budget}); a whole-partition read would look exactly like this"
    );
    println!(
        "read {read} bytes of {partition_size} in {} fetches",
        tails[0].fetches
    );
}

#[testkit::integration_test]
async fn a_compacted_topic_with_offset_gaps_terminates_and_counts_correctly() {
    // The second failure mode: offsets are no longer one apart, so offset
    // arithmetic over-estimates every window and a fixed step crawls. This
    // asserts both that it terminates and that the count is right.
    let (fixture, cluster) = setup(
        "compacted",
        1,
        BrokerConfig::new()
            .with_property("log.cleaner.enable", "true")
            .with_property("log.cleaner.backoff.ms", "1000")
            .with_property("log.cleaner.min.cleanable.ratio", "0.01")
            // 1 MiB is the *minimum* Kafka accepts for the broker-level
            // setting; 16384 makes the broker refuse to start outright:
            //   Invalid value 16384 for configuration log.segment.bytes:
            //   Value must be at least 1048576
            // The topic-level `segment.bytes` below has no such floor, and it
            // is the one that actually drives compaction for this topic — so
            // the small-segment intent survives.
            .with_property("log.segment.bytes", "1048576")
            .with_property("log.roll.ms", "1000"),
    )
    .await;

    let admin = Admin::new(cluster.clone());
    admin
        .alter_configs([(
            kafka_admin::ConfigResource::topic("compacted"),
            vec![
                kafka_admin::ConfigChange::set("cleanup.policy", "compact"),
                kafka_admin::ConfigChange::set("min.cleanable.dirty.ratio", "0.01"),
                kafka_admin::ConfigChange::set("segment.bytes", "16384"),
                kafka_admin::ConfigChange::set("delete.retention.ms", "1000"),
                kafka_admin::ConfigChange::set("max.compaction.lag.ms", "1000"),
            ],
        )])
        .await
        .unwrap();

    // Write the same 200 keys over and over. Compaction collapses each key to
    // its newest value, leaving large offset gaps behind.
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "for round in $(seq 1 50); do \
                   for k in $(seq 1 200); do echo \"key$k:v$round\"; done \
                 done | /opt/kafka/bin/kafka-console-producer.sh \
                   --bootstrap-server localhost:9093 --topic compacted \
                   --property parse.key=true --property key.separator=:"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    // Give the cleaner time to actually create the gaps.
    tokio::time::sleep(Duration::from_secs(25)).await;

    let started = std::time::Instant::now();
    let tails = tokio::time::timeout(
        Duration::from_secs(60),
        kafka_read::tail(&cluster, &TailSpec::new("compacted", 100)),
    )
    .await
    .expect("the backward walk must terminate on a compacted topic")
    .expect("tail");

    assert_eq!(tails.len(), 1);
    let records = &tails[0].records;
    assert_eq!(records.len(), 100, "exact count despite the offset gaps");
    assert!(
        records.windows(2).all(|w| w[0].offset < w[1].offset),
        "out of order"
    );
    println!(
        "compacted tail took {:?} and {} fetches, offsets {}..{}",
        started.elapsed(),
        tails[0].fetches,
        records[0].offset,
        records[99].offset
    );
}

#[testkit::integration_test]
async fn a_tail_longer_than_the_partition_returns_everything_without_looping() {
    let (fixture, cluster) = setup("short", 1, BrokerConfig::new()).await;
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 25 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9093 --topic short"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    let tails = tokio::time::timeout(
        Duration::from_secs(30),
        kafka_read::tail(&cluster, &TailSpec::new("short", 1000)),
    )
    .await
    .expect("must not loop looking for records that do not exist")
    .unwrap();
    assert_eq!(tails[0].records.len(), 25);
}

#[testkit::integration_test]
async fn an_empty_partition_tails_to_nothing() {
    let (_fixture, cluster) = setup("empty", 1, BrokerConfig::new()).await;
    let tails = tokio::time::timeout(
        Duration::from_secs(30),
        kafka_read::tail(&cluster, &TailSpec::new("empty", 100)),
    )
    .await
    .expect("must not loop on an empty partition")
    .unwrap();
    assert!(tails[0].records.is_empty());
    assert_eq!(tails[0].fetches, 0, "an empty partition needs no fetch");
}

#[testkit::integration_test]
async fn a_multi_partition_tail_spreads_the_limit() {
    let (fixture, cluster) = setup("spread", 4, BrokerConfig::new()).await;
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                // Round-robin, not the default sticky partitioner. Sticky
                // fills one partition per batch, which is right for
                // throughput and useless here: this test is about the tail
                // limit being *spread* across partitions, so the fixture has
                // to actually spread. With sticky, 4000 records landed on two
                // partitions and the test failed as "200 records is too few",
                // blaming the reader for the producer's batching.
                "seq 1 4000 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9093 --topic spread \
                 --producer-property \
                 partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    let tails = kafka_read::tail(&cluster, &TailSpec::new("spread", 400))
        .await
        .unwrap();
    assert_eq!(tails.len(), 4);
    let total: usize = tails.iter().map(|t| t.records.len()).sum();
    // The topic holds 4000, so the limit is a target that is met — not a
    // ration that partitions may leave partly unspent.
    assert_eq!(total, 400, "records for a limit of 400");
    for tail in &tails {
        assert!(tail.records.len() <= 100, "{} records", tail.records.len());
        assert!(
            !tail.reached_log_start,
            "partition {} holds ~1000 records; its walk stopped at its share",
            tail.partition
        );
        assert!(
            tail.log_end > tail.log_start,
            "the bounds the walk measured must ride along"
        );
    }
}

#[testkit::integration_test]
async fn idle_partitions_do_not_eat_the_limit() {
    // The shape that motivated the fix (`kaas-canary-v1`): three partitions,
    // two of which hold nothing. Dividing the limit before looking meant
    // ⌈500/3⌉ from the one full partition — 167 rows of the 500 asked for.
    let (fixture, cluster) = setup("lopsided", 3, BrokerConfig::new()).await;
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                // One shared key, so every record lands on one partition and
                // the other two stay empty.
                "seq 1 2000 | sed 's/^/k:/' | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9093 --topic lopsided \
                 --property parse.key=true --property key.separator=:"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    let tails = kafka_read::tail(&cluster, &TailSpec::new("lopsided", 500))
        .await
        .unwrap();
    assert_eq!(tails.len(), 3);
    let total: usize = tails.iter().map(|t| t.records.len()).sum();
    assert_eq!(total, 500, "idle partitions must not eat the limit");

    for tail in &tails {
        if tail.records.is_empty() {
            assert_eq!(tail.log_start, tail.log_end, "an idle partition");
            assert!(
                tail.reached_log_start,
                "nothing below an empty partition's window"
            );
        } else {
            assert!(
                !tail.reached_log_start,
                "1500 records remain below the 500 returned, and the caller \
                 must be able to page to them"
            );
        }
    }
}

#[testkit::integration_test]
async fn an_offset_anchored_tail_includes_the_anchor_and_stops_there() {
    // The property the whole anchor exists for: `Offset(n)` is an *inclusive*
    // upper bound. An off-by-one here is invisible on a live cluster — the
    // window still looks plausible — so it is asserted on both ends.
    let (fixture, cluster) = setup("anchored", 1, BrokerConfig::new()).await;
    produce_randomised(&fixture, "anchored", 100_000).await;

    let anchor = 40_000i64;
    let tails = kafka_read::tail(
        &cluster,
        &TailSpec::new("anchored", 500).ending_at(TailAnchor::Offset(anchor)),
    )
    .await
    .expect("anchored tail");

    let records = &tails[0].records;
    assert_eq!(records.len(), 500, "exactly the number asked for");
    assert_eq!(
        records.last().map(|r| r.offset),
        Some(anchor),
        "the anchor itself must be the newest record in the window"
    );
    assert_eq!(
        records.first().map(|r| r.offset),
        Some(anchor - 499),
        "and the window must extend backwards from it, not forwards"
    );
    assert!(
        records.iter().all(|r| r.offset <= anchor),
        "nothing above the anchor may appear"
    );
}

#[testkit::integration_test]
async fn a_partition_whose_log_end_is_below_the_anchor_yields_its_own_tail() {
    // Partitions of one topic sit at different offsets, so a single anchor is
    // routinely past the end of some of them. That is a result, not an error:
    // the alternative is a multi-partition window that fails whenever the
    // partitions are unevenly filled, which is always.
    let (fixture, cluster) = setup("shortanchor", 1, BrokerConfig::new()).await;
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 40 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9093 --topic shortanchor"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    let tails = tokio::time::timeout(
        Duration::from_secs(30),
        kafka_read::tail(
            &cluster,
            &TailSpec::new("shortanchor", 10).ending_at(TailAnchor::Offset(9_000_000)),
        ),
    )
    .await
    .expect("an anchor past the log end must not loop")
    .expect("tail");

    let anchored = &tails[0].records;
    assert_eq!(anchored.len(), 10);

    let plain = kafka_read::tail(&cluster, &TailSpec::new("shortanchor", 10))
        .await
        .unwrap();
    assert_eq!(
        anchored.iter().map(|r| r.offset).collect::<Vec<_>>(),
        plain[0]
            .records
            .iter()
            .map(|r| r.offset)
            .collect::<Vec<_>>(),
        "clamped to the log end, an anchored tail is the ordinary tail"
    );
}

#[testkit::integration_test]
async fn an_anchor_below_the_log_start_is_an_empty_result() {
    let (fixture, cluster) = setup("expired", 1, BrokerConfig::new()).await;
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 40 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9093 --topic expired"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    // Offset -1 is below any log start. Invariant 3: a window with nothing in
    // it is a successful description of a partition, not a failure of one.
    let tails = tokio::time::timeout(
        Duration::from_secs(30),
        kafka_read::tail(
            &cluster,
            &TailSpec::new("expired", 10).ending_at(TailAnchor::Offset(-1)),
        ),
    )
    .await
    .expect("must not loop below the log start")
    .expect("an out-of-range anchor is a result, not an error");
    assert!(tails[0].records.is_empty());
    assert_eq!(tails[0].fetches, 0, "and it costs no fetch");
}

#[testkit::integration_test]
async fn an_anchored_tail_converges_on_a_compacted_topic() {
    // The step-growing behaviour is the reason §3.3 could be additive rather
    // than a second implementation: an arbitrary anchor faces exactly the
    // offset gaps the log end does, and needs exactly the same convergence.
    let (fixture, cluster) = setup(
        "compactedanchor",
        1,
        BrokerConfig::new()
            .with_property("log.cleaner.enable", "true")
            .with_property("log.cleaner.backoff.ms", "1000")
            .with_property("log.cleaner.min.cleanable.ratio", "0.01")
            .with_property("log.segment.bytes", "1048576")
            .with_property("log.roll.ms", "1000"),
    )
    .await;

    let admin = Admin::new(cluster.clone());
    admin
        .alter_configs([(
            kafka_admin::ConfigResource::topic("compactedanchor"),
            vec![
                kafka_admin::ConfigChange::set("cleanup.policy", "compact"),
                kafka_admin::ConfigChange::set("min.cleanable.dirty.ratio", "0.01"),
                kafka_admin::ConfigChange::set("segment.bytes", "16384"),
                kafka_admin::ConfigChange::set("delete.retention.ms", "1000"),
                kafka_admin::ConfigChange::set("max.compaction.lag.ms", "1000"),
            ],
        )])
        .await
        .unwrap();

    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "for round in $(seq 1 50); do \
                   for k in $(seq 1 200); do echo \"key$k:v$round\"; done \
                 done | /opt/kafka/bin/kafka-console-producer.sh \
                   --bootstrap-server localhost:9093 --topic compactedanchor \
                   --property parse.key=true --property key.separator=:"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(25)).await;

    let latest = admin
        .list_offsets([("compactedanchor".to_owned(), 0)], OffsetSpec::Latest)
        .await
        .unwrap();
    let end = latest[0].1.as_ref().unwrap().offset.unwrap();
    // Anchor well inside the compacted region, where the offsets are sparse
    // and a fixed step would crawl.
    let anchor = end / 2;

    let started = std::time::Instant::now();
    let tails = tokio::time::timeout(
        Duration::from_secs(60),
        kafka_read::tail(
            &cluster,
            &TailSpec::new("compactedanchor", 100).ending_at(TailAnchor::Offset(anchor)),
        ),
    )
    .await
    .expect("an anchored walk must converge over offset gaps too")
    .expect("tail");

    let records = &tails[0].records;
    assert_eq!(records.len(), 100, "exact count despite the offset gaps");
    assert!(
        records.iter().all(|r| r.offset <= anchor),
        "nothing above the anchor"
    );
    assert!(
        records.windows(2).all(|w| w[0].offset < w[1].offset),
        "out of order"
    );
    println!(
        "anchored compacted tail took {:?} and {} fetches",
        started.elapsed(),
        tails[0].fetches
    );
}

#[testkit::integration_test]
async fn a_timestamp_anchor_stops_at_or_before_the_instant() {
    let (fixture, cluster) = setup("timed", 1, BrokerConfig::new()).await;

    let produce = |range: &'static str| {
        let fixture = &fixture;
        async move {
            fixture
                .exec(
                    0,
                    vec![
                        "bash".to_owned(),
                        "-c".to_owned(),
                        format!(
                            "seq {range} | /opt/kafka/bin/kafka-console-producer.sh \
                             --bootstrap-server localhost:9093 --topic timed"
                        ),
                    ],
                )
                .await
                .unwrap();
        }
    };

    produce("1 500").await;
    // Wide enough that clock skew between the test process and the broker
    // cannot move a record across the boundary.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let boundary = now_millis();
    tokio::time::sleep(Duration::from_secs(3)).await;
    produce("501 1000").await;

    let tails = kafka_read::tail(
        &cluster,
        &TailSpec::new("timed", 50).ending_at(TailAnchor::Timestamp(boundary)),
    )
    .await
    .expect("timestamp-anchored tail");

    let records = &tails[0].records;
    assert_eq!(records.len(), 50);
    assert!(
        records.iter().all(|r| r.timestamp <= boundary),
        "a timestamp anchor is an at-or-before bound"
    );
    // The newest record in the window must be the last one written before the
    // boundary — the 500th — rather than merely *some* record before it.
    assert_eq!(
        records.last().map(|r| r.offset),
        Some(499),
        "the window must end at the boundary, not short of it"
    );
}

#[testkit::integration_test]
async fn a_timestamp_anchor_after_every_record_reads_the_whole_tail() {
    let (fixture, cluster) = setup("timedpast", 1, BrokerConfig::new()).await;
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 100 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9093 --topic timedpast"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    // `ListOffsets` answers -1 — "no offset at or after this" — which means
    // every record is at or before the instant, not that there is nothing to
    // read. Reading that as an empty window is the obvious wrong turn.
    let far_future = now_millis() + 86_400_000;

    let tails = kafka_read::tail(
        &cluster,
        &TailSpec::new("timedpast", 10).ending_at(TailAnchor::Timestamp(far_future)),
    )
    .await
    .unwrap();
    assert_eq!(tails[0].records.len(), 10);
    assert_eq!(tails[0].records.last().map(|r| r.offset), Some(99));
}

/// Wall-clock now, in the units the wire uses.
///
/// `try_from` rather than `as`: the workspace denies silent narrowing in tests
/// too, and a truncated timestamp would make a timestamp-anchored assertion
/// fail for a reason that has nothing to do with the anchor.
fn now_millis() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

/// Bytes read from every connection in the pool.
async fn partition_bytes_read(cluster: &Cluster) -> u64 {
    let snapshot = cluster.snapshot();
    let mut total = 0;
    for broker in snapshot.brokers() {
        if let Ok(connection) = cluster.pool().get(broker.node_id).await {
            total += connection.stats_snapshot().bytes_received;
        }
    }
    total
}
