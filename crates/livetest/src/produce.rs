//! The write path, against a cluster we do not own.
//!
//! M12's container acceptance test proves the round trip against a broker
//! configured the way we chose. This proves it against one we did not: a
//! three-node Strimzi cluster running a Kafka build newer than our schemas,
//! where the partition leader is a different machine from the one that
//! answered metadata and where read-after-write is not immediate.
//!
//! Same discipline as `smoke`: everything created is prefixed, and cleanup
//! runs on the error path too.

use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use bytes::Bytes;
use futures::StreamExt;
use kafka_admin::{Admin, NewTopic};
use kafka_meta::Cluster;
use kafka_produce::{Compression, Producer, ProducerConfig, ProducerRecord, partition_for_key};
use kafka_read::{ScanEvent, ScanSpec, StartPosition};

use crate::report::{Outcome, Report};
use crate::target::{Target, run_token};

/// How many partitions the scratch topic gets.
const PARTITIONS: i32 = 6;
/// How long to wait for a produced record to become readable.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Which topic to produce to.
#[derive(Debug, Default)]
pub struct Options {
    /// An existing topic to use instead of creating a scratch one.
    ///
    /// The point is comparability: pointed at a topic another client already
    /// described, the leader map this reports can be diffed against that
    /// client's view of the *same* topic. Comparing leaders across two
    /// separately-created topics proves nothing, since each gets its own
    /// assignment.
    pub topic: Option<String>,
}

impl Options {
    pub fn parse(args: &[String]) -> Result<Self> {
        let mut options = Options::default();
        let mut rest = args.iter();
        while let Some(arg) = rest.next() {
            match arg.as_str() {
                "--topic" => {
                    options.topic = Some(
                        rest.next()
                            .ok_or_else(|| anyhow::anyhow!("--topic needs a name"))?
                            .clone(),
                    );
                }
                other => bail!("unknown produce option {other:?}"),
            }
        }
        Ok(options)
    }
}

/// Produce to a real cluster and read it back.
pub async fn produce(target: &Target, options: &Options) -> Result<Outcome> {
    target.require_writable("the produce suite")?;

    let cluster = Cluster::connect(target.bootstrap.clone(), target.cluster_config()).await?;
    let admin = Admin::new(cluster.clone());

    let mut report = Report::new();
    report.note(format!("target: {}", target.label));

    let (topic, ours) = match &options.topic {
        Some(existing) => {
            report.note(format!("using existing topic: {existing}"));
            (existing.clone(), false)
        }
        None => {
            let topic = target.scoped_name("produce", &run_token());
            report.note(format!("scratch topic: {topic}"));
            // Replication factor 3 deliberately: with acks=all the leader
            // waits on real followers on other machines, which is the half of
            // the write path a single-broker fixture cannot exercise at all.
            admin
                .create_topics([NewTopic::new(&topic, PARTITIONS, 3)])
                .await?;
            (topic, true)
        }
    };

    let outcome = run(&cluster, &admin, &topic, &mut report).await;

    if ours {
        match admin.delete_topics([topic.clone()]).await {
            Ok(results) => {
                let deleted = results.iter().all(|(_, result)| result.is_ok());
                report.set("cleanup.deleted", deleted);
                if !deleted {
                    for (name, error) in kafka_admin::errs(&results) {
                        report.note(format!("cleanup failed for {name}: {error}"));
                    }
                }
            }
            Err(error) => {
                report.set("cleanup.deleted", false);
                report.note(format!("cleanup failed: {error}"));
            }
        }
    }

    Ok(match outcome {
        Ok(()) => Outcome::ok(report),
        Err(error) => Outcome::failed(report, error),
    })
}

async fn run(cluster: &Cluster, admin: &Admin, topic: &str, report: &mut Report) -> Result<()> {
    // The topic has to be visible on whichever broker answers metadata before
    // the producer can resolve a leader for it.
    await_topic(admin, topic, report).await?;

    // The leader map as *we* see it, before producing. When a produce is
    // refused with NOT_LEADER_OR_FOLLOWER this is the first thing to diff
    // against another client's view of the same topic — it separates "our
    // metadata is wrong" from "we sent to the wrong broker anyway".
    let snapshot = cluster.refresh_topics(&[topic]).await?;
    if let Some(info) = snapshot.topic(topic) {
        report.set("produce.partitions", info.partitions.len());
        for partition in &info.partitions {
            report.set_opt(
                format!("produce.leader.p{}", partition.partition),
                partition.leader,
            );
            report.set(
                format!("produce.replicas.p{}", partition.partition),
                partition
                    .replicas
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join("/"),
            );
        }
    }

    let negotiated = cluster
        .negotiated_for::<kafka_conn::protocol::messages::ProduceRequest>()
        .await?;
    report.set("produce.version", negotiated);
    // Above v13 the request carries a topic uuid and no name at all. Recording
    // it makes a diff between two clusters show which branch each one took.
    report.set("produce.by_topic_id", negotiated >= 13);

    let producer = Producer::new(cluster.clone(), ProducerConfig::new());

    // 1. A record with everything on it, to an explicit partition.
    let stamped = 1_765_000_000_000;
    let started = Instant::now();
    let metadata = producer
        .send(
            ProducerRecord::new(topic)
                .partition(3)
                .key("customer-7")
                .value("{\"total\":42}")
                .header("content-type", "application/json")
                .null_header("tombstoned-header")
                .timestamp(stamped),
        )
        .await?;
    report.set("produce.one.partition", metadata.partition);
    report.set("produce.one.offset", metadata.offset);
    report.set("produce.one.ack_ms", started.elapsed().as_millis());
    report.set_opt("produce.one.broker_timestamp", metadata.timestamp);

    if metadata.partition != 3 {
        bail!(
            "explicit partition ignored: asked for 3, landed in {}",
            metadata.partition
        );
    }

    // 2. A tombstone and an empty value, which must not collapse into each
    //    other. Same partition so one read checks both.
    producer
        .send(ProducerRecord::new(topic).partition(3).key("gone"))
        .await?;
    producer
        .send(
            ProducerRecord::new(topic)
                .partition(3)
                .key("blank")
                .value(Bytes::new()),
        )
        .await?;

    // 3. Every codec, so a compression bug shows up against a real broker's
    //    validation rather than only against our own decoder.
    for compression in [
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        let codec_producer = Producer::new(
            cluster.clone(),
            ProducerConfig::new().compression(compression),
        );
        codec_producer
            .send(
                ProducerRecord::new(topic)
                    .partition(3)
                    .key(format!("codec-{compression:?}"))
                    .value(format!("payload-{compression:?}")),
            )
            .await
            .map_err(|error| anyhow::anyhow!("{compression:?} was rejected: {error}"))?;
    }

    let expected = 7;
    let records = await_records(cluster, topic, 3, expected, report).await?;
    report.set("produce.read_back", records.len());

    // Found by key, not by position: with `--topic` the partition already
    // holds records from earlier runs, and asserting on `records[0]` would be
    // checking somebody else's record and calling it a pass.
    let first = find(&records, b"customer-7")?;
    check("key", first.key.as_deref() == Some(&b"customer-7"[..]))?;
    check(
        "value",
        first.value.as_deref() == Some(&b"{\"total\":42}"[..]),
    )?;
    check("timestamp", first.timestamp == stamped)?;
    check(
        "headers",
        first.headers
            == vec![
                (
                    "content-type".to_owned(),
                    Some(Bytes::from_static(b"application/json")),
                ),
                ("tombstoned-header".to_owned(), None),
            ],
    )?;
    report.set("produce.fields_survived", true);

    let tombstone = find(&records, b"gone")?;
    let blank = find(&records, b"blank")?;
    check("tombstone stayed null", tombstone.value.is_none())?;
    check(
        "empty value stayed empty",
        blank.value == Some(Bytes::new()),
    )?;
    report.set("produce.tombstone_distinct", true);

    // Every codec decoded back to its payload.
    // Distinct keys rather than record count, for the same reason: a reused
    // topic holds one copy per previous run, and a count would grow forever
    // while proving nothing extra.
    let codecs_read: std::collections::BTreeSet<Vec<u8>> = records
        .iter()
        .filter_map(|record| record.key.as_deref())
        .filter(|key| key.starts_with(b"codec-"))
        .map(<[u8]>::to_vec)
        .collect();
    report.set("produce.codecs_read_back", codecs_read.len());
    if codecs_read.len() != 4 {
        bail!(
            "expected all 4 compressed codecs to read back, got {}",
            codecs_read.len()
        );
    }

    // 4. The partitioner against the real partition count: produce without
    //    naming a partition and assert the broker put each record where our
    //    murmur2 said it would.
    let mut routed = 0;
    for i in 0..48 {
        let key = format!("key-{i}");
        let placed = producer
            .send(ProducerRecord::new(topic).key(key.clone()).value("v"))
            .await?;
        let ours = partition_for_key(key.as_bytes(), PARTITIONS);
        if placed.partition != ours {
            bail!(
                "key {key}: partitioner chose {ours}, record landed in {}",
                placed.partition
            );
        }
        routed += 1;
    }
    report.set("produce.keys_routed", routed);

    Ok(())
}

fn check(what: &str, holds: bool) -> Result<()> {
    if !holds {
        bail!("{what} did not survive the round trip");
    }
    Ok(())
}

fn find<'a>(records: &'a [kafka_read::Record], key: &[u8]) -> Result<&'a kafka_read::Record> {
    records
        .iter()
        .find(|record| record.key.as_deref() == Some(key))
        .ok_or_else(|| anyhow::anyhow!("no record with key {}", String::from_utf8_lossy(key)))
}

/// Wait for a created topic to be describable on whichever broker answers.
async fn await_topic(admin: &Admin, topic: &str, report: &mut Report) -> Result<()> {
    let started = Instant::now();
    let deadline = started + SETTLE_TIMEOUT;
    loop {
        if let Ok(results) = admin.describe_topics([topic.to_owned()]).await
            && results.iter().any(|(_, result)| result.is_ok())
        {
            report.set("produce.topic_settle_ms", started.elapsed().as_millis());
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("{topic} never became describable");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Read a partition until it holds at least `expected` records.
///
/// Bounded polling rather than a sleep: a produce is acknowledged by the
/// leader, and the scan may reach a different broker's view of the log.
async fn await_records(
    cluster: &Cluster,
    topic: &str,
    partition: i32,
    expected: usize,
    report: &mut Report,
) -> Result<Vec<kafka_read::Record>> {
    let started = Instant::now();
    let deadline = started + SETTLE_TIMEOUT;
    loop {
        let records = read_partition(cluster, topic, partition).await?;
        if records.len() >= expected {
            report.set("produce.read_settle_ms", started.elapsed().as_millis());
            return Ok(records);
        }
        if Instant::now() >= deadline {
            bail!(
                "expected {expected} records in {topic}-{partition}, saw {}",
                records.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn read_partition(
    cluster: &Cluster,
    topic: &str,
    partition: i32,
) -> Result<Vec<kafka_read::Record>> {
    let spec = ScanSpec::new(topic)
        .with_partitions([partition])
        .from(StartPosition::Earliest);
    let mut stream = Box::pin(kafka_read::scan(cluster, spec).await?);

    let mut records = Vec::new();
    while let Some(event) = stream.next().await {
        match event? {
            ScanEvent::Record(record) => records.push(record),
            ScanEvent::Malformed { offset, reason, .. } => {
                bail!("we wrote offset {offset} and could not read it back: {reason}")
            }
            _ => {}
        }
    }
    Ok(records)
}
