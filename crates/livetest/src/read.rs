//! The read path, against records we did not write.
//!
//! This is the part of a live run that unit tests and container fixtures cannot
//! reproduce. The topics on a shared cluster hold batches produced by the Java
//! client and by librdkafka, with whatever compression, batch sizes, headers,
//! tombstones and transaction markers those clients chose — which is precisely
//! the silent-wrongness surface M11's interop job exists for, available here
//! for free and at a scale no fixture will match.
//!
//! Read-only by construction: scanning does not mutate a cluster, so this is
//! safe with `KAAS_TEST_READ_ONLY=1` and against topics owned by other people.

use anyhow::{Result, bail};
use futures::StreamExt;
use kafka_admin::Admin;
use kafka_meta::Cluster;
use kafka_read::{ScanEvent, ScanSpec, TailSpec};

use crate::report::{Report, one_line};
use crate::target::Target;

/// How much to read.
#[derive(Debug, Clone)]
pub struct Options {
    /// Topics to read, or empty to pick from what the cluster has.
    pub topics: Vec<String>,
    /// Require exactly this many records from each named topic.
    pub expect: Option<usize>,
    /// Stop a scan after this many records.
    pub limit: usize,
    /// How many topics to pick when none were named.
    pub max_topics: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            topics: Vec::new(),
            expect: None,
            // A shared cluster's benchmark topics run to millions of records.
            // Reading all of them would take longer than anyone will wait, and
            // proves nothing the first twenty thousand did not.
            limit: 20_000,
            max_topics: 5,
        }
    }
}

impl Options {
    /// Parse the command line.
    pub fn parse(args: &[String]) -> Result<Self> {
        let mut options = Options::default();
        let mut iter = args.iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--topic" => match iter.next() {
                    Some(topic) => options.topics.push(topic.clone()),
                    None => bail!("--topic needs a value"),
                },
                "--expect" => match iter.next().map(|v| v.parse()) {
                    Some(Ok(count)) => options.expect = Some(count),
                    _ => bail!("--expect needs a number"),
                },
                "--limit" => match iter.next().map(|v| v.parse()) {
                    Some(Ok(limit)) => options.limit = limit,
                    _ => bail!("--limit needs a number"),
                },
                "--max-topics" => match iter.next().map(|v| v.parse()) {
                    Some(Ok(max)) => options.max_topics = max,
                    _ => bail!("--max-topics needs a number"),
                },
                other => bail!("unknown option {other:?}"),
            }
        }
        if options.expect.is_some() && options.topics.is_empty() {
            bail!("--expect only means something with an explicit --topic");
        }
        Ok(options)
    }
}

/// Read topics and report what the decoder made of them.
pub async fn read(target: &Target, options: &Options) -> Result<Report> {
    let cluster = Cluster::connect(target.bootstrap.clone(), target.cluster_config()).await?;
    let admin = Admin::new(cluster.clone());

    let mut report = Report::new();
    report.note(format!("target: {}", target.label));

    let topics = if options.topics.is_empty() {
        pick_topics(&cluster, options.max_topics)
    } else {
        options.topics.clone()
    };
    if topics.is_empty() {
        report.set("read.topics", 0);
        report.note("no readable topics on this cluster");
        return Ok(report);
    }
    report.set("read.topics", topics.len());
    report.note(format!("reading: {}", topics.join(", ")));

    let mut total_records = 0u64;
    let mut total_malformed = 0u64;

    for topic in &topics {
        let stats = scan_one(&cluster, topic, options).await?;
        let key = format!("read.topic.{topic}");
        report.set(format!("{key}.records"), stats.records);
        report.set(format!("{key}.malformed"), stats.malformed);
        report.set(format!("{key}.tombstones"), stats.tombstones);
        report.set(format!("{key}.with_headers"), stats.with_headers);
        report.set(format!("{key}.transactional"), stats.transactional);
        report.set(format!("{key}.ordered"), stats.ordered);
        report.set(format!("{key}.partitions_seen"), stats.partitions_seen);
        report.set(format!("{key}.hit_limit"), stats.hit_limit);
        report.set(format!("{key}.saw_progress"), stats.saw_progress);
        total_records += stats.records;
        total_malformed += stats.malformed;

        if !stats.ordered {
            bail!("{topic}: records came out of log order within a partition");
        }
        if let Some(expected) = options.expect
            && u64::try_from(expected).unwrap_or(u64::MAX) != stats.records
        {
            bail!(
                "{topic}: expected {expected} records, scanned {}",
                stats.records
            );
        }

        // The tail is the most-used view in any UI, and the one whose
        // arithmetic breaks on exactly the topics a shared cluster has:
        // compacted, truncated, with offset gaps.
        match tail_one(&cluster, &admin, topic).await {
            Ok(tail) => {
                report.set(format!("{key}.tail.records"), tail.records);
                report.set(format!("{key}.tail.ordered"), tail.ordered);
                report.set(format!("{key}.tail.ends_at_high_watermark"), tail.at_end);
                report.set(format!("{key}.tail.fetches"), tail.fetches);
                if !tail.ordered {
                    bail!("{topic}: the tail came back out of order");
                }
            }
            Err(error) => {
                report.set(format!("{key}.tail.error"), one_line(&error.to_string()));
                bail!("{topic}: tail failed: {error}");
            }
        }
    }

    report.set("read.total_records", total_records);
    report.set("read.total_malformed", total_malformed);
    Ok(report)
}

/// Topics worth reading: real ones, with data, that are not internal.
fn pick_topics(cluster: &Cluster, max: usize) -> Vec<String> {
    let snapshot = cluster.snapshot();
    let mut names: Vec<String> = snapshot
        .topics()
        .iter()
        .filter(|topic| !topic.internal)
        // `__` prefixed topics are internal even when the broker does not say
        // so, and parsing `__consumer_offsets` is forbidden outright.
        .filter(|topic| !topic.name.starts_with("__"))
        .filter(|topic| topic.error.is_none())
        .filter(|topic| !topic.partitions.is_empty())
        .map(|topic| topic.name.clone())
        .collect();
    names.sort();
    names.truncate(max);
    names
}

#[derive(Debug, Default)]
struct ScanStats {
    records: u64,
    malformed: u64,
    tombstones: u64,
    with_headers: u64,
    transactional: u64,
    partitions_seen: usize,
    ordered: bool,
    hit_limit: bool,
    saw_progress: bool,
}

async fn scan_one(cluster: &Cluster, topic: &str, options: &Options) -> Result<ScanStats> {
    let mut spec = ScanSpec::new(topic);
    spec.limit = Some(options.limit);

    let mut stream = Box::pin(kafka_read::scan(cluster, spec).await?);
    let mut stats = ScanStats {
        ordered: true,
        ..ScanStats::default()
    };
    let mut last_offset: std::collections::HashMap<i32, i64> = std::collections::HashMap::new();

    while let Some(event) = stream.next().await {
        match event? {
            ScanEvent::Record(record) => {
                stats.records += 1;
                if record.is_tombstone() {
                    stats.tombstones += 1;
                }
                if !record.headers.is_empty() {
                    stats.with_headers += 1;
                }
                if record.transactional {
                    stats.transactional += 1;
                }
                // Per-partition log order is exact, always — the one ordering
                // guarantee the scan makes unconditionally.
                match last_offset.insert(record.partition, record.offset) {
                    Some(previous) if previous >= record.offset => stats.ordered = false,
                    _ => {}
                }
            }
            ScanEvent::Malformed { .. } => stats.malformed += 1,
            ScanEvent::Progress(_) => stats.saw_progress = true,
            ScanEvent::PartitionComplete { .. } => {}
            ScanEvent::Done(progress) => {
                stats.hit_limit =
                    progress.records_emitted >= u64::try_from(options.limit).unwrap_or(u64::MAX);
            }
        }
    }

    stats.partitions_seen = last_offset.len();
    Ok(stats)
}

#[derive(Debug, Default)]
struct TailStats {
    records: usize,
    fetches: usize,
    ordered: bool,
    at_end: bool,
}

async fn tail_one(cluster: &Cluster, admin: &Admin, topic: &str) -> Result<TailStats> {
    const WANT: usize = 200;

    let tails = kafka_read::tail(cluster, &TailSpec::new(topic, WANT)).await?;
    let latest = admin
        .topic_offset_range(topic)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(partition, range)| range.ok().map(|(_, high)| (partition, high)))
        .collect::<std::collections::HashMap<_, _>>();

    let mut stats = TailStats {
        ordered: true,
        at_end: true,
        ..TailStats::default()
    };
    for tail in &tails {
        stats.records += tail.records.len();
        stats.fetches += tail.fetches;
        let strictly_increasing = tail
            .records
            .windows(2)
            .all(|w| matches!((w.first(), w.get(1)), (Some(a), Some(b)) if a.offset < b.offset));
        if !strictly_increasing {
            stats.ordered = false;
        }
        // A tail that does not end at the high watermark has read the wrong
        // window — the failure mode that looks like success until someone
        // notices the newest message is missing.
        if let (Some(last), Some(Some(high))) = (tail.records.last(), latest.get(&tail.partition))
            && last.offset != high.saturating_sub(1)
        {
            stats.at_end = false;
        }
    }
    Ok(stats)
}
