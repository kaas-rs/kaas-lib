//! Live validation for issues #17, #18, #19 and #20 against a real cluster.
//!
//! Not a regression net — the unit and acceptance suites are that — but the
//! proof that each fix holds on the shared clusters where the issues were
//! filed, per the repo's workflow of validating on a live broker before an
//! issue is closed.
//!
//! ```sh
//! eval "$(.claude/skills/live-cluster/resolve-target.sh strimzi)"
//! cargo run -q -p livetest --example issue_checks -- read
//! cargo run -q -p livetest --example issue_checks -- negotiate
//! eval "$(.claude/skills/live-cluster/resolve-target.sh kaas)"
//! cargo run -q -p livetest --example issue_checks -- negotiate
//! ```
//!
//! `read` wants a topic with idle partitions beside a full one —
//! `kaas-canary-v1` is the measured case from #17 — and a wide one,
//! `kperf-bench`. Override with `--lopsided <t>` / `--wide <t>`.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use kafka_consume::{ConsumerConfig, GroupProtocol, NegotiatedConsumer};
use kafka_meta::{Cluster, ClusterConfig};
use kafka_read::{ScanEvent, ScanSpec, StartPosition, StartSubstitution, TailSpec};

fn bootstrap() -> Result<Vec<String>> {
    let raw = std::env::var("KAAS_TEST_BOOTSTRAP")
        .context("KAAS_TEST_BOOTSTRAP is not set; eval resolve-target.sh first")?;
    Ok(raw.split(',').map(str::trim).map(str::to_owned).collect())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = args.first().map(String::as_str).unwrap_or("read");
    let arg = |name: &str, default: &str| -> String {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
            .unwrap_or_else(|| default.to_owned())
    };

    let cluster = Cluster::connect(bootstrap()?, ClusterConfig::default()).await?;
    match mode {
        "read" => {
            check_17_tail_limit(&cluster, &arg("--lopsided", "kaas-canary-v1")).await?;
            check_17_even_topic(&cluster, &arg("--wide", "kperf-bench")).await?;
            check_18_substitutions(&cluster, &arg("--lopsided", "kaas-canary-v1")).await?;
            check_19_reorder_window(&cluster, &arg("--wide", "kperf-bench")).await?;
        }
        "negotiate" => check_20_negotiation(&cluster, &arg("--topic", "kaas-canary-v1")).await?,
        other => bail!("unknown mode {other}; use read or negotiate"),
    }
    println!("issue_checks: all checks passed");
    Ok(())
}

/// #17 — the measured failure: limit 500 against a topic whose records sit in
/// a fraction of its partitions returned ⌈500/partitions⌉ from the full ones.
async fn check_17_tail_limit(cluster: &Cluster, topic: &str) -> Result<()> {
    let tails = kafka_read::tail(cluster, &TailSpec::new(topic, 500)).await?;
    let held: i64 = tails.iter().map(|t| t.log_end - t.log_start).sum();
    let total: usize = tails.iter().map(|t| t.records.len()).sum();
    for tail in &tails {
        println!(
            "17.{topic}.p{}: records={} bounds={}..{} reached_log_start={} fetches={}",
            tail.partition,
            tail.records.len(),
            tail.log_start,
            tail.log_end,
            tail.reached_log_start,
            tail.fetches
        );
    }
    let expected = usize::try_from(held.clamp(0, 500)).unwrap_or(500);
    if total != expected {
        bail!("#17: asked 500 of {topic} (holds {held} by offsets), got {total}");
    }
    // The paging fact: a partition with records below its window must say so.
    for tail in &tails {
        let holds = tail.log_end - tail.log_start;
        let more = holds > i64::try_from(tail.records.len()).unwrap_or(i64::MAX);
        if more && tail.reached_log_start {
            bail!(
                "#17: partition {} claims the bottom with {} of {holds} offsets read",
                tail.partition,
                tail.records.len()
            );
        }
    }
    println!("17.{topic}: PASS total={total}");
    Ok(())
}

/// #17 must not regress the even case: a wide topic where every partition
/// holds plenty still returns exactly the limit.
async fn check_17_even_topic(cluster: &Cluster, topic: &str) -> Result<()> {
    let tails = kafka_read::tail(cluster, &TailSpec::new(topic, 500)).await?;
    let total: usize = tails.iter().map(|t| t.records.len()).sum();
    if total != 500 {
        bail!("#17: asked 500 of {topic}, got {total}");
    }
    println!("17.{topic}: PASS total={total} partitions={}", tails.len());
    Ok(())
}

/// #18 — a start the log cannot honour is reported per partition, before any
/// record, rather than silently substituted.
async fn check_18_substitutions(cluster: &Cluster, topic: &str) -> Result<()> {
    // Beyond every log end: each partition must announce OffsetBeyondLogEnd.
    let starts = collect_starts(
        cluster,
        ScanSpec::new(topic).from(StartPosition::Offset(i64::MAX)),
    )
    .await?;
    for (partition, substituted) in &starts {
        match substituted {
            Some(StartSubstitution::OffsetBeyondLogEnd { .. }) => {}
            other => bail!("#18: p{partition} start Offset(i64::MAX) reported {other:?}"),
        }
    }
    println!(
        "18.{topic}.offset_beyond: PASS ({} partitions)",
        starts.len()
    );

    // A timestamp after every record: TimestampUnresolved, with the log end
    // as the substituted start.
    let future = 4_102_444_800_000i64; // 2100-01-01, safely after any record
    let starts = collect_starts(
        cluster,
        ScanSpec::new(topic).from(StartPosition::Timestamp(future)),
    )
    .await?;
    for (partition, substituted) in &starts {
        match substituted {
            Some(StartSubstitution::TimestampUnresolved { .. }) => {}
            other => bail!("#18: p{partition} future timestamp reported {other:?}"),
        }
    }
    println!(
        "18.{topic}.timestamp_unresolved: PASS ({} partitions)",
        starts.len()
    );

    // Below the log start, where the topic has one: OffsetBelowLogStart on
    // exactly the partitions whose earliest is above zero, honoured elsewhere.
    let starts =
        collect_starts(cluster, ScanSpec::new(topic).from(StartPosition::Offset(0))).await?;
    let mut below = 0usize;
    for (partition, substituted) in &starts {
        let (earliest, _) = kafka_read::partition_bounds(cluster, topic, *partition).await?;
        match (earliest > 0, substituted) {
            (true, Some(StartSubstitution::OffsetBelowLogStart { log_start, .. }))
                if *log_start == earliest =>
            {
                below += 1;
            }
            (false, None) => {}
            (expired, other) => {
                bail!("#18: p{partition} earliest={earliest} expired={expired} reported {other:?}")
            }
        }
    }
    println!("18.{topic}.offset_below: PASS (substituted on {below} partitions)");
    Ok(())
}

async fn collect_starts(
    cluster: &Cluster,
    spec: ScanSpec,
) -> Result<BTreeMap<i32, Option<StartSubstitution>>> {
    let mut stream = Box::pin(kafka_read::scan(cluster, spec.limit(1)).await?);
    let mut starts = BTreeMap::new();
    while let Some(event) = stream.next().await {
        match event? {
            ScanEvent::PartitionStarted {
                partition,
                substituted,
                ..
            } => {
                starts.insert(partition, substituted);
            }
            ScanEvent::Record(_) | ScanEvent::Done(_) => break,
            _ => {}
        }
    }
    Ok(starts)
}

/// #19 — a wide scan squeezed under a tiny buffer reports a non-zero reorder
/// window sized from its own numbers; a single-partition scan under the same
/// pressure reports zero, because within a partition order is exact.
async fn check_19_reorder_window(cluster: &Cluster, topic: &str) -> Result<()> {
    let mut wide = ScanSpec::new(topic).limit(2_000);
    wide.max_buffered_records = 64;
    let done = scan_to_done(cluster, wide).await?;
    println!(
        "19.{topic}.wide: reorder_window={} partitions_planned={} partitions_active={}",
        done.reorder_window, done.partitions_planned, done.partitions_active
    );
    if done.partitions_planned < 2 {
        bail!("#19: {topic} is not wide enough to exercise the merge");
    }
    if done.reorder_window == 0 {
        bail!(
            "#19: a 64-record buffer over {} partitions did not degrade",
            done.partitions_planned
        );
    }
    if done.partitions_active != 0 {
        bail!("#19: Done must still zero the active count");
    }

    let mut narrow = ScanSpec::new(topic).partitions([0]).limit(2_000);
    narrow.max_buffered_records = 64;
    let done = scan_to_done(cluster, narrow).await?;
    if done.reorder_window != 0 {
        bail!(
            "#19: single-partition scan reported reorder_window={}",
            done.reorder_window
        );
    }
    println!("19.{topic}.single: PASS reorder_window=0");
    Ok(())
}

async fn scan_to_done(cluster: &Cluster, spec: ScanSpec) -> Result<kafka_read::ScanProgress> {
    let mut stream = Box::pin(kafka_read::scan(cluster, spec).await?);
    while let Some(event) = stream.next().await {
        if let ScanEvent::Done(progress) = event? {
            return Ok(progress);
        }
    }
    bail!("scan ended without Done")
}

/// #20 — Auto lands on whichever protocol this broker serves, and the result
/// consumes. Against kaas (no key 68) that must be Classic; against Strimzi,
/// Consumer (KIP-848).
///
/// Self-contained: fresh groups start at the earliest committed-or-earliest
/// position, so the records to read are produced here, into a prefixed topic
/// that is deleted on the way out (and matches `sweep`'s filter if we crash).
async fn check_20_negotiation(cluster: &Cluster, _topic: &str) -> Result<()> {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default();
    let topic = format!("kaaslib-live-negotiate-{unique}");
    let group = format!("kaaslib-live-negotiate-{unique}");

    let admin = kafka_admin::Admin::new(cluster.clone());
    admin
        .create_topics([kafka_admin::NewTopic::new(&topic, 1, 1)])
        .await?
        .into_iter()
        .map(|(_, result)| result)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("#20: creating the scratch topic")?;

    // Read-after-write is not immediate on a real multi-broker cluster: the
    // topic exists on the controller before the broker answering the next
    // metadata request has heard. Bounded settle, as smoke does.
    let mut settled = false;
    for _ in 0..50 {
        if let Ok(results) = admin.describe_topics([topic.clone()]).await
            && results
                .iter()
                .any(|(_, result)| result.as_ref().is_ok_and(|d| !d.partitions.is_empty()))
        {
            settled = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    if !settled {
        let _ = admin.delete_topics([topic.clone()]).await;
        bail!("#20: {topic} did not become describable within 10s");
    }

    let outcome = negotiate_and_consume(cluster, &topic, &group).await;
    // Clean up before judging the outcome, error path included.
    let _ = admin.delete_topics([topic.clone()]).await;
    outcome
}

async fn negotiate_and_consume(cluster: &Cluster, topic: &str, group: &str) -> Result<()> {
    const WANT: usize = 20;
    let producer =
        kafka_produce::Producer::new(cluster.clone(), kafka_produce::ProducerConfig::new());
    for i in 0..WANT {
        producer
            .send(kafka_produce::ProducerRecord::new(topic).with_value(format!("negotiate-{i}")))
            .await
            .context("#20: seeding the scratch topic")?;
    }
    producer.flush().await?;

    let mut consumer = NegotiatedConsumer::subscribe(
        cluster.clone(),
        ConsumerConfig::new()
            .max_wait_ms(300)
            .with_group_protocol(GroupProtocol::Auto),
        group,
        [topic],
    )
    .await
    .context("#20: Auto subscribe must not error on either protocol")?;
    println!("20.protocol: {:?}", consumer.protocol());

    let mut records = 0usize;
    for _ in 0..120 {
        records += consumer.poll().await?.len();
        if records >= WANT {
            break;
        }
    }
    consumer.leave().await?;
    if records < WANT {
        bail!(
            "#20: negotiated {:?} consumer read {records} of {WANT} from {topic}",
            consumer.protocol()
        );
    }
    println!(
        "20: PASS protocol={:?} records={records} group={group}",
        consumer.protocol()
    );
    Ok(())
}
