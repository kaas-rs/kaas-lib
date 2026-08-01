//! The admin round trip, against a cluster we do not own.
//!
//! Creates only prefixed resources and removes them again — including when an
//! assertion fails part way through, because a shared cluster that accumulates
//! one abandoned topic per failed run becomes unusable faster than the bug gets
//! fixed. Cleanup runs on the error path and its own failures are reported
//! rather than swallowed.

use std::future::Future;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use kafka_admin::{Admin, ConfigChange, ConfigResource, ConfigSource, NewTopic, OffsetSpec};
use kafka_meta::Cluster;

use crate::report::{Report, one_line};
use crate::target::{Target, run_token};

/// Run the admin round trip.
pub async fn smoke(target: &Target) -> Result<Report> {
    target.require_writable("the smoke suite")?;

    let cluster = Cluster::connect(target.bootstrap.clone(), target.cluster_config()).await?;
    let admin = Admin::new(cluster.clone());

    let token = run_token();
    let topic = target.scoped_name("smoke", &token);
    let mut report = Report::new();
    report.note(format!("target: {}", target.label));
    report.note(format!("scratch topic: {topic}"));

    // Everything after this point is fallible *and* has to be cleaned up, so
    // the result is captured rather than propagated.
    let outcome = run(&admin, &topic, &mut report).await;

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

    outcome?;
    Ok(report)
}

/// How long to wait for a change to become visible on an arbitrary broker.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(30);
/// How often to re-ask while waiting.
const SETTLE_INTERVAL: Duration = Duration::from_millis(250);

/// How long a change took to become visible, and how many reads it cost.
#[derive(Debug, Clone, Copy)]
struct Settled {
    reads: u32,
    elapsed_ms: u64,
}

/// Poll until `check` succeeds, bounded.
///
/// Read-after-write on a multi-broker cluster is not immediate. A topic is
/// created on the controller and a describe is answered by whichever broker
/// `send_any` picked, which may not have applied that metadata record yet. A
/// UI meets this constantly — create a topic, land on its page, get
/// `UNKNOWN_TOPIC_OR_PARTITION` — so the delay is *measured and reported*
/// rather than slept through with a fixed sleep or asserted away.
///
/// A container fixture cannot show this at all: one broker is its own
/// controller, so every write is visible to the next read by construction.
async fn settle<T, F, Fut>(what: &str, mut check: F) -> Result<(T, Settled)>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    let started = Instant::now();
    let deadline = started + SETTLE_TIMEOUT;
    let mut reads = 0;
    loop {
        reads += 1;
        if let Some(value) = check().await? {
            return Ok((
                value,
                Settled {
                    reads,
                    elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                },
            ));
        }
        if Instant::now() >= deadline {
            bail!(
                "{what} did not settle after {reads} reads over {:?}",
                started.elapsed()
            );
        }
        tokio::time::sleep(SETTLE_INTERVAL).await;
    }
}

fn record_settle(report: &mut Report, key: &str, settled: Settled) {
    report.set(format!("{key}.reads_to_settle"), settled.reads);
    report.set(format!("{key}.settle_ms"), settled.elapsed_ms);
}

async fn run(admin: &Admin, topic: &str, report: &mut Report) -> Result<()> {
    // The replication factor a shared cluster will actually accept is not ours
    // to assume: Strimzi's default here is 1, a production cluster's is 3.
    // Asking the cluster is the difference between a portable test and one
    // that only passes where it was written.
    let brokers = admin.cluster().snapshot().brokers().len();
    let replication = i16::try_from(brokers.min(3)).unwrap_or(1).max(1);
    report.set("smoke.replication_factor", replication);

    // Create.
    let created = admin
        .create_topics([NewTopic::new(topic, 3, replication).with_config("retention.ms", "600000")])
        .await?;
    let Some((_, result)) = created.first() else {
        bail!("create_topics returned no result for {topic}");
    };
    match result {
        Ok(created) => {
            report.set("smoke.created.partitions", created.partitions);
            report.set("smoke.created.replication", created.replication_factor);
        }
        Err(error) => bail!("creating {topic}: {error}"),
    }

    // Describe. Also the DescribeTopicPartitions-or-Metadata decision, against
    // a real broker.
    let (info, settled) = settle("the new topic becoming visible", || async {
        let described = admin.describe_topics([topic]).await?;
        Ok(described
            .into_iter()
            .next()
            .and_then(|(_, result)| result.ok()))
    })
    .await?;
    record_settle(report, "smoke.create", settled);
    report.set("smoke.described.partitions", info.partitions.len());
    report.set(
        "smoke.described.leaders_resolved",
        info.partitions.iter().all(|p| p.leader.is_some()),
    );
    report.set(
        "smoke.described.leader_in_replicas",
        info.partitions
            .iter()
            .all(|p| p.leader.is_some_and(|leader| p.replicas.contains(&leader))),
    );
    report.set("smoke.described.has_topic_id", !info.topic_id.is_zero());

    // Alter a config, then read it back. The read-back matters: a broker that
    // accepts an incremental alter and does not apply it is a real failure mode
    // and an easy one to miss.
    let altered = admin
        .alter_configs([(
            ConfigResource::topic(topic),
            vec![ConfigChange::set("retention.ms", "1200000")],
        )])
        .await?;
    match altered.first().map(|(_, result)| result) {
        Some(Ok(())) => report.set("smoke.altered", true),
        Some(Err(error)) => bail!("altering {topic}: {error}"),
        None => bail!("alter_configs returned no result"),
    }

    let (retention, settled) = settle("retention.ms reaching 1200000", || async {
        let configs = admin
            .describe_configs([ConfigResource::topic(topic)])
            .await?;
        let entry = configs
            .into_iter()
            .next()
            .and_then(|(_, result)| result.ok())
            .and_then(|entries| {
                entries
                    .into_iter()
                    .find(|entry| entry.name == "retention.ms")
            });
        Ok(entry.filter(|entry| entry.value.as_deref() == Some("1200000")))
    })
    .await?;
    record_settle(report, "smoke.alter", settled);
    report.set_opt("smoke.retention.value", retention.value.clone());
    report.set(
        "smoke.retention.source_is_explicit",
        retention.source.is_explicit(),
    );
    report.set(
        "smoke.retention.source_is_topic_config",
        retention.source == ConfigSource::TopicConfig,
    );

    // Grow.
    let grown = admin.create_partitions([(topic.to_owned(), 6)]).await?;
    match grown.first().map(|(_, result)| result) {
        Some(Ok(())) => report.set("smoke.grown", true),
        Some(Err(error)) => bail!("growing {topic}: {error}"),
        None => bail!("create_partitions returned no result"),
    }

    // Both halves matter: the new partitions have to exist *and* have leaders.
    // A partition that exists without one answers `LEADER_NOT_AVAILABLE` to
    // ListOffsets, which is a transient a UI must ride out and a test must not
    // race against.
    let (_, settled) = settle("the new partitions becoming led", || async {
        let described = admin.describe_topics([topic]).await?;
        Ok(described
            .into_iter()
            .next()
            .and_then(|(_, result)| result.ok())
            .filter(|info| {
                info.partitions.len() == 6 && info.partitions.iter().all(|p| p.leader.is_some())
            }))
    })
    .await?;
    record_settle(report, "smoke.grow", settled);

    // Offsets on an empty topic, across every reachable sentinel.
    let mut answered = 0;
    for spec in OffsetSpec::REACHABLE {
        let listed = admin
            .list_offsets((0..6).map(|p| (topic.to_owned(), p)), spec)
            .await?;
        let ok = kafka_admin::oks(&listed).count();
        report.set(format!("smoke.offsets.{spec:?}.ok"), ok);
        if ok == 6 {
            answered += 1;
        } else if let Some((_, error)) = kafka_admin::errs(&listed).next() {
            report.set(
                format!("smoke.offsets.{spec:?}.error"),
                one_line(&error.to_string()),
            );
        }
    }
    report.set("smoke.offsets.sentinels_answered", answered);
    if answered != OffsetSpec::REACHABLE.len() {
        bail!(
            "only {answered} of {} reachable ListOffsets sentinels answered for all partitions",
            OffsetSpec::REACHABLE.len()
        );
    }

    // Rule 4, against a real broker: a batch with one bad name must come back
    // as one error and the rest as answers.
    let mixed = admin
        .describe_topics([topic, "kaas-lib-smoke-no-such-topic"])
        .await?;
    report.set("smoke.per_item.ok", kafka_admin::oks(&mixed).count());
    report.set("smoke.per_item.err", kafka_admin::errs(&mixed).count());
    if kafka_admin::oks(&mixed).count() != 1 || kafka_admin::errs(&mixed).count() != 1 {
        bail!("expected exactly one ok and one err, got {mixed:?}");
    }

    Ok(())
}
