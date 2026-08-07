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
use kafka_consume::{ClassicConsumer, Consumer, ConsumerConfig, GroupConsumer, Position};
use kafka_meta::Cluster;
use kafka_produce::{Compression, Producer, ProducerConfig, ProducerRecord, partition_for_key};
use kafka_read::{ScanEvent, ScanSpec, StartPosition, Visibility};

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
                .with_partition(3)
                .with_key("customer-7")
                .with_value("{\"total\":42}")
                .with_header("content-type", "application/json")
                .with_null_header("tombstoned-header")
                .with_timestamp(stamped),
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
        .send(
            ProducerRecord::new(topic)
                .with_partition(3)
                .with_key("gone"),
        )
        .await?;
    producer
        .send(
            ProducerRecord::new(topic)
                .with_partition(3)
                .with_key("blank")
                .with_value(Bytes::new()),
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
                    .with_partition(3)
                    .with_key(format!("codec-{compression:?}"))
                    .with_value(format!("payload-{compression:?}")),
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
            .send(
                ProducerRecord::new(topic)
                    .with_key(key.clone())
                    .with_value("v"),
            )
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

    // 5. The accumulator (M13). Everything above sends one record per request,
    //    which is the shape M12 shipped; this is the assertion that the
    //    batching added on top is real.
    batching(cluster, topic, report).await?;

    // 6. Transactions (M15), against a real transaction coordinator.
    transactions(cluster, topic, report).await?;

    // 7. The consumer (M16): read it all back through incremental fetch
    //    sessions rather than one-shot scans.
    consume(cluster, topic, report).await?;

    // 8. KIP-848 group membership (M17), against a real group coordinator.
    group(cluster, topic, report).await?;

    // 9. Exactly-once end to end (KIP-447), which is the only section that
    //    touches the transaction *and* group coordinators inside one
    //    transaction — usually two different machines here.
    exactly_once(cluster, topic, report).await?;

    // 10. The same, committed as a group member: the only path that puts a real
    //     member id and epoch on the wire, both gated behind TxnOffsetCommit v3.
    exactly_once_as_member(cluster, topic, report).await?;

    Ok(())
}

/// Poll one classic member from its own task until told to stop, publishing
/// its assignment as it changes, and hand the member back.
///
/// A task each rather than one loop over `tokio::join!(a.poll(), b.poll())`:
/// that loop cannot poll `a` again until `b`'s poll returns, and a classic
/// `JoinGroup` blocks on the coordinator until the whole group has re-joined.
/// Lose the race for the two joins to arrive in the same rebalance window and
/// `b`'s blocking join starves `a`'s heartbeat until the coordinator evicts
/// it, handing `b` every partition while `a` still reports the ones it has not
/// learned it lost. The acceptance suite failed exactly that way; see
/// `kafka_consume::classic`.
fn drive_classic(
    mut consumer: ClassicConsumer,
    assignment: tokio::sync::watch::Sender<Vec<(String, i32)>>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<(ClassicConsumer, Option<kafka_consume::Error>)> {
    tokio::spawn(async move {
        let mut failure = None;
        loop {
            tokio::select! {
                result = consumer.poll() => match result {
                    Ok(_) => {
                        let _ = assignment.send(consumer.assignment());
                    }
                    Err(error) => {
                        failure = Some(error);
                        break;
                    }
                },
                _ = stop.changed() => break,
            }
        }
        (consumer, failure)
    })
}

/// M17: two members join one group, and between them own every partition
/// exactly once.
///
/// Full coverage and empty intersection is the assertion that matters. A
/// reconciliation that acknowledges before revoking produces an *overlap*,
/// which delivers records twice and reports nothing at all — so counting
/// records would pass while the bug is live.
async fn group(cluster: &Cluster, topic: &str, report: &mut Report) -> Result<()> {
    let group_id = format!("kaaslib-live-group-{}", run_token());
    let config = || {
        ConsumerConfig::new()
            .visibility(Visibility::All)
            .max_wait_ms(200)
    };

    let mut first = GroupConsumer::subscribe(cluster.clone(), config(), &group_id, [topic]).await?;
    let mut second =
        GroupConsumer::subscribe(cluster.clone(), config(), &group_id, [topic]).await?;

    // Poll both until the coordinator has settled an assignment on each.
    let started = Instant::now();
    let deadline = started + Duration::from_secs(90);
    while Instant::now() < deadline {
        first.poll().await?;
        second.poll().await?;
        let a = first.assignment().len();
        let b = second.assignment().len();
        if a + b == usize::try_from(PARTITIONS).unwrap_or(0) && a > 0 && b > 0 {
            break;
        }
    }

    let a: std::collections::BTreeSet<(String, i32)> = first.assignment().into_iter().collect();
    let b: std::collections::BTreeSet<(String, i32)> = second.assignment().into_iter().collect();

    report.set("group.settle_ms", started.elapsed().as_millis());
    report.set("group.member_a", a.len());
    report.set("group.member_b", b.len());
    report.set("group.member_a_id_issued", !first.member_id().is_empty());
    report.set("group.member_b_id_issued", !second.member_id().is_empty());

    let overlap: Vec<_> = a.intersection(&b).collect();
    report.set("group.overlap", overlap.len());
    if !overlap.is_empty() {
        bail!(
            "two members own the same partitions, which delivers every record              twice and reports nothing: {overlap:?}"
        );
    }

    let union: std::collections::BTreeSet<_> = a.union(&b).collect();
    report.set("group.union", union.len());
    if union.len() != usize::try_from(PARTITIONS).unwrap_or(0) {
        bail!(
            "the group covers {} of {PARTITIONS} partitions; a gap means              records nobody is reading",
            union.len()
        );
    }
    report.set("group.covers_every_partition", true);

    // Leaving releases the assignment rather than stranding it.
    first.leave().await?;
    let rebalanced = Instant::now() + Duration::from_secs(90);
    while Instant::now() < rebalanced {
        second.poll().await?;
        if second.assignment().len() == usize::try_from(PARTITIONS).unwrap_or(0) {
            break;
        }
    }
    report.set("group.after_leave", second.assignment().len());
    if second.assignment().len() != usize::try_from(PARTITIONS).unwrap_or(0) {
        bail!(
            "after one member left, the survivor holds {} of {PARTITIONS}              partitions; the rest are assigned to nobody",
            second.assignment().len()
        );
    }
    report.set("group.reassigned_on_leave", true);
    second.leave().await?;

    // M18: the same coverage property under the classic protocol, which a 4.x
    // broker still speaks even though KIP-848 is its default.
    let classic_id = format!("kaaslib-live-classic-{}", run_token());
    // A `Cluster` each, deliberately. `JoinGroup` blocks on the coordinator and
    // the broker mutes a connection while a request is in flight, so two
    // members sharing one would deadlock: the second member's join is never
    // read, the group never forms, and the first waits out its rebalance
    // timeout. See `kafka_consume::classic`.
    let cluster_a = kafka_meta::Cluster::connect(
        cluster.pool().bootstrap().to_vec(),
        kafka_meta::ClusterConfig::default(),
    )
    .await?;
    let cluster_b = kafka_meta::Cluster::connect(
        cluster.pool().bootstrap().to_vec(),
        kafka_meta::ClusterConfig::default(),
    )
    .await?;
    let ca = ClassicConsumer::subscribe(cluster_a, config(), &classic_id, [topic]).await?;
    let cb = ClassicConsumer::subscribe(cluster_b, config(), &classic_id, [topic]).await?;

    // Concurrently, and each from its own task — see `drive_classic`. Polling
    // one member to completion before touching the other deadlocks the
    // rebalance outright; polling both from one loop is subtler and starves
    // whichever member is not mid-join.
    let started = Instant::now();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let (a_tx, a_rx) = tokio::sync::watch::channel(Vec::new());
    let (b_tx, b_rx) = tokio::sync::watch::channel(Vec::new());
    let a_task = drive_classic(ca, a_tx, stop_rx.clone());
    let b_task = drive_classic(cb, b_tx, stop_rx);

    let deadline = started + Duration::from_secs(120);
    while Instant::now() < deadline {
        let a = a_rx.borrow().len();
        let b = b_rx.borrow().len();
        if a + b == usize::try_from(PARTITIONS).unwrap_or(0) && a > 0 && b > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let _ = stop_tx.send(true);
    let (mut ca, ca_failure) = a_task.await?;
    let (mut cb, cb_failure) = b_task.await?;
    if let Some(error) = ca_failure.or(cb_failure) {
        return Err(error.into());
    }

    let a: std::collections::BTreeSet<(String, i32)> = ca.assignment().into_iter().collect();
    let b: std::collections::BTreeSet<(String, i32)> = cb.assignment().into_iter().collect();
    report.set("classic.settle_ms", started.elapsed().as_millis());
    report.set("classic.member_a", a.len());
    report.set("classic.member_b", b.len());
    report.set("classic.leader_elected", ca.is_leader() || cb.is_leader());
    report.set("classic.overlap", a.intersection(&b).count());
    report.set("classic.union", a.union(&b).count());

    if !a.is_disjoint(&b) {
        bail!("two classic members own the same partitions");
    }
    if a.union(&b).count() != usize::try_from(PARTITIONS).unwrap_or(0) {
        bail!(
            "the classic group covers {} of {PARTITIONS} partitions",
            a.union(&b).count()
        );
    }
    report.set("classic.covers_every_partition", true);
    ca.leave().await?;
    cb.leave().await?;

    Ok(())
}

/// M16: assign every partition, drain the topic, and prove the fetch session
/// is incremental rather than a full fetch wearing a session id.
///
/// Against a three-broker cluster this also exercises the part the container
/// fixture cannot: partitions spread over three leaders, which is what makes
/// "one request per broker" mean anything.
async fn consume(cluster: &Cluster, topic: &str, report: &mut Report) -> Result<()> {
    let assignment: Vec<(String, i32)> = (0..PARTITIONS).map(|p| (topic.to_owned(), p)).collect();
    let mut consumer = Consumer::new(
        cluster.clone(),
        ConsumerConfig::new()
            .visibility(Visibility::All)
            .max_wait_ms(200)
            .group_id(format!("kaaslib-live-consume-{}", run_token())),
    );
    consumer
        .assign(assignment.clone(), Position::Earliest)
        .await?;
    report.set("consume.assigned", consumer.assignment().len());

    // Drain to the end. Bounded by wall clock rather than by an expected
    // count, because with `--topic` the topic may already hold records from
    // an earlier run.
    let started = Instant::now();
    let deadline = started + Duration::from_secs(120);
    let mut total = 0usize;
    let mut idle = 0;
    while Instant::now() < deadline && idle < 5 {
        let batch = consumer.poll().await?;
        if batch.is_empty() {
            idle += 1;
        } else {
            idle = 0;
            total += batch.len();
        }
    }
    report.set("consume.records", total);
    report.set("consume.drain_ms", started.elapsed().as_millis());

    if total == 0 {
        bail!("the consumer read nothing from a topic we had just written to");
    }

    // Caught up and unchanged: this is where an incremental session shows.
    let brokers = broker_ids(cluster).await?;
    let mark = traffic(cluster, &brokers).await;
    for _ in 0..10 {
        consumer.poll().await?;
    }
    let used = traffic(cluster, &brokers)
        .await
        .snapshot
        .since(&mark.snapshot);

    report.set("consume.steady_requests", used.requests_sent);
    report.set("consume.steady_bytes", used.bytes_sent);
    if used.requests_sent == 0 {
        bail!("request counting is broken: ten polls cannot take zero requests");
    }
    let per_request = used.bytes_sent / used.requests_sent;
    report.set("consume.steady_bytes_per_request", per_request);
    if per_request >= 200 {
        bail!(
            "steady-state fetches average {per_request} bytes, which is a full              fetch wearing a session id rather than an incremental one"
        );
    }
    report.set("consume.session_incremental", true);

    // Offsets for a non-member.
    for (key, result) in consumer.commit().await? {
        result.map_err(|error| anyhow::anyhow!("{}-{} did not commit: {error}", key.0, key.1))?;
    }
    let committed = consumer.committed().await?;
    report.set("consume.committed_partitions", committed.len());
    if committed.is_empty() {
        bail!("a commit that reads back as nothing committed is not a commit");
    }
    report.set("consume.offsets_round_trip", true);

    Ok(())
}

/// How many records each transaction writes.
const TXN_RECORDS: usize = 50;

/// M15: a committed transaction and an aborted one on the same partition, and
/// two readers that must disagree about what is there.
///
/// The container test can assert this too, but only here does it run against a
/// real transaction coordinator on a different machine from the leader, with
/// `AddPartitionsToTxn` actually crossing a network. It is also the first time
/// `kafka-read`'s aborted-transaction filter sees data it has to filter.
async fn transactions(cluster: &Cluster, topic: &str, report: &mut Report) -> Result<()> {
    let token = run_token();
    let marker = format!("m15-{token}");
    let producer = Producer::new(
        cluster.clone(),
        ProducerConfig::new().transactional_id(format!("kaaslib-live-txn-{token}")),
    );

    producer.init_transactions().await?;
    report.set("txn.initialised", true);

    for (kind, commit) in [("committed", true), ("aborted", false)] {
        producer.begin_transaction()?;
        let mut pending = Vec::with_capacity(TXN_RECORDS);
        for i in 0..TXN_RECORDS {
            pending.push(
                producer
                    .enqueue(
                        ProducerRecord::new(topic)
                            .with_partition(1)
                            .with_value(format!("{marker}-{kind}-{i}")),
                    )
                    .await?,
            );
        }
        for delivery in pending {
            delivery.await?;
        }
        if commit {
            producer.commit_transaction().await?;
        } else {
            producer.abort_transaction().await?;
        }
    }

    // Counted by marker, not by total: partition 1 already holds records from
    // the keyed-spread section and, with `--topic`, from earlier runs.
    let ours = |records: &[kafka_read::Record], kind: &str| -> usize {
        let needle = format!("{marker}-{kind}-");
        records
            .iter()
            .filter_map(|record| record.value.as_deref())
            .filter(|value| String::from_utf8_lossy(value).starts_with(&needle))
            .count()
    };

    let everything = read_partition_with(cluster, topic, 1, Visibility::All).await?;

    // Poll rather than read once. `CommittedOnly` stops at the last stable
    // offset, which does not advance until the commit marker has been written
    // and replicated — so an immediate read can legitimately see none of a
    // transaction that has already been acknowledged. Racing it here would
    // report a filter bug that is really replication lag.
    let settle = Instant::now() + SETTLE_TIMEOUT;
    let committed = loop {
        let seen = read_partition_with(cluster, topic, 1, Visibility::CommittedOnly).await?;
        if ours(&seen, "committed") == TXN_RECORDS || Instant::now() >= settle {
            break seen;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    report.set("txn.all.committed_records", ours(&everything, "committed"));
    report.set("txn.all.aborted_records", ours(&everything, "aborted"));
    report.set(
        "txn.committed_only.committed_records",
        ours(&committed, "committed"),
    );
    report.set(
        "txn.committed_only.aborted_records",
        ours(&committed, "aborted"),
    );

    if ours(&everything, "committed") != TXN_RECORDS || ours(&everything, "aborted") != TXN_RECORDS
    {
        bail!(
            "Visibility::All must show both transactions: {} committed, {} aborted",
            ours(&everything, "committed"),
            ours(&everything, "aborted")
        );
    }
    if ours(&committed, "committed") != TXN_RECORDS {
        bail!(
            "Visibility::CommittedOnly lost committed records: {}",
            ours(&committed, "committed")
        );
    }
    if ours(&committed, "aborted") != 0 {
        bail!(
            "an aborted transaction leaked into the committed view: {} records",
            ours(&committed, "aborted")
        );
    }
    report.set("txn.filter_holds", true);

    Ok(())
}

/// How many records the exactly-once section moves through the cycle.
const EOS_RECORDS: usize = 25;

/// KIP-447: a consume-process-produce cycle whose offsets move only when the
/// transaction commits.
///
/// Worth running here rather than only in a container because the two hops go
/// to **different coordinators** — `AddOffsetsToTxn` to the transaction
/// coordinator, `TxnOffsetCommit` to the group's — and on a real cluster those
/// are usually different machines, neither of them the partition leader. A
/// single-broker fixture makes all three the same process and proves nothing
/// about the routing.
///
/// The aborted half is the assertion that matters. An offset commit that merely
/// runs *next to* a transaction passes a commit-only check and fails this one.
async fn exactly_once(cluster: &Cluster, topic: &str, report: &mut Report) -> Result<()> {
    let token = run_token();
    let group = format!("kaaslib-live-eos-{token}");
    let marker = format!("kip447-{token}");

    // Seed a partition of its own, so the cycle reads records this section
    // wrote rather than whatever the earlier sections left behind.
    let seeder = Producer::new(cluster.clone(), ProducerConfig::new());
    for i in 0..EOS_RECORDS {
        seeder
            .send(
                ProducerRecord::new(topic)
                    .with_partition(2)
                    .with_value(format!("{marker}-in-{i}")),
            )
            .await?;
    }

    let mut consumer = Consumer::new(
        cluster.clone(),
        ConsumerConfig::new()
            .group_id(&group)
            .visibility(Visibility::All)
            .max_wait_ms(200),
    );
    consumer
        .assign([(topic.to_owned(), 2)], Position::Latest)
        .await?;
    // Latest, then step back over exactly what we seeded: the partition is
    // shared with other sections and a full drain would read their records too.
    let start = consumer
        .position(topic, 2)
        .unwrap_or(0)
        .saturating_sub(i64::try_from(EOS_RECORDS).unwrap_or(0));
    consumer.seek(topic, 2, start)?;

    let deadline = Instant::now() + SETTLE_TIMEOUT;
    let mut consumed = 0usize;
    while consumed < EOS_RECORDS && Instant::now() < deadline {
        consumed += consumer.poll().await?.len();
    }
    if consumed < EOS_RECORDS {
        bail!("the cycle only read {consumed} of {EOS_RECORDS} seeded records");
    }
    let positions = consumer.positions();
    report.set("eos.consumed", consumed);

    let producer = Producer::new(
        cluster.clone(),
        ProducerConfig::new().transactional_id(format!("kaaslib-live-eos-{token}")),
    );
    producer.init_transactions().await?;

    let cycle = async |kind: &str, commit: bool| -> Result<()> {
        producer.begin_transaction()?;
        for i in 0..EOS_RECORDS {
            producer
                .send(
                    ProducerRecord::new(topic)
                        .with_partition(3)
                        .with_value(format!("{marker}-{kind}-{i}")),
                )
                .await?;
        }
        producer
            .send_offsets_to_transaction(positions.clone(), &consumer.group_metadata()?)
            .await?;
        if commit {
            producer.commit_transaction().await?;
        } else {
            producer.abort_transaction().await?;
        }
        Ok(())
    };

    // 1. Aborted: the offsets must go with it.
    cycle("aborted", false).await?;
    let after_abort = consumer.committed().await?;
    report.set("eos.committed_after_abort", after_abort.len());
    if !after_abort.is_empty() {
        bail!(
            "an aborted transaction moved the group's offset to {:?}, so the \
             offsets were never inside it",
            after_abort.get(&(topic.to_owned(), 2)).map(|e| e.offset)
        );
    }

    // 2. Committed: the offsets must arrive with the records.
    cycle("committed", true).await?;
    let settle = Instant::now() + SETTLE_TIMEOUT;
    let stored = loop {
        let stored = consumer.committed().await?;
        if !stored.is_empty() || Instant::now() >= settle {
            break stored;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    let expected = positions
        .iter()
        .find(|(key, _)| key == &(topic.to_owned(), 2))
        .map(|(_, offset)| *offset);
    let actual = stored.get(&(topic.to_owned(), 2)).map(|entry| entry.offset);
    report.set_opt("eos.committed_offset", actual);
    if actual.is_none() || actual != expected {
        bail!("a committed transaction stored {actual:?}, not the consumed position {expected:?}");
    }
    report.set("eos.offsets_move_with_the_transaction", true);

    Ok(())
}

/// The same cycle, but committed **as a group member**.
///
/// Not a duplicate of the section above, and the difference is the whole point:
/// a standalone consumer commits with `member_id = ""` and `generation = -1`,
/// which are `TxnOffsetCommit`'s defaults and encode at *any* version. A member
/// sends a real member id and epoch, which the schema gates behind v3+ and the
/// coordinator checks against the group's current generation. Only this half
/// exercises either.
async fn exactly_once_as_member(cluster: &Cluster, topic: &str, report: &mut Report) -> Result<()> {
    let token = run_token();
    let group = format!("kaaslib-live-eos-member-{token}");
    let marker = format!("kip447m-{token}");

    let mut consumer = GroupConsumer::subscribe(
        cluster.clone(),
        ConsumerConfig::new()
            .visibility(Visibility::All)
            .max_wait_ms(200),
        &group,
        [topic],
    )
    .await?
    // The transaction owns these offsets; an auto-commit would write them
    // outside it, which is the split KIP-447 exists to remove.
    .auto_commit(false);

    // A fresh group has nothing committed, so the member starts at the earliest
    // retained offset and reads what the earlier sections wrote.
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    let mut consumed = 0usize;
    while (consumer.assignment().is_empty() || consumed == 0) && Instant::now() < deadline {
        consumed += consumer.poll().await?.len();
    }
    if consumed == 0 {
        bail!("the member never read anything to commit");
    }

    let metadata = consumer.group_metadata()?;
    report.set("eos.member.consumed", consumed);
    report.set("eos.member.epoch", metadata.generation);
    report.set("eos.member.id_issued", !metadata.member_id.is_empty());
    report.set("eos.member.is_member", metadata.is_member());
    let positions = consumer.positions();

    let producer = Producer::new(
        cluster.clone(),
        ProducerConfig::new().transactional_id(format!("kaaslib-live-eos-member-{token}")),
    );
    producer.init_transactions().await?;

    // Each cycle is short on purpose: a classic-free KIP-848 member still only
    // heartbeats when polled, and the whole cycle has to fit inside the session
    // timeout.
    let cycle = async |kind: &str, commit: bool| -> Result<()> {
        producer.begin_transaction()?;
        producer
            .send(
                ProducerRecord::new(topic)
                    .with_partition(4)
                    .with_value(format!("{marker}-{kind}")),
            )
            .await?;
        producer
            .send_offsets_to_transaction(positions.clone(), &consumer.group_metadata()?)
            .await?;
        if commit {
            producer.commit_transaction().await?;
        } else {
            producer.abort_transaction().await?;
        }
        Ok(())
    };

    cycle("aborted", false).await?;
    let after_abort = consumer.committed().await?;
    report.set("eos.member.committed_after_abort", after_abort.len());
    if !after_abort.is_empty() {
        bail!("an aborted transaction moved a group member's offsets");
    }

    cycle("committed", true).await?;
    let settle = Instant::now() + SETTLE_TIMEOUT;
    let stored = loop {
        let stored = consumer.committed().await?;
        if stored.len() >= positions.len() || Instant::now() >= settle {
            break stored;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    report.set("eos.member.committed_partitions", stored.len());
    let mismatched: Vec<String> = positions
        .iter()
        .filter(|(key, offset)| stored.get(key).map(|entry| entry.offset) != Some(*offset))
        .map(|(key, offset)| {
            format!(
                "{}-{} wanted {offset}, stored {:?}",
                key.0,
                key.1,
                stored.get(key).map(|entry| entry.offset)
            )
        })
        .collect();
    if !mismatched.is_empty() {
        bail!("a member's transactional commit stored the wrong offsets: {mismatched:?}");
    }
    report.set("eos.member.offsets_move_with_the_transaction", true);

    consumer.leave().await?;
    Ok(())
}

/// How many records the batching section writes.
const BATCHED: usize = 20_000;

/// M13: many records, few requests, and the log in the order they were
/// enqueued.
///
/// The request count is the load-bearing number. A producer whose accumulator
/// does nothing still delivers every record in order and passes every
/// correctness check here — it just sends 20,000 requests to do it. Against a
/// three-broker cluster this also exercises the part a single-broker fixture
/// cannot: batches for partitions on different leaders are grouped into one
/// request *per broker*, so the count should scale with brokers, not with
/// partitions.
async fn batching(cluster: &Cluster, topic: &str, report: &mut Report) -> Result<()> {
    // A run-unique marker, because `--topic` may point at a topic that already
    // holds records from an earlier run. Counting everything in the partition
    // would then measure someone else's traffic and call it a pass.
    let marker = format!("m13-{}", run_token());
    let producer = Producer::new(cluster.clone(), ProducerConfig::new());

    // Captured once and reused for both ends of the measurement — see
    // `Traffic`. Taken before the warmup produce, because a produce can
    // invalidate the snapshot out from under the very refresh that reads it.
    let brokers = broker_ids(cluster).await?;
    report.set("batching.brokers", brokers.len());

    // Warm the pool before the mark so the delta is produce traffic and not
    // the first Metadata round trip to each broker.
    producer
        .send(
            ProducerRecord::new(topic)
                .with_partition(0)
                .with_value(format!("{marker}-warmup")),
        )
        .await?;

    let mark = traffic(cluster, &brokers).await;
    let started = Instant::now();

    // Enqueue everything before awaiting anything: awaiting each send in turn
    // keeps exactly one record in flight and batches nothing.
    let mut pending = Vec::with_capacity(BATCHED);
    for i in 0..BATCHED {
        pending.push(
            producer
                .enqueue(
                    ProducerRecord::new(topic)
                        .with_partition(0)
                        .with_value(format!("{marker}-{i}")),
                )
                .await?,
        );
    }
    let mut delivered = 0;
    for delivery in pending {
        delivery.await?;
        delivered += 1;
    }

    let after = traffic(cluster, &brokers).await;
    let used = after.snapshot.since(&mark.snapshot);
    report.set("batching.records", delivered);
    report.set("batching.requests", used.requests_sent);
    report.set("batching.bytes_sent", used.bytes_sent);
    report.set("batching.elapsed_ms", started.elapsed().as_millis());
    report.set(
        "batching.records_per_request",
        u64::try_from(delivered)
            .unwrap_or(u64::MAX)
            .checked_div(used.requests_sent)
            .unwrap_or(0),
    );

    if delivered != BATCHED {
        bail!("enqueued {BATCHED} records, {delivered} were delivered");
    }

    // Before believing the count, check that it was measured at all.
    //
    // The first live run of this section reported zero requests for 20,000
    // delivered records and *passed*, because a sum that silently skipped a
    // broker came back lower than the mark and saturating subtraction floored
    // the delta at zero. A vacuous pass here is worse than no assertion: the
    // whole point of counting requests is to catch a producer that batches
    // nothing, and a measurement that can read zero cannot distinguish perfect
    // batching from a broken counter.
    if mark.sampled != after.sampled {
        bail!(
            "request counting is unreliable: sampled {} brokers before and {} after",
            mark.sampled,
            after.sampled
        );
    }
    if used.requests_sent == 0 {
        bail!(
            "request counting is broken: {BATCHED} records were delivered and \
             acknowledged across {} broker connections, which cannot have taken \
             zero requests",
            after.sampled
        );
    }
    // Deliberately loose: the exact number depends on the broker's batch
    // validation and on how fast the round trips come back. What it rules out
    // is the failure this section exists for — no batching at all.
    if used.requests_sent >= 500 {
        bail!(
            "batching did not happen: {} requests for {BATCHED} records",
            used.requests_sent
        );
    }

    // Order is the property the one-batch-per-partition rule protects, and
    // partition 0 is where it can be checked.
    let records = read_partition(cluster, topic, 0).await?;
    let observed: Vec<usize> = records
        .iter()
        .filter_map(|record| record.value.as_deref())
        .filter_map(|value| {
            let text = String::from_utf8_lossy(value);
            text.strip_prefix(&format!("{marker}-"))
                .and_then(|suffix| suffix.parse().ok())
        })
        .collect();

    report.set("batching.read_back", observed.len());
    if observed.len() != BATCHED {
        bail!("wrote {BATCHED} records and read back {}", observed.len());
    }
    if observed.iter().copied().ne(0..BATCHED) {
        bail!("the log's order does not match the order records were enqueued in");
    }
    report.set("batching.in_order", true);

    Ok(())
}

/// Traffic summed across a fixed set of broker connections, and how many of
/// them the sum actually covers.
///
/// Summed rather than per-connection because the accumulator groups batches by
/// leader, so on a three-broker cluster the requests are spread over three
/// sockets by design.
///
/// # Why the broker ids are passed in rather than read from the snapshot
///
/// [`Cluster::invalidate`] installs an *empty* snapshot rather than marking the
/// existing one stale, and the produce dispatcher invalidates whenever a batch
/// comes back with an error a refresh would fix — an ordinary occurrence while
/// a freshly created topic settles its leaders. A helper that reads
/// `snapshot.brokers()` at both ends of a measurement therefore samples three
/// brokers before the run and *zero* after it, and `saturating_sub` turns that
/// into a delta of zero. That is exactly what happened on the first live run of
/// this section: it reported zero requests for 20,000 delivered records and
/// passed.
///
/// Capturing the ids once makes both ends cover the same set by construction,
/// and `sampled` catches the residual case where the pool declines to hand a
/// connection back.
#[derive(Debug, Clone, Copy)]
struct Traffic {
    snapshot: kafka_conn::StatsSnapshot,
    sampled: usize,
}

/// The cluster's broker ids, retried until the snapshot actually has some.
///
/// `Cluster::invalidate` installs an empty snapshot, and a produce running
/// concurrently invalidates on any error a refresh would fix. That race can
/// empty the snapshot *between* `refresh_topics` fetching and returning it, so
/// even an explicit refresh can hand back zero brokers — observed on one live
/// run in three. Retrying is the honest fix here; the alternative is a
/// measurement that silently covers nothing.
async fn broker_ids(cluster: &Cluster) -> Result<Vec<i32>> {
    for _ in 0..10 {
        let ids: Vec<i32> = cluster
            .refresh()
            .await?
            .brokers()
            .iter()
            .map(|broker| broker.node_id)
            .collect();
        if !ids.is_empty() {
            return Ok(ids);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    bail!("the cluster reported no brokers after ten refreshes")
}

async fn traffic(cluster: &Cluster, brokers: &[i32]) -> Traffic {
    let mut total = kafka_conn::StatsSnapshot::default();
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
    read_partition_with(cluster, topic, partition, Visibility::All).await
}

async fn read_partition_with(
    cluster: &Cluster,
    topic: &str,
    partition: i32,
    visibility: Visibility,
) -> Result<Vec<kafka_read::Record>> {
    let spec = ScanSpec::new(topic)
        .partitions([partition])
        .from(StartPosition::Earliest)
        .visibility(visibility);
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
