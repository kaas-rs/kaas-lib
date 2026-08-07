//! M15 acceptance: transactions.
//!
//! `cargo test -p kafka-produce --test transactions -- --ignored`
//!
//! This is also the first time `kafka-read`'s `Visibility::CommittedOnly` is
//! exercised against data that actually needs it. M9 built the aborted-
//! transaction filter and the `AbortedTransactions` handling, but nothing in
//! the workspace could *produce* an aborted transaction, so until now the
//! filter was asserted only against data it never had to filter.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::time::{Duration, Instant};

use futures::StreamExt;
use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_consume::{ConsumerConfig, GroupConsumer};
use kafka_produce::{Producer, ProducerConfig, ProducerRecord};
use kafka_read::{Cluster, ScanEvent, ScanSpec, StartPosition, Visibility};
use testkit::{Cluster as _, KafkaCluster};

async fn setup(topic: &str) -> (KafkaCluster, Cluster, Admin) {
    let fixture = testkit::cluster(3).await.expect("cluster");
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .expect("admin");
    admin
        .create_topics([NewTopic::new(topic, 1, 3)])
        .await
        .expect("topic");
    await_topic(&admin, topic).await;
    let cluster = admin.cluster().clone();
    (fixture, cluster, admin)
}

async fn await_topic(admin: &Admin, topic: &str) {
    for _ in 0..50 {
        if let Ok(results) = admin.describe_topics([topic.to_owned()]).await
            && results.iter().any(|(_, result)| result.is_ok())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("{topic} never became describable");
}

async fn read(cluster: &Cluster, topic: &str, visibility: Visibility) -> Vec<kafka_read::Record> {
    let spec = ScanSpec::new(topic)
        .partitions([0])
        .from(StartPosition::Earliest)
        .visibility(visibility);
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

async fn write_transaction(producer: &Producer, topic: &str, prefix: &str, count: usize) {
    producer.begin_transaction().expect("begin");
    let mut pending = Vec::with_capacity(count);
    for i in 0..count {
        pending.push(
            producer
                .enqueue(
                    ProducerRecord::new(topic)
                        .with_partition(0)
                        .with_value(format!("{prefix}-{i}")),
                )
                .await
                .expect("enqueued"),
        );
    }
    for delivery in pending {
        delivery.await.expect("delivered");
    }
}

/// The acceptance case: one committed transaction, one aborted, same
/// partition, and two readers that must disagree about what is there.
#[testkit::integration_test]
async fn a_committed_transaction_is_visible_and_an_aborted_one_is_not() {
    const TOPIC: &str = "txn-visibility";
    const RECORDS: usize = 100;

    let (_fixture, cluster, _admin) = setup(TOPIC).await;
    let producer = Producer::new(
        cluster.clone(),
        ProducerConfig::new().transactional_id("txn-visibility-1"),
    );
    producer.init_transactions().await.expect("init");

    write_transaction(&producer, TOPIC, "committed", RECORDS).await;
    producer.commit_transaction().await.expect("commit");

    write_transaction(&producer, TOPIC, "aborted", RECORDS).await;
    producer.abort_transaction().await.expect("abort");

    // `All` is the UI's question — what is in this partition — and a rolled
    // back record *is* in the partition.
    let everything = read(&cluster, TOPIC, Visibility::All).await;
    assert_eq!(
        everything.len(),
        RECORDS * 2,
        "an aborted transaction's records are still in the log; \
         Visibility::All must show them"
    );

    // `CommittedOnly` is the consumer's question. It is answered up to the
    // last stable offset, and the transaction markers that advance it are
    // written by the coordinator *after* the EndTxn responses awaited above —
    // so a single immediate read races the markers and can see nothing at
    // all. Poll until the committed half is visible; the deadline turns a
    // marker that never lands into a failure that says so.
    let deadline = Instant::now() + Duration::from_secs(30);
    let committed = loop {
        let committed = read(&cluster, TOPIC, Visibility::CommittedOnly).await;
        if committed.len() >= RECORDS || Instant::now() >= deadline {
            break committed;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    assert_eq!(
        committed.len(),
        RECORDS,
        "Visibility::CommittedOnly must hide exactly the aborted transaction"
    );
    assert!(
        committed
            .iter()
            .filter_map(|record| record.value.as_ref())
            .all(|value| value.starts_with(b"committed-")),
        "an aborted record leaked into the committed view"
    );
}

/// **KIP-447**: a consume-process-produce cycle where the offsets move only if
/// the transaction commits.
///
/// The aborted half is the one that proves anything. A commit-only test passes
/// even when the offset commit is an ordinary non-transactional write happening
/// to run next to a transaction — it is the abort that separates "inside the
/// transaction" from "alongside it", because only an offset genuinely enrolled
/// in the transaction is discarded with it.
#[testkit::integration_test]
async fn consumed_offsets_move_with_the_transaction_and_only_with_it() {
    const INPUT: &str = "txn-eos-input";
    const OUTPUT: &str = "txn-eos-output";
    const GROUP: &str = "txn-eos-group";
    const RECORDS: usize = 20;

    let (_fixture, cluster, admin) = setup(INPUT).await;
    admin
        .create_topics([NewTopic::new(OUTPUT, 1, 3)])
        .await
        .expect("output topic");
    await_topic(&admin, OUTPUT).await;

    // Seed the input topic.
    let seeder = Producer::new(cluster.clone(), ProducerConfig::new());
    for i in 0..RECORDS {
        seeder
            .send(
                ProducerRecord::new(INPUT)
                    .with_partition(0)
                    .with_value(format!("in-{i}")),
            )
            .await
            .expect("seed");
    }

    // A real group member, so the commit carries a member id and an epoch —
    // the path a non-member commit would not exercise at all.
    let mut consumer = GroupConsumer::subscribe(
        cluster.clone(),
        ConsumerConfig::new()
            .visibility(Visibility::All)
            .max_wait_ms(200),
        GROUP,
        [INPUT],
    )
    .await
    .expect("subscribe")
    // The transaction owns these offsets now. Auto-commit would write them
    // outside it, which is the bug this test exists to catch.
    .auto_commit(false);

    let deadline = Instant::now() + Duration::from_secs(45);
    let mut consumed = Vec::new();
    while consumed.len() < RECORDS && Instant::now() < deadline {
        consumed.extend(consumer.poll().await.expect("poll"));
    }
    assert_eq!(
        consumed.len(),
        RECORDS,
        "the group never delivered the input"
    );

    let producer = Producer::new(
        cluster.clone(),
        ProducerConfig::new().transactional_id("txn-eos-writer"),
    );
    producer.init_transactions().await.expect("init");

    // The processed positions, which is what both halves below send.
    let positions = consumer.positions();
    assert_eq!(
        positions.iter().map(|(_, offset)| *offset).sum::<i64>(),
        i64::try_from(RECORDS).unwrap(),
        "a committed offset is the next record to read, not the last one handled"
    );

    // 1. The aborted cycle.
    producer.begin_transaction().expect("begin");
    for record in &consumed {
        producer
            .send(
                ProducerRecord::new(OUTPUT)
                    .with_partition(0)
                    .with_value(format!("aborted-{}", record.offset)),
            )
            .await
            .expect("produce");
    }
    producer
        .send_offsets_to_transaction(
            positions.clone(),
            &consumer.group_metadata().expect("group"),
        )
        .await
        .expect("send offsets");
    producer.abort_transaction().await.expect("abort");

    assert!(
        consumer.committed().await.expect("committed").is_empty(),
        "an aborted transaction moved the group's offset, so the offsets were \
         never really inside it"
    );

    // 2. The committed cycle.
    producer.begin_transaction().expect("begin");
    for record in &consumed {
        producer
            .send(
                ProducerRecord::new(OUTPUT)
                    .with_partition(0)
                    .with_value(format!("committed-{}", record.offset)),
            )
            .await
            .expect("produce");
    }
    producer
        .send_offsets_to_transaction(
            positions.clone(),
            &consumer.group_metadata().expect("group"),
        )
        .await
        .expect("send offsets");
    producer.commit_transaction().await.expect("commit");

    // The markers the coordinator writes after `EndTxn` answers are what make
    // both halves visible, so poll rather than reading once — same race the
    // visibility test above documents.
    let deadline = Instant::now() + Duration::from_secs(30);
    let stored = loop {
        let stored = consumer.committed().await.expect("committed");
        if !stored.is_empty() || Instant::now() >= deadline {
            break stored;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    assert_eq!(
        stored.get(&(INPUT.to_owned(), 0)).map(|entry| entry.offset),
        Some(i64::try_from(RECORDS).unwrap()),
        "a committed transaction must advance the group's offset"
    );

    let written = read(&cluster, OUTPUT, Visibility::CommittedOnly).await;
    assert_eq!(
        written.len(),
        RECORDS,
        "the output holds exactly the committed cycle's records"
    );
    assert!(
        written
            .iter()
            .filter_map(|record| record.value.as_ref())
            .all(|value| value.starts_with(b"committed-")),
        "the aborted cycle's records leaked into the committed view"
    );

    consumer.leave().await.expect("leave");
}

/// Fencing: a second producer sharing the transactional id wins, and the first
/// must fail terminally rather than retry into a wall.
#[testkit::integration_test]
async fn a_fenced_producer_fails_terminally_rather_than_retrying() {
    const TOPIC: &str = "txn-fencing";
    const ID: &str = "txn-fencing-shared";

    let (_fixture, cluster, _admin) = setup(TOPIC).await;

    let first = Producer::new(cluster.clone(), ProducerConfig::new().transactional_id(ID));
    first.init_transactions().await.expect("first init");

    // The second claim bumps the epoch, which is exactly what a transactional
    // id is for: a restarted application takes over from the instance it
    // replaced.
    let second = Producer::new(cluster.clone(), ProducerConfig::new().transactional_id(ID));
    second.init_transactions().await.expect("second init");

    first.begin_transaction().expect("begin");
    let outcome = first
        .send(
            ProducerRecord::new(TOPIC)
                .with_partition(0)
                .with_value("fenced"),
        )
        .await;

    let error = outcome.expect_err("the fenced producer must not succeed");
    assert!(
        matches!(
            error.code(),
            Some(kafka_conn::ErrorCode::ProducerFenced)
                | Some(kafka_conn::ErrorCode::InvalidProducerEpoch)
        ),
        "expected a fencing error, got {error}"
    );

    // Terminal, not transient: every later call fails the same way without
    // reaching the network.
    let again = first.begin_transaction();
    assert_eq!(
        again.expect_err("still fenced").code(),
        Some(kafka_conn::ErrorCode::ProducerFenced),
        "a fenced producer must stay fenced; retrying is an infinite loop"
    );

    // And the producer that won still works.
    second.begin_transaction().expect("begin");
    second
        .send(
            ProducerRecord::new(TOPIC)
                .with_partition(0)
                .with_value("winner"),
        )
        .await
        .expect("the fencing producer owns the id");
    second.commit_transaction().await.expect("commit");
}

/// The API refuses the orders that cannot work, before the network.
#[testkit::integration_test]
async fn the_transaction_api_refuses_an_impossible_order() {
    const TOPIC: &str = "txn-order";

    let (_fixture, cluster, _admin) = setup(TOPIC).await;

    // No transactional id at all.
    let plain = Producer::new(cluster.clone(), ProducerConfig::new());
    assert!(
        plain.begin_transaction().is_err(),
        "a producer with no transactional id cannot begin a transaction"
    );
    assert!(plain.commit_transaction().await.is_err());

    // A transactional id, but no `init_transactions`.
    let uninitialised = Producer::new(
        cluster.clone(),
        ProducerConfig::new().transactional_id("txn-order-1"),
    );
    assert!(
        uninitialised.begin_transaction().is_err(),
        "begin before init must be refused, not sent"
    );

    uninitialised.init_transactions().await.expect("init");
    uninitialised.begin_transaction().expect("begin");
    assert!(
        uninitialised.begin_transaction().is_err(),
        "a second begin while one is open must be refused"
    );
    assert!(uninitialised.in_transaction());
    uninitialised.abort_transaction().await.expect("abort");
    assert!(!uninitialised.in_transaction());
}
