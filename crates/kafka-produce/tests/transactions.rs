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

use std::time::Duration;

use futures::StreamExt;
use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_produce::{Producer, ProducerConfig, ProducerRecord};
use kafka_read::{Cluster, ScanEvent, ScanSpec, StartPosition, Visibility};
use testkit::{Cluster as _, KafkaCluster};

/// The one cluster this binary's tests share.
///
/// Each test already names its own topic, so nothing here needed splitting —
/// only the clusters, which were one 3-node fixture per test to assert things
/// that do not care whose broker they run on.
///
/// Never dropped, because a `static` is not: the containers go with the
/// ephemeral runner pod on CI, and may want `docker container prune` locally.
static SHARED: tokio::sync::OnceCell<Shared> = tokio::sync::OnceCell::const_new();

struct Shared {
    _fixture: KafkaCluster,
    admin: Admin,
}

async fn shared() -> &'static Shared {
    SHARED
        .get_or_init(|| async {
            let fixture = testkit::cluster(3).await.expect("cluster");
            let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
                .await
                .expect("admin");
            Shared {
                _fixture: fixture,
                admin,
            }
        })
        .await
}

async fn setup(topic: &str) -> (Cluster, Admin) {
    let shared = shared().await;
    shared
        .admin
        .create_topics([NewTopic::new(topic, 1, 3)])
        .await
        .expect("topic");
    await_topic(&shared.admin, topic).await;
    (shared.admin.cluster().clone(), shared.admin.clone())
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
                        .partition(0)
                        .value(format!("{prefix}-{i}")),
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
#[tokio::test]
#[ignore = "needs Docker"]
async fn a_committed_transaction_is_visible_and_an_aborted_one_is_not() {
    const TOPIC: &str = "txn-visibility";
    const RECORDS: usize = 100;

    let (cluster, _admin) = setup(TOPIC).await;
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

    // `CommittedOnly` is the consumer's question.
    let committed = read(&cluster, TOPIC, Visibility::CommittedOnly).await;
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

/// Fencing: a second producer sharing the transactional id wins, and the first
/// must fail terminally rather than retry into a wall.
#[tokio::test]
#[ignore = "needs Docker"]
async fn a_fenced_producer_fails_terminally_rather_than_retrying() {
    const TOPIC: &str = "txn-fencing";
    const ID: &str = "txn-fencing-shared";

    let (cluster, _admin) = setup(TOPIC).await;

    let first = Producer::new(cluster.clone(), ProducerConfig::new().transactional_id(ID));
    first.init_transactions().await.expect("first init");

    // The second claim bumps the epoch, which is exactly what a transactional
    // id is for: a restarted application takes over from the instance it
    // replaced.
    let second = Producer::new(cluster.clone(), ProducerConfig::new().transactional_id(ID));
    second.init_transactions().await.expect("second init");

    first.begin_transaction().expect("begin");
    let outcome = first
        .send(ProducerRecord::new(TOPIC).partition(0).value("fenced"))
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
        .send(ProducerRecord::new(TOPIC).partition(0).value("winner"))
        .await
        .expect("the fencing producer owns the id");
    second.commit_transaction().await.expect("commit");
}

/// The API refuses the orders that cannot work, before the network.
#[tokio::test]
#[ignore = "needs Docker"]
async fn the_transaction_api_refuses_an_impossible_order() {
    const TOPIC: &str = "txn-order";

    let (cluster, _admin) = setup(TOPIC).await;

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
