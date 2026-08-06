//! Live repro for the transactions visibility failure seen in CI.
//!
//! Mirrors `tests/transactions.rs::a_committed_transaction_is_visible_and_an_aborted_one_is_not`
//! against a real cluster instead of a container fixture:
//!
//! ```sh
//! eval "$(.claude/skills/live-cluster/resolve-target.sh strimzi)"
//! cargo run -q -p kafka-produce --example txn_visibility_live
//! ```
//!
//! Creates a `kaaslib-live-` prefixed topic and deletes it on the way out.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::{Duration, Instant};

use futures::StreamExt;
use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_produce::{Producer, ProducerConfig, ProducerRecord};
use kafka_read::{Cluster, ScanEvent, ScanSpec, StartPosition, Visibility};

const RECORDS: usize = 100;

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

#[tokio::main]
async fn main() {
    let bootstrap = std::env::var("KAAS_TEST_BOOTSTRAP").expect("KAAS_TEST_BOOTSTRAP");
    let topic = format!(
        "kaaslib-live-txnrepro-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let admin = Admin::connect(vec![bootstrap], ClusterConfig::default())
        .await
        .expect("admin");
    admin
        .create_topics([NewTopic::new(&topic, 1, 3)])
        .await
        .expect("create");
    // Read-after-create is not immediate on a real cluster; wait until the
    // topic describes before producing to it.
    for _ in 0..50 {
        if let Ok(results) = admin.describe_topics([topic.clone()]).await
            && results.iter().any(|(_, result)| result.is_ok())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    eprintln!("topic = {topic}");

    let cluster = admin.cluster().clone();
    let producer = Producer::new(
        cluster.clone(),
        ProducerConfig::new().transactional_id(format!("{topic}-producer")),
    );
    producer.init_transactions().await.expect("init");

    write_transaction(&producer, &topic, "committed", RECORDS).await;
    producer.commit_transaction().await.expect("commit");

    write_transaction(&producer, &topic, "aborted", RECORDS).await;
    producer.abort_transaction().await.expect("abort");

    let everything = read(&cluster, &topic, Visibility::All).await;
    println!("all = {}", everything.len());

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut rounds = 0u32;
    let committed = loop {
        let committed = read(&cluster, &topic, Visibility::CommittedOnly).await;
        rounds += 1;
        if committed.len() >= RECORDS || Instant::now() >= deadline {
            break committed;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    println!("committed = {} (rounds = {rounds})", committed.len());
    let leaked = committed
        .iter()
        .filter_map(|record| record.value.as_ref())
        .filter(|value| value.starts_with(b"aborted-"))
        .count();
    println!("leaked_aborted = {leaked}");

    // The two views that explain a pinned last stable offset: which producer
    // the partition leader thinks still has a transaction open, and what the
    // coordinator thinks the transaction's state is.
    let producers = admin
        .describe_producers([(topic.clone(), 0)])
        .await
        .expect("describe_producers");
    println!("producers = {producers:?}");
    let transactions = admin
        .describe_transactions([format!("{topic}-producer")])
        .await
        .expect("describe_transactions");
    println!("transactions = {transactions:?}");

    admin.delete_topics([topic.as_str()]).await.expect("delete");

    assert_eq!(everything.len(), RECORDS * 2, "All must see both halves");
    assert_eq!(
        committed.len(),
        RECORDS,
        "CommittedOnly must see the committed half"
    );
    assert_eq!(leaked, 0, "no aborted record may leak");
    println!("PASS");
}
