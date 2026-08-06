//! Live repro for the cooperative-rebalance `UnknownMemberId` failure in CI.
//!
//! Mirrors `tests/group_classic.rs::a_cooperative_rebalance_keeps_what_it_can`
//! against a real cluster:
//!
//! ```sh
//! eval "$(.claude/skills/live-cluster/resolve-target.sh strimzi)"
//! cargo run -q -p kafka-consume --example coop_rebalance_live
//! ```
//!
//! Creates a `kaaslib-live-` prefixed topic and deletes it on the way out.
//! `RUST_LOG=kafka_consume=debug` shows the join/sync rounds.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_consume::{Assignor, ClassicConsumer, ConsumerConfig};
use kafka_produce::{Producer, ProducerConfig, ProducerRecord};
use kafka_read::Visibility;

const PARTITIONS: i32 = 12;

fn config() -> ConsumerConfig {
    ConsumerConfig::new()
        .visibility(Visibility::All)
        .max_wait_ms(200)
}

async fn cooperative(bootstrap: &str, group: &str, topic: &str) -> ClassicConsumer {
    let cluster = kafka_meta::Cluster::connect(
        vec![bootstrap.to_owned()],
        kafka_meta::ClusterConfig::default(),
    )
    .await
    .expect("cluster");
    ClassicConsumer::subscribe(cluster, config(), group, [topic])
        .await
        .expect("subscribe")
        .assignors([Assignor::CooperativeSticky])
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let bootstrap = std::env::var("KAAS_TEST_BOOTSTRAP").expect("KAAS_TEST_BOOTSTRAP");
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let topic = format!("kaaslib-live-cooprepro-{stamp}");
    let group = format!("kaaslib-live-coopgroup-{stamp}");

    let admin = Admin::connect(vec![bootstrap.clone()], ClusterConfig::default())
        .await
        .expect("admin");
    admin
        .create_topics([NewTopic::new(&topic, PARTITIONS, 3)])
        .await
        .expect("create");
    for _ in 0..50 {
        if let Ok(results) = admin.describe_topics([topic.clone()]).await
            && results.iter().any(|(_, result)| result.is_ok())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    eprintln!("topic = {topic}");

    let producer = Producer::new(admin.cluster().clone(), ProducerConfig::new());
    for i in 0..600 {
        producer
            .send(ProducerRecord::new(&topic).value(format!("v{i}")))
            .await
            .expect("seed");
    }

    let mut a = cooperative(&bootstrap, &group, &topic).await;
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline && a.assignment().len() != usize::try_from(PARTITIONS).unwrap()
    {
        a.poll().await.expect("a alone");
    }
    let alone: BTreeSet<(String, i32)> = a.assignment().into_iter().collect();
    assert_eq!(
        alone.len(),
        usize::try_from(PARTITIONS).unwrap(),
        "a never owned the topic"
    );
    println!("phase1 = a owns {}", alone.len());

    // `a` moves to its own task before `b` arrives: a staggered classic
    // rebalance blocks `b`'s JoinGroup until `a` re-joins, and `a` only acts
    // — only *heartbeats* — when polled. See the module docs on
    // `kafka_consume::classic`: each member needs its own task, exactly as it
    // needs its own connection.
    let (assignment_tx, assignment_rx) = tokio::sync::watch::channel(Vec::new());
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let a_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                result = a.poll() => {
                    result.expect("a");
                    let _ = assignment_tx.send(a.assignment());
                }
                _ = stop_rx.changed() => break,
            }
        }
        a
    });

    let mut b = cooperative(&bootstrap, &group, &topic).await;
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        b.poll().await.expect("b");
        let owned_a = assignment_rx.borrow().len();
        let owned_b = b.assignment().len();
        if owned_a + owned_b == usize::try_from(PARTITIONS).unwrap() && owned_a > 0 && owned_b > 0 {
            break;
        }
    }
    stop_tx.send(true).expect("stop");
    let mut a = a_task.await.expect("a task");

    let owned_a: BTreeSet<(String, i32)> = a.assignment().into_iter().collect();
    let owned_b: BTreeSet<(String, i32)> = b.assignment().into_iter().collect();
    println!("phase2 = a {} b {}", owned_a.len(), owned_b.len());

    assert!(
        owned_a.is_disjoint(&owned_b),
        "overlap: {:?}",
        owned_a.intersection(&owned_b).collect::<Vec<_>>()
    );
    assert_eq!(
        owned_a.union(&owned_b).count(),
        usize::try_from(PARTITIONS).unwrap(),
        "the group left partitions unassigned"
    );
    assert!(!owned_a.is_empty() && !owned_b.is_empty(), "no rebalance");
    assert!(owned_a.iter().all(|key| alone.contains(key)), "not sticky");

    a.leave().await.expect("leave a");
    b.leave().await.expect("leave b");
    admin.delete_topics([topic.as_str()]).await.expect("delete");
    println!("PASS");
}
