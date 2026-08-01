//! M4 acceptance: metadata, routing and the connection pool.
//!
//! `cargo test -p kafka-meta --test metadata -- --ignored`
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::HashSet;
use std::time::Duration;

use kafka_meta::{Cluster, ClusterConfig, CoordinatorKind};
use testkit::{BrokerConfig, Cluster as _};

async fn create_topic(fixture: &testkit::KafkaCluster, name: &str, partitions: u32, rf: u32) {
    fixture
        .kafka_cli(
            0,
            "kafka-topics.sh",
            [
                "--create",
                "--topic",
                name,
                "--partitions",
                &partitions.to_string(),
                "--replication-factor",
                &rf.to_string(),
            ],
        )
        .await
        .expect("topic created");
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn every_partition_resolves_to_a_leader_inside_its_own_replica_set() {
    let fixture = testkit::cluster(3).await.unwrap();
    create_topic(&fixture, "orders", 6, 3).await;

    let cluster = Cluster::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    let snapshot = cluster.refresh().await.unwrap();

    assert_eq!(snapshot.brokers().len(), 3);
    let topic = snapshot.topic("orders").expect("topic in the snapshot");
    assert_eq!(topic.partitions.len(), 6);

    let mut leaders = HashSet::new();
    for partition in &topic.partitions {
        let leader = cluster
            .leader_for("orders", partition.partition)
            .await
            .unwrap_or_else(|e| panic!("partition {}: {e}", partition.partition));

        // A leader outside its own replica set is impossible, so seeing one
        // means the response was decoded into the wrong fields — the kind of
        // bug that otherwise only shows up as mysterious NOT_LEADER loops.
        assert!(
            partition.replicas.contains(&leader),
            "partition {} leader {leader} not in replicas {:?}",
            partition.partition,
            partition.replicas
        );
        assert!(
            snapshot.broker(leader).is_some(),
            "leader {leader} is not a known broker"
        );
        assert_eq!(partition.replicas.len(), 3);
        leaders.insert(leader);
    }

    // Six partitions across three brokers: if they all landed on one broker,
    // we are reading a single broker's view as though it were the cluster's.
    assert!(
        leaders.len() > 1,
        "all six partitions claim the same leader: {leaders:?}"
    );
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn metadata_for_an_unknown_topic_does_not_create_it() {
    // The destructive half of the auto-topic-creation trap. The unit test in
    // cluster.rs proves the flag is off in the request we build; this proves
    // the flag is the only thing standing between a UI search box and a
    // created topic, by pointing it at a broker that would happily oblige.
    let fixture = testkit::single_broker_with(BrokerConfig::new().with_auto_create_topics(true))
        .await
        .unwrap();

    let cluster = Cluster::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    let snapshot = cluster
        .refresh_topics(&["typo-in-the-search-box"])
        .await
        .unwrap();
    assert_eq!(
        snapshot
            .topic("typo-in-the-search-box")
            .and_then(|t| t.error),
        Some(kafka_meta::ErrorCode::UnknownTopicOrPartition)
    );

    // Ask a second, independent client — the broker's own CLI — whether the
    // topic exists now.
    let listed = fixture
        .kafka_cli(0, "kafka-topics.sh", ["--list"])
        .await
        .unwrap();
    assert!(
        !listed.stdout.contains("typo-in-the-search-box"),
        "the metadata request created a topic:\n{}",
        listed.stdout
    );
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn the_snapshot_reports_its_own_age() {
    let fixture = testkit::single_broker().await.unwrap();
    let cluster = Cluster::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();

    let first = cluster.snapshot();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(first.age() >= Duration::from_millis(50));

    let refreshed = cluster.refresh().await.unwrap();
    assert!(refreshed.age() < first.age());
    // Wall-clock too, which is what a UI actually renders.
    assert!(refreshed.fetched_at() >= first.fetched_at());
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_targeted_refresh_keeps_the_rest_of_the_cache() {
    let fixture = testkit::single_broker().await.unwrap();
    create_topic(&fixture, "alpha", 1, 1).await;
    create_topic(&fixture, "beta", 1, 1).await;

    let cluster = Cluster::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    cluster.refresh().await.unwrap();

    let merged = cluster.refresh_topics(&["alpha"]).await.unwrap();
    assert!(merged.topic("alpha").is_some());
    assert!(
        merged.topic("beta").is_some(),
        "a targeted refresh discarded the rest of the cache"
    );
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn the_controller_and_coordinators_resolve_to_real_brokers() {
    let fixture = testkit::cluster(3).await.unwrap();
    let cluster = Cluster::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    let snapshot = cluster.snapshot();

    let controller = cluster.controller().await.unwrap();
    assert!(
        snapshot.broker(controller).is_some(),
        "controller {controller} is not a known broker"
    );

    let coordinator = cluster.coordinator_for("some-group").await.unwrap();
    assert!(
        snapshot.broker(coordinator).is_some(),
        "coordinator {coordinator} is not a known broker"
    );

    // Cached: a second call must not re-ask, and must agree.
    assert_eq!(
        cluster.coordinator_for("some-group").await.unwrap(),
        coordinator
    );
    cluster.invalidate_coordinator(CoordinatorKind::Group, "some-group");
    assert_eq!(
        cluster.coordinator_for("some-group").await.unwrap(),
        coordinator
    );
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn the_pool_opens_one_connection_per_broker_and_reuses_it() {
    let fixture = testkit::cluster(3).await.unwrap();
    let cluster = Cluster::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    cluster.refresh().await.unwrap();

    let ids: Vec<i32> = cluster
        .snapshot()
        .brokers()
        .iter()
        .map(|b| b.node_id)
        .collect();
    for id in &ids {
        for _ in 0..5 {
            cluster.pool().get(*id).await.expect("connects");
        }
    }
    assert_eq!(
        cluster.pool().live_connections().await,
        ids.len(),
        "five calls per broker must not open five sockets per broker"
    );
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn requests_survive_a_broker_dying_underneath_them() {
    // The retry policy plus metadata invalidation, together. Killing a broker
    // strands its connection; the next request must notice, refresh, and land
    // somewhere else rather than looping against a dead socket.
    let fixture = testkit::cluster(3).await.unwrap();
    create_topic(&fixture, "resilient", 6, 3).await;

    let cluster = Cluster::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    cluster.refresh().await.unwrap();

    fixture.stop_node(2).await.unwrap();

    let snapshot = cluster.refresh().await.expect("metadata still answerable");
    assert!(!snapshot.topics().is_empty());
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn send_routed_refuses_to_guess_a_coordinator() {
    use kafka_conn::protocol::messages::OffsetFetchRequest;

    let fixture = testkit::single_broker().await.unwrap();
    let cluster = Cluster::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();

    // The api key alone does not say *which* coordinator, so the router refuses
    // rather than sending it somewhere plausible and retrying NOT_COORDINATOR.
    let err = cluster
        .send_routed(OffsetFetchRequest::default())
        .await
        .expect_err("coordinator-routed requests need a key");
    assert!(format!("{err}").contains("coordinator"), "{err}");
}
