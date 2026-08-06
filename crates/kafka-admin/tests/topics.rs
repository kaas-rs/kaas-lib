//! M6 acceptance: topics, configs, offsets.
//!
//! `cargo test -p kafka-admin --test topics -- --ignored`
//!
//! In-container shell tools bootstrap `localhost:9093`, the BROKER
//! listener — see `testkit::INTERNAL_BOOTSTRAP`. Port 9092 is advertised
//! as the *host-mapped* port for the test process, so a client inside the
//! container follows metadata to a port nothing is listening on and dies
//! with a bare TimeoutException.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::time::{Duration, Instant};

use kafka_admin::{
    Admin, ClusterConfig, ConfigChange, ConfigEntry, ConfigResource, ConfigSource, ErrorCode,
    NewTopic, OffsetSpec,
};
use testkit::Cluster as _;

/// How long a config change may take to reach the broker serving describes.
const CONFIG_TIMEOUT: Duration = Duration::from_secs(60);

/// Wait until every named topic describes successfully.
///
/// Same propagation window as [`await_config`], one level up: the controller
/// has committed the creation, the broker has not yet applied it, and a
/// describe in between reports the topics as missing.
async fn await_topics_visible(admin: &Admin, names: &[String]) {
    let deadline = Instant::now() + CONFIG_TIMEOUT;
    loop {
        let described = admin.describe_topics(names.to_vec()).await.unwrap();
        let visible = kafka_admin::oks(&described).count();
        if visible == names.len() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "only {visible}/{} topics became visible within {CONFIG_TIMEOUT:?}",
            names.len()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Wait for a topic config to report `expected`, then return the entry.
///
/// The broker acks an alter once the controller commits it, but serves
/// describes from the metadata it has applied — which in KRaft trails the log
/// asynchronously. Asserting on a single read makes the test a race that an
/// idle machine wins and a loaded one loses.
async fn await_config(admin: &Admin, topic: &str, name: &str, expected: &str) -> ConfigEntry {
    let deadline = Instant::now() + CONFIG_TIMEOUT;
    loop {
        let configs = admin
            .describe_configs([ConfigResource::topic(topic)])
            .await
            .unwrap();
        let entries = find(&configs, &ConfigResource::topic(topic))
            .as_ref()
            .expect("described");
        let entry = entries
            .iter()
            .find(|entry| entry.name == name)
            .unwrap_or_else(|| panic!("{name} is always present"));
        if entry.value.as_deref() == Some(expected) {
            return entry.clone();
        }
        assert!(
            Instant::now() < deadline,
            "{topic}.{name} never became {expected:?} within {CONFIG_TIMEOUT:?}; last saw {:?}",
            entry.value
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn find<'a, K: PartialEq, T>(
    items: &'a [(K, Result<T, kafka_admin::Error>)],
    key: &K,
) -> &'a Result<T, kafka_admin::Error> {
    items
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v)
        .expect("a result for every requested item")
}

#[testkit::integration_test]
async fn create_describe_alter_verify_delete() {
    let fixture = testkit::cluster(3).await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();

    // Create.
    let created = admin
        .create_topics([NewTopic::new("orders", 6, 3).with_config("retention.ms", "600000")])
        .await
        .unwrap();
    assert!(find(&created, &"orders".to_owned()).is_ok(), "{created:?}");

    // Describe.
    let described = admin.describe_topics(["orders"]).await.unwrap();
    let info = find(&described, &"orders".to_owned())
        .as_ref()
        .expect("described");
    assert_eq!(info.partitions.len(), 6);
    for partition in &info.partitions {
        assert_eq!(partition.replicas.len(), 3, "RF=3");
        assert!(partition.leader.is_some());
    }

    // Alter retention.
    let altered = admin
        .alter_configs([(
            ConfigResource::topic("orders"),
            vec![ConfigChange::set("retention.ms", "1200000")],
        )])
        .await
        .unwrap();
    assert!(
        find(&altered, &ConfigResource::topic("orders")).is_ok(),
        "{altered:?}"
    );

    // Verify.
    //
    // Polled, not read once. `IncrementalAlterConfigs` is acked when the
    // controller has committed the change, but a broker serves
    // `DescribeConfigs` from the metadata state it has *applied*, and in KRaft
    // it applies records from the log asynchronously. So there is a real
    // window where the alter has succeeded and a describe still returns the
    // old value — narrow on an idle machine, wide enough on a loaded CI runner
    // to fail as `left: Some("600000"), right: Some("1200000")`.
    //
    // Waiting for the value the alter promised is the assertion; the timeout
    // is what turns "never converged" into a failure rather than a hang.
    let retention = await_config(&admin, "orders", "retention.ms", "1200000").await;
    assert_eq!(retention.value.as_deref(), Some("1200000"));
    assert_eq!(
        retention.source,
        ConfigSource::TopicConfig,
        "an explicitly set value must not report as a default"
    );

    // Grow.
    let grown = admin
        .create_partitions([("orders".to_owned(), 9)])
        .await
        .unwrap();
    assert!(find(&grown, &"orders".to_owned()).is_ok(), "{grown:?}");

    // Delete.
    let deleted = admin.delete_topics(["orders"]).await.unwrap();
    assert!(find(&deleted, &"orders".to_owned()).is_ok(), "{deleted:?}");
}

#[testkit::integration_test]
async fn describing_fifty_topics_with_two_missing_gives_48_ok_and_2_err() {
    // Rule 4, stated as an assertion. A global error here would make the topic
    // list unusable on any cluster where something is mid-deletion.
    let fixture = testkit::single_broker().await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();

    let existing: Vec<String> = (0..48).map(|i| format!("topic-{i:02}")).collect();
    let created = admin
        .create_topics(existing.iter().map(|name| NewTopic::new(name, 1, 1)))
        .await
        .unwrap();
    assert_eq!(kafka_admin::oks(&created).count(), 48);

    // `create_topics` is acked by the controller; the broker answers describes
    // from applied metadata. Asking immediately can return
    // UNKNOWN_TOPIC_OR_PARTITION for every topic that was just created — which
    // fails this test as `left: 0, right: 48`, looking exactly like the
    // per-item behaviour being broken rather than the topics not being there
    // yet. Wait for them before asserting on the partial-failure property.
    await_topics_visible(&admin, &existing).await;

    let mut asked = existing.clone();
    asked.push("does-not-exist-a".to_owned());
    asked.push("does-not-exist-b".to_owned());

    let described = admin.describe_topics(asked).await.unwrap();
    assert_eq!(described.len(), 50);
    assert_eq!(kafka_admin::oks(&described).count(), 48);
    assert_eq!(kafka_admin::errs(&described).count(), 2);

    for (name, error) in kafka_admin::errs(&described) {
        assert!(name.starts_with("does-not-exist"));
        assert_eq!(error.code(), Some(ErrorCode::UnknownTopicOrPartition));
    }
}

#[testkit::integration_test]
async fn per_topic_size_on_rf3_is_the_single_replica_size_not_three_times_it() {
    let fixture = testkit::cluster(3).await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();

    admin
        .create_topics([NewTopic::new("sized", 3, 3)])
        .await
        .unwrap();

    // Produce a known quantity so there is something to measure.
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 5000 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9093 --topic sized"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    // Log dir sizes lag the write by a moment.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let sizes = admin.topic_sizes().await.unwrap();
    let size = find(&sizes, &"sized".to_owned())
        .as_ref()
        .expect("a size for the topic we just wrote");

    assert!(size.logical_bytes > 0, "{size:?}");
    assert_eq!(
        size.partitions.len(),
        3,
        "one entry per partition, counted at its leader"
    );

    // The assertion this test exists for. Every broker reports the bytes it
    // holds, so a naive sum over an RF=3 topic is three times too large.
    let ratio = f64::from(i32::try_from(size.replicated_bytes).unwrap())
        / f64::from(i32::try_from(size.logical_bytes).unwrap());
    assert!(
        (2.5..=3.5).contains(&ratio),
        "replicated/logical was {ratio}; logical={} replicated={}",
        size.logical_bytes,
        size.replicated_bytes
    );
}

#[testkit::integration_test]
async fn all_five_reachable_offset_sentinels_answer() {
    let fixture = testkit::single_broker().await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();

    admin
        .create_topics([NewTopic::new("offsets", 1, 1)])
        .await
        .unwrap();
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 100 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9093 --topic offsets"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    for spec in OffsetSpec::REACHABLE {
        let listed = admin
            .list_offsets([("offsets".to_owned(), 0)], spec)
            .await
            .unwrap();
        let result = find(&listed, &("offsets".to_owned(), 0));
        assert!(result.is_ok(), "{spec:?}: {result:?}");
    }

    // And the earliest/latest pair is the range a UI renders.
    let latest = admin
        .list_offsets([("offsets".to_owned(), 0)], OffsetSpec::Latest)
        .await
        .unwrap();
    let latest = find(&latest, &("offsets".to_owned(), 0))
        .as_ref()
        .expect("latest");
    assert_eq!(latest.offset, Some(100));
}

#[testkit::integration_test]
async fn the_kip_1023_sentinel_reports_the_gap_rather_than_guessing() {
    // An honest blocker made executable: `kafka-protocol` 0.17 caps ListOffsets
    // at v10 and `-6` needs v11. The right behaviour is an error that names the
    // reason, not a request the broker cannot interpret.
    let fixture = testkit::single_broker().await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    admin
        .create_topics([NewTopic::new("tiered", 1, 1)])
        .await
        .unwrap();

    let listed = admin
        .list_offsets(
            [("tiered".to_owned(), 0)],
            OffsetSpec::EarliestPendingUploadTimestamp,
        )
        .await
        .unwrap();
    let error = find(&listed, &("tiered".to_owned(), 0))
        .as_ref()
        .expect_err("unreachable in this build");
    let rendered = error.to_string();
    assert!(rendered.contains("v11"), "{rendered}");
}

#[testkit::integration_test]
async fn describe_cluster_and_log_dirs_agree_with_metadata() {
    let fixture = testkit::cluster(3).await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();

    let description = admin.describe_cluster().await.unwrap();
    assert_eq!(description.brokers.len(), 3);
    assert!(!description.cluster_id.is_empty());
    let controller = description.controller_id.expect("a controller");
    assert!(description.brokers.iter().any(|b| b.node_id == controller));

    for broker in &description.brokers {
        let dirs = admin.describe_log_dirs(broker.node_id).await.unwrap();
        assert!(
            !dirs.is_empty(),
            "broker {} has no log dirs",
            broker.node_id
        );
    }
}

#[testkit::integration_test]
async fn delete_records_moves_the_low_watermark() {
    let fixture = testkit::single_broker().await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();

    admin
        .create_topics([NewTopic::new("truncate-me", 1, 1)])
        .await
        .unwrap();
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 100 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9093 --topic truncate-me"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    let deleted = admin
        .delete_records([("truncate-me".to_owned(), 0, 50)])
        .await
        .unwrap();
    assert_eq!(
        find(&deleted, &("truncate-me".to_owned(), 0)).as_ref().ok(),
        Some(&50)
    );

    let earliest = admin
        .list_offsets([("truncate-me".to_owned(), 0)], OffsetSpec::Earliest)
        .await
        .unwrap();
    let earliest = find(&earliest, &("truncate-me".to_owned(), 0))
        .as_ref()
        .expect("earliest");
    assert_eq!(earliest.offset, Some(50));
}
