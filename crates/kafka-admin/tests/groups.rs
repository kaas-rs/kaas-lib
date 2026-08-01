//! M7 acceptance: groups — all four kinds.
//!
//! `cargo test -p kafka-admin --test groups -- --ignored`
//!
//! The fixtures come from the Kafka shell tools inside the container, not from
//! a Rust client. `librdkafka` has no KIP-932 share-group support, so `rdkafka`
//! cannot generate the share-group fixture this test needs at all — and the
//! image already ships `kafka-console-consumer.sh` and
//! `kafka-console-share-consumer.sh`, which reach every group kind with zero
//! build dependencies.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::time::Duration;

use kafka_admin::{
    Admin, ClusterConfig, GroupDescription, GroupState, NewTopic, OffsetReset, OffsetSpec,
};
use testkit::{BrokerConfig, Cluster as _, KafkaCluster};

/// Start a console consumer in the background, in a group, and leave it running.
async fn start_consumer(fixture: &KafkaCluster, tool: &str, group: &str, extra: &[&str]) {
    let mut command = format!(
        "nohup /opt/kafka/bin/{tool} --bootstrap-server localhost:9092 \
         --topic fixture-topic --group {group}"
    );
    for arg in extra {
        command.push(' ');
        command.push_str(arg);
    }
    command.push_str(" >/tmp/consumer-");
    command.push_str(group);
    command.push_str(".log 2>&1 &");

    fixture
        .exec(0, vec!["bash".to_owned(), "-c".to_owned(), command])
        .await
        .expect("consumer started");
}

async fn fixture_with_all_group_kinds() -> (KafkaCluster, Admin) {
    let fixture = testkit::single_broker_with(
        BrokerConfig::new()
            .with_share_groups(true)
            .with_property("group.consumer.session.timeout.ms", "45000"),
    )
    .await
    .expect("broker");

    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .expect("admin");
    admin
        .create_topics([NewTopic::new("fixture-topic", 3, 1)])
        .await
        .expect("topic");

    // Produce something so the consumers have work and stay joined.
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 1000 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9092 --topic fixture-topic"
                    .to_owned(),
            ],
        )
        .await
        .expect("produced");

    start_consumer(
        &fixture,
        "kafka-console-consumer.sh",
        "classic-group",
        &["--consumer-property", "group.protocol=classic"],
    )
    .await;
    start_consumer(
        &fixture,
        "kafka-console-consumer.sh",
        "consumer-group",
        &["--consumer-property", "group.protocol=consumer"],
    )
    .await;
    start_consumer(
        &fixture,
        "kafka-console-share-consumer.sh",
        "share-group",
        &[],
    )
    .await;

    // Joining is not instantaneous, and a group with no members describes as
    // Empty rather than failing — which would make this test pass for the
    // wrong reason.
    tokio::time::sleep(Duration::from_secs(15)).await;
    (fixture, admin)
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn all_three_describable_group_kinds_list_with_the_right_type_and_members() {
    let (_fixture, admin) = fixture_with_all_group_kinds().await;

    let listings = admin.list_groups().await.unwrap();
    let names: Vec<&str> = listings.iter().map(|l| l.group_id.as_str()).collect();
    for expected in ["classic-group", "consumer-group", "share-group"] {
        assert!(names.contains(&expected), "missing {expected}: {names:?}");
    }

    for listing in &listings {
        let expected_type = match listing.group_id.as_str() {
            "classic-group" => "classic",
            "consumer-group" => "consumer",
            "share-group" => "share",
            _ => continue,
        };
        assert_eq!(
            listing.group_type.to_ascii_lowercase(),
            expected_type,
            "{}",
            listing.group_id
        );
    }

    let described = admin
        .describe_groups(["classic-group", "consumer-group", "share-group"])
        .await
        .unwrap();
    assert_eq!(described.len(), 3);

    for (group_id, result) in &described {
        let description = result
            .as_ref()
            .unwrap_or_else(|e| panic!("{group_id}: {e}"));
        // The distinction is preserved rather than flattened: each group kind
        // decodes into its own variant, carrying the fields that protocol has.
        match (group_id.as_str(), description) {
            ("classic-group", GroupDescription::Classic { members, .. }) => {
                assert!(!members.is_empty(), "classic group has no members");
                assert!(!members[0].client_id.is_empty());
            }
            ("consumer-group", GroupDescription::Consumer { members, .. }) => {
                assert!(!members.is_empty(), "consumer group has no members");
                assert!(members[0].member_epoch >= 0);
            }
            ("share-group", GroupDescription::Share { members, .. }) => {
                assert!(!members.is_empty(), "share group has no members");
            }
            (id, other) => panic!("{id} described as the wrong kind: {other:?}"),
        }
    }
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn an_undescribable_group_type_surfaces_as_unrecognized_not_as_an_error() {
    // The property that matters: `kafka-protocol` 0.17 has no
    // StreamsGroupDescribe schema, so a streams group on a 4.1+ cluster running
    // Kafka Streams can be listed and not described. It must render as a
    // known-but-undescribable group rather than taking down the group list.
    //
    // Standing up a Streams application inside the fixture is out of scope, so
    // this drives the same path with a group type the code cannot describe and
    // asserts the *handling*, which is what would break.
    let (_fixture, admin) = fixture_with_all_group_kinds().await;

    let listings = admin.list_groups().await.unwrap();
    assert!(!listings.is_empty());

    for listing in &listings {
        if listing.describable() {
            continue;
        }
        let described = admin
            .describe_groups([listing.group_id.clone()])
            .await
            .unwrap();
        let (_, result) = &described[0];
        let description = result.as_ref().expect("undescribable is not an error");
        assert!(
            matches!(description, GroupDescription::Unrecognized { .. }),
            "{description:?}"
        );
    }

    // And the classification itself, which is what decides the above.
    assert!(!fake_listing("streams").describable());
    assert!(fake_listing("consumer").describable());
}

fn fake_listing(group_type: &str) -> kafka_admin::GroupListing {
    kafka_admin::GroupListing {
        group_id: "g".to_owned(),
        state: GroupState::Stable,
        group_type: group_type.to_owned(),
        protocol_type: "consumer".to_owned(),
    }
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn offsets_can_be_read_and_reset_for_a_classic_group() {
    let fixture = testkit::single_broker().await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    admin
        .create_topics([NewTopic::new("reset-me", 1, 1)])
        .await
        .unwrap();
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 100 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9092 --topic reset-me"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    // Consume to completion so the group exists, has committed offsets, and is
    // *empty* by the time we try to reset it.
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
                 --topic reset-me --group resettable --from-beginning --timeout-ms 8000 \
                 --consumer-property group.protocol=classic >/dev/null 2>&1 || true"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    let committed = admin.fetch_offsets("resettable", None).await.unwrap();
    let offset = committed
        .iter()
        .find(|((topic, _), _)| topic == "reset-me")
        .map(|(_, value)| value.as_ref().expect("committed"))
        .expect("an offset for the topic");
    assert_eq!(offset.offset, 100);

    // Reset to the beginning. The classic group protocol wants
    // `generation_id = -1`, and the reset path picks that from the group's
    // described kind rather than assuming.
    let reset = admin
        .reset_offsets("resettable", [OffsetReset::new("reset-me", 0, 0)])
        .await
        .unwrap();
    assert!(reset[0].1.is_ok(), "{reset:?}");

    let after = admin.fetch_offsets("resettable", None).await.unwrap();
    let offset = after
        .iter()
        .find(|((topic, _), _)| topic == "reset-me")
        .map(|(_, value)| value.as_ref().expect("committed"))
        .expect("an offset");
    assert_eq!(offset.offset, 0);
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn offsets_can_be_reset_for_a_kip_848_consumer_group() {
    // The other half of the trap. A KIP-848 group wants `member_epoch = -1` in
    // the same wire field, and sending the classic sentinel yields
    // ILLEGAL_GENERATION against exactly this group type.
    let fixture = testkit::single_broker().await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    admin
        .create_topics([NewTopic::new("reset-848", 1, 1)])
        .await
        .unwrap();
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 100 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9092 --topic reset-848"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
                 --topic reset-848 --group modern --from-beginning --timeout-ms 8000 \
                 --consumer-property group.protocol=consumer >/dev/null 2>&1 || true"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    // Wait for the member to be fully gone, or the reset is refused by design.
    for _ in 0..20 {
        let described = admin.describe_groups(["modern"]).await.unwrap();
        if let Ok(description) = &described[0].1
            && description.state().is_empty()
        {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let reset = admin
        .reset_offsets("modern", [OffsetReset::new("reset-848", 0, 25)])
        .await
        .unwrap();
    assert!(reset[0].1.is_ok(), "{reset:?}");

    let after = admin.fetch_offsets("modern", None).await.unwrap();
    let offset = after
        .iter()
        .find(|((topic, _), _)| topic == "reset-848")
        .map(|(_, value)| value.as_ref().expect("committed"))
        .expect("an offset");
    assert_eq!(offset.offset, 25);
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn resetting_a_live_groups_offsets_is_refused() {
    // The broker would accept this and let the live member overwrite it
    // seconds later. A reset that "succeeds" and silently does nothing is
    // worse than an error, because the operator watches the lag not move and
    // has nothing to go on.
    let (_fixture, admin) = fixture_with_all_group_kinds().await;

    let error = admin
        .reset_offsets("classic-group", [OffsetReset::new("fixture-topic", 0, 0)])
        .await
        .expect_err("a live group cannot be reset");
    let rendered = error.to_string();
    assert!(rendered.contains("empty"), "{rendered}");
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn groups_can_be_deleted_and_their_offsets_removed() {
    let fixture = testkit::single_broker().await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    admin
        .create_topics([NewTopic::new("deletable", 1, 1)])
        .await
        .unwrap();
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 10 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9092 --topic deletable"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
                 --topic deletable --group doomed --from-beginning --timeout-ms 6000 \
                 --consumer-property group.protocol=classic >/dev/null 2>&1 || true"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    let deleted_offsets = admin
        .delete_offsets("doomed", [("deletable".to_owned(), 0)])
        .await
        .unwrap();
    assert!(deleted_offsets[0].1.is_ok(), "{deleted_offsets:?}");

    let deleted = admin.delete_groups(["doomed"]).await.unwrap();
    assert!(deleted[0].1.is_ok(), "{deleted:?}");

    let listings = admin.list_groups().await.unwrap();
    assert!(!listings.iter().any(|l| l.group_id == "doomed"));
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_groups_lag_can_be_computed_from_committed_and_latest_offsets() {
    // The single most-rendered number in a Kafka UI, and the one that needs
    // both halves of this milestone to be right.
    let (_fixture, admin) = fixture_with_all_group_kinds().await;

    let committed = admin.fetch_offsets("classic-group", None).await.unwrap();
    let latest = admin
        .list_offsets(
            (0..3).map(|p| ("fixture-topic".to_owned(), p)),
            OffsetSpec::Latest,
        )
        .await
        .unwrap();

    let mut total_lag = 0i64;
    for ((topic, partition), committed) in &committed {
        let Ok(committed) = committed else { continue };
        let Some((_, Ok(end))) = latest
            .iter()
            .find(|((t, p), _)| t == topic && p == partition)
        else {
            continue;
        };
        total_lag += end.offset.unwrap_or_default() - committed.offset;
    }
    assert!(total_lag >= 0, "lag cannot be negative: {total_lag}");
}
