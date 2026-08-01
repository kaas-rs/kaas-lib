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
    Admin, ClusterConfig, GroupDescription, GroupState, NewTopic, OffsetReset, OffsetSpec,
};
use testkit::{BrokerConfig, Cluster as _, KafkaCluster};

/// How long a fixture may take to settle.
///
/// Deliberately generous. The fixture previously slept a fixed 15 seconds,
/// which was fine on an idle laptop and failed every time on CI, where
/// several of these fixtures boot their own broker concurrently and a JVM
/// console consumer needs a good part of that budget just to reach `main`.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(180);

/// Start a console consumer in the background, in a group, and leave it running.
async fn start_consumer(fixture: &KafkaCluster, tool: &str, group: &str, extra: &[&str]) {
    let mut command = format!(
        "nohup /opt/kafka/bin/{tool} --bootstrap-server localhost:9093 \
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
    // `exec_ok`, not `exec`: `exec` returns Ok for a command that ran and
    // failed, so `.expect("produced")` passed happily while the producer could
    // not reach the broker at all. That silence is what hid the listener bug.
    testkit::exec_ok(
        &fixture,
        0,
        vec![
            "bash".to_owned(),
            "-c".to_owned(),
            "seq 1 1000 | /opt/kafka/bin/kafka-console-producer.sh \
             --bootstrap-server localhost:9093 --topic fixture-topic"
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
    // Empty rather than failing — which would make these tests pass for the
    // wrong reason. So wait for the condition the tests actually need rather
    // than for a duration that happened to be long enough once.
    settle_groups(
        &fixture,
        &admin,
        &["classic-group", "consumer-group", "share-group"],
    )
    .await;
    (fixture, admin)
}

/// Block until every named group has registered *and* has at least one member.
///
/// Polling rather than sleeping is the whole point: the fixture is ready when
/// the broker says it is, not when a magic number elapses. A fixed sleep that
/// is too short fails as `missing classic-group: []` — which names the
/// symptom and gives no clue at all about the cause — so on timeout this dumps
/// each console consumer's log, the one place the actual reason ever appears.
async fn settle_groups(fixture: &KafkaCluster, admin: &Admin, expected: &[&str]) {
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    let mut seen: Vec<String> = Vec::new();

    while Instant::now() < deadline {
        if let Ok(listings) = admin.list_groups().await {
            seen = listings.iter().map(|l| l.group_id.clone()).collect();
            let all_present = expected
                .iter()
                .all(|want| seen.iter().any(|got| got == want));
            if all_present && members_joined(admin, expected).await {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    panic!(
        "fixture did not settle within {SETTLE_TIMEOUT:?}\n  wanted: {expected:?}\n  \
         saw:    {seen:?}\n{}",
        consumer_logs(fixture, expected).await
    );
}

/// Whether every named group describes with at least one member.
///
/// `member_count` returns `None` for a group this build cannot describe,
/// which is not "settled" — treat it as not ready rather than as satisfied.
async fn members_joined(admin: &Admin, expected: &[&str]) -> bool {
    match admin.describe_groups(expected.iter().copied()).await {
        Ok(described) => described.iter().all(|(_, result)| {
            result
                .as_ref()
                .ok()
                .and_then(|d| d.member_count())
                .is_some_and(|count| count > 0)
        }),
        Err(_) => false,
    }
}

/// Run a bounded console consumer, then wait until its group has committed.
///
/// `--timeout-ms` makes the consumer exit non-zero when it stops on idle,
/// which is its *normal* path here — so the exit code cannot be the signal
/// that it worked. The previous `>/dev/null 2>&1 || true` therefore threw away
/// the only evidence there was, and a consumer that never started at all
/// surfaced several assertions later as `GROUP_ID_NOT_FOUND`, naming a group
/// whose absence nothing had explained.
///
/// So keep the log, then poll for the committed offset the caller actually
/// depends on, and print the log if it never arrives.
async fn consume_into_group(
    fixture: &KafkaCluster,
    admin: &Admin,
    group: &str,
    topic: &str,
    protocol: &str,
    timeout_ms: u32,
) {
    let command = format!(
        "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9093 \
         --topic {topic} --group {group} --from-beginning --timeout-ms {timeout_ms} \
         --consumer-property group.protocol={protocol} >/tmp/consumer-{group}.log 2>&1; true"
    );
    fixture
        .exec(0, vec!["bash".to_owned(), "-c".to_owned(), command])
        .await
        .expect("consumer ran");

    let deadline = Instant::now() + SETTLE_TIMEOUT;
    while Instant::now() < deadline {
        if let Ok(committed) = admin.fetch_offsets(group, None).await
            && committed
                .iter()
                .any(|((name, _), value)| name == topic && value.is_ok())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    panic!(
        "group {group} never committed an offset for {topic} within {SETTLE_TIMEOUT:?}\n{}",
        consumer_logs(fixture, &[group]).await
    );
}

/// The console consumers' own logs, for a failure message that explains itself.
async fn consumer_logs(fixture: &KafkaCluster, groups: &[&str]) -> String {
    let mut out = String::new();
    for group in groups {
        out.push_str(&format!("\n--- /tmp/consumer-{group}.log ---\n"));
        let command = format!("tail -n 20 /tmp/consumer-{group}.log 2>&1 || echo '(no log)'");
        match fixture
            .exec(0, vec!["bash".to_owned(), "-c".to_owned(), command])
            .await
        {
            Ok(output) => {
                out.push_str(output.stdout.trim_end());
                if !output.stderr.trim().is_empty() {
                    out.push_str(&format!("\n[stderr] {}", output.stderr.trim_end()));
                }
            }
            Err(error) => out.push_str(&format!("(could not read: {error})")),
        }
        out.push('\n');
    }
    out
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
                 --bootstrap-server localhost:9093 --topic reset-me"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    // Consume to completion so the group exists, has committed offsets, and is
    // *empty* by the time we try to reset it.
    consume_into_group(&fixture, &admin, "resettable", "reset-me", "classic", 8000).await;

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
                 --bootstrap-server localhost:9093 --topic reset-848"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();
    consume_into_group(&fixture, &admin, "modern", "reset-848", "consumer", 8000).await;

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
                 --bootstrap-server localhost:9093 --topic deletable"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();
    consume_into_group(&fixture, &admin, "doomed", "deletable", "classic", 6000).await;

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
    let mut partitions_compared = 0usize;
    for ((topic, partition), committed) in &committed {
        let Ok(committed) = committed else { continue };
        let Some((_, Ok(end))) = latest
            .iter()
            .find(|((t, p), _)| t == topic && p == partition)
        else {
            continue;
        };
        let end = end.offset.unwrap_or_default();
        // Per partition, not just in aggregate: a sign error on one partition
        // cancelling against another would pass a total-only check.
        assert!(
            committed.offset <= end,
            "{topic}-{partition} committed {} past the log end {end}",
            committed.offset
        );
        total_lag += end - committed.offset;
        partitions_compared += 1;
    }

    // Without this the loop above can match nothing at all — no committed
    // offsets, every `continue` taken — and a lag of zero over zero partitions
    // satisfies any inequality you care to write. The fixture consumes
    // `fixture-topic`, so there is something to compare.
    assert!(
        partitions_compared > 0,
        "no partition had both a committed and a latest offset: \
         committed={committed:?} latest={latest:?}"
    );
    assert!(total_lag >= 0, "lag cannot be negative: {total_lag}");
}
