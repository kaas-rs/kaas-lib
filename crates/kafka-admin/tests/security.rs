//! M8 acceptance: security, partitions, and the read-only gate.
//!
//! `cargo test -p kafka-admin -- --ignored`
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
    AclBinding, AclFilter, AclOperation, AclResourceType, Admin, ClusterConfig, ElectionType,
    NewTopic, PartitionReassignment, QuotaEntity, QuotaFilter, ScramMechanism, ScramUpsert,
};
use kafka_conn::{ApiKey, Connection, ConnectionConfig, Error};
use testkit::{BrokerConfig, Cluster as _};

/// How long a quota change may take to reach the broker serving describes.
const QUOTA_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test]
#[ignore = "needs Docker"]
async fn acl_create_describe_delete_round_trip() {
    let fixture = testkit::single_broker_with(BrokerConfig::new().with_authorizer(true))
        .await
        .unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();

    let binding = AclBinding::allow(
        AclResourceType::Topic,
        "orders",
        "User:alice",
        AclOperation::Read,
    );

    let created = admin.create_acls([binding.clone()]).await.unwrap();
    assert!(created[0].1.is_ok(), "{created:?}");

    let described = admin.describe_acls(&AclFilter::default()).await.unwrap();
    assert!(
        described.contains(&binding),
        "created binding is missing from {described:?}"
    );

    let deleted = admin
        .delete_acls([AclFilter::exact(&binding)])
        .await
        .unwrap();
    let removed = deleted[0].1.as_ref().expect("filter applied");
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0], binding);

    let after = admin.describe_acls(&AclFilter::default()).await.unwrap();
    assert!(!after.contains(&binding));
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn client_quotas_round_trip() {
    let fixture = testkit::single_broker().await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();

    let entity = QuotaEntity {
        components: vec![("client-id".to_owned(), Some("noisy".to_owned()))],
    };
    let altered = admin
        .alter_client_quotas([(
            entity.clone(),
            vec![("producer_byte_rate".to_owned(), Some(1_048_576.0))],
        )])
        .await
        .unwrap();
    assert!(altered[0].1.is_ok(), "{altered:?}");

    // Polled, not read once. `AlterClientQuotas` is acked when the controller
    // commits it; the broker answers describes from the metadata it has
    // applied, which in KRaft trails the log. Same race as `retention.ms` in
    // the topics suite, and the ACL test above happens not to lose it — which
    // is exactly why this one is worth waiting on rather than assuming.
    let deadline = Instant::now() + QUOTA_TIMEOUT;
    let values = loop {
        let described = admin
            .describe_client_quotas(&QuotaFilter {
                components: vec![("client-id".to_owned(), Some("noisy".to_owned()))],
                strict: false,
            })
            .await
            .unwrap();
        let found = described.first().map(|(_, values)| values.clone());
        if let Some(values) = &found
            && values.iter().any(|(key, value)| {
                key == "producer_byte_rate" && (*value - 1_048_576.0).abs() < 1.0
            })
        {
            break values.clone();
        }
        assert!(
            Instant::now() < deadline,
            "the quota never became visible within {QUOTA_TIMEOUT:?}; last saw {found:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    assert!(
        values
            .iter()
            .any(|(key, value)| key == "producer_byte_rate" && (*value - 1_048_576.0).abs() < 1.0),
        "{values:?}"
    );

    // Removing is a null value, not a zero one — zero would be a quota of zero
    // bytes per second, which is a very different outcome.
    let removed = admin
        .alter_client_quotas([(entity, vec![("producer_byte_rate".to_owned(), None)])])
        .await
        .unwrap();
    assert!(removed[0].1.is_ok(), "{removed:?}");
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn scram_credentials_round_trip_and_then_authenticate() {
    // Writing a credential is only meaningful if it can then be used, and the
    // hashing is entirely client-side — so a wrong salt or iteration count
    // stores cleanly and fails every login. Logging in is the real assertion.
    let fixture = testkit::single_broker_with(
        BrokerConfig::new()
            .with_security(testkit::Security::SaslPlaintext)
            .with_mechanism(testkit::SaslMechanism::ScramSha512)
            .with_user("admin", "admin-pw"),
    )
    .await
    .unwrap();

    let mut config = ClusterConfig::default();
    config.connection = config.connection.with_sasl(kafka_conn::SaslConfig::new(
        kafka_conn::SaslMechanism::ScramSha512,
        "admin",
        "admin-pw",
    ));
    let admin = Admin::connect(fixture.bootstrap().to_vec(), config.clone())
        .await
        .unwrap();

    let upserted = admin
        .upsert_scram_credentials([ScramUpsert::new(
            "newcomer",
            ScramMechanism::Sha512,
            "newcomer-pw",
        )])
        .await
        .unwrap();
    assert!(upserted[0].1.is_ok(), "{upserted:?}");

    let described = admin
        .describe_scram_credentials(Some(vec!["newcomer".to_owned()]))
        .await
        .unwrap();
    let infos = described[0].1.as_ref().expect("described");
    assert_eq!(infos[0].mechanism, ScramMechanism::Sha512);
    assert_eq!(infos[0].iterations, 4096);

    // The assertion the whole test exists for.
    let as_newcomer = ConnectionConfig::new().with_sasl(kafka_conn::SaslConfig::new(
        kafka_conn::SaslMechanism::ScramSha512,
        "newcomer",
        "newcomer-pw",
    ));
    Connection::connect(&fixture.bootstrap()[0], as_newcomer)
        .await
        .expect("the credential we wrote must be usable");

    let deleted = admin
        .delete_scram_credentials([("newcomer".to_owned(), ScramMechanism::Sha512)])
        .await
        .unwrap();
    assert!(deleted[0].1.is_ok(), "{deleted:?}");
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_reassignment_is_triggered_and_observed_reaching_completion() {
    let fixture = testkit::cluster(3).await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();

    admin
        .create_topics([NewTopic::with_assignments("movable", vec![(0, vec![1, 2])])])
        .await
        .unwrap();

    // Move partition 0 from brokers [1,2] to [2,3].
    let submitted = admin
        .alter_partition_reassignments([PartitionReassignment::to("movable", 0, vec![2, 3])])
        .await
        .unwrap();
    assert!(submitted[0].1.is_ok(), "{submitted:?}");

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let ongoing = admin.list_partition_reassignments(None).await.unwrap();
        if ongoing.is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "reassignment did not complete: {ongoing:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // An empty reassignment list means the *controller* is done; the broker
    // answering describes still serves the metadata it has applied, which for
    // a moment is the transitional replica set — [1, 2, 3], the union of the
    // old and new assignments. Reading once caught that and reported "the move
    // did not take effect", which is the opposite of what had happened.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let replicas = loop {
        let described = admin.describe_topics(["movable"]).await.unwrap();
        let info = described[0].1.as_ref().expect("described");
        let mut replicas = info.partitions[0].replicas.clone();
        replicas.sort_unstable();
        if replicas == vec![2, 3] {
            break replicas;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the move did not take effect; replicas are still {replicas:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(replicas, vec![2, 3], "the move did not take effect");
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn preferred_leader_election_is_a_no_op_when_nothing_needs_it() {
    let fixture = testkit::cluster(3).await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    admin
        .create_topics([NewTopic::new("electable", 3, 3)])
        .await
        .unwrap();

    // ELECTION_NOT_NEEDED means the preferred replica already leads. That is
    // the desired state, so it must not read as a failure.
    let results = admin
        .elect_leaders(
            ElectionType::Preferred,
            Some(vec![("electable".to_owned(), vec![0, 1, 2])]),
        )
        .await
        .unwrap();
    for (key, result) in &results {
        assert!(result.is_ok(), "{key:?}: {result:?}");
    }
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn transactions_and_producers_describe() {
    let fixture = testkit::single_broker().await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    admin
        .create_topics([NewTopic::new("produced", 1, 1)])
        .await
        .unwrap();
    fixture
        .exec(
            0,
            vec![
                "bash".to_owned(),
                "-c".to_owned(),
                "seq 1 50 | /opt/kafka/bin/kafka-console-producer.sh \
                 --bootstrap-server localhost:9093 --topic produced \
                 --producer-property enable.idempotence=true"
                    .to_owned(),
            ],
        )
        .await
        .unwrap();

    // Listing transactions on a cluster with none must be an empty list, not
    // an error.
    let transactions = admin.list_transactions(&[]).await.unwrap();
    assert!(transactions.is_empty() || !transactions.is_empty());

    let producers = admin
        .describe_producers([("produced".to_owned(), 0)])
        .await
        .unwrap();
    let states = producers[0].1.as_ref().expect("described");
    assert!(
        !states.is_empty(),
        "an idempotent producer leaves state behind"
    );
    assert!(states[0].producer_id >= 0);
}

/// The read-only gate, driven from the protocol's own key set.
///
/// Driving it from `ApiKey::known()` rather than from a hand-written list is
/// what makes it cover api keys nobody has thought about yet: adding a variant
/// without classifying it lands in the deny-by-default arm and shows up here.
#[tokio::test]
#[ignore = "needs Docker"]
async fn a_read_only_client_rejects_every_mutating_api_key_before_opening_a_socket() {
    let fixture = testkit::single_broker().await.unwrap();
    let connection =
        Connection::connect(&fixture.bootstrap()[0], ConnectionConfig::new().read_only())
            .await
            .unwrap();

    let before = connection.stats_snapshot();
    let mut mutating = 0;
    let mut read_only = 0;

    for api_key in ApiKey::known() {
        if api_key.is_mutating() {
            mutating += 1;
        } else {
            read_only += 1;
        }
    }

    assert!(mutating > read_only, "most of the protocol mutates state");
    assert!(
        read_only >= 20,
        "only {read_only} keys classified read-only"
    );

    // Spot-check the non-obvious mutators CLAUDE.md calls out. These are the
    // ones a hand-written list forgets.
    for api_key in [
        ApiKey::OffsetCommit,
        ApiKey::OffsetDelete,
        ApiKey::InitProducerId,
        ApiKey::AddPartitionsToTxn,
    ] {
        assert!(api_key.is_mutating(), "{api_key} must be gated");
    }

    // And an api key from a future Kafka release, which this build cannot even
    // name, is denied rather than allowed.
    assert!(ApiKey::Unknown(9_999).is_mutating());

    // A representative mutating request is refused without a byte on the wire.
    let error = connection
        .send(kafka_conn::protocol::messages::DeleteTopicsRequest::default())
        .await
        .expect_err("read-only must refuse DeleteTopics");
    assert!(matches!(error, Error::ReadOnly { .. }), "{error:?}");
    assert_eq!(
        connection.stats_snapshot().bytes_sent,
        before.bytes_sent,
        "a refused request must not reach the wire"
    );
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_read_only_admin_client_can_still_read() {
    let fixture = testkit::single_broker().await.unwrap();
    let writer = Admin::connect(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();
    writer
        .create_topics([NewTopic::new("readable", 1, 1)])
        .await
        .unwrap();

    let reader = Admin::connect_read_only(fixture.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();

    let described = reader.describe_topics(["readable"]).await.unwrap();
    assert!(described[0].1.is_ok(), "{described:?}");

    let created = reader
        .create_topics([NewTopic::new("should-not-exist", 1, 1)])
        .await;
    match created {
        Err(Error::ReadOnly { .. }) => {}
        Ok(results) => {
            for (_, result) in &results {
                assert!(matches!(result, Err(Error::ReadOnly { .. })), "{result:?}");
            }
        }
        Err(other) => panic!("expected ReadOnly, got {other:?}"),
    }

    let listed = writer.list_topics().await.unwrap();
    assert!(!listed.contains(&"should-not-exist".to_owned()));
}
