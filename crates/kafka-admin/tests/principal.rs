//! What a cluster records about one principal.
//!
//! `cargo test -p kafka-admin --test principal -- --ignored`
//!
//! The delegation-token half of `describe_principal` needs a broker with
//! `delegation.token.secret.key` and lives in `tokens.rs` alongside the rest of
//! the token fixture. Everything here runs against a SASL/SCRAM broker with an
//! authorizer, because those are the two stores a principal can actually be
//! recorded in.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use kafka_admin::{
    AclBinding, AclOperation, AclResourceType, Admin, AuthMechanism, ClusterConfig, ListenerAuth,
    Principal, ScramMechanism, SecurityProtocol, VerdictBasis,
};
use testkit::{BrokerConfig, Cluster as _};

fn broker_config() -> BrokerConfig {
    BrokerConfig::new()
        .with_security(testkit::Security::SaslPlaintext)
        .with_mechanism(testkit::SaslMechanism::ScramSha512)
        .with_user("alice", "alice-pw")
        .with_authorizer(true)
        // A **bare** name: the setter adds the `User:` prefix itself, and
        // passing `User:alice` here produces `User:User:alice`, which matches
        // nothing and denies everything.
        .with_super_user("alice")
}

fn as_alice() -> ClusterConfig {
    let mut config = ClusterConfig::default();
    config.connection = config.connection.with_sasl(kafka_conn::SaslConfig::new(
        kafka_conn::SaslMechanism::ScramSha512,
        "alice",
        "alice-pw",
    ));
    config
}

#[testkit::integration_test]
async fn a_scram_principal_describes_its_credential_and_its_acls() {
    let fixture = testkit::single_broker_with(broker_config()).await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), as_alice())
        .await
        .unwrap();

    let alice = Principal::user("alice");
    let described = admin.describe_principal(&alice).await.unwrap();

    // The credential the broker was formatted with. This is the one source
    // that positively answers "how does this principal log in".
    let credentials = described.scram.as_ref().expect("credentials are readable");
    assert_eq!(
        described.scram_mechanisms(),
        vec![ScramMechanism::Sha512],
        "{credentials:?}"
    );
    assert!(credentials[0].iterations >= 4096, "{credentials:?}");
    assert!(described.has_stored_credentials());
    assert!(!described.is_unrecorded());

    // Quotas are a source, not an assertion: propagation to the broker serving
    // describes is slow enough to have its own timeout in `security.rs`, and
    // what matters here is that the call is wired and permitted.
    assert!(described.quotas.is_ok(), "{:?}", described.quotas);

    // Nothing has been granted to alice explicitly — she is a super user,
    // which is a broker property and not an ACL.
    assert!(
        described
            .acls
            .as_ref()
            .expect("acls are readable")
            .is_empty(),
        "{:?}",
        described.acls
    );

    let binding = AclBinding::allow(
        AclResourceType::Topic,
        "principal-acl-topic",
        "User:alice",
        AclOperation::Read,
    );
    let created = admin.create_acls([binding.clone()]).await.unwrap();
    assert!(created[0].1.is_ok(), "{created:?}");

    let described = admin.describe_principal(&alice).await.unwrap();
    assert_eq!(
        described.acls.as_ref().expect("acls are readable"),
        &vec![binding]
    );
}

/// The whole chain, and the reason the listener half exists: what the cluster
/// stores about a principal, crossed with what its listeners accept, naming
/// one mechanism.
#[testkit::integration_test]
async fn a_principals_mechanism_is_named_by_crossing_credentials_with_listeners() {
    let fixture = testkit::single_broker_with(broker_config()).await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), as_alice())
        .await
        .unwrap();

    let listeners = admin.describe_authentication().await.unwrap();

    // The controller and inter-broker listeners are both PLAINTEXT here, so a
    // reader that counted them would report an open cluster.
    let client: Vec<&ListenerAuth> = listeners.client_listeners().collect();
    assert_eq!(client.len(), 1, "{:?}", listeners.listeners);
    assert_eq!(client[0].protocol, SecurityProtocol::SaslPlaintext);
    assert_eq!(
        listeners.client_mechanisms(),
        vec![AuthMechanism::Scram(ScramMechanism::Sha512)]
    );

    let alice = admin
        .describe_principal(&Principal::user("alice"))
        .await
        .unwrap();
    let verdict = alice.likely_mechanism(&listeners);
    assert!(verdict.is_conclusive(), "{verdict:?}");
    assert_eq!(verdict.basis, VerdictBasis::StoredCredential);
    assert_eq!(verdict.to_string(), "SCRAM-SHA-512 (stored credential)");

    // And the elimination that the principal half alone could not reach: this
    // cluster offers only SCRAM, and the cluster stores no credential for
    // nobody-at-all, so there is no way in at all.
    let nobody = admin
        .describe_principal(&Principal::user("nobody-at-all"))
        .await
        .unwrap();
    let verdict = nobody.likely_mechanism(&listeners);
    assert_eq!(verdict.basis, VerdictBasis::Elimination, "{verdict:?}");
    assert!(verdict.candidates.is_empty(), "{verdict:?}");
}

#[testkit::integration_test]
async fn a_principal_the_cluster_never_heard_of_describes_as_unrecorded() {
    let fixture = testkit::single_broker_with(broker_config()).await.unwrap();
    let admin = Admin::connect(fixture.bootstrap().to_vec(), as_alice())
        .await
        .unwrap();

    let described = admin
        .describe_principal(&Principal::user("nobody-at-all"))
        .await
        .unwrap();

    // No SCRAM credential is an *answer* — `RESOURCE_NOT_FOUND` normalised —
    // and not an error, which is what keeps it distinguishable from a
    // credential store the caller may not read.
    assert_eq!(
        described.scram.as_ref().expect("credentials are readable"),
        &Vec::new()
    );
    assert!(!described.has_stored_credentials());

    // True here even though this fixture has no token master key: that error
    // is a statement about the cluster, not about this principal. The claim it
    // makes is narrow — the cluster records nothing — and a mutual-TLS or
    // OAUTHBEARER principal would describe exactly the same way while being
    // perfectly able to connect.
    assert!(
        described.is_unrecorded(),
        "scram={:?} tokens={:?} acls={:?} quotas={:?}",
        described.scram,
        described.tokens,
        described.acls,
        described.quotas
    );
}
