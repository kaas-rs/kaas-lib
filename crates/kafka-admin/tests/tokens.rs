//! Delegation tokens, KIP-48: issue one, describe it, renew it, use it.
//!
//! `cargo test -p kafka-admin --test tokens -- --ignored`
//!
//! Every case here needs a broker with `delegation.token.secret.key` set — the
//! feature is off without one — and a SASL listener, because Kafka refuses
//! token requests from an unauthenticated channel. That second rule is the one
//! worth knowing: on a PLAINTEXT fixture these calls come back
//! `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`, which reads like a permissions
//! problem and is really a listener one.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::time::{Duration, Instant};

use kafka_admin::{
    Admin, ClusterConfig, DelegationToken, NewDelegationToken, Principal, ScramMechanism,
};
use kafka_conn::{
    Connection, ConnectionConfig, Error, ErrorCode, SaslConfig, SaslMechanism, ScramHash,
};
use testkit::{BrokerConfig, Cluster, Security};

/// How long a created token may take to reach the broker serving describes.
const TOKEN_TIMEOUT: Duration = Duration::from_secs(30);

/// The master key the broker signs tokens with. Any non-empty value does; it
/// has to be identical across brokers, which a single-node fixture makes moot.
const SECRET: &str = "kaas-lib-delegation-token-secret";

fn broker_config() -> BrokerConfig {
    BrokerConfig::new()
        .with_security(Security::SaslPlaintext)
        .with_mechanism(testkit::SaslMechanism::ScramSha256)
        .with_user("alice", "alice-pw")
        .with_property("delegation.token.secret.key", SECRET)
        // Well past the length of any test, so an expiry seen here is one the
        // test asked for rather than the broker's default arriving early.
        .with_property("delegation.token.max.lifetime.ms", "86400000")
        .with_property("delegation.token.expiry.time.ms", "3600000")
}

fn alice() -> ConnectionConfig {
    ConnectionConfig::new().with_sasl(SaslConfig::new(
        SaslMechanism::ScramSha256,
        "alice",
        "alice-pw",
    ))
}

/// Wait until a created token is visible, and return the broker's own record
/// of it.
///
/// `CreateDelegationToken` is acked once the controller commits the record;
/// the broker answers describes — and SCRAM logins — from the
/// `DelegationTokenCache` it fills by applying that record, which in KRaft
/// trails the log asynchronously. So a describe or a login immediately after
/// the create is a race that an idle machine wins and a loaded CI runner
/// loses, and it loses in the most confusing way available: the login failure
/// is `invalid credentials`, which reads as a wrong password rather than as a
/// token the broker has not heard of yet.
///
/// Both come from the same cache, so waiting for the token to describe is also
/// waiting for it to be usable as a credential.
async fn await_token_visible(admin: &Admin, token_id: &str) -> DelegationToken {
    let deadline = Instant::now() + TOKEN_TIMEOUT;
    loop {
        let described = admin.describe_delegation_tokens(None).await.unwrap();
        if let Some(found) = described
            .iter()
            .find(|candidate| candidate.token_id == token_id)
        {
            return found.clone();
        }
        assert!(
            Instant::now() < deadline,
            "token {token_id} never became visible within {TOKEN_TIMEOUT:?}; \
             the broker lists {:?}",
            described
                .iter()
                .map(|token| &token.token_id)
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[testkit::integration_test]
async fn a_delegation_token_is_created_described_renewed_and_expired() {
    let broker = testkit::single_broker_with(broker_config()).await.unwrap();
    let admin = Admin::connect(
        broker.bootstrap().to_vec(),
        ClusterConfig {
            connection: alice(),
            ..ClusterConfig::default()
        },
    )
    .await
    .unwrap();

    let token = admin
        .create_delegation_token(
            &NewDelegationToken::new()
                .with_renewer(Principal::user("bob"))
                // Four hours, against the fixture's one-hour expiry window.
                // The ceiling has to exceed that window or the token is born
                // with its expiry already *at* its maximum, and every renewal
                // clamps to the same instant — a token that cannot be renewed
                // from the moment it exists.
                .with_max_lifetime(Duration::from_secs(4 * 3600)),
        )
        .await
        .expect("a token for the authenticated principal");

    assert_eq!(token.owner, Principal::user("alice"));
    assert_eq!(token.requester, Principal::user("alice"));
    assert!(!token.token_id.is_empty());
    assert!(!token.hmac.is_empty(), "the HMAC is the credential");
    assert!(
        token.expiry_timestamp_ms <= token.max_timestamp_ms,
        "{token:?}"
    );

    // Describing it back is the only way to see what the broker actually
    // recorded — the create response does not echo the renewer list.
    let found = await_token_visible(&admin, &token.token_id).await;
    assert_eq!(found.owner, Principal::user("alice"));
    assert_eq!(found.renewers, vec![Principal::user("bob")]);
    assert_eq!(found.hmac, token.hmac, "the owner may see its own HMAC");

    // Renewing sets the expiry to `now + period` rather than adding to the one
    // it has, so the period must exceed the life the token has left — the
    // fixture's `delegation.token.expiry.time.ms` is an hour — for this to be
    // an extension at all. Two hours is inside the four-hour ceiling asked for
    // above, so the clamp is not what is under test here.
    let renewed = admin
        .renew_delegation_token(token.hmac.clone(), Duration::from_secs(7200))
        .await
        .expect("the owner may renew");
    assert!(
        renewed > token.expiry_timestamp_ms,
        "renewing for longer than the remaining life did not extend it: \
         {renewed} <= {}",
        token.expiry_timestamp_ms
    );
    assert!(
        renewed <= token.max_timestamp_ms,
        "renewed past the ceiling: {renewed} > {}",
        token.max_timestamp_ms
    );

    // And the direction that surprises people: a period shorter than the
    // remaining life brings the expiry in. This is the assertion that would
    // have caught the wrong mental model in the doc comment.
    let shortened = admin
        .renew_delegation_token(token.hmac.clone(), Duration::from_secs(600))
        .await
        .expect("the owner may renew");
    assert!(
        shortened < renewed,
        "a shorter renewal period should move the expiry in: {shortened} >= {renewed}"
    );

    // A negative period is the revocation path.
    admin
        .expire_delegation_token(token.hmac.clone(), -1)
        .await
        .expect("the owner may expire");

    let after = admin.describe_delegation_tokens(None).await.unwrap();
    assert!(
        !after
            .iter()
            .any(|candidate| candidate.token_id == token.token_id),
        "an expired token is still listed: {after:?}"
    );
}

/// The other half of KIP-48, and the reason the SCRAM extension work exists: a
/// token id and its HMAC authenticate as a SCRAM credential *only* when
/// `tokenauth=true` rides along in client-first. Without the extension this
/// same exchange is a login attempt for a user named after a token id.
#[testkit::integration_test]
async fn a_delegation_token_authenticates_a_connection() {
    let broker = testkit::single_broker_with(broker_config()).await.unwrap();
    let admin = Admin::connect(
        broker.bootstrap().to_vec(),
        ClusterConfig {
            connection: alice(),
            ..ClusterConfig::default()
        },
    )
    .await
    .unwrap();

    let token = admin
        .create_delegation_token(&NewDelegationToken::new())
        .await
        .unwrap();
    await_token_visible(&admin, &token.token_id).await;

    let config = ConnectionConfig::new().with_sasl(SaslConfig::delegation_token(
        ScramHash::Sha256,
        token.token_id.clone(),
        token.password(),
    ));
    let connection = Connection::connect(&broker.bootstrap()[0], config)
        .await
        .expect("the token authenticates");
    connection
        .send(
            kafka_conn::protocol::messages::MetadataRequest::default()
                .with_topics(Some(vec![]))
                .with_allow_auto_topic_creation(false),
        )
        .await
        .expect("and the connection works afterwards");

    // A token cannot beget a token: the broker refuses to issue one to a
    // principal that authenticated with one, which is what stops a leaked
    // token renewing itself into a new identity indefinitely.
    let token_admin = Admin::connect(
        broker.bootstrap().to_vec(),
        ClusterConfig {
            connection: ConnectionConfig::new().with_sasl(SaslConfig::delegation_token(
                ScramHash::Sha256,
                token.token_id.clone(),
                token.password(),
            )),
            ..ClusterConfig::default()
        },
    )
    .await
    .unwrap();
    let err = token_admin
        .create_delegation_token(&NewDelegationToken::new())
        .await
        .expect_err("a token-authenticated principal cannot create tokens");
    assert_eq!(
        err.code(),
        Some(ErrorCode::DelegationTokenRequestNotAllowed),
        "{err:?}"
    );
}

/// The credential is wrong, not the mechanism. It must fail as an
/// authentication error rather than hanging until the connect deadline, which
/// is what a UI renders as "the cluster is unreachable".
#[testkit::integration_test]
async fn a_forged_token_hmac_is_rejected() {
    let broker = testkit::single_broker_with(broker_config()).await.unwrap();
    let admin = Admin::connect(
        broker.bootstrap().to_vec(),
        ClusterConfig {
            connection: alice(),
            ..ClusterConfig::default()
        },
    )
    .await
    .unwrap();

    let token = admin
        .create_delegation_token(&NewDelegationToken::new())
        .await
        .unwrap();
    // Without the wait this passes for the wrong reason: a token the broker
    // has not applied yet is refused whatever HMAC is presented, so the case
    // would prove nothing about the forgery.
    await_token_visible(&admin, &token.token_id).await;

    let config = ConnectionConfig::new()
        .with_connect_timeout(Duration::from_secs(20))
        .with_sasl(SaslConfig::delegation_token(
            ScramHash::Sha256,
            token.token_id.clone(),
            "bm90LXRoZS1obWFj",
        ));
    let err = Connection::connect(&broker.bootstrap()[0], config)
        .await
        .expect_err("a forged HMAC cannot authenticate");
    assert!(matches!(err, Error::Authentication(_)), "{err:?}");
}

/// Without the master key the feature is off, and the broker says so with a
/// code the error table already names. Worth asserting because it is the first
/// thing anyone will hit on a cluster that has not enabled tokens, and the
/// remedy is broker configuration rather than anything a caller can fix.
#[testkit::integration_test]
async fn tokens_are_refused_when_the_broker_has_no_master_key() {
    let broker = testkit::single_broker_with(
        BrokerConfig::new()
            .with_security(Security::SaslPlaintext)
            .with_mechanism(testkit::SaslMechanism::ScramSha256)
            .with_user("alice", "alice-pw"),
    )
    .await
    .unwrap();
    let admin = Admin::connect(
        broker.bootstrap().to_vec(),
        ClusterConfig {
            connection: alice(),
            ..ClusterConfig::default()
        },
    )
    .await
    .unwrap();

    let err = admin
        .create_delegation_token(&NewDelegationToken::new())
        .await
        .expect_err("no master key, no tokens");
    assert_eq!(
        err.code(),
        Some(ErrorCode::DelegationTokenAuthDisabled),
        "{err:?}"
    );
}

/// A token is a credential that authenticates *as* its owner, so an audit of
/// "who can be alice" that stops at her password is incomplete by one live
/// SCRAM login per outstanding token. `describe_principal` reports both, and
/// this is the half that needs a master key.
#[testkit::integration_test]
async fn describe_principal_reports_a_principals_delegation_tokens() {
    let broker = testkit::single_broker_with(broker_config()).await.unwrap();
    let admin = Admin::connect(
        broker.bootstrap().to_vec(),
        ClusterConfig {
            connection: alice(),
            ..ClusterConfig::default()
        },
    )
    .await
    .unwrap();

    let token = admin
        .create_delegation_token(&NewDelegationToken::new())
        .await
        .expect("a token for the authenticated principal");
    await_token_visible(&admin, &token.token_id).await;

    let alice = Principal::user("alice");
    let described = admin.describe_principal(&alice).await.unwrap();

    let tokens = described.tokens.as_ref().expect("tokens are readable");
    let found = tokens
        .iter()
        .find(|candidate| candidate.token_id == token.token_id)
        .unwrap_or_else(|| panic!("the created token is missing from {tokens:?}"));
    assert_eq!(found.requester, alice);
    assert_eq!(found.expiry_timestamp_ms, token.expiry_timestamp_ms);

    // The fixture authenticates alice with SCRAM-SHA-256, so both credential
    // stores answer for her — and a principal with a token is not
    // credential-less even before the SCRAM entry is counted.
    assert!(described.has_stored_credentials());
    assert!(!described.is_unrecorded());
    assert_eq!(described.scram_mechanisms(), vec![ScramMechanism::Sha256]);
}
