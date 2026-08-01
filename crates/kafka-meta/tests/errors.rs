//! M5 acceptance: the error taxonomy.
//!
//! `cargo test -p kafka-meta --test errors` — no Docker for the table-driven
//! half; the integration case at the bottom is `#[ignore]`d as usual.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::HashSet;

use kafka_meta::{Cluster, ClusterConfig, ErrorCode, KNOWN_ERROR_CODES};

#[test]
fn every_known_code_round_trips_through_its_wire_value() {
    for code in KNOWN_ERROR_CODES {
        let wire = code.code();
        assert_ne!(wire, 0, "{code} claims the success code");
        assert_eq!(
            ErrorCode::from_code(wire),
            Some(code),
            "{code} does not round-trip"
        );
        assert!(code.name().is_some(), "{code} has no name");
        assert!(code.description().is_some(), "{code} has no description");
    }
}

#[test]
fn wire_values_are_unique() {
    let mut seen = HashSet::new();
    for code in KNOWN_ERROR_CODES {
        assert!(seen.insert(code.code()), "duplicate wire value for {code}");
    }
}

#[test]
fn zero_is_success_and_has_no_error_code() {
    assert_eq!(ErrorCode::from_code(0), None);
}

/// The whole point of `Unknown(i16)`. `kafka-protocol` 0.17 knows codes through
/// Kafka 4.1 and the acceptance suite runs against 4.3.1, so a code with no
/// name here is the *expected* case, not a corrupt response.
#[test]
fn a_code_no_kafka_release_defines_lands_in_unknown_and_still_renders() {
    let code = ErrorCode::from_code(30_000).expect("non-zero is always an error");
    assert_eq!(code, ErrorCode::Unknown(30_000));
    assert_eq!(code.code(), 30_000);
    assert_eq!(code.name(), None);
    assert_eq!(code.to_string(), "UNKNOWN(30000)");

    // And it is classified conservatively rather than panicking or guessing.
    assert!(!code.retriable());
    assert!(!code.needs_metadata_refresh());
    assert!(!code.needs_coordinator_refresh());
    assert!(!code.is_authentication());
    assert!(!code.is_authorization());
}

#[test]
fn negative_codes_are_errors_too() {
    // -1 is UNKNOWN_SERVER_ERROR; a signed field with a negative valid value is
    // easy to fumble into "not an error".
    assert_eq!(
        ErrorCode::from_code(-1),
        Some(ErrorCode::UnknownServerError)
    );
    assert_eq!(
        ErrorCode::from_code(-30_000),
        Some(ErrorCode::Unknown(-30_000))
    );
}

#[test]
fn the_three_axes_are_independent() {
    // Retriable but not a cache problem.
    assert!(ErrorCode::RequestTimedOut.retriable());
    assert!(!ErrorCode::RequestTimedOut.needs_metadata_refresh());
    assert!(!ErrorCode::RequestTimedOut.needs_coordinator_refresh());

    // Retriable *and* the metadata is stale — retrying against the same stale
    // leader is the infinite loop this axis exists to prevent.
    assert!(ErrorCode::NotLeaderOrFollower.retriable());
    assert!(ErrorCode::NotLeaderOrFollower.needs_metadata_refresh());
    assert!(!ErrorCode::NotLeaderOrFollower.needs_coordinator_refresh());

    // Coordinator moved; partition leadership says nothing about it.
    assert!(ErrorCode::NotCoordinator.needs_coordinator_refresh());
    assert!(!ErrorCode::NotCoordinator.needs_metadata_refresh());

    // Neither axis, and not retriable.
    assert!(!ErrorCode::InvalidTopicException.retriable());
    assert!(!ErrorCode::InvalidTopicException.needs_metadata_refresh());
    assert!(!ErrorCode::InvalidTopicException.needs_coordinator_refresh());
}

#[test]
fn retriability_comes_from_the_protocol_not_from_us() {
    // A sample across the range, all matching Errors.java. If this table were
    // hand-transcribed instead of delegated, this is where the drift would
    // show up.
    for (code, retriable) in [
        (ErrorCode::UnknownServerError, false),
        (ErrorCode::CorruptMessage, true),
        (ErrorCode::UnknownTopicOrPartition, true),
        (ErrorCode::OffsetOutOfRange, false),
        (ErrorCode::NotController, true),
        (ErrorCode::KafkaStorageError, true),
        (ErrorCode::PolicyViolation, false),
        (ErrorCode::ThrottlingQuotaExceeded, true),
        (ErrorCode::UnsupportedVersion, false),
    ] {
        assert_eq!(code.retriable(), retriable, "{code}");
    }
}

#[test]
fn a_named_resource_that_does_not_exist_is_not_worth_retrying() {
    for code in [
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::UnknownTopicId,
        ErrorCode::GroupIdNotFound,
        ErrorCode::TransactionalIdNotFound,
    ] {
        assert!(!code.retriable_for_named_resource(), "{code}");
    }
    // ... while a genuinely transient failure still is.
    assert!(ErrorCode::RequestTimedOut.retriable_for_named_resource());
    assert!(ErrorCode::NotLeaderOrFollower.retriable_for_named_resource());
    // ... and something the protocol calls terminal stays terminal.
    assert!(!ErrorCode::InvalidTopicException.retriable_for_named_resource());
}

#[test]
fn authentication_and_authorization_are_distinguishable() {
    for code in [
        ErrorCode::SaslAuthenticationFailed,
        ErrorCode::UnsupportedSaslMechanism,
        ErrorCode::IllegalSaslState,
    ] {
        assert!(code.is_authentication(), "{code}");
        assert!(!code.is_authorization(), "{code}");
    }
    for code in [
        ErrorCode::TopicAuthorizationFailed,
        ErrorCode::GroupAuthorizationFailed,
        ErrorCode::ClusterAuthorizationFailed,
        ErrorCode::TransactionalIdAuthorizationFailed,
        ErrorCode::DelegationTokenAuthorizationFailed,
    ] {
        assert!(code.is_authorization(), "{code}");
        assert!(!code.is_authentication(), "{code}");
        // Never retriable: retrying an authorization failure is a way to get
        // an account locked out, not a way to succeed.
        assert!(!code.retriable(), "{code}");
    }
}

#[test]
fn every_known_code_renders_with_its_number() {
    for code in KNOWN_ERROR_CODES {
        let rendered = code.to_string();
        assert!(
            rendered.contains(&code.code().to_string()),
            "{rendered} does not show its wire value"
        );
    }
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn describing_a_nonexistent_topic_is_a_non_retriable_unknown_topic() {
    use kafka_conn::protocol::messages::MetadataRequest;
    use kafka_conn::protocol::messages::metadata_request::MetadataRequestTopic;
    use kafka_conn::protocol::{StrBytes, messages::TopicName};
    use testkit::Cluster as _;

    let broker = testkit::single_broker().await.unwrap();
    let cluster = Cluster::connect(broker.bootstrap().to_vec(), ClusterConfig::default())
        .await
        .unwrap();

    let response = cluster
        .send_any(
            MetadataRequest::default()
                .with_allow_auto_topic_creation(false)
                .with_topics(Some(vec![MetadataRequestTopic::default().with_name(Some(
                    TopicName(StrBytes::from_static_str("definitely-not-a-topic")),
                ))])),
        )
        .await
        .expect("the request itself succeeds — the *topic* is what is missing");

    let topic = response.topics.first().expect("one topic entry");
    let code = ErrorCode::from_code(topic.error_code).expect("an error code");
    assert_eq!(code, ErrorCode::UnknownTopicOrPartition);

    // Both halves of the disagreement PLAN.md's M5 acceptance surfaces. The
    // protocol calls this code retriable — for a topic mid-creation it is —
    // and the table reports that faithfully because it is derived rather than
    // transcribed. For a topic the caller *named* and the broker has never
    // heard of, retrying is a spinner, and that is the axis a UI asks about.
    assert!(code.retriable(), "the protocol's answer");
    assert!(
        !code.retriable_for_named_resource(),
        "a named topic that does not exist will not start existing"
    );
    assert!(code.needs_metadata_refresh());
}
