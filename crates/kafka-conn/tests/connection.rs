//! M2 acceptance: the connection actor.
//!
//! `cargo test -p kafka-conn --test connection -- --ignored`
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::time::{Duration, Instant};

use kafka_conn::protocol::messages::MetadataRequest;
use kafka_conn::{Connection, ConnectionConfig, Error};
use testkit::Cluster;

fn metadata_request() -> MetadataRequest {
    // The trap from CLAUDE.md: the schema default for this field is `true`, and
    // the crate honours it. Every metadata request in this workspace turns it
    // off; here it matters because these tests ask for a topic that does not
    // exist, on purpose.
    MetadataRequest::default()
        .with_topics(Some(vec![]))
        .with_allow_auto_topic_creation(false)
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_hundred_concurrent_requests_all_resolve_on_one_connection() {
    let broker = testkit::single_broker().await.unwrap();
    let conn = Connection::connect(&broker.bootstrap()[0], ConnectionConfig::new())
        .await
        .unwrap();

    let mut tasks = Vec::with_capacity(100);
    for _ in 0..100 {
        let conn = conn.clone();
        tasks.push(tokio::spawn(
            async move { conn.send(metadata_request()).await },
        ));
    }

    let mut brokers_seen = None;
    for task in tasks {
        let response = task.await.unwrap().expect("request resolves");
        // Every response must be a *correct* response, not merely a response:
        // a correlation bug shows up as bodies landing on the wrong caller,
        // which only a content check catches.
        assert!(!response.brokers.is_empty());
        let count = response.brokers.len();
        assert_eq!(*brokers_seen.get_or_insert(count), count);
    }

    let stats = conn.stats_snapshot();
    // 100 requests plus the ApiVersions handshake.
    assert_eq!(stats.requests_sent, 101, "{stats:?}");
    assert_eq!(stats.responses_received, 101, "{stats:?}");
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn killing_the_broker_resolves_every_pending_future() {
    let broker = testkit::single_broker().await.unwrap();
    let conn = Connection::connect(&broker.bootstrap()[0], ConnectionConfig::new())
        .await
        .unwrap();

    let mut tasks = Vec::new();
    for _ in 0..20 {
        let conn = conn.clone();
        tasks.push(tokio::spawn(
            async move { conn.send(metadata_request()).await },
        ));
    }

    broker.stop_node(0).await.unwrap();

    let started = Instant::now();
    let mut errors = 0;
    for task in tasks {
        // The property under test is that this *returns*. A client that hangs
        // one future per dead broker looks like it is working while doing
        // nothing at all.
        let outcome = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("no pending future may outlive the connection by 5s")
            .unwrap();
        if let Err(error) = outcome {
            assert!(error.retriable(), "{error:?}");
            errors += 1;
        }
    }
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(
        errors > 0,
        "expected the kill to strand at least one request"
    );

    // And the connection stays dead, rather than hanging the next caller.
    let after = conn.send(metadata_request()).await;
    assert!(matches!(
        after,
        Err(Error::ConnectionClosed { .. } | Error::Transport { .. })
    ));
    assert!(conn.is_closed());
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_deadline_is_the_callers_to_set() {
    let broker = testkit::single_broker().await.unwrap();
    let conn = Connection::connect(&broker.bootstrap()[0], ConnectionConfig::new())
        .await
        .unwrap();

    // A deadline already in the past must fail as a timeout rather than
    // silently waiting for the default.
    let deadline = Instant::now() - Duration::from_millis(1);
    let err = conn
        .send_until(metadata_request(), deadline)
        .await
        .expect_err("an expired deadline cannot succeed");
    assert!(matches!(err, Error::Timeout { .. }), "{err:?}");

    // ... and the connection is still usable afterwards.
    conn.send(metadata_request())
        .await
        .expect("a timed-out request does not poison the socket");
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn dropping_a_request_future_leaves_the_connection_consistent() {
    // Rule 5. Cancelling mid-flight must not desynchronise the socket: the
    // response still arrives, is discarded, and the next request gets its own
    // answer rather than the abandoned one.
    let broker = testkit::single_broker().await.unwrap();
    let conn = Connection::connect(&broker.bootstrap()[0], ConnectionConfig::new())
        .await
        .unwrap();

    for _ in 0..25 {
        let mut fut = Box::pin(conn.send(metadata_request()));
        // Poll once so the request is genuinely written, then drop.
        let _ = tokio::time::timeout(Duration::from_micros(1), &mut fut).await;
        drop(fut);
    }

    for _ in 0..10 {
        let response = conn
            .send(metadata_request())
            .await
            .expect("connection survives cancellation");
        assert!(!response.brokers.is_empty());
    }
    assert!(!conn.is_closed());
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_read_only_client_refuses_before_opening_a_socket() {
    let broker = testkit::single_broker().await.unwrap();
    let conn = Connection::connect(&broker.bootstrap()[0], ConnectionConfig::new().read_only())
        .await
        .unwrap();

    let before = conn.stats_snapshot();
    let err = conn
        .send(
            kafka_conn::protocol::messages::CreateTopicsRequest::default()
                .with_topics(Default::default()),
        )
        .await
        .expect_err("read-only must refuse CreateTopics");
    assert!(matches!(err, Error::ReadOnly { .. }), "{err:?}");
    assert_eq!(
        conn.stats_snapshot().bytes_sent,
        before.bytes_sent,
        "a refused request must not reach the wire"
    );

    // Reads still work.
    conn.send(metadata_request()).await.unwrap();
}
