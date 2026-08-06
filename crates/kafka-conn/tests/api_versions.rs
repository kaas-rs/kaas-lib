//! M1 acceptance: one round trip.
//!
//! `cargo test -p kafka-conn --test api_versions -- --ignored --nocapture`
//!
//! This is the milestone that validates framing and header versions — the two
//! things most likely to be subtly wrong — so it prints the whole negotiated
//! table rather than just asserting on it. A failure here is almost always a
//! header version, and seeing the table makes that obvious.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use kafka_conn::{ApiKey, Connection, ConnectionConfig};
use testkit::Cluster;

#[testkit::integration_test]
async fn api_versions_round_trip_and_negotiation() {
    let broker = testkit::single_broker().await.unwrap();
    let addr = broker.bootstrap()[0].clone();

    let conn = Connection::connect(&addr, ConnectionConfig::new())
        .await
        .expect("handshake completes");

    println!(
        "{:<32} {:>4} {:>9} {:>9} {:>10}",
        "api", "key", "broker", "ours", "negotiated"
    );
    let mut broker_ahead = Vec::new();
    let mut unnameable = Vec::new();

    for entry in conn.versions().entries() {
        let ours = entry
            .ours
            .map(|r| format!("{}..{}", r.min, r.max))
            .unwrap_or_else(|| "-".to_owned());
        println!(
            "{:<32} {:>4} {:>9} {:>9} {:>10}",
            entry.api_key.name(),
            entry.api_key.code(),
            format!("{}..{}", entry.broker.min, entry.broker.max),
            ours,
            entry
                .negotiated()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_owned()),
        );
        if entry.broker_ahead() {
            broker_ahead.push(entry.api_key);
        }
        if entry.ours.is_none() {
            unnameable.push(entry.api_key);
        }
    }

    assert!(
        conn.versions().supports(ApiKey::Metadata),
        "a broker that does not offer Metadata is not a Kafka broker"
    );

    // The clamp has to be ours, not theirs. `kafka-protocol` 0.17 ships Kafka
    // 4.0 schemas and we test against 4.3.1, so there must be at least one key
    // where the broker offers more than we can encode — if there is not, we are
    // either not talking to the broker we think we are, or the negotiation is
    // silently taking the broker's number.
    assert!(
        !broker_ahead.is_empty(),
        "expected at least one api key where the broker outruns our schemas"
    );
    println!(
        "\nbroker ahead of our schemas on {} keys:",
        broker_ahead.len()
    );
    for key in &broker_ahead {
        let entry = conn.versions().get(*key).unwrap();
        println!(
            "  {} broker max {} > our max {}",
            key,
            entry.broker.max,
            entry.ours.unwrap().max
        );
    }

    // Keys with no schema in this build must survive as Unknown rather than
    // being dropped — that is how a streams group ends up merely undescribable
    // instead of taking down the group list.
    println!("\napi keys this build cannot name: {unnameable:?}");
    for key in &unnameable {
        assert!(matches!(key, ApiKey::Unknown(_)));
    }
}

#[testkit::integration_test]
async fn negotiation_picks_the_lower_of_the_two_ceilings() {
    let broker = testkit::single_broker().await.unwrap();
    let conn = Connection::connect(&broker.bootstrap()[0], ConnectionConfig::new())
        .await
        .unwrap();

    for entry in conn.versions().entries() {
        let Some(ours) = entry.ours else { continue };
        let Some(negotiated) = entry.negotiated() else {
            continue;
        };
        assert!(
            negotiated <= ours.max && negotiated <= entry.broker.max,
            "{} negotiated {negotiated} outside broker {:?} / ours {ours:?}",
            entry.api_key,
            entry.broker
        );
        assert!(
            negotiated >= ours.min && negotiated >= entry.broker.min,
            "{} negotiated {negotiated} below a minimum",
            entry.api_key
        );
    }
}

#[testkit::integration_test]
async fn the_handshake_counts_its_own_bytes() {
    // The counters exist from M2 but are fed by the handshake too; M10's
    // acceptance arithmetic is only meaningful if nothing bypasses them.
    let broker = testkit::single_broker().await.unwrap();
    let conn = Connection::connect(&broker.bootstrap()[0], ConnectionConfig::new())
        .await
        .unwrap();
    let stats = conn.stats_snapshot();
    assert!(stats.bytes_sent > 0, "{stats:?}");
    assert!(stats.bytes_received > 0, "{stats:?}");
    assert_eq!(stats.requests_sent, 1);
    assert_eq!(stats.responses_received, 1);
}
