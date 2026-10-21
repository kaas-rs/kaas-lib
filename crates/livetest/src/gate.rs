//! The mutation ceiling and a scoped handle, against a cluster we do not own —
//! changing nothing on it.
//!
//! The container suite proves both gates against a broker configured for the
//! test. This proves them against a shared, long-lived cluster running a build
//! we did not choose, and it can do so without a scratch topic because the
//! one admitted write is a `validate_only` `CreateTopics`: it crosses the gate
//! and reaches the controller, which answers and creates nothing. Every other
//! write here is expected to be refused before it is sent, and a refusal is
//! the whole assertion — so a run that *did* send one would show it as
//! `sent`, and fail.

use anyhow::Result;
use kafka_admin::{Admin, Error, NewTopic, ops};
use kafka_meta::Cluster;

use crate::report::{Outcome, Report};
use crate::target::{Target, run_token};

/// What a gate made of one request.
fn verdict<T>(
    outcome: &kafka_admin::Result<Vec<(String, kafka_admin::Result<T>)>>,
) -> &'static str {
    let refused = |error: &Error| matches!(error, Error::ReadOnly { .. });
    match outcome {
        Err(error) if refused(error) => "refused",
        Ok(items)
            if !items.is_empty()
                && items
                    .iter()
                    .all(|(_, item)| item.as_ref().err().is_some_and(refused)) =>
        {
            "refused"
        }
        // The broker answered — success, or its own refusal (authorization,
        // policy). Either way the request was sent: the gate admitted it.
        _ => "sent",
    }
}

/// Run the gate checks.
pub async fn gate(target: &Target) -> Result<Outcome> {
    let mut report = Report::new();
    report.note(format!("target: {}", target.label));
    let topic = target.scoped_name("gate", &run_token());
    report.note(format!("validated, never created: {topic}"));

    let mut config = target.cluster_config();
    config.connection = config
        .connection
        .with_allowed_mutations(ops::CREATE_TOPICS.iter().copied());
    let ceiling = Admin::new(Cluster::connect(target.bootstrap.clone(), config).await?);

    let mut expectations = Vec::new();
    let mut check = |report: &mut Report, key: &str, got: &'static str, want: &'static str| {
        report.set(key, got);
        if got != want {
            expectations.push(format!("{key}: {got}, expected {want}"));
        }
    };

    let topics = || [NewTopic::new(topic.clone(), 1, 1)];

    // The ceiling admits CreateTopics and nothing else that mutates.
    let validated = ceiling.validate_topics(topics()).await;
    check(
        &mut report,
        "ceiling.create_topics.validate",
        verdict(&validated),
        "sent",
    );
    let deleted = ceiling.delete_topics([topic.clone()]).await;
    check(
        &mut report,
        "ceiling.delete_topics",
        verdict(&deleted),
        "refused",
    );
    report.set("ceiling.reads", ceiling.list_topics().await.is_ok());

    // A scope narrows over the ceiling: CreateTopics still admitted...
    let creator = ceiling.scoped(ops::CREATE_TOPICS.iter().copied());
    let validated = creator.validate_topics(topics()).await;
    check(
        &mut report,
        "scoped.create_topics.validate",
        verdict(&validated),
        "sent",
    );
    // ...a scope without it refuses it, though the ceiling would admit it...
    let resetter = ceiling.scoped(ops::RESET_OFFSETS.iter().copied());
    let validated = resetter.validate_topics(topics()).await;
    check(
        &mut report,
        "scoped_elsewhere.create_topics.validate",
        verdict(&validated),
        "refused",
    );
    // ...and a scope cannot widen past the ceiling: DeleteTopics stays refused.
    let widened = ceiling.scoped(ops::DELETE_TOPICS.iter().copied());
    let deleted = widened.delete_topics([topic.clone()]).await;
    check(
        &mut report,
        "scoped_wider.delete_topics",
        verdict(&deleted),
        "refused",
    );
    report.set("scoped.reads", resetter.list_topics().await.is_ok());

    Ok(if expectations.is_empty() {
        Outcome::ok(report)
    } else {
        Outcome::failed(report, anyhow::anyhow!(expectations.join("; ")))
    })
}
