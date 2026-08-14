//! How a cluster authenticates, and how one principal gets in.
//!
//! Read-only, like `probe`, and diffable for the same reason: the listener
//! inventory is exactly the kind of fact two brokers disagree about.
//!
//! The interesting assertion here cannot be made in a container fixture. Every
//! fixture in this workspace runs one client listener with one mechanism, so
//! the elimination that `likely_mechanism` performs has nothing to eliminate.
//! A real cluster with four listeners — plaintext, server TLS, OAUTHBEARER and
//! mutual TLS — is where the reasoning either holds or does not, and where a
//! certificate principal is a distinguished name rather than a test fixture's
//! idea of one.

use anyhow::{Context, Result};
use kafka_admin::{Admin, ClusterAuthentication, Principal};
use kafka_meta::Cluster;

use crate::report::{Report, one_line};
use crate::target::Target;

/// Principals to describe when none are named.
///
/// `ANONYMOUS` is what an unauthenticated connection is called, so a run
/// against a plaintext listener describes the identity it is actually using.
const DEFAULT_PRINCIPALS: [&str; 1] = ["User:ANONYMOUS"];

/// Report a cluster's listener authentication, and a verdict per principal.
pub async fn principal(target: &Target, names: &[String]) -> Result<Report> {
    let mut report = Report::new();
    report.note(format!("target: {}", target.label));

    let cluster = Cluster::connect(target.bootstrap.clone(), target.cluster_config())
        .await
        .context("opening a routed cluster client")?;
    let admin = Admin::new(cluster);

    let listeners = admin
        .describe_authentication()
        .await
        .context("describing listener authentication")?;
    listener_facts(&listeners, &mut report);

    let requested: Vec<String> = if names.is_empty() {
        DEFAULT_PRINCIPALS.iter().map(|n| (*n).to_owned()).collect()
    } else {
        names.to_vec()
    };

    for (index, name) in requested.iter().enumerate() {
        let principal = Principal::parse(name);
        let key = format!("principal.{index}");
        report.set(format!("{key}.name"), one_line(&principal.to_string()));
        report.set(
            format!("{key}.is_distinguished_name"),
            principal.is_distinguished_name(),
        );

        let described = admin
            .describe_principal(&principal)
            .await
            .with_context(|| format!("describing {principal}"))?;

        // Each source separately: on a cluster with an authorizer, an
        // unprivileged principal reads some of these and not others, and
        // collapsing that into one line loses the reason.
        match &described.scram {
            Ok(infos) => {
                report.set(format!("{key}.scram.count"), infos.len());
                report.set(
                    format!("{key}.scram.mechanisms"),
                    described
                        .scram_mechanisms()
                        .iter()
                        .map(|m| format!("{m:?}"))
                        .collect::<Vec<_>>()
                        .join(","),
                );
            }
            Err(error) => report.set(format!("{key}.scram.error"), one_line(&error.to_string())),
        }
        match &described.tokens {
            Ok(tokens) => report.set(format!("{key}.tokens.count"), tokens.len()),
            Err(error) => report.set(format!("{key}.tokens.error"), one_line(&error.to_string())),
        }
        match &described.acls {
            Ok(acls) => report.set(format!("{key}.acls.count"), acls.len()),
            Err(error) => report.set(format!("{key}.acls.error"), one_line(&error.to_string())),
        }
        match &described.quotas {
            Ok(quotas) => report.set(format!("{key}.quotas.count"), quotas.len()),
            Err(error) => report.set(format!("{key}.quotas.error"), one_line(&error.to_string())),
        }

        report.set(
            format!("{key}.has_stored_credentials"),
            described.has_stored_credentials(),
        );
        report.set(format!("{key}.is_unrecorded"), described.is_unrecorded());

        let verdict = described.likely_mechanism(&listeners);
        report.set(format!("{key}.verdict"), one_line(&verdict.to_string()));
        report.set(
            format!("{key}.verdict.basis"),
            format!("{:?}", verdict.basis),
        );
        report.set(format!("{key}.verdict.conclusive"), verdict.is_conclusive());
    }

    Ok(report)
}

/// The listener inventory, one key per listener so a diff points at the
/// listener that differs rather than at a joined string.
fn listener_facts(listeners: &ClusterAuthentication, report: &mut Report) {
    report.set("auth.described_broker", listeners.node_id);
    report.set("auth.listener.count", listeners.listeners.len());
    report.set(
        "auth.client_listener.count",
        listeners.client_listeners().count(),
    );
    report.set_opt(
        "auth.principal_mapping_rules",
        listeners.principal_mapping_rules.as_deref().map(one_line),
    );
    report.set(
        "auth.maps_certificates_to_subjects",
        listeners.maps_certificates_to_subjects(),
    );
    report.set(
        "auth.client_mechanisms",
        listeners
            .client_mechanisms()
            .iter()
            .map(|m| m.to_string())
            .collect::<Vec<_>>()
            .join(","),
    );

    for listener in &listeners.listeners {
        let key = format!("auth.listener.{}", listener.name.to_ascii_lowercase());
        report.set(format!("{key}.protocol"), &listener.protocol);
        report.set(
            format!("{key}.sasl_mechanisms"),
            listener
                .sasl_mechanisms
                .iter()
                .map(|m| m.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        report.set(
            format!("{key}.client_auth"),
            format!("{:?}", listener.client_auth),
        );
        report.set(format!("{key}.inter_broker"), listener.is_inter_broker);
        report.set(format!("{key}.controller"), listener.is_controller);
    }
}
