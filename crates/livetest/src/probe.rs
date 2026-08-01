//! Read-only inventory of a live cluster.
//!
//! Touches nothing. Safe to point at production, safe to run against a cluster
//! someone else is benchmarking, and safe to run with `KAAS_TEST_READ_ONLY=1`
//! — which is worth doing, because it also exercises the M8 gate against a real
//! broker rather than a fixture.
//!
//! The output is a diffable report, so two probes are a conformance check:
//!
//! ```sh
//! livetest probe > /tmp/strimzi.txt   # against Apache Kafka
//! livetest probe > /tmp/kaas.txt      # against the kaas broker
//! diff -u /tmp/strimzi.txt /tmp/kaas.txt
//! ```

use anyhow::{Context, Result};
use kafka_admin::{Admin, ConfigResource};
use kafka_conn::{ApiKey, Connection};
use kafka_meta::Cluster;

use crate::report::{Report, one_line};
use crate::target::Target;

/// Probe a cluster and return a diffable report.
pub async fn probe(target: &Target) -> Result<Report> {
    let mut report = Report::new();
    report.note(format!("target: {}", target.label));
    report.note(format!("bootstrap: {}", target.bootstrap.join(",")));

    // 1. The handshake, on a bare connection. Anything wrong here makes the
    //    rest of the report meaningless, so it is first and it is fatal.
    let bootstrap = target
        .bootstrap
        .first()
        .context("no bootstrap address")?
        .clone();
    let connection = Connection::connect(&bootstrap, target.connection().clone())
        .await
        .with_context(|| format!("connecting to {bootstrap}"))?;

    api_versions(&connection, &mut report);

    // 2. Everything above the wire, through the routed client.
    let cluster = Cluster::connect(target.bootstrap.clone(), target.cluster_config())
        .await
        .context("opening a routed cluster client")?;
    let admin = Admin::new(cluster.clone());

    cluster_shape(&admin, &cluster, &mut report).await;
    topics(&admin, &cluster, &mut report).await;
    groups(&admin, &mut report).await;
    security(&admin, &mut report).await;
    transactions(&admin, &mut report).await;

    Ok(report)
}

/// The negotiated version table — the single most useful thing to diff.
fn api_versions(connection: &Connection, report: &mut Report) {
    let versions = connection.versions();
    report.set("api.count", versions.len());

    let mut ahead = 0;
    let mut unnameable = 0;
    for entry in versions.entries() {
        // Keyed by wire code as well as name, so an api key one cluster names
        // and the other does not still lines up in a diff.
        let key = format!("api.{:03}.{}", entry.api_key.code(), entry.api_key.name());
        report.set(
            format!("{key}.broker"),
            format!("{}..{}", entry.broker.min, entry.broker.max),
        );
        report.set_opt(
            format!("{key}.ours"),
            entry.ours.map(|r| format!("{}..{}", r.min, r.max)),
        );
        report.set_opt(format!("{key}.negotiated"), entry.negotiated());

        if entry.broker_ahead() {
            ahead += 1;
        }
        if entry.ours.is_none() {
            unnameable += 1;
        }
    }

    // The two numbers that say how far the crate's schemas are behind this
    // broker. `kafka-protocol` 0.17 ships Kafka 4.0 schemas, so both are
    // expected to be non-zero against anything newer — and a zero here means
    // either a very old broker or a negotiation that is not clamping on our
    // side at all.
    report.set("api.broker_ahead_count", ahead);
    report.set("api.unnameable_count", unnameable);
    report.set(
        "api.supports.describe_topic_partitions",
        versions.supports(ApiKey::DescribeTopicPartitions),
    );
    report.set(
        "api.supports.consumer_group_describe",
        versions.supports(ApiKey::ConsumerGroupDescribe),
    );
    report.set(
        "api.supports.share_group_describe",
        versions.supports(ApiKey::ShareGroupDescribe),
    );
    report.set(
        "api.supports.describe_cluster",
        versions.supports(ApiKey::DescribeCluster),
    );

    // `*.ours` above is `ApiKey::valid_versions()`, which is derived per api
    // key and reports the wider range where a request and its response have
    // different schemas. The version actually sent is the narrower, typed one.
    // Recording both is what makes a divergence visible instead of a puzzling
    // encode failure later.
    macro_rules! typed {
        ($name:literal, $request:ty) => {
            report.set_opt(
                concat!("api.typed.", $name),
                connection.negotiated_for::<$request>().ok(),
            );
        };
    }
    use kafka_conn::protocol::messages as m;
    typed!("Metadata", m::MetadataRequest);
    typed!("Fetch", m::FetchRequest);
    typed!("ListOffsets", m::ListOffsetsRequest);
    typed!("FindCoordinator", m::FindCoordinatorRequest);
    typed!("OffsetFetch", m::OffsetFetchRequest);
    typed!("OffsetCommit", m::OffsetCommitRequest);
    typed!("DescribeGroups", m::DescribeGroupsRequest);
    typed!("ListGroups", m::ListGroupsRequest);
    typed!("CreateTopics", m::CreateTopicsRequest);
    typed!("DeleteTopics", m::DeleteTopicsRequest);
    typed!("DescribeConfigs", m::DescribeConfigsRequest);
    typed!("DescribeCluster", m::DescribeClusterRequest);
    typed!("DescribeLogDirs", m::DescribeLogDirsRequest);
    typed!("DescribeTopicPartitions", m::DescribeTopicPartitionsRequest);
    typed!("ConsumerGroupDescribe", m::ConsumerGroupDescribeRequest);
    typed!("ShareGroupDescribe", m::ShareGroupDescribeRequest);
}

async fn cluster_shape(admin: &Admin, cluster: &Cluster, report: &mut Report) {
    match admin.describe_cluster().await {
        Ok(description) => {
            // The id itself is not diffable between clusters, only its shape.
            report.set("cluster.id.present", !description.cluster_id.is_empty());
            report.set_opt(
                "cluster.controller.present",
                description.controller_id.map(|_| true),
            );
            report.set("cluster.brokers", description.brokers.len());
            report.set(
                "cluster.brokers.fenced",
                description.brokers.iter().filter(|b| b.is_fenced).count(),
            );
            report.set(
                "cluster.brokers.racked",
                description
                    .brokers
                    .iter()
                    .filter(|b| b.rack.is_some())
                    .count(),
            );
        }
        Err(error) => report.set("cluster.describe.error", one_line(&error.to_string())),
    }

    let snapshot = cluster.snapshot();
    report.set("metadata.brokers", snapshot.brokers().len());
    report.set("metadata.topics", snapshot.topics().len());
    report.set(
        "metadata.topics.internal",
        snapshot.topics().iter().filter(|t| t.internal).count(),
    );
    report.set(
        "metadata.partitions",
        snapshot
            .topics()
            .iter()
            .map(|t| t.partitions.len())
            .sum::<usize>(),
    );
    report.set(
        "metadata.partitions.leaderless",
        snapshot
            .topics()
            .iter()
            .flat_map(|t| &t.partitions)
            .filter(|p| p.leader.is_none())
            .count(),
    );
    report.set(
        "metadata.partitions.under_replicated",
        snapshot
            .topics()
            .iter()
            .flat_map(|t| &t.partitions)
            .filter(|p| p.under_replicated())
            .count(),
    );
    report.set(
        "metadata.topic_ids.present",
        snapshot
            .topics()
            .iter()
            .filter(|t| !t.topic_id.is_zero())
            .count(),
    );

    // Broker configs are the richest single source of behavioural difference
    // between two implementations, but the values are cluster-specific. The
    // *key set* is what is worth diffing.
    if let Some(broker) = snapshot.brokers().first() {
        match admin
            .describe_configs([ConfigResource::broker(broker.node_id)])
            .await
        {
            Ok(results) => match results.into_iter().next() {
                Some((_, Ok(entries))) => {
                    report.set("broker_config.count", entries.len());
                    report.set(
                        "broker_config.explicit",
                        entries.iter().filter(|e| e.source.is_explicit()).count(),
                    );
                    report.set(
                        "broker_config.sensitive",
                        entries.iter().filter(|e| e.is_sensitive).count(),
                    );
                    for key in [
                        "auto.create.topics.enable",
                        "compression.type",
                        "message.max.bytes",
                        "num.partitions",
                        "default.replication.factor",
                        "group.coordinator.rebalance.protocols",
                        "log.message.format.version",
                        "unstable.api.versions.enable",
                    ] {
                        report.set_opt(
                            format!("broker_config.{key}"),
                            entries
                                .iter()
                                .find(|entry| entry.name == key)
                                .and_then(|entry| entry.value.clone())
                                .map(|value| one_line(&value)),
                        );
                    }
                }
                Some((_, Err(error))) => {
                    report.set("broker_config.error", one_line(&error.to_string()));
                }
                None => report.set("broker_config.error", "no result returned"),
            },
            Err(error) => report.set("broker_config.error", one_line(&error.to_string())),
        }
    }
}

async fn topics(admin: &Admin, cluster: &Cluster, report: &mut Report) {
    let snapshot = cluster.snapshot();
    let names: Vec<String> = snapshot.topics().iter().map(|t| t.name.clone()).collect();

    // Does the paginating describe api agree with metadata? A disagreement is
    // exactly the kind of silent wrongness the M6 fallback exists to avoid.
    match admin.describe_topics(names.iter().take(50).cloned()).await {
        Ok(results) => {
            report.set("describe_topics.asked", results.len());
            report.set("describe_topics.ok", kafka_admin::oks(&results).count());
            report.set("describe_topics.err", kafka_admin::errs(&results).count());

            let mut disagreements = 0;
            for (name, result) in &results {
                let Ok(info) = result else { continue };
                if snapshot
                    .topic(name)
                    .is_some_and(|meta| meta.partitions.len() != info.partitions.len())
                {
                    disagreements += 1;
                }
            }
            report.set("describe_topics.disagrees_with_metadata", disagreements);
        }
        Err(error) => report.set("describe_topics.error", one_line(&error.to_string())),
    }

    // A name no cluster has. The error code it comes back with is a real
    // conformance data point.
    match admin
        .describe_topics(["kaas-lib-probe-no-such-topic"])
        .await
    {
        Ok(results) => {
            let code = results
                .first()
                .and_then(|(_, result)| result.as_ref().err())
                .and_then(kafka_conn::Error::code);
            report.set_opt("unknown_topic.code", code);
        }
        Err(error) => report.set("unknown_topic.error", one_line(&error.to_string())),
    }

    match admin.describe_all_log_dirs().await {
        Ok(per_broker) => {
            report.set("log_dirs.brokers_ok", kafka_admin::oks(&per_broker).count());
            report.set(
                "log_dirs.brokers_err",
                kafka_admin::errs(&per_broker).count(),
            );
            let dirs: usize = kafka_admin::oks(&per_broker).map(|(_, d)| d.len()).sum();
            report.set("log_dirs.count", dirs);
            report.set(
                "log_dirs.reports_total_bytes",
                kafka_admin::oks(&per_broker)
                    .flat_map(|(_, dirs)| dirs.iter())
                    .any(|dir| dir.total_bytes.is_some()),
            );
        }
        Err(error) => report.set("log_dirs.error", one_line(&error.to_string())),
    }
}

async fn groups(admin: &Admin, report: &mut Report) {
    let listings = match admin.list_groups().await {
        Ok(listings) => listings,
        Err(error) => {
            report.set("groups.error", one_line(&error.to_string()));
            return;
        }
    };

    report.set("groups.count", listings.len());
    // Group *types* are the headline conformance fact on a 4.x cluster: how
    // many kinds the broker reports, and how many of them we can describe.
    let mut types: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for listing in &listings {
        let key = if listing.group_type.is_empty() {
            "unreported".to_owned()
        } else {
            listing.group_type.to_ascii_lowercase()
        };
        *types.entry(key).or_default() += 1;
    }
    for (group_type, count) in &types {
        report.set(format!("groups.type.{group_type}"), count);
    }
    report.set(
        "groups.describable",
        listings.iter().filter(|l| l.describable()).count(),
    );
    report.set(
        "groups.undescribable",
        listings.iter().filter(|l| !l.describable()).count(),
    );

    // Describing every group is the strongest single assertion this probe
    // makes: a group kind we cannot describe must come back Unrecognized, not
    // as an error, or a UI's group list dies on the first Kafka Streams
    // application it meets.
    let ids: Vec<String> = listings.iter().map(|l| l.group_id.clone()).collect();
    match admin.describe_groups(ids).await {
        Ok(described) => {
            let mut classic = 0;
            let mut consumer = 0;
            let mut share = 0;
            let mut unrecognized = 0;
            let mut failed = 0;
            let mut first_failure = None;
            for (group_id, result) in &described {
                if let Err(error) = result
                    && first_failure.is_none()
                {
                    // A count without a reason is not actionable. The first
                    // failure's code and message are what turn "3 groups did
                    // not describe" into something someone can fix.
                    first_failure = Some(format!("{group_id}: {error}"));
                }
                match result {
                    Ok(kafka_admin::GroupDescription::Classic { .. }) => classic += 1,
                    Ok(kafka_admin::GroupDescription::Consumer { .. }) => consumer += 1,
                    Ok(kafka_admin::GroupDescription::Share { .. }) => share += 1,
                    Ok(kafka_admin::GroupDescription::Unrecognized { .. }) => unrecognized += 1,
                    Err(_) => failed += 1,
                }
            }
            report.set("groups.described.classic", classic);
            report.set("groups.described.consumer", consumer);
            report.set("groups.described.share", share);
            report.set("groups.described.unrecognized", unrecognized);
            report.set("groups.described.failed", failed);
            report.set_opt(
                "groups.described.first_failure",
                first_failure.as_deref().map(one_line),
            );
        }
        Err(error) => report.set("groups.describe.error", one_line(&error.to_string())),
    }
}

async fn security(admin: &Admin, report: &mut Report) {
    // No authorizer is a perfectly good answer, and the *error* is the fact
    // worth recording: it tells a reader why the ACL half of M8 cannot be
    // exercised here.
    match admin
        .describe_acls(&kafka_admin::AclFilter::default())
        .await
    {
        Ok(bindings) => {
            report.set("acls.supported", true);
            report.set("acls.count", bindings.len());
        }
        Err(error) => {
            report.set("acls.supported", false);
            report.set_opt("acls.error_code", error.code());
        }
    }

    match admin.describe_scram_credentials(None).await {
        Ok(results) => {
            report.set("scram.supported", true);
            report.set("scram.users", kafka_admin::oks(&results).count());
            report.set(
                "scram.credentials",
                kafka_admin::oks(&results)
                    .map(|(_, infos)| infos.len())
                    .sum::<usize>(),
            );
        }
        Err(error) => {
            report.set("scram.supported", false);
            report.set_opt("scram.error_code", error.code());
        }
    }

    match admin
        .describe_client_quotas(&kafka_admin::QuotaFilter::default())
        .await
    {
        Ok(entries) => {
            report.set("quotas.supported", true);
            report.set("quotas.entries", entries.len());
        }
        Err(error) => {
            report.set("quotas.supported", false);
            report.set_opt("quotas.error_code", error.code());
        }
    }

    match admin.list_partition_reassignments(None).await {
        Ok(ongoing) => {
            report.set("reassignments.supported", true);
            report.set("reassignments.ongoing", ongoing.len());
        }
        Err(error) => {
            report.set("reassignments.supported", false);
            report.set_opt("reassignments.error_code", error.code());
        }
    }
}

async fn transactions(admin: &Admin, report: &mut Report) {
    match admin.list_transactions(&[]).await {
        Ok(listings) => {
            report.set("transactions.supported", true);
            report.set("transactions.count", listings.len());
            let mut states: std::collections::BTreeMap<String, usize> =
                std::collections::BTreeMap::new();
            for listing in &listings {
                *states.entry(listing.state.clone()).or_default() += 1;
            }
            for (state, count) in &states {
                report.set(format!("transactions.state.{state}"), count);
            }
        }
        Err(error) => {
            report.set("transactions.supported", false);
            report.set_opt("transactions.error_code", error.code());
        }
    }
}
