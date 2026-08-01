//! Remove what a crashed run left behind.
//!
//! The smoke suite cleans up after itself, including on its error path. A
//! process that is killed cannot, so this exists to be run afterwards.
//!
//! The one rule: nothing without the configured prefix is ever deleted. These
//! are shared clusters holding other people's benchmarks and other people's
//! data, and a sweeper that gets its filter wrong is the most destructive thing
//! in this repository. So the prefix check is a separate, tested function and
//! the deletion path takes only names that passed it.

use anyhow::Result;
use kafka_admin::Admin;
use kafka_meta::Cluster;

use crate::target::Target;

/// Delete every topic and group this tool owns.
pub async fn sweep(target: &Target) -> Result<Vec<String>> {
    target.require_writable("sweeping")?;

    let cluster = Cluster::connect(target.bootstrap.clone(), target.cluster_config()).await?;
    let admin = Admin::new(cluster.clone());
    let mut removed = Vec::new();

    let snapshot = cluster.refresh().await?;
    let topics: Vec<String> = snapshot
        .topics()
        .iter()
        .map(|topic| topic.name.clone())
        .filter(|name| target.owns(name))
        .collect();

    if !topics.is_empty() {
        for (name, result) in admin.delete_topics(topics).await? {
            match result {
                Ok(()) => removed.push(format!("topic {name}")),
                Err(error) => tracing::warn!(%name, %error, "could not delete topic"),
            }
        }
    }

    let groups: Vec<String> = admin
        .list_groups()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|listing| listing.group_id)
        .filter(|id| target.owns(id))
        .collect();

    if !groups.is_empty() {
        for (id, result) in admin.delete_groups(groups).await? {
            match result {
                Ok(()) => removed.push(format!("group {id}")),
                Err(error) => tracing::warn!(%id, %error, "could not delete group"),
            }
        }
    }

    removed.sort();
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use crate::target::owns;

    /// The assertion that stands between this tool and someone's production
    /// topic list.
    #[test]
    fn the_filter_keeps_everything_it_does_not_own() {
        let prefix = crate::target::DEFAULT_PREFIX;
        for name in [
            "__consumer_offsets",
            "__transaction_state",
            "orders",
            "events",
            "kperf-bench",
            "strimzi-internal",
            // Close enough to be dangerous, and still not ours.
            "kaaslib",
            "kaaslib-liveness",
            "kaaslib-liveness-probe",
            "live-kaaslib-topic",
            "x-kaaslib-live-topic-abc",
        ] {
            assert!(!owns(prefix, name), "{name} would have been deleted");
        }

        for name in [
            "kaaslib-live-smoke-abc123",
            "kaaslib-live-topic-1",
            "kaaslib-live-group-9",
        ] {
            assert!(owns(prefix, name), "{name} would have been left behind");
        }
    }

    /// A prefix set to something empty or trivial must not turn the sweeper
    /// into "delete everything".
    #[test]
    fn an_empty_prefix_still_requires_the_separator() {
        assert!(!owns("", "orders"));
        assert!(owns("", "-orders"));
    }
}
