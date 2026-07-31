//! Container-backed Kafka fixtures.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use futures::future::LocalBoxFuture;
use testcontainers::{
    ContainerAsync, ImageExt,
    core::{CmdWaitFor, ContainerPort, ExecCommand},
    runners::AsyncRunner,
};

use crate::config::{BrokerConfig, CA_PEM_PATH, CONTROLLER_PORT, EXTERNAL_PORT};
use crate::error::{Error, Result};
use crate::harness::{Cluster, ExecOutput};
use crate::image::KafkaImage;

/// A running Kafka cluster in containers.
///
/// Dropping it removes the containers.
#[derive(Debug)]
pub struct KafkaCluster {
    containers: Vec<ContainerAsync<KafkaImage>>,
    bootstrap: Vec<String>,
}

/// Boot a single PLAINTEXT broker.
pub async fn single_broker() -> Result<KafkaCluster> {
    single_broker_with(BrokerConfig::new()).await
}

/// Boot a single broker with the given configuration.
pub async fn single_broker_with(config: BrokerConfig) -> Result<KafkaCluster> {
    KafkaCluster::start(config.with_nodes(1)).await
}

/// Boot an `n`-node cluster.
///
/// M6 onward needs this: log directories, partition reassignment and
/// replica-aware sizing are all untestable against a single broker.
pub async fn cluster(nodes: usize) -> Result<KafkaCluster> {
    KafkaCluster::start(BrokerConfig::new().with_nodes(nodes)).await
}

/// Boot a cluster with the given configuration.
pub async fn cluster_with(config: BrokerConfig) -> Result<KafkaCluster> {
    KafkaCluster::start(config).await
}

impl KafkaCluster {
    async fn start(config: BrokerConfig) -> Result<Self> {
        config.validate()?;

        let run = run_id();
        let network = format!("kaas-net-{run}");
        let hostnames: Vec<String> = (1..=config.nodes())
            .map(|i| format!("kaas-{run}-{i}"))
            .collect();

        // KRaft's quorum has to be known before any node starts, so the
        // addresses are derived from names we choose up front rather than from
        // anything the runtime hands back. Static voters (as opposed to a
        // KIP-853 dynamic quorum) are what let `kafka-storage format` stay a
        // one-liner with no `--standalone` / `--initial-controllers` split
        // between the single-node and multi-node paths.
        let mut voters = Vec::with_capacity(hostnames.len());
        for (index, host) in hostnames.iter().enumerate() {
            let node_id = node_id(index)?;
            voters.push(format!("{node_id}@{host}:{CONTROLLER_PORT}"));
        }
        let voters = voters.join(",");

        let mut pending = Vec::with_capacity(hostnames.len());
        for (index, host) in hostnames.iter().enumerate() {
            let image = KafkaImage::new(
                config.clone(),
                node_id(index)?,
                host.clone(),
                voters.clone(),
            );
            let request = image
                .with_network(network.clone())
                .with_container_name(host.clone())
                .with_hostname(host.clone())
                .with_startup_timeout(config.startup_timeout());
            pending.push(request.start());
        }

        // Concurrently, and this is not an optimisation. A KRaft node does not
        // finish starting until the controller quorum has a majority, so
        // starting node 1 and waiting for it to be ready before starting node 2
        // deadlocks on any cluster larger than one.
        let containers = futures::future::try_join_all(pending).await?;

        let mut bootstrap = Vec::with_capacity(containers.len());
        for container in &containers {
            let host = container.get_host().await?;
            let port = container
                .get_host_port_ipv4(ContainerPort::Tcp(EXTERNAL_PORT))
                .await?;
            bootstrap.push(format!("{host}:{port}"));
        }

        Ok(Self {
            containers,
            bootstrap,
        })
    }

    /// The bootstrap address of one node.
    pub fn bootstrap_for(&self, index: usize) -> Result<&str> {
        self.bootstrap
            .get(index)
            .map(String::as_str)
            .ok_or(Error::NoSuchNode {
                index,
                size: self.bootstrap.len(),
            })
    }

    /// The PEM-encoded CA certificate the node's TLS listener chains to.
    ///
    /// Each node generates its own CA, so a TLS test against a multi-node
    /// cluster has to trust each one. Only meaningful when the fixture was
    /// configured with [`crate::Security::Ssl`] or [`crate::Security::SaslSsl`].
    pub async fn ca_pem(&self, index: usize) -> Result<String> {
        let out = crate::harness::exec_ok(self, index, ["cat", CA_PEM_PATH]).await?;
        Ok(out.stdout)
    }

    /// Run one of the Kafka shell tools bundled in the image.
    ///
    /// `--bootstrap-server` is filled in from the node's *internal* listener,
    /// because the tool runs inside the container where the host-mapped port
    /// does not exist.
    pub async fn kafka_cli(
        &self,
        index: usize,
        tool: &str,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<ExecOutput> {
        let mut argv = vec![
            format!("/opt/kafka/bin/{tool}"),
            "--bootstrap-server".to_owned(),
            format!("localhost:{EXTERNAL_PORT}"),
        ];
        argv.extend(args.into_iter().map(Into::into));
        crate::harness::exec_ok(self, index, argv).await
    }
}

impl Cluster for KafkaCluster {
    fn bootstrap(&self) -> &[String] {
        &self.bootstrap
    }

    fn nodes(&self) -> usize {
        self.containers.len()
    }

    fn exec<'a>(
        &'a self,
        index: usize,
        argv: Vec<String>,
    ) -> LocalBoxFuture<'a, Result<ExecOutput>> {
        Box::pin(async move {
            let container = self.containers.get(index).ok_or(Error::NoSuchNode {
                index,
                size: self.containers.len(),
            })?;
            let mut result = container
                .exec(ExecCommand::new(argv).with_cmd_ready_condition(CmdWaitFor::exit()))
                .await?;
            let stdout = String::from_utf8_lossy(&result.stdout_to_vec().await?).into_owned();
            let stderr = String::from_utf8_lossy(&result.stderr_to_vec().await?).into_owned();
            let code = result.exit_code().await?;
            Ok(ExecOutput {
                code,
                stdout,
                stderr,
            })
        })
    }
}

fn node_id(index: usize) -> Result<i32> {
    let id = i32::try_from(index)
        .map_err(|_| Error::config("cluster is too large to assign node ids"))?;
    id.checked_add(1)
        .ok_or_else(|| Error::config("cluster is too large to assign node ids"))
}

/// A short token unique to this fixture, for container and network names.
fn run_id() -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or_default();
    format!("{:x}{:x}{:x}", std::process::id(), nanos, seq)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_ids_are_one_based() {
        assert_eq!(node_id(0).ok(), Some(1));
        assert_eq!(node_id(2).ok(), Some(3));
    }

    #[test]
    fn run_ids_differ_between_calls() {
        assert_ne!(run_id(), run_id());
    }
}
