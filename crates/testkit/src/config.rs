//! Fixture configuration.
//!
//! PLAN.md M0 asks for the config builder *now* rather than when the
//! milestones that need it land: M3 wants SASL_PLAINTEXT/PLAIN and
//! SASL_SSL/SCRAM-SHA-512 brokers, M8 wants an authorizer, M10 wants
//! aggressive compaction. Retrofitting the same seam three times costs more
//! than designing it once.
//!
//! The knobs render into a complete `server.properties` that the fixture
//! writes into the container itself, rather than into `KAFKA_*` environment
//! variables interpreted by the image's own `configure` script. That is
//! deliberate: the env-var mangling rules (`_` → `.`, `__` → `_`, `___` → `-`)
//! are an image implementation detail, and every property we would want to set
//! for SASL has an underscore in its listener name. Writing the file directly
//! means the only thing we depend on from the image is that `/opt/kafka/bin`
//! and a shell exist.

use std::time::Duration;

use crate::error::{Error, Result};

/// Default broker image. Overridable — see [`BrokerConfig::with_image`] and
/// the conformance-harness argument in CLAUDE.md.
pub const DEFAULT_IMAGE: &str = "apache/kafka";
/// Default broker tag: the release PLAN.md pins the acceptance suite to.
pub const DEFAULT_TAG: &str = "4.3.1";

/// Port the external (test-facing) listener binds inside the container.
pub(crate) const EXTERNAL_PORT: u16 = 9092;
/// Port the inter-broker listener binds inside the container.
pub(crate) const BROKER_PORT: u16 = 9093;
/// Port the KRaft controller listener binds inside the container.
pub(crate) const CONTROLLER_PORT: u16 = 9094;

/// Where the fixture keeps everything it generates inside the container.
pub(crate) const WORK_DIR: &str = "/tmp/kaas-testkit";
/// Path of the keystore the TLS listeners use, when TLS is enabled.
pub(crate) const KEYSTORE_PATH: &str = "/tmp/kaas-testkit/kafka.keystore.jks";
/// Path of the PEM-encoded test CA, readable via [`crate::KafkaCluster::ca_pem`].
pub(crate) const CA_PEM_PATH: &str = "/tmp/kaas-testkit/ca.pem";
/// Password for every generated keystore. Test fixtures only.
pub(crate) const KEYSTORE_PASSWORD: &str = "kaas-testkit";

/// Wire security for the *external* listener — the one tests connect to.
///
/// The inter-broker and controller listeners stay PLAINTEXT regardless. The
/// point of these fixtures is to exercise our client's handshake, not to
/// re-test Kafka's own inter-broker security, and keeping the cluster's
/// internals unauthenticated means a broken client handshake shows up as a
/// client failure rather than as a cluster that never forms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Security {
    /// No encryption, no authentication.
    Plaintext,
    /// SASL over a plaintext socket.
    SaslPlaintext,
    /// TLS, no SASL.
    Ssl,
    /// SASL over TLS.
    SaslSsl,
}

impl Security {
    /// The `listener.security.protocol.map` value for the external listener.
    pub(crate) fn protocol(self) -> &'static str {
        match self {
            Security::Plaintext => "PLAINTEXT",
            Security::SaslPlaintext => "SASL_PLAINTEXT",
            Security::Ssl => "SSL",
            Security::SaslSsl => "SASL_SSL",
        }
    }

    /// Whether this listener needs a keystore generated.
    pub(crate) fn needs_tls(self) -> bool {
        matches!(self, Security::Ssl | Security::SaslSsl)
    }

    /// Whether this listener needs SASL configuration.
    pub(crate) fn needs_sasl(self) -> bool {
        matches!(self, Security::SaslPlaintext | Security::SaslSsl)
    }
}

/// A SASL mechanism the external listener will accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanism {
    /// `PLAIN` — credentials in the JAAS config, no per-user broker state.
    Plain,
    /// `SCRAM-SHA-256` — credentials stored in the metadata log.
    ScramSha256,
    /// `SCRAM-SHA-512` — credentials stored in the metadata log.
    ScramSha512,
}

impl SaslMechanism {
    /// The name as it appears on the wire and in broker config.
    pub fn as_str(self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    /// Lowercased, as the `listener.name.<listener>.<mechanism>.*` prefix wants it.
    fn config_key(self) -> &'static str {
        match self {
            SaslMechanism::Plain => "plain",
            SaslMechanism::ScramSha256 => "scram-sha-256",
            SaslMechanism::ScramSha512 => "scram-sha-512",
        }
    }

    fn is_scram(self) -> bool {
        matches!(
            self,
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512
        )
    }
}

/// A user the broker will accept.
#[derive(Debug, Clone)]
pub struct SaslUser {
    /// Username.
    pub name: String,
    /// Password, in the clear — these are throwaway fixtures.
    pub password: String,
}

impl SaslUser {
    /// Build a user.
    pub fn new(name: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            password: password.into(),
        }
    }
}

/// How to build a broker or cluster fixture.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    nodes: usize,
    security: Security,
    mechanisms: Vec<SaslMechanism>,
    users: Vec<SaslUser>,
    authorizer: bool,
    auto_create_topics: bool,
    share_groups: bool,
    properties: Vec<(String, String)>,
    features: Vec<String>,
    startup_timeout: Duration,
    image: String,
    tag: String,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            nodes: 1,
            security: Security::Plaintext,
            mechanisms: Vec::new(),
            users: Vec::new(),
            authorizer: false,
            // Kafka's own default is `true`. We invert it because a fixture
            // that creates topics behind the test's back turns "assert this
            // topic does not exist" into a coin flip — and M4's acceptance
            // test is exactly that assertion, so it needs to opt back in
            // explicitly.
            auto_create_topics: false,
            share_groups: false,
            properties: Vec::new(),
            features: Vec::new(),
            startup_timeout: Duration::from_secs(180),
            image: DEFAULT_IMAGE.to_owned(),
            tag: DEFAULT_TAG.to_owned(),
        }
    }
}

impl BrokerConfig {
    /// A single PLAINTEXT broker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of combined broker+controller nodes. Must be at least one.
    pub fn with_nodes(mut self, nodes: usize) -> Self {
        self.nodes = nodes;
        self
    }

    /// Wire security for the external listener.
    pub fn with_security(mut self, security: Security) -> Self {
        self.security = security;
        self
    }

    /// Enable a SASL mechanism on the external listener.
    pub fn with_mechanism(mut self, mechanism: SaslMechanism) -> Self {
        if !self.mechanisms.contains(&mechanism) {
            self.mechanisms.push(mechanism);
        }
        self
    }

    /// Add a user the broker will accept.
    pub fn with_user(mut self, name: impl Into<String>, password: impl Into<String>) -> Self {
        self.users.push(SaslUser::new(name, password));
        self
    }

    /// Turn on the KRaft `StandardAuthorizer`.
    ///
    /// `User:ANONYMOUS` is made a superuser so an unauthenticated test client
    /// can still drive the ACL RPCs. M8 is testing our ACL *encoding*, not
    /// Kafka's enforcement.
    pub fn with_authorizer(mut self, enabled: bool) -> Self {
        self.authorizer = enabled;
        self
    }

    /// Allow the broker to create topics on metadata requests.
    ///
    /// Only M4's acceptance test wants this: it is the fixture half of proving
    /// we never send `allow_auto_topic_creation: true`.
    pub fn with_auto_create_topics(mut self, enabled: bool) -> Self {
        self.auto_create_topics = enabled;
        self
    }

    /// Enable KIP-932 share groups.
    pub fn with_share_groups(mut self, enabled: bool) -> Self {
        self.share_groups = enabled;
        self
    }

    /// Set an arbitrary `server.properties` entry. Later calls win.
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.push((key.into(), value.into()));
        self
    }

    /// Pass `--feature <spec>` to `kafka-storage format`.
    pub fn with_feature(mut self, spec: impl Into<String>) -> Self {
        self.features.push(spec.into());
        self
    }

    /// How long to wait for a node to report itself started.
    pub fn with_startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    /// Use a different broker image — the seam that lets this suite run against
    /// a `kaas` broker instead of Apache Kafka.
    pub fn with_image(mut self, image: impl Into<String>, tag: impl Into<String>) -> Self {
        self.image = image.into();
        self.tag = tag.into();
        self
    }

    /// Node count.
    pub fn nodes(&self) -> usize {
        self.nodes
    }

    /// External listener security.
    pub fn security(&self) -> Security {
        self.security
    }

    /// Configured users.
    pub fn users(&self) -> &[SaslUser] {
        &self.users
    }

    /// Startup timeout per node.
    pub fn startup_timeout(&self) -> Duration {
        self.startup_timeout
    }

    /// Image name.
    pub fn image(&self) -> &str {
        &self.image
    }

    /// Image tag.
    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.nodes == 0 {
            return Err(Error::config("a cluster needs at least one node"));
        }
        if self.security.needs_sasl() && self.mechanisms.is_empty() {
            return Err(Error::config(
                "SASL security was requested but no mechanism was enabled",
            ));
        }
        if self.security.needs_sasl() && self.users.is_empty() {
            return Err(Error::config(
                "SASL security was requested but no user was configured",
            ));
        }
        if !self.security.needs_sasl() && !self.mechanisms.is_empty() {
            return Err(Error::config(
                "SASL mechanisms were configured on a listener that does not use SASL",
            ));
        }
        for user in &self.users {
            if user.name.contains('"') || user.password.contains('"') {
                return Err(Error::config(
                    "fixture credentials may not contain a double quote — the JAAS config is quoted",
                ));
            }
        }
        Ok(())
    }

    /// `--add-scram` arguments for `kafka-storage format`.
    ///
    /// SCRAM credentials in KRaft live in the metadata log, so they have to be
    /// seeded at format time; there is no static-file equivalent of the PLAIN
    /// JAAS block.
    pub(crate) fn scram_format_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        for mechanism in self.mechanisms.iter().filter(|m| m.is_scram()) {
            for user in &self.users {
                args.push("--add-scram".to_owned());
                args.push(format!(
                    "{}=[name={},password={}]",
                    mechanism.as_str(),
                    user.name,
                    user.password
                ));
            }
        }
        args
    }

    /// `--feature` arguments for `kafka-storage format`.
    pub(crate) fn feature_format_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        // KIP-932 share groups are gated behind a cluster feature as well as a
        // broker config; setting only the config leaves ShareGroupDescribe
        // answering UNSUPPORTED_VERSION, which reads like a client bug.
        if self.share_groups
            && !self
                .features
                .iter()
                .any(|f| f.starts_with("share.version="))
        {
            args.push("--feature".to_owned());
            args.push("share.version=1".to_owned());
        }
        for feature in &self.features {
            args.push("--feature".to_owned());
            args.push(feature.clone());
        }
        args
    }

    /// Render `server.properties` for one node.
    ///
    /// `advertised_external` is the address the *test process* must use, which
    /// is only known after the container is running and the port mapping
    /// exists — hence the whole start-script dance in [`crate::image`].
    pub(crate) fn render_properties(
        &self,
        node_id: i32,
        hostname: &str,
        voters: &str,
        advertised_external: &str,
    ) -> String {
        let replication = self.replication_factor();
        let min_isr = replication.min(2);
        let mut props: Vec<(String, String)> = Vec::new();

        let mut set = |key: &str, value: String| props.push((key.to_owned(), value));

        set("process.roles", "broker,controller".to_owned());
        set("node.id", node_id.to_string());
        set("controller.quorum.voters", voters.to_owned());
        set("controller.listener.names", "CONTROLLER".to_owned());
        set("inter.broker.listener.name", "BROKER".to_owned());
        set(
            "listeners",
            format!(
                "EXTERNAL://0.0.0.0:{EXTERNAL_PORT},BROKER://0.0.0.0:{BROKER_PORT},CONTROLLER://0.0.0.0:{CONTROLLER_PORT}"
            ),
        );
        // The controller listener is deliberately absent here: Kafka rejects a
        // configuration that advertises it, and controller endpoints are
        // discovered through `controller.quorum.voters` instead.
        set(
            "advertised.listeners",
            format!("EXTERNAL://{advertised_external},BROKER://{hostname}:{BROKER_PORT}"),
        );
        set(
            "listener.security.protocol.map",
            format!(
                "EXTERNAL:{},BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
                self.security.protocol()
            ),
        );
        set("log.dirs", format!("{WORK_DIR}/logs"));

        set("offsets.topic.replication.factor", replication.to_string());
        set("default.replication.factor", replication.to_string());
        set(
            "transaction.state.log.replication.factor",
            replication.to_string(),
        );
        set("transaction.state.log.min.isr", min_isr.to_string());
        set(
            "share.coordinator.state.topic.replication.factor",
            replication.to_string(),
        );
        set("share.coordinator.state.topic.min.isr", min_isr.to_string());
        set("num.partitions", "1".to_owned());
        set(
            "auto.create.topics.enable",
            self.auto_create_topics.to_string(),
        );
        // Classic groups otherwise sit in a three-second rebalance delay, which
        // every group fixture then pays for.
        set("group.initial.rebalance.delay.ms", "0".to_owned());

        if self.share_groups {
            set(
                "group.coordinator.rebalance.protocols",
                "classic,consumer,share".to_owned(),
            );
        }

        if self.authorizer {
            set(
                "authorizer.class.name",
                "org.apache.kafka.metadata.authorizer.StandardAuthorizer".to_owned(),
            );
            set("allow.everyone.if.no.acl.found", "false".to_owned());
            let mut supers = vec!["User:ANONYMOUS".to_owned()];
            supers.extend(self.users.iter().map(|u| format!("User:{}", u.name)));
            set("super.users", supers.join(";"));
        }

        if self.security.needs_tls() {
            set("ssl.keystore.location", KEYSTORE_PATH.to_owned());
            set("ssl.keystore.password", KEYSTORE_PASSWORD.to_owned());
            set("ssl.key.password", KEYSTORE_PASSWORD.to_owned());
            set("ssl.keystore.type", "PKCS12".to_owned());
            set("ssl.client.auth", "none".to_owned());
        }

        if self.security.needs_sasl() {
            let enabled: Vec<&str> = self.mechanisms.iter().map(|m| m.as_str()).collect();
            set("sasl.enabled.mechanisms", enabled.join(","));
            set(
                "listener.name.external.sasl.enabled.mechanisms",
                enabled.join(","),
            );
            for mechanism in &self.mechanisms {
                set(
                    &format!(
                        "listener.name.external.{}.sasl.jaas.config",
                        mechanism.config_key()
                    ),
                    self.jaas_config(*mechanism),
                );
            }
        }

        // User overrides go last so `with_property` genuinely wins.
        for (key, value) in &self.properties {
            props.push((key.clone(), value.clone()));
        }

        let mut out = String::from("# generated by kaas-lib testkit — do not edit\n");
        for (key, value) in props {
            out.push_str(&key);
            out.push('=');
            out.push_str(&value);
            out.push('\n');
        }
        out
    }

    fn jaas_config(&self, mechanism: SaslMechanism) -> String {
        match mechanism {
            SaslMechanism::Plain => {
                // PLAIN has no broker-side credential store: the users live in
                // the JAAS block itself, as `user_<name>="<password>"`.
                let mut cfg = String::from(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required",
                );
                for user in &self.users {
                    cfg.push_str(&format!(" user_{}=\"{}\"", user.name, user.password));
                }
                cfg.push(';');
                cfg
            }
            // SCRAM reads from the metadata log, seeded by `--add-scram`.
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                "org.apache.kafka.common.security.scram.ScramLoginModule required;".to_owned()
            }
        }
    }

    fn replication_factor(&self) -> usize {
        self.nodes.min(3)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(cfg: &BrokerConfig) -> String {
        cfg.render_properties(1, "node-1", "1@node-1:9094", "localhost:32770")
    }

    #[test]
    fn plaintext_defaults_do_not_auto_create_topics() {
        let rendered = props(&BrokerConfig::new());
        assert!(rendered.contains("auto.create.topics.enable=false"));
    }

    #[test]
    fn controller_listener_is_never_advertised() {
        let rendered = props(&BrokerConfig::new());
        let advertised = rendered
            .lines()
            .find(|l| l.starts_with("advertised.listeners="))
            .unwrap_or_default();
        assert!(!advertised.contains("CONTROLLER"), "{advertised}");
        assert!(advertised.contains("EXTERNAL://localhost:32770"));
    }

    #[test]
    fn replication_factor_tracks_node_count_up_to_three() {
        for (nodes, expected) in [(1usize, "1"), (2, "2"), (3, "3"), (5, "3")] {
            let cfg = BrokerConfig::new().with_nodes(nodes);
            let rendered = props(&cfg);
            assert!(
                rendered.contains(&format!("offsets.topic.replication.factor={expected}\n")),
                "nodes={nodes}"
            );
        }
    }

    #[test]
    fn plain_users_land_in_the_jaas_block() {
        let cfg = BrokerConfig::new()
            .with_security(Security::SaslPlaintext)
            .with_mechanism(SaslMechanism::Plain)
            .with_user("alice", "alice-pw");
        let rendered = props(&cfg);
        assert!(rendered.contains("listener.name.external.plain.sasl.jaas.config="));
        assert!(rendered.contains("user_alice=\"alice-pw\""));
        assert!(rendered.contains("listener.security.protocol.map=EXTERNAL:SASL_PLAINTEXT"));
    }

    #[test]
    fn scram_users_are_seeded_at_format_time_not_in_jaas() {
        let cfg = BrokerConfig::new()
            .with_security(Security::SaslSsl)
            .with_mechanism(SaslMechanism::ScramSha512)
            .with_user("bob", "bob-pw");
        let rendered = props(&cfg);
        assert!(!rendered.contains("bob-pw"));
        assert_eq!(
            cfg.scram_format_args(),
            vec![
                "--add-scram".to_string(),
                "SCRAM-SHA-512=[name=bob,password=bob-pw]".to_string()
            ]
        );
        assert!(rendered.contains("ssl.keystore.location="));
    }

    #[test]
    fn share_groups_set_both_the_config_and_the_feature() {
        let cfg = BrokerConfig::new().with_share_groups(true);
        assert!(
            props(&cfg).contains("group.coordinator.rebalance.protocols=classic,consumer,share")
        );
        assert_eq!(
            cfg.feature_format_args(),
            vec!["--feature".to_string(), "share.version=1".to_string()]
        );
    }

    #[test]
    fn explicit_properties_override_generated_ones() {
        let cfg = BrokerConfig::new().with_property("num.partitions", "6");
        let rendered = props(&cfg);
        let last = rendered.lines().rfind(|l| l.starts_with("num.partitions="));
        assert_eq!(last, Some("num.partitions=6"));
    }

    #[test]
    fn sasl_without_a_mechanism_is_rejected() {
        let cfg = BrokerConfig::new().with_security(Security::SaslPlaintext);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn zero_nodes_is_rejected() {
        assert!(BrokerConfig::new().with_nodes(0).validate().is_err());
    }
}
