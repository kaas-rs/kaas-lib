//! The container image definition, and the start-script dance it needs.
//!
//! # Why the container does not just run Kafka
//!
//! A broker has to advertise an address its clients can reach. For a container
//! fixture that address contains the *host-mapped* port, which does not exist
//! until the container is already running — a chicken-and-egg problem every
//! Kafka test harness has to solve somehow.
//!
//! The way out: the container's command is a poll loop waiting for a start
//! script that does not exist yet. Once the container is up, testcontainers
//! calls [`Image::exec_before_ready`] with the port mapping in hand, we render
//! the script (advertised address and all) and drop it in, the poll loop picks
//! it up, and only then does Kafka boot. The readiness wait happens after all
//! of that, so a test never sees a half-configured broker.
//!
//! The script is written to a temporary name and `mv`d into place, because
//! `mv` within a filesystem is atomic and `cat >` is not — without that, the
//! poll loop can win the race against the write and source a truncated file.

use std::borrow::Cow;

use testcontainers::{
    Image,
    core::{CmdWaitFor, ContainerPort, ContainerState, ExecCommand, WaitFor},
};

use crate::config::{
    BrokerConfig, CA_PEM_PATH, CLIENTS_CA_PEM_PATH, EXTERNAL_PORT, KEYSTORE_PASSWORD,
    KEYSTORE_PATH, TRUSTSTORE_PATH, WORK_DIR,
};

/// Where the generated start script lands inside the container.
const START_SCRIPT: &str = "/tmp/kaas-testkit-start.sh";

/// A valid base64url-encoded 16-byte KRaft cluster id.
///
/// Fixed rather than random: every node of a cluster must agree on it, and
/// clusters are already isolated from each other by their own docker network,
/// so uniqueness buys nothing and reproducibility is worth having.
const CLUSTER_ID: &str = "4L6g3nShT-eMCtK--X86sw";

/// The log line the broker prints once it is serving.
const READY_LOG: &str = "Kafka Server started";

/// One node of a fixture cluster.
#[derive(Debug, Clone)]
pub(crate) struct KafkaImage {
    config: BrokerConfig,
    node_id: i32,
    hostname: String,
    voters: String,
    /// The clients CA certificate, when this fixture verifies client
    /// certificates. Generated once per cluster so every node trusts the same
    /// client — a per-node CA would make a certificate work against whichever
    /// broker happened to answer first.
    clients_ca_pem: Option<String>,
    cmd: Vec<String>,
    env: Vec<(String, String)>,
    ports: Vec<ContainerPort>,
}

impl KafkaImage {
    pub(crate) fn new(
        config: BrokerConfig,
        node_id: i32,
        hostname: String,
        voters: String,
        clients_ca_pem: Option<String>,
    ) -> Self {
        Self {
            config,
            node_id,
            hostname,
            voters,
            clients_ca_pem,
            cmd: vec![
                "-c".to_owned(),
                format!(
                    "while [ ! -f {START_SCRIPT} ]; do sleep 0.2; done; exec bash {START_SCRIPT}"
                ),
            ],
            env: vec![(
                "KAFKA_HEAP_OPTS".to_owned(),
                // Three JVMs on a CI box with default heap sizing is how a
                // cluster fixture turns into an OOM kill.
                "-Xmx512M -Xms256M".to_owned(),
            )],
            ports: vec![ContainerPort::Tcp(EXTERNAL_PORT)],
        }
    }

    /// The script that configures and launches the broker.
    fn start_script(&self, advertised_external: &str) -> String {
        let properties = self.config.render_properties(
            self.node_id,
            &self.hostname,
            &self.voters,
            advertised_external,
        );

        let tls = if self.config.security().needs_tls() {
            self.tls_setup()
        } else {
            String::new()
        };

        let mut format_args = vec![
            "-t".to_owned(),
            CLUSTER_ID.to_owned(),
            "-c".to_owned(),
            format!("{WORK_DIR}/server.properties"),
            "--ignore-formatted".to_owned(),
        ];
        format_args.extend(self.config.scram_format_args());
        format_args.extend(self.config.feature_format_args());
        let format_args = shell_join(&format_args);

        format!(
            "set -eo pipefail\n\
             mkdir -p {WORK_DIR}\n\
             cat > {WORK_DIR}/server.properties <<'KAAS_PROPS_EOF'\n\
             {properties}\
             KAAS_PROPS_EOF\n\
             {tls}\
             /opt/kafka/bin/kafka-storage.sh format {format_args}\n\
             exec /opt/kafka/bin/kafka-server-start.sh {WORK_DIR}/server.properties\n"
        )
    }

    /// Generate a throwaway CA and a broker certificate signed by it.
    ///
    /// A self-signed *leaf* would be simpler, but rustls will not accept one as
    /// a trust anchor — anchors have to carry `basicConstraints:CA`. So: a real
    /// two-level chain, built with the `keytool` that ships in every JRE, and
    /// the CA exported as PEM for the test process to trust.
    fn tls_setup(&self) -> String {
        let san = format!("DNS:localhost,DNS:{},IP:127.0.0.1", self.hostname);
        let pw = KEYSTORE_PASSWORD;
        let truststore = self.truststore_setup();
        format!(
            "KEYTOOL=keytool\n\
             command -v keytool >/dev/null 2>&1 || KEYTOOL=\"${{JAVA_HOME:-/opt/java/openjdk}}/bin/keytool\"\n\
             \"$KEYTOOL\" -genkeypair -alias ca -keyalg RSA -keysize 2048 -validity 3650 \
             -dname 'CN=kaas-testkit-ca,O=kaas-testkit' -ext bc:c \
             -keystore {WORK_DIR}/ca.p12 -storetype PKCS12 -storepass {pw} -keypass {pw}\n\
             \"$KEYTOOL\" -exportcert -rfc -alias ca -keystore {WORK_DIR}/ca.p12 -storepass {pw} \
             -file {CA_PEM_PATH}\n\
             \"$KEYTOOL\" -genkeypair -alias broker -keyalg RSA -keysize 2048 -validity 3650 \
             -dname 'CN=localhost,O=kaas-testkit' -ext 'SAN={san}' \
             -keystore {KEYSTORE_PATH} -storetype PKCS12 -storepass {pw} -keypass {pw}\n\
             \"$KEYTOOL\" -certreq -alias broker -keystore {KEYSTORE_PATH} -storepass {pw} \
             -file {WORK_DIR}/broker.csr\n\
             \"$KEYTOOL\" -gencert -alias ca -keystore {WORK_DIR}/ca.p12 -storepass {pw} \
             -infile {WORK_DIR}/broker.csr -outfile {WORK_DIR}/broker.pem -rfc -validity 3650 \
             -ext 'SAN={san}'\n\
             \"$KEYTOOL\" -importcert -noprompt -alias ca -keystore {KEYSTORE_PATH} \
             -storepass {pw} -file {CA_PEM_PATH}\n\
             \"$KEYTOOL\" -importcert -noprompt -alias broker -keystore {KEYSTORE_PATH} \
             -storepass {pw} -file {WORK_DIR}/broker.pem\n\
             {truststore}"
        )
    }

    /// Import the clients CA into a truststore, so the broker will verify a
    /// client certificate that CA issued.
    ///
    /// The certificate is written from here rather than generated in the
    /// container: `keytool` cannot export a private key and the test process
    /// needs one, so the whole client half is generated in Rust and only its
    /// CA certificate crosses over. See [`crate::certs`].
    fn truststore_setup(&self) -> String {
        let Some(clients_ca) = &self.clients_ca_pem else {
            return String::new();
        };
        let pw = KEYSTORE_PASSWORD;
        format!(
            "cat > {CLIENTS_CA_PEM_PATH} <<'KAAS_CLIENTS_CA_EOF'\n\
             {clients_ca}\n\
             KAAS_CLIENTS_CA_EOF\n\
             \"$KEYTOOL\" -importcert -noprompt -alias clients-ca -keystore {TRUSTSTORE_PATH} \
             -storetype PKCS12 -storepass {pw} -file {CLIENTS_CA_PEM_PATH}\n"
        )
    }
}

impl Image for KafkaImage {
    fn name(&self) -> &str {
        self.config.image()
    }

    fn tag(&self) -> &str {
        self.config.tag()
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        // Either stream: the image has moved its log4j appender between
        // releases, and a fixture that hangs for the full startup timeout
        // because it watched the wrong stream is a miserable thing to debug.
        vec![WaitFor::message_on_either_std(READY_LOG)]
    }

    fn entrypoint(&self) -> Option<&str> {
        // Replaces the image's own `/etc/kafka/docker/run`, which would
        // configure the broker from `KAFKA_*` environment variables. We render
        // `server.properties` ourselves — see the module docs on config.rs.
        Some("bash")
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        self.cmd.iter().map(String::as_str)
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        self.env.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &self.ports
    }

    fn exec_before_ready(
        &self,
        cs: ContainerState,
    ) -> testcontainers::core::error::Result<Vec<ExecCommand>> {
        let port = cs.host_port_ipv4(ContainerPort::Tcp(EXTERNAL_PORT))?;
        let advertised = format!("{}:{}", cs.host(), port);
        let script = self.start_script(&advertised);

        let installer = format!(
            "set -e\n\
             mkdir -p {WORK_DIR}\n\
             cat > {START_SCRIPT}.tmp <<'KAAS_START_EOF'\n\
             {script}\
             KAAS_START_EOF\n\
             mv {START_SCRIPT}.tmp {START_SCRIPT}\n"
        );

        Ok(vec![
            ExecCommand::new(["bash".to_owned(), "-c".to_owned(), installer])
                .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
        ])
    }
}

/// Single-quote arguments for embedding in the generated shell script.
fn shell_join(args: &[String]) -> String {
    args.iter()
        .map(|a| format!("'{}'", a.replace('\'', r"'\''")))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SaslMechanism, Security};

    fn image(config: BrokerConfig) -> KafkaImage {
        KafkaImage::new(
            config,
            1,
            "node-1".to_owned(),
            "1@node-1:9094".to_owned(),
            None,
        )
    }

    fn image_with_clients_ca(config: BrokerConfig) -> KafkaImage {
        KafkaImage::new(
            config,
            1,
            "node-1".to_owned(),
            "1@node-1:9094".to_owned(),
            Some("-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----".to_owned()),
        )
    }

    /// #27: the truststore the broker verifies *client* certificates with is
    /// built from a CA the container never generates — it arrives from the
    /// Rust side, because keytool cannot export the private key the test
    /// process needs.
    #[test]
    fn client_auth_imports_the_clients_ca_into_a_truststore() {
        let cfg = BrokerConfig::new()
            .with_security(Security::Ssl)
            .with_client_auth(crate::config::ClientAuth::Required);
        let script = image_with_clients_ca(cfg).start_script("127.0.0.1:1");

        assert!(script.contains("ssl.client.auth=required"), "{script}");
        assert!(script.contains(&format!("ssl.truststore.location={TRUSTSTORE_PATH}")));
        assert!(script.contains("-alias clients-ca"), "{script}");
        assert!(
            script.contains("-----BEGIN CERTIFICATE-----"),
            "the clients CA must reach the container: {script}"
        );
        // The two anchors must stay distinct, which is the mix-up the whole
        // fixture exists to catch.
        assert_ne!(CA_PEM_PATH, CLIENTS_CA_PEM_PATH);
        assert!(script.contains(CA_PEM_PATH));
        assert!(script.contains(CLIENTS_CA_PEM_PATH));
    }

    /// And the default stays exactly as it was: no truststore, no client
    /// certificate asked for.
    #[test]
    fn without_client_auth_the_listener_asks_for_nothing() {
        let cfg = BrokerConfig::new().with_security(Security::Ssl);
        let script = image(cfg).start_script("127.0.0.1:1");
        assert!(script.contains("ssl.client.auth=none"), "{script}");
        assert!(!script.contains("ssl.truststore.location"), "{script}");
        assert!(!script.contains("clients-ca"), "{script}");
    }

    #[test]
    fn start_script_installs_atomically() {
        // The poll loop tests for the final path, so the script must never
        // exist there in a partial state.
        let img = image(BrokerConfig::new());
        let cmd = img.cmd.join(" ");
        assert!(cmd.contains(&format!("[ ! -f {START_SCRIPT} ]")));
    }

    #[test]
    fn start_script_carries_the_advertised_address_through() {
        let script = image(BrokerConfig::new()).start_script("127.0.0.1:49999");
        assert!(script.contains("advertised.listeners=EXTERNAL://127.0.0.1:49999"));
        assert!(script.contains("kafka-storage.sh format"));
        assert!(script.contains("exec /opt/kafka/bin/kafka-server-start.sh"));
    }

    #[test]
    fn heredoc_delimiters_do_not_collide_with_content() {
        let script = image(BrokerConfig::new()).start_script("127.0.0.1:1");
        // The properties heredoc lives inside the installer heredoc; if either
        // delimiter appeared in the payload the whole thing would silently
        // truncate.
        assert!(!script.contains("KAAS_START_EOF"));
        assert_eq!(script.matches("KAAS_PROPS_EOF").count(), 2);
    }

    #[test]
    fn scram_credentials_reach_the_format_command() {
        let cfg = BrokerConfig::new()
            .with_security(Security::SaslPlaintext)
            .with_mechanism(SaslMechanism::ScramSha512)
            .with_user("alice", "alice-pw");
        let script = image(cfg).start_script("127.0.0.1:1");
        assert!(script.contains("'--add-scram' 'SCRAM-SHA-512=[name=alice,password=alice-pw]'"));
    }

    #[test]
    fn tls_setup_only_appears_when_tls_is_asked_for() {
        assert!(
            !image(BrokerConfig::new())
                .start_script("h:1")
                .contains("keytool")
        );

        let cfg = BrokerConfig::new().with_security(Security::Ssl);
        let script = image(cfg).start_script("h:1");
        assert!(script.contains("-ext bc:c"), "the CA must be a CA");
        assert!(script.contains("SAN=DNS:localhost,DNS:node-1,IP:127.0.0.1"));
        assert!(script.contains(CA_PEM_PATH));
    }

    #[test]
    fn shell_join_escapes_quotes() {
        assert_eq!(shell_join(&["a b".to_owned()]), "'a b'");
        assert_eq!(shell_join(&["it's".to_owned()]), r"'it'\''s'");
    }
}
