//! Where to point a live run, and how it was configured.
//!
//! Deliberately knows nothing about Kubernetes. Resolving a service into a
//! bootstrap address is the `live-cluster` skill's job, and keeping that out of
//! here means the same binary works against a port-forward, a laptop broker, or
//! a cluster in another account — and that a cluster's shape cannot leak into
//! the tool as a hardcoded assumption.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use kafka_conn::{
    ConnectionConfig, OidcConfig, OidcTokenProvider, SaslConfig, SaslMechanism, TlsConfig,
};
use kafka_meta::{ClusterConfig, RetryPolicy};

/// Environment variable holding comma-separated bootstrap addresses.
///
/// The same name `testkit::ExternalCluster::from_env` reads, so a fixture and a
/// live run are configured identically.
pub const BOOTSTRAP_ENV: &str = "KAAS_TEST_BOOTSTRAP";
/// A label for the cluster, used in reports and in artefact filenames.
pub const LABEL_ENV: &str = "KAAS_TEST_LABEL";
/// PEM-encoded CA bundle to trust, inline.
pub const CA_PEM_ENV: &str = "KAAS_TEST_CA_PEM";
/// Path to a PEM-encoded CA bundle.
pub const CA_FILE_ENV: &str = "KAAS_TEST_CA_FILE";
/// Name to verify the broker certificate against, overriding the host.
pub const TLS_SERVER_NAME_ENV: &str = "KAAS_TEST_TLS_SERVER_NAME";
/// SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` or `OAUTHBEARER`.
pub const SASL_MECHANISM_ENV: &str = "KAAS_TEST_SASL_MECHANISM";
/// SASL username.
pub const SASL_USERNAME_ENV: &str = "KAAS_TEST_SASL_USERNAME";
/// SASL password.
pub const SASL_PASSWORD_ENV: &str = "KAAS_TEST_SASL_PASSWORD";
/// `OAUTHBEARER`: a bearer token the caller has already obtained.
pub const OAUTH_TOKEN_ENV: &str = "KAAS_TEST_OAUTH_TOKEN";
/// `OAUTHBEARER`: an OIDC token endpoint to fetch tokens from instead.
pub const OAUTH_TOKEN_ENDPOINT_ENV: &str = "KAAS_TEST_OAUTH_TOKEN_ENDPOINT";
/// `OAUTHBEARER`: the OAuth client id.
pub const OAUTH_CLIENT_ID_ENV: &str = "KAAS_TEST_OAUTH_CLIENT_ID";
/// `OAUTHBEARER`: the OAuth client secret.
pub const OAUTH_CLIENT_SECRET_ENV: &str = "KAAS_TEST_OAUTH_CLIENT_SECRET";
/// `OAUTHBEARER`: the scope to request, if the issuer wants one.
pub const OAUTH_SCOPE_ENV: &str = "KAAS_TEST_OAUTH_SCOPE";
/// `OAUTHBEARER`: the audience to request, if the issuer wants one.
pub const OAUTH_AUDIENCE_ENV: &str = "KAAS_TEST_OAUTH_AUDIENCE";
/// Set to `1` to refuse every mutating api key.
pub const READ_ONLY_ENV: &str = "KAAS_TEST_READ_ONLY";
/// Prefix for topics and groups this tool creates.
pub const PREFIX_ENV: &str = "KAAS_TEST_PREFIX";

/// The default prefix for anything a live run creates.
///
/// Everything is namespaced and swept up afterwards, because these clusters are
/// shared and long-lived: a test that leaves `orders` behind has broken the
/// next person's afternoon, and a test that *deletes* `orders` has broken
/// something worse.
pub const DEFAULT_PREFIX: &str = "kaaslib-live";

/// A cluster to run against.
#[derive(Debug, Clone)]
pub struct Target {
    /// Human-readable label — `strimzi`, `kaas`, whatever the caller chose.
    pub label: String,
    /// Bootstrap addresses.
    pub bootstrap: Vec<String>,
    /// Prefix for created topics and groups.
    pub prefix: String,
    /// Whether this run may mutate the cluster.
    pub read_only: bool,
    connection: ConnectionConfig,
}

impl Target {
    /// Build a target from the environment.
    pub fn from_env() -> Result<Self> {
        let raw = std::env::var(BOOTSTRAP_ENV).with_context(|| {
            format!(
                "{BOOTSTRAP_ENV} is not set. The live-cluster skill resolves it from \
                 Kubernetes; see .claude/skills/live-cluster/SKILL.md"
            )
        })?;
        let bootstrap: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect();
        if bootstrap.is_empty() {
            bail!("{BOOTSTRAP_ENV} is set but empty");
        }

        let label = std::env::var(LABEL_ENV).unwrap_or_else(|_| {
            // Fall back to the first address's host, which is usually the
            // service name and therefore already the cluster's name.
            bootstrap
                .first()
                .and_then(|addr| addr.split(':').next())
                .and_then(|host| host.split('.').next())
                .unwrap_or("unknown")
                .to_owned()
        });

        let read_only = matches!(
            std::env::var(READ_ONLY_ENV).as_deref(),
            Ok("1" | "true" | "yes")
        );
        let prefix = std::env::var(PREFIX_ENV).unwrap_or_else(|_| DEFAULT_PREFIX.to_owned());

        let mut connection = ConnectionConfig::new()
            .with_client_id(format!("kaas-lib-livetest/{label}"))
            // Generous: a shared cluster under someone else's benchmark is
            // slow, not broken, and a tight timeout turns their load test into
            // our failure.
            .with_connect_timeout(Duration::from_secs(20))
            .with_request_timeout(Duration::from_secs(30));
        if read_only {
            connection = connection.read_only();
        }

        if let Some(tls) = tls_from_env()? {
            connection = connection.with_tls(tls);
        }
        if let Some(sasl) = sasl_from_env()? {
            connection = connection.with_sasl(sasl);
        }

        Ok(Self {
            label,
            bootstrap,
            prefix,
            read_only,
            connection,
        })
    }

    /// The connection settings this target resolved to.
    pub fn connection(&self) -> &ConnectionConfig {
        &self.connection
    }

    /// Cluster settings for a live run.
    pub fn cluster_config(&self) -> ClusterConfig {
        ClusterConfig {
            connection: self.connection.clone(),
            // A shared cluster genuinely does move leaders under us, and a
            // reassignment or a rolling restart is a normal Tuesday. Retrying a
            // little harder than the default is the difference between a real
            // finding and a flake.
            retry: RetryPolicy {
                max_attempts: 6,
                ..RetryPolicy::default()
            },
            refresh_interval: Duration::from_secs(30),
            max_staleness: Duration::from_secs(60),
        }
    }

    /// A name inside this run's namespace.
    ///
    /// `<prefix>-<kind>-<unique>`: unique per run so two runs against the same
    /// cluster cannot collide, and prefixed so the sweeper can find everything
    /// a crashed run left behind.
    pub fn scoped_name(&self, kind: &str, unique: &str) -> String {
        format!("{}-{kind}-{unique}", self.prefix)
    }

    /// Whether a name belongs to this tool.
    ///
    /// The sweeper's safety catch: nothing without this prefix is ever deleted.
    pub fn owns(&self, name: &str) -> bool {
        owns(&self.prefix, name)
    }

    /// Refuse to continue when a run needs to write and was told not to.
    pub fn require_writable(&self, what: &str) -> Result<()> {
        if self.read_only {
            bail!("{what} needs to create resources, but {READ_ONLY_ENV} is set");
        }
        Ok(())
    }
}

fn tls_from_env() -> Result<Option<TlsConfig>> {
    let pem = match (
        std::env::var(CA_PEM_ENV).ok(),
        std::env::var(CA_FILE_ENV).ok(),
    ) {
        (Some(pem), _) if !pem.trim().is_empty() => Some(pem.into_bytes()),
        (_, Some(path)) if !path.trim().is_empty() => {
            Some(std::fs::read(&path).with_context(|| format!("reading {CA_FILE_ENV} at {path}"))?)
        }
        _ => None,
    };
    let Some(pem) = pem else {
        return Ok(None);
    };

    let mut tls = TlsConfig::with_ca_pem(pem);
    if let Ok(name) = std::env::var(TLS_SERVER_NAME_ENV)
        && !name.trim().is_empty()
    {
        // Strimzi's broker certificates carry the *service* name, so a run that
        // connects by cluster IP or through a forwarded port has to say which
        // name it expects. Without this the handshake fails with a name
        // mismatch that reads like a broken certificate.
        tls = tls.with_server_name(name);
    }
    Ok(Some(tls))
}

fn sasl_from_env() -> Result<Option<SaslConfig>> {
    let Ok(mechanism) = std::env::var(SASL_MECHANISM_ENV) else {
        return Ok(None);
    };
    if mechanism.trim().is_empty() {
        return Ok(None);
    }

    let mechanism = match mechanism.to_ascii_uppercase().as_str() {
        "PLAIN" => SaslMechanism::Plain,
        "SCRAM-SHA-256" | "SCRAM_SHA_256" => SaslMechanism::ScramSha256,
        "SCRAM-SHA-512" | "SCRAM_SHA_512" => SaslMechanism::ScramSha512,
        "OAUTHBEARER" => return oauth_bearer_from_env().map(Some),
        other => bail!("unknown {SASL_MECHANISM_ENV}: {other}"),
    };
    let username = std::env::var(SASL_USERNAME_ENV)
        .with_context(|| format!("{SASL_MECHANISM_ENV} is set, so {SASL_USERNAME_ENV} must be"))?;
    let password = std::env::var(SASL_PASSWORD_ENV)
        .with_context(|| format!("{SASL_MECHANISM_ENV} is set, so {SASL_PASSWORD_ENV} must be"))?;

    let config = SaslConfig::new(mechanism, username, password);
    // PLAIN over an unencrypted socket is refused unless asked for. A live run
    // against a listener that offers it is a legitimate reason to ask, and the
    // CA settings are right there in the same environment to show whether the
    // socket is encrypted.
    let config = if matches!(mechanism, SaslMechanism::Plain)
        && std::env::var(CA_PEM_ENV).is_err()
        && std::env::var(CA_FILE_ENV).is_err()
    {
        config.allow_plaintext_password()
    } else {
        config
    };
    Ok(Some(config))
}

/// `OAUTHBEARER`: either a token the caller already has, or the
/// `client_credentials` flow against a real issuer.
///
/// Both are worth having here. A pre-fetched token is the cheapest way to point
/// a live run at an OAuth-secured listener; the issuer path is the only way to
/// exercise the refresh, which is the half that fails hours in rather than at
/// connect.
fn oauth_bearer_from_env() -> Result<SaslConfig> {
    let plaintext = std::env::var(CA_PEM_ENV).is_err() && std::env::var(CA_FILE_ENV).is_err();

    if let Ok(token) = std::env::var(OAUTH_TOKEN_ENV)
        && !token.trim().is_empty()
    {
        let config = SaslConfig::oauth_bearer_token(token.trim());
        return Ok(if plaintext {
            config.allow_plaintext_password()
        } else {
            config
        });
    }

    let endpoint = std::env::var(OAUTH_TOKEN_ENDPOINT_ENV).with_context(|| {
        format!(
            "{SASL_MECHANISM_ENV}=OAUTHBEARER needs either {OAUTH_TOKEN_ENV} or \
             {OAUTH_TOKEN_ENDPOINT_ENV} + {OAUTH_CLIENT_ID_ENV} + {OAUTH_CLIENT_SECRET_ENV}"
        )
    })?;
    let client_id = std::env::var(OAUTH_CLIENT_ID_ENV).with_context(|| {
        format!("{OAUTH_TOKEN_ENDPOINT_ENV} is set, so {OAUTH_CLIENT_ID_ENV} must be")
    })?;
    let client_secret = std::env::var(OAUTH_CLIENT_SECRET_ENV).with_context(|| {
        format!("{OAUTH_TOKEN_ENDPOINT_ENV} is set, so {OAUTH_CLIENT_SECRET_ENV} must be")
    })?;

    let oidc = OidcConfig::new(endpoint, client_id, client_secret)
        .with_maybe_scope(
            std::env::var(OAUTH_SCOPE_ENV)
                .ok()
                .filter(|s| !s.is_empty()),
        )
        .with_maybe_audience(
            std::env::var(OAUTH_AUDIENCE_ENV)
                .ok()
                .filter(|s| !s.is_empty()),
        );
    let config = SaslConfig::oauth_bearer(OidcTokenProvider::new(oidc)?);
    Ok(if plaintext {
        config.allow_plaintext_password()
    } else {
        config
    })
}

/// Whether `name` was created by a run using `prefix`.
///
/// A free function so the sweeper's safety catch can be tested without a
/// cluster, a connection, or a mutated process environment. It is the most
/// destructive decision in this crate and deserves to be the most directly
/// testable one.
pub fn owns(prefix: &str, name: &str) -> bool {
    // The separator is part of the test: `kaaslib-live` must not own
    // `kaaslib-liveness-probe`.
    name.starts_with(&format!("{prefix}-"))
}

/// A short unique token, for names that must not collide between runs.
pub fn run_token() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or_default();
    format!("{:x}{:x}", std::process::id(), nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(prefix: &str) -> Target {
        Target {
            label: "test".to_owned(),
            bootstrap: vec!["localhost:9092".to_owned()],
            prefix: prefix.to_owned(),
            read_only: false,
            connection: ConnectionConfig::new(),
        }
    }

    #[test]
    fn scoped_names_are_owned_and_unscoped_ones_are_not() {
        let target = target("kaaslib-live");
        let name = target.scoped_name("topic", "abc123");
        assert_eq!(name, "kaaslib-live-topic-abc123");
        assert!(target.owns(&name));

        // The safety catch. These clusters are shared and long-lived, and the
        // sweeper must never touch anything it did not create.
        assert!(!target.owns("orders"));
        assert!(!target.owns("__consumer_offsets"));
        assert!(!target.owns("kperf-bench"));
        // Not even a name that merely starts with the same letters.
        assert!(!target.owns("kaaslib-liveness-probe"));
    }

    #[test]
    fn a_read_only_target_refuses_to_create_anything() {
        let mut target = target("kaaslib-live");
        target.read_only = true;
        assert!(target.require_writable("the smoke suite").is_err());
        target.read_only = false;
        assert!(target.require_writable("the smoke suite").is_ok());
    }

    #[test]
    fn run_tokens_differ_between_runs() {
        assert_ne!(run_token(), run_token());
    }
}
