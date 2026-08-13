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
/// PEM-encoded client certificate chain for mutual TLS, inline.
pub const CLIENT_CERT_PEM_ENV: &str = "KAAS_TEST_CLIENT_CERT_PEM";
/// Path to a PEM-encoded client certificate chain.
pub const CLIENT_CERT_FILE_ENV: &str = "KAAS_TEST_CLIENT_CERT_FILE";
/// PEM-encoded private key for the client certificate, inline.
pub const CLIENT_KEY_PEM_ENV: &str = "KAAS_TEST_CLIENT_KEY_PEM";
/// Path to a PEM-encoded private key for the client certificate.
pub const CLIENT_KEY_FILE_ENV: &str = "KAAS_TEST_CLIENT_KEY_FILE";
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

        let tls = tls_from_env()?;
        let encrypted = tls.is_some();
        if let Some(tls) = tls {
            connection = connection.with_tls(tls);
        }
        if let Some(sasl) = sasl_from_env(encrypted)? {
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

    /// These settings with the client certificate removed, or `None` when no
    /// client certificate is configured.
    ///
    /// The negative half of mutual TLS: connecting with this against an mTLS
    /// listener must fail as an authentication error, which is how a probe
    /// shows the listener *requires* the certificate rather than merely
    /// tolerating one.
    pub fn connection_without_client_certificate(&self) -> Option<ConnectionConfig> {
        let tls = self.connection.tls.as_deref()?;
        tls.client_certificate.as_ref()?;
        let mut stripped = tls.clone();
        stripped.client_certificate = None;
        let mut connection = self.connection.clone();
        connection.tls = Some(std::sync::Arc::new(stripped));
        Some(connection)
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

/// A PEM either inline in one variable or in a file named by another.
///
/// One helper rather than the same six lines per credential: the CA had them,
/// and a client certificate and its key would have made three copies of a
/// fallback whose two halves have to agree about what "set but empty" means.
fn pem_from_env(inline: &str, file: &str) -> Result<Option<Vec<u8>>> {
    if let Ok(pem) = std::env::var(inline)
        && !pem.trim().is_empty()
    {
        return Ok(Some(pem.into_bytes()));
    }
    if let Ok(path) = std::env::var(file)
        && !path.trim().is_empty()
    {
        return Ok(Some(
            std::fs::read(&path).with_context(|| format!("reading {file} at {path}"))?,
        ));
    }
    Ok(None)
}

/// A client certificate is both halves or neither.
///
/// Named here rather than left to the handshake: a chain without a key falls
/// through to a server-auth connection, which the broker refuses with
/// `SSLHandshakeException` and no hint about which half is missing — and the
/// operator, who set two of the four variables, has no reason to suspect the
/// one they got wrong.
fn client_certificate(
    chain: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    match (chain, key) {
        (Some(chain), Some(key)) => Ok(Some((chain, key))),
        (None, None) => Ok(None),
        (Some(_), None) => bail!(
            "a client certificate was configured without its key; set \
             {CLIENT_KEY_PEM_ENV} or {CLIENT_KEY_FILE_ENV} too"
        ),
        (None, Some(_)) => bail!(
            "a client key was configured without its certificate; set \
             {CLIENT_CERT_PEM_ENV} or {CLIENT_CERT_FILE_ENV} too"
        ),
    }
}

fn tls_from_env() -> Result<Option<TlsConfig>> {
    let ca = pem_from_env(CA_PEM_ENV, CA_FILE_ENV)?;
    let chain = pem_from_env(CLIENT_CERT_PEM_ENV, CLIENT_CERT_FILE_ENV)?;
    let key = pem_from_env(CLIENT_KEY_PEM_ENV, CLIENT_KEY_FILE_ENV)?;

    let client = client_certificate(chain, key)?;

    // A client certificate is reason enough to speak TLS: an mTLS listener
    // whose broker certificate chains to a public CA needs no CA of ours.
    let mut tls = match ca {
        Some(pem) => TlsConfig::with_ca_pem(pem),
        None if client.is_some() => TlsConfig::system(),
        None => return Ok(None),
    };

    if let Some((chain, key)) = client {
        tls = tls.with_client_certificate(chain, key);
    }

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

fn sasl_from_env(encrypted: bool) -> Result<Option<SaslConfig>> {
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
    // against a listener that offers it is a legitimate reason to ask, and
    // whether this run resolved any TLS at all is what says whether the socket
    // is encrypted — asked of the resolved configuration rather than of the
    // environment, so the two cannot disagree.
    let config = if matches!(mechanism, SaslMechanism::Plain) && !encrypted {
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
    let config = match std::env::var(OAUTH_TOKEN_ENV)
        .ok()
        .filter(|token| !token.trim().is_empty())
    {
        Some(token) => SaslConfig::oauth_bearer_token(token.trim()),
        None => SaslConfig::oauth_bearer(OidcTokenProvider::new(oidc_from_env()?)?),
    };

    // One gate for both paths, applied once here rather than on each branch: a
    // bearer token is as reusable as a password, and the two ways of obtaining
    // one must never disagree about when it may go out in the clear.
    Ok(
        if std::env::var(CA_PEM_ENV).is_err() && std::env::var(CA_FILE_ENV).is_err() {
            config.allow_plaintext_password()
        } else {
            config
        },
    )
}

/// The `client_credentials` half of [`oauth_bearer_from_env`].
fn oidc_from_env() -> Result<OidcConfig> {
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

    Ok(OidcConfig::new(endpoint, client_id, client_secret)
        .with_maybe_scope(
            std::env::var(OAUTH_SCOPE_ENV)
                .ok()
                .filter(|s| !s.is_empty()),
        )
        .with_maybe_audience(
            std::env::var(OAUTH_AUDIENCE_ENV)
                .ok()
                .filter(|s| !s.is_empty()),
        ))
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

    /// Env vars are process-global, so the rule lives in a function that takes
    /// what was read rather than reading it — which is also the only way two
    /// of these can run at once.
    #[test]
    fn half_a_client_certificate_is_named_rather_than_left_to_the_handshake() {
        let chain = || Some(b"chain".to_vec());
        let key = || Some(b"key".to_vec());

        assert!(client_certificate(None, None).unwrap().is_none());
        assert_eq!(
            client_certificate(chain(), key()).unwrap(),
            Some((b"chain".to_vec(), b"key".to_vec()))
        );

        let no_key = client_certificate(chain(), None).unwrap_err().to_string();
        assert!(no_key.contains(CLIENT_KEY_FILE_ENV), "{no_key}");
        let no_chain = client_certificate(None, key()).unwrap_err().to_string();
        assert!(no_chain.contains(CLIENT_CERT_FILE_ENV), "{no_chain}");
    }
}
