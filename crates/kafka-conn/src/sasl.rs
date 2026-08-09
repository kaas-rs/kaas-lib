//! SASL authentication, and KIP-368 re-authentication.
//!
//! The exchange is driven over a [`SaslTransport`] rather than directly over a
//! socket, because it happens in two different places and must behave
//! identically in both: once on a bare framed stream before the connection
//! actor starts, and again — on a live, fully multiplexed connection — every
//! time the broker's session lifetime is about to expire.
//!
//! That second case is KIP-368 and it is not optional. Where
//! `connections.max.reauth.ms` is set (Confluent Cloud sets it), the broker
//! *kills* the connection at expiry. A UI backend holds connections for hours,
//! so without re-authentication you get periodic unexplained disconnects that
//! look exactly like a network fault and nothing like an auth problem.

use std::fmt;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::oauth::{self, StaticToken, TokenProvider};
use crate::scram::{ScramClient, ScramHash, random_nonce};

/// A SASL mechanism this client can speak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanism {
    /// `PLAIN` — username and password in the clear.
    Plain,
    /// `SCRAM-SHA-256`.
    ScramSha256,
    /// `SCRAM-SHA-512`.
    ScramSha512,
    /// `OAUTHBEARER` — an OAuth 2 bearer token, RFC 7628.
    ///
    /// Needs a token source rather than a username and a password, so build
    /// the configuration with [`SaslConfig::oauth_bearer`] or
    /// [`SaslConfig::oauth_bearer_token`]; selecting this mechanism through
    /// [`SaslConfig::new`] leaves nothing to authenticate with and fails before
    /// anything is sent.
    OauthBearer,
}

impl SaslMechanism {
    /// The name as it appears in `SaslHandshake`.
    pub const fn as_str(self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
            SaslMechanism::OauthBearer => "OAUTHBEARER",
        }
    }

    /// Whether the mechanism puts a directly reusable credential on the wire.
    ///
    /// True for `PLAIN`, which sends the password, and for `OAUTHBEARER`, which
    /// sends a bearer token — bearer-grade by definition, so whoever reads it
    /// off the wire can use it until it expires. SCRAM sends proofs instead and
    /// is safe on an unencrypted socket.
    const fn sends_reusable_credential(self) -> bool {
        matches!(self, SaslMechanism::Plain | SaslMechanism::OauthBearer)
    }
}

impl fmt::Display for SaslMechanism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Credentials and mechanism.
#[derive(Clone)]
pub struct SaslConfig {
    /// Which mechanism to negotiate.
    pub mechanism: SaslMechanism,
    /// Username. Empty for `OAUTHBEARER`, whose principal comes from the token.
    pub username: String,
    /// Password. Empty for `OAUTHBEARER`.
    pub password: String,
    /// Permit a mechanism that sends a reusable credential — `PLAIN`'s password
    /// or `OAUTHBEARER`'s token — over an unencrypted socket.
    ///
    /// Off by default, because the failure mode of getting this wrong is
    /// silent: everything works, and the credential is readable by anything on
    /// the path. SCRAM over plaintext is fine and is not gated.
    pub allow_plaintext_password: bool,
    /// Where `OAUTHBEARER` gets its token, asked again on every
    /// re-authentication. `None` for every other mechanism.
    pub token_provider: Option<Arc<dyn TokenProvider>>,
    /// Extra `key=value` pairs carried in the mechanism's first client
    /// message: RFC 7628 `SaslExtensions` for `OAUTHBEARER`, which some managed
    /// clusters use to select a logical cluster or an identity pool, and RFC
    /// 5802 attributes on `client-first-message-bare` for SCRAM, which is where
    /// KIP-48's `tokenauth=true` goes. `PLAIN` has nowhere to put them and
    /// refuses rather than dropping them. Empty unless
    /// [`SaslConfig::with_extension`] added one.
    pub extensions: Vec<(String, String)>,
}

impl fmt::Debug for SaslConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SaslConfig")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("allow_plaintext_password", &self.allow_plaintext_password)
            // A token provider is a credential source; even its type name can
            // name a secret store. The interesting fact is whether there is
            // one, which is what a misconfiguration turns on.
            .field(
                "token_provider",
                match &self.token_provider {
                    Some(_) => &"<set>",
                    None => &"<unset>",
                },
            )
            .field("extensions", &self.extensions)
            .finish()
    }
}

impl SaslConfig {
    /// Build a configuration for a mechanism that authenticates with a username
    /// and a password: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`.
    pub fn new(
        mechanism: SaslMechanism,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            mechanism,
            username: username.into(),
            password: password.into(),
            allow_plaintext_password: false,
            token_provider: None,
            extensions: Vec::new(),
        }
    }

    /// Authenticate with `OAUTHBEARER`, taking a token from `provider`.
    ///
    /// The provider is asked once per SASL exchange — at connect, and again on
    /// every KIP-368 re-authentication, which happens on a timer this crate
    /// owns and can fire hours later. That is why this takes a source rather
    /// than a string: a token captured once has expired by the second call, and
    /// the symptom is a connection that dies mid-afternoon for no visible
    /// reason.
    ///
    /// Any `Fn() -> impl Future<Output = Result<String>>` is a
    /// [`TokenProvider`]. For the `client_credentials` flow against an OIDC
    /// issuer, [`OidcTokenProvider`](crate::OidcTokenProvider) is one already.
    pub fn oauth_bearer(provider: impl TokenProvider) -> Self {
        Self {
            mechanism: SaslMechanism::OauthBearer,
            username: String::new(),
            password: String::new(),
            allow_plaintext_password: false,
            token_provider: Some(Arc::new(provider)),
            extensions: Vec::new(),
        }
    }

    /// Authenticate with `OAUTHBEARER` using one token, fixed now.
    ///
    /// Right for a short-lived process — a CLI run, a one-shot job — and wrong
    /// for anything long-lived: a re-authentication hours later will present
    /// this same expired token and the broker will close the connection. Use
    /// [`SaslConfig::oauth_bearer`] there.
    pub fn oauth_bearer_token(token: impl Into<String>) -> Self {
        Self::oauth_bearer(StaticToken::new(token.into()))
    }

    /// Authenticate with a KIP-48 delegation token.
    ///
    /// A delegation token is a SCRAM credential in disguise: the token id is
    /// the username, the token HMAC is the password, and a `tokenauth=true`
    /// SCRAM extension is what tells the broker to look the pair up in the
    /// token cache instead of the user store. Omit the extension and the
    /// exchange fails as a bad password for a user that does not exist, which
    /// is a long way from what went wrong.
    ///
    /// `hash` picks the mechanism: a token can be presented over
    /// `SCRAM-SHA-256` or `SCRAM-SHA-512`, and which of the two a listener
    /// enables is the broker's decision, not the token's.
    ///
    /// The token itself comes from `kafka-admin`'s
    /// `Admin::create_delegation_token`, on a connection authenticated some
    /// other way — the broker refuses to issue a token to a principal that
    /// authenticated with one.
    pub fn delegation_token(
        hash: crate::scram::ScramHash,
        token_id: impl Into<String>,
        hmac: impl Into<String>,
    ) -> Self {
        let mechanism = match hash {
            crate::scram::ScramHash::Sha256 => SaslMechanism::ScramSha256,
            crate::scram::ScramHash::Sha512 => SaslMechanism::ScramSha512,
        };
        Self::new(mechanism, token_id, hmac).with_extension("tokenauth", "true")
    }

    /// Add an extension pair to the mechanism's first client message.
    ///
    /// Appends: two calls send two pairs. Keys are ASCII letters, and the names
    /// the mechanism has already spoken for are refused — `auth` for
    /// `OAUTHBEARER`, which carries the token, and RFC 5802's reserved single
    /// letters for SCRAM, which carry the username and the nonce.
    ///
    /// `PLAIN` has no field for these at all. Setting one there fails the
    /// exchange before anything is sent rather than authenticating without it:
    /// an extension that selects a logical cluster or an identity pool changes
    /// *who you are*, and silently dropping it is the one outcome with no
    /// symptom.
    #[must_use]
    pub fn with_extension(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extensions.push((key.into(), value.into()));
        self
    }

    /// Permit a reusable credential — `PLAIN`'s password, `OAUTHBEARER`'s
    /// token — on an unencrypted socket.
    #[must_use]
    pub fn allow_plaintext_password(mut self) -> Self {
        self.allow_plaintext_password = true;
        self
    }

    /// Reject a combination that would leak the credential.
    pub(crate) fn check_encryption(&self, encrypted: bool) -> Result<()> {
        if self.mechanism.sends_reusable_credential()
            && !encrypted
            && !self.allow_plaintext_password
        {
            return Err(Error::Authentication(format!(
                "{} over an unencrypted connection would send a reusable credential in the \
                 clear; use TLS or opt in with SaslConfig::allow_plaintext_password",
                self.mechanism
            )));
        }
        Ok(())
    }
}

/// What a `SaslAuthenticate` round trip produced.
#[derive(Debug, Clone)]
pub struct AuthOutcome {
    /// The broker's SASL token.
    pub auth_bytes: Vec<u8>,
    /// How long this authentication is good for, in milliseconds.
    ///
    /// Zero means "no expiry" — the broker has no `connections.max.reauth.ms`
    /// configured for this listener.
    pub session_lifetime_ms: i64,
}

/// The two RPCs a SASL exchange needs, abstracted over where it runs.
pub(crate) trait SaslTransport {
    /// Send `SaslHandshake`, returning the mechanisms the broker enables.
    async fn handshake(&mut self, mechanism: &str) -> Result<Vec<String>>;

    /// Send `SaslAuthenticate` with a client token.
    async fn authenticate(&mut self, token: Vec<u8>) -> Result<AuthOutcome>;

    /// How long one step of the exchange may take.
    ///
    /// The round trips are already bounded by whichever timeout the transport
    /// was built with. This exists for the step that is *not* a round trip:
    /// asking a caller-supplied [`TokenProvider`] for a token. Awaiting that
    /// unbounded inside `Connection::connect` would let one stuck token source
    /// hang a connection attempt for ever, which is the failure this codebase
    /// spends most of its error handling avoiding.
    fn step_timeout(&self) -> std::time::Duration;
}

/// Run a complete SASL exchange. Returns the session lifetime in milliseconds.
pub(crate) async fn authenticate<T: SaslTransport>(
    config: &SaslConfig,
    transport: &mut T,
) -> Result<i64> {
    // Before the handshake, because a setting that cannot be honoured is a
    // caller error rather than a broker one, and because an extension can be
    // the difference between two principals. `PLAIN` is RFC 4616's three
    // NUL-separated fields and has no room for anything else.
    if matches!(config.mechanism, SaslMechanism::Plain) && !config.extensions.is_empty() {
        return Err(Error::InvalidRequest(format!(
            "{} has no field for SASL extensions, and [{}] would be silently dropped; \
             use OAUTHBEARER or SCRAM, which do",
            config.mechanism,
            config
                .extensions
                .iter()
                .map(|(key, _)| key.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    let enabled = transport.handshake(config.mechanism.as_str()).await?;
    if !enabled.is_empty() && !enabled.iter().any(|m| m == config.mechanism.as_str()) {
        return Err(Error::Authentication(format!(
            "broker does not offer {}; it enables [{}]",
            config.mechanism,
            enabled.join(", ")
        )));
    }

    match config.mechanism {
        SaslMechanism::Plain => {
            // RFC 4616: authzid NUL authcid NUL passwd, with an empty authzid.
            let mut token = Vec::new();
            token.push(0);
            token.extend_from_slice(config.username.as_bytes());
            token.push(0);
            token.extend_from_slice(config.password.as_bytes());
            let outcome = transport.authenticate(token).await?;
            Ok(outcome.session_lifetime_ms)
        }
        SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
            let hash = if config.mechanism == SaslMechanism::ScramSha256 {
                ScramHash::Sha256
            } else {
                ScramHash::Sha512
            };
            let mut client = ScramClient::new(
                hash,
                &config.username,
                &config.password,
                random_nonce(),
                &config.extensions,
            )?;

            let server_first = transport.authenticate(client.client_first()).await?;
            let client_final = client.client_final(&server_first.auth_bytes)?;
            let server_final = transport.authenticate(client_final).await?;
            client.verify_server_final(&server_final.auth_bytes)?;
            Ok(server_final.session_lifetime_ms)
        }
        SaslMechanism::OauthBearer => oauth_bearer_exchange(config, transport).await,
    }
}

/// The OAUTHBEARER half of [`authenticate`], RFC 7628.
///
/// One round trip when the token is good, two when it is not — and the second
/// one is not optional. See [`crate::oauth`].
async fn oauth_bearer_exchange<T: SaslTransport>(
    config: &SaslConfig,
    transport: &mut T,
) -> Result<i64> {
    let Some(provider) = config.token_provider.clone() else {
        return Err(Error::InvalidRequest(
            "OAUTHBEARER needs a token source; build the configuration with \
             SaslConfig::oauth_bearer or SaslConfig::oauth_bearer_token"
                .to_owned(),
        ));
    };

    // Asked here rather than at construction, so a re-authentication three
    // hours in presents a current token instead of the expired one.
    let step = transport.step_timeout();
    let started = std::time::Instant::now();
    let token = match tokio::time::timeout(step, provider.token()).await {
        Ok(token) => token?,
        Err(_) => {
            // Typed as a timeout, so the pool treats it as retriable, and
            // logged so the line names the thing that is actually stuck — the
            // error variant has nowhere to say "token source".
            tracing::warn!(
                ?step,
                "the OAUTHBEARER token source did not answer in time; abandoning the exchange"
            );
            return Err(Error::Timeout {
                api_key: crate::api_key::ApiKey::SaslAuthenticate,
                elapsed: started.elapsed(),
            });
        }
    };
    let initial = oauth::initial_client_response(&token, &config.extensions)?;
    let outcome = transport.authenticate(initial).await?;
    if outcome.auth_bytes.is_empty() {
        return Ok(outcome.session_lifetime_ms);
    }

    // A non-empty challenge on a mechanism with one round trip means the token
    // was refused: the broker has answered with the RFC 7628 JSON failure and
    // is now waiting for a single %x01 before it will complete the exchange.
    // Sending it is what turns a hung handshake into an authentication error.
    let rejection = oauth::TokenRejection::parse(&outcome.auth_bytes);
    let broker_message = match transport.authenticate(vec![oauth::KVSEP]).await {
        // The expected path: the broker fails the exchange, which arrives here
        // as SASL_AUTHENTICATION_FAILED.
        Err(Error::Authentication(message)) => Some(message),
        // Anything else — a dead socket, a timeout — is its own problem and
        // says more about what happened than the challenge does.
        Err(other) => return Err(other),
        // A broker that accepts the dummy response has not followed the RFC.
        // The challenge said the token was rejected, so treat it as rejected.
        Ok(_) => None,
    };

    let mut message = format!("broker rejected the OAUTHBEARER token: {rejection}");
    if let Some(broker) = broker_message.filter(|m| !m.is_empty()) {
        message.push_str(&format!("; broker said: {broker}"));
    }

    // `insufficient_scope` is not an authentication failure. The token
    // authenticated — the principal it names is simply not allowed here, which
    // is the "ask your admin" case, and a UI that renders it as bad credentials
    // sends someone to re-enter a password that was never wrong. The code stays
    // the one the broker actually sent; the scope the token would have needed
    // is in the detail, because it is the only part anyone can act on.
    if rejection.is_insufficient_scope() {
        return Err(Error::Authorization {
            code: crate::error_code::ErrorCode::SaslAuthenticationFailed,
            detail: Some(message),
        });
    }
    Err(Error::Authentication(message))
}

/// When to re-authenticate, given a session lifetime.
///
/// Deliberately early. The broker closes the connection *at* expiry with no
/// grace period, and a re-authentication that starts at 95% of the window can
/// lose the race to a slow round trip; losing it means dropping every in-flight
/// request on that socket.
pub(crate) fn reauth_delay(session_lifetime_ms: i64) -> Option<std::time::Duration> {
    if session_lifetime_ms <= 0 {
        return None;
    }
    let lifetime = u64::try_from(session_lifetime_ms).unwrap_or(u64::MAX);
    // 80% of the window, and never less than a second — a broker configured
    // with a pathologically short lifetime should not turn into a re-auth
    // storm.
    let delay = lifetime.saturating_mul(8) / 10;
    Some(std::time::Duration::from_millis(delay.max(1_000)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A transport that records what it was asked and replays canned answers.
    struct Fake {
        handshake_mechanisms: Vec<String>,
        tokens_sent: Vec<Vec<u8>>,
        replies: Vec<Result<AuthOutcome>>,
        step_timeout: std::time::Duration,
    }

    impl Fake {
        /// A transport offering `mechanism` and nothing queued to say back.
        fn offering(mechanism: &str) -> Self {
            Self {
                handshake_mechanisms: vec![mechanism.to_owned()],
                tokens_sent: Vec::new(),
                replies: Vec::new(),
                step_timeout: std::time::Duration::from_secs(10),
            }
        }

        /// Shorten the per-step budget.
        fn with_step_timeout(mut self, timeout: std::time::Duration) -> Self {
            self.step_timeout = timeout;
            self
        }

        /// Queue a successful reply.
        fn then_ok(mut self, auth_bytes: &[u8], session_lifetime_ms: i64) -> Self {
            self.replies.push(Ok(AuthOutcome {
                auth_bytes: auth_bytes.to_vec(),
                session_lifetime_ms,
            }));
            self
        }

        /// Queue a failure — what the broker's error code arrives here as.
        fn then_err(mut self, error: Error) -> Self {
            self.replies.push(Err(error));
            self
        }
    }

    impl SaslTransport for Fake {
        fn step_timeout(&self) -> std::time::Duration {
            self.step_timeout
        }

        async fn handshake(&mut self, _mechanism: &str) -> Result<Vec<String>> {
            Ok(self.handshake_mechanisms.clone())
        }

        async fn authenticate(&mut self, token: Vec<u8>) -> Result<AuthOutcome> {
            self.tokens_sent.push(token);
            if self.replies.is_empty() {
                panic!("more authenticate calls than canned replies");
            }
            self.replies.remove(0)
        }
    }

    #[tokio::test]
    async fn plain_sends_the_rfc4616_token_in_one_round_trip() {
        let mut fake = Fake::offering("PLAIN").then_ok(&[], 0);
        let cfg = SaslConfig::new(SaslMechanism::Plain, "alice", "s3cret");
        let lifetime = authenticate(&cfg, &mut fake).await.unwrap();
        assert_eq!(lifetime, 0);
        assert_eq!(fake.tokens_sent.len(), 1);
        assert_eq!(fake.tokens_sent[0], b"\0alice\0s3cret");
    }

    #[tokio::test]
    async fn a_mechanism_the_broker_does_not_offer_fails_before_any_token_is_sent() {
        let mut fake = Fake::offering("SCRAM-SHA-512");
        let cfg = SaslConfig::new(SaslMechanism::Plain, "alice", "s3cret");
        let err = authenticate(&cfg, &mut fake).await.unwrap_err();
        assert!(matches!(err, Error::Authentication(_)), "{err:?}");
        assert!(fake.tokens_sent.is_empty(), "password must not be sent");
    }

    #[test]
    fn plain_over_plaintext_is_refused_unless_opted_into() {
        let cfg = SaslConfig::new(SaslMechanism::Plain, "a", "b");
        assert!(cfg.check_encryption(false).is_err());
        assert!(cfg.check_encryption(true).is_ok());
        assert!(
            cfg.clone()
                .allow_plaintext_password()
                .check_encryption(false)
                .is_ok()
        );
    }

    /// The silent-wrongness case. An extension can select a logical cluster or
    /// an identity pool — it decides *who you are* — so a mechanism that cannot
    /// carry one has to say so rather than authenticate as somebody else.
    #[tokio::test]
    async fn plain_refuses_extensions_rather_than_dropping_them() {
        let mut fake = Fake::offering("PLAIN");
        let cfg = SaslConfig::new(SaslMechanism::Plain, "alice", "s3cret")
            .with_extension("logicalCluster", "lkc-1");
        let err = authenticate(&cfg, &mut fake).await.unwrap_err();
        let Error::InvalidRequest(message) = err else {
            panic!("expected an invalid-request error, got {err:?}");
        };
        assert!(message.contains("logicalCluster"), "{message}");
        assert!(fake.tokens_sent.is_empty(), "nothing may be sent");
    }

    /// KIP-48 end to end at this layer: the token id is the username, the HMAC
    /// is the password, and `tokenauth=true` is what sends the broker to the
    /// token cache.
    #[tokio::test]
    async fn a_delegation_token_authenticates_as_scram_with_tokenauth() {
        let mut fake = Fake::offering("SCRAM-SHA-512")
            .then_ok(b"r=nonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096", 0);
        let cfg = SaslConfig::delegation_token(ScramHash::Sha512, "token-id", "aG1hYw==");
        // The canned server nonce does not extend our random one, so the
        // exchange stops after one round trip. That is enough: the message
        // under test is the one already on the wire.
        let _ = authenticate(&cfg, &mut fake).await;
        let first = String::from_utf8(fake.tokens_sent[0].clone()).unwrap();
        assert!(first.starts_with("n,,n=token-id,r="), "{first}");
        assert!(first.ends_with(",tokenauth=true"), "{first}");
        assert_eq!(cfg.mechanism, SaslMechanism::ScramSha512);
    }

    #[test]
    fn scram_over_plaintext_is_fine() {
        let cfg = SaslConfig::new(SaslMechanism::ScramSha512, "a", "b");
        assert!(cfg.check_encryption(false).is_ok());
    }

    #[test]
    fn config_debug_never_prints_the_password() {
        let cfg = SaslConfig::new(SaslMechanism::Plain, "alice", "hunter2");
        assert!(!format!("{cfg:?}").contains("hunter2"));
    }

    #[tokio::test]
    async fn oauth_bearer_authenticates_in_one_round_trip() {
        let mut fake = Fake::offering("OAUTHBEARER").then_ok(&[], 30_000);
        let cfg = SaslConfig::oauth_bearer_token("tok.en");
        let lifetime = authenticate(&cfg, &mut fake).await.unwrap();
        assert_eq!(lifetime, 30_000);
        assert_eq!(fake.tokens_sent.len(), 1);
        assert_eq!(fake.tokens_sent[0], b"n,,\x01auth=Bearer tok.en\x01\x01");
    }

    #[tokio::test]
    async fn extensions_ride_along_in_the_initial_response() {
        let mut fake = Fake::offering("OAUTHBEARER").then_ok(&[], 0);
        let cfg = SaslConfig::oauth_bearer_token("tok").with_extension("logicalCluster", "lkc-1");
        authenticate(&cfg, &mut fake).await.unwrap();
        assert_eq!(
            fake.tokens_sent[0],
            b"n,,\x01auth=Bearer tok\x01logicalCluster=lkc-1\x01\x01"
        );
    }

    /// The part that gets missed: without the second message the broker never
    /// answers, and the handshake fails by connect timeout instead of by
    /// rejection — which a UI renders as "cluster unreachable".
    #[tokio::test]
    async fn a_rejected_token_completes_the_x01_exchange_and_reports_the_status() {
        let challenge = br#"{"status":"invalid_token","scope":"kafka"}"#;
        let mut fake = Fake::offering("OAUTHBEARER")
            .then_ok(challenge, 0)
            .then_err(Error::Authentication("Authentication failed".to_owned()));

        let cfg = SaslConfig::oauth_bearer_token("expired");
        let err = authenticate(&cfg, &mut fake).await.unwrap_err();

        assert_eq!(fake.tokens_sent.len(), 2, "the %x01 message must be sent");
        assert_eq!(fake.tokens_sent[1], vec![0x01]);
        let Error::Authentication(message) = err else {
            panic!("expected an authentication error, got {err:?}");
        };
        assert!(message.contains("status=invalid_token"), "{message}");
        assert!(message.contains("Authentication failed"), "{message}");
    }

    /// `insufficient_scope` means the token authenticated and the principal is
    /// not allowed here. Rendering that as bad credentials sends someone to
    /// re-enter a password that was never wrong; the remedy is an admin, and
    /// the taxonomy exists to say which.
    #[tokio::test]
    async fn an_insufficient_scope_challenge_is_an_authorization_failure() {
        let challenge = br#"{"status":"insufficient_scope","scope":"kafka:write"}"#;
        let mut fake = Fake::offering("OAUTHBEARER")
            .then_ok(challenge, 0)
            .then_err(Error::Authentication("Authentication failed".to_owned()));

        let cfg = SaslConfig::oauth_bearer_token("valid.but.narrow");
        let err = authenticate(&cfg, &mut fake).await.unwrap_err();

        assert_eq!(fake.tokens_sent.len(), 2, "the %x01 message is still sent");
        let Error::Authorization { code, detail } = err else {
            panic!("expected an authorization error, got {err:?}");
        };
        assert_eq!(code, crate::error_code::ErrorCode::SaslAuthenticationFailed);
        // The scope is the only actionable part, so losing it would make the
        // reclassification a downgrade.
        let detail = detail.unwrap_or_default();
        assert!(detail.contains("scope=kafka:write"), "{detail}");
    }

    /// The other statuses stay authentication failures: an expired token is
    /// fixed by getting another one, which is a different action entirely.
    #[tokio::test]
    async fn an_invalid_token_challenge_stays_an_authentication_failure() {
        let mut fake = Fake::offering("OAUTHBEARER")
            .then_ok(br#"{"status":"invalid_token"}"#, 0)
            .then_err(Error::Authentication("Authentication failed".to_owned()));
        let cfg = SaslConfig::oauth_bearer_token("expired");
        let err = authenticate(&cfg, &mut fake).await.unwrap_err();
        assert!(matches!(err, Error::Authentication(_)), "{err:?}");
    }

    /// A dead socket during the failure round trip is a transport problem, and
    /// dressing it up as "your token is bad" sends the operator to the wrong
    /// place entirely.
    #[tokio::test]
    async fn a_socket_that_dies_mid_rejection_keeps_its_own_error() {
        let mut fake = Fake::offering("OAUTHBEARER")
            .then_ok(br#"{"status":"invalid_token"}"#, 0)
            .then_err(Error::ConnectionClosed {
                peer: "broker:9093".to_owned(),
            });
        let cfg = SaslConfig::oauth_bearer_token("expired");
        let err = authenticate(&cfg, &mut fake).await.unwrap_err();
        assert!(matches!(err, Error::ConnectionClosed { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn every_exchange_asks_the_token_source_again() {
        // The KIP-368 property, at the level where it is decidable without a
        // broker: re-authentication runs this same function again, so what
        // matters is that the token comes from the provider each time rather
        // than being captured once.
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let cfg = SaslConfig::oauth_bearer({
            let calls = calls.clone();
            move || {
                let calls = calls.clone();
                async move { Ok(format!("token-{}", calls.fetch_add(1, Ordering::SeqCst))) }
            }
        });

        let mut fake = Fake::offering("OAUTHBEARER")
            .then_ok(&[], 0)
            .then_ok(&[], 0);
        authenticate(&cfg, &mut fake).await.unwrap();
        authenticate(&cfg, &mut fake).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(fake.tokens_sent[0], b"n,,\x01auth=Bearer token-0\x01\x01");
        assert_eq!(fake.tokens_sent[1], b"n,,\x01auth=Bearer token-1\x01\x01");
    }

    #[tokio::test]
    async fn a_token_source_failure_is_not_reported_as_a_broker_problem() {
        let cfg = SaslConfig::oauth_bearer(|| async {
            Err(Error::Unsupported("no token today".to_owned()))
        });
        let mut fake = Fake::offering("OAUTHBEARER");
        let err = authenticate(&cfg, &mut fake).await.unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)), "{err:?}");
        assert!(fake.tokens_sent.is_empty());
    }

    /// A token source that never answers must not hang `Connection::connect`.
    #[tokio::test]
    async fn a_token_source_that_never_answers_times_out_rather_than_hanging() {
        let cfg =
            SaslConfig::oauth_bearer(|| async { std::future::pending::<Result<String>>().await });
        // Real time rather than a paused clock: `tokio`'s `test-util` feature
        // is not in `full`, and 50ms is not worth a dependency change.
        let mut fake =
            Fake::offering("OAUTHBEARER").with_step_timeout(std::time::Duration::from_millis(50));
        let err = authenticate(&cfg, &mut fake).await.unwrap_err();
        assert!(matches!(err, Error::Timeout { .. }), "{err:?}");
        assert!(err.retriable(), "the next attempt may find a live source");
        assert!(fake.tokens_sent.is_empty());
    }

    #[tokio::test]
    async fn oauth_bearer_without_a_token_source_fails_before_the_socket() {
        // Reachable through `SaslConfig::new`, which has no token to give.
        let cfg = SaslConfig::new(SaslMechanism::OauthBearer, "", "");
        let mut fake = Fake::offering("OAUTHBEARER");
        let err = authenticate(&cfg, &mut fake).await.unwrap_err();
        assert!(matches!(err, Error::InvalidRequest(_)), "{err:?}");
        assert!(fake.tokens_sent.is_empty());
    }

    #[test]
    fn a_bearer_token_is_gated_on_plaintext_exactly_like_a_password() {
        let cfg = SaslConfig::oauth_bearer_token("tok");
        assert!(
            cfg.check_encryption(false).is_err(),
            "a token read off the wire is usable until it expires"
        );
        assert!(cfg.check_encryption(true).is_ok());
        assert!(
            cfg.clone()
                .allow_plaintext_password()
                .check_encryption(false)
                .is_ok()
        );
    }

    #[test]
    fn config_debug_never_prints_the_token_or_its_source() {
        let cfg = SaslConfig::oauth_bearer_token("eyJhbGciOiJub25lIn0.secret.");
        let rendered = format!("{cfg:?}");
        assert!(!rendered.contains("secret"), "{rendered}");
        assert!(rendered.contains("token_provider: \"<set>\""), "{rendered}");
    }

    #[test]
    fn no_expiry_means_no_reauth_timer() {
        assert!(reauth_delay(0).is_none());
        assert!(reauth_delay(-1).is_none());
    }

    #[test]
    fn reauth_fires_comfortably_before_expiry() {
        let delay = reauth_delay(60_000).unwrap();
        assert_eq!(delay, std::time::Duration::from_millis(48_000));
        // A pathologically short lifetime must not become a re-auth storm.
        assert_eq!(
            reauth_delay(100).unwrap(),
            std::time::Duration::from_secs(1)
        );
    }
}
