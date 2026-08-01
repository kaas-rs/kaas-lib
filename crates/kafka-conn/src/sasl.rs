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

use crate::error::{Error, Result};
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
}

impl SaslMechanism {
    /// The name as it appears in `SaslHandshake`.
    pub const fn as_str(self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    /// Whether the mechanism sends a recoverable password over the wire.
    const fn sends_cleartext_password(self) -> bool {
        matches!(self, SaslMechanism::Plain)
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
    /// Username.
    pub username: String,
    /// Password.
    pub password: String,
    /// Permit `PLAIN` over an unencrypted socket.
    ///
    /// Off by default: `SASL_PLAINTEXT` with `PLAIN` puts a recoverable
    /// password on the wire, and the failure mode of getting this wrong is
    /// silent. SCRAM over plaintext is fine and is not gated.
    pub allow_plaintext_password: bool,
}

impl fmt::Debug for SaslConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SaslConfig")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("allow_plaintext_password", &self.allow_plaintext_password)
            .finish()
    }
}

impl SaslConfig {
    /// Build a configuration.
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
        }
    }

    /// Permit `PLAIN` without TLS.
    pub fn allow_plaintext_password(mut self) -> Self {
        self.allow_plaintext_password = true;
        self
    }

    /// Reject a combination that would leak the password.
    pub(crate) fn check_encryption(&self, encrypted: bool) -> Result<()> {
        if self.mechanism.sends_cleartext_password() && !encrypted && !self.allow_plaintext_password
        {
            return Err(Error::Authentication(format!(
                "{} over an unencrypted connection would send the password in the clear; \
                 use TLS or opt in with SaslConfig::allow_plaintext_password",
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
}

/// Run a complete SASL exchange. Returns the session lifetime in milliseconds.
pub(crate) async fn authenticate<T: SaslTransport>(
    config: &SaslConfig,
    transport: &mut T,
) -> Result<i64> {
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
            let mut client =
                ScramClient::new(hash, &config.username, &config.password, random_nonce())?;

            let server_first = transport.authenticate(client.client_first()).await?;
            let client_final = client.client_final(&server_first.auth_bytes)?;
            let server_final = transport.authenticate(client_final).await?;
            client.verify_server_final(&server_final.auth_bytes)?;
            Ok(server_final.session_lifetime_ms)
        }
    }
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
        replies: Vec<AuthOutcome>,
    }

    impl SaslTransport for Fake {
        async fn handshake(&mut self, _mechanism: &str) -> Result<Vec<String>> {
            Ok(self.handshake_mechanisms.clone())
        }

        async fn authenticate(&mut self, token: Vec<u8>) -> Result<AuthOutcome> {
            self.tokens_sent.push(token);
            if self.replies.is_empty() {
                panic!("more authenticate calls than canned replies");
            }
            Ok(self.replies.remove(0))
        }
    }

    #[tokio::test]
    async fn plain_sends_the_rfc4616_token_in_one_round_trip() {
        let mut fake = Fake {
            handshake_mechanisms: vec!["PLAIN".to_owned()],
            tokens_sent: Vec::new(),
            replies: vec![AuthOutcome {
                auth_bytes: Vec::new(),
                session_lifetime_ms: 0,
            }],
        };
        let cfg = SaslConfig::new(SaslMechanism::Plain, "alice", "s3cret");
        let lifetime = authenticate(&cfg, &mut fake).await.unwrap();
        assert_eq!(lifetime, 0);
        assert_eq!(fake.tokens_sent.len(), 1);
        assert_eq!(fake.tokens_sent[0], b"\0alice\0s3cret");
    }

    #[tokio::test]
    async fn a_mechanism_the_broker_does_not_offer_fails_before_any_token_is_sent() {
        let mut fake = Fake {
            handshake_mechanisms: vec!["SCRAM-SHA-512".to_owned()],
            tokens_sent: Vec::new(),
            replies: Vec::new(),
        };
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
