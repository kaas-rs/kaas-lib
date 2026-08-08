//! SASL/OAUTHBEARER, RFC 7628 — and where the bearer token comes from.
//!
//! Four details, each of which is a way to ship something that looks finished
//! and is not:
//!
//! * **The `%x01` separators are the message format**, not decoration around
//!   it. Kafka's own client shipped them missing
//!   ([KAFKA-7182](https://issues.apache.org/jira/browse/KAFKA-7182)) and its
//!   broker agreed, so the two interoperated with each other and with nothing
//!   else. [`initial_client_response`] asserts the exact bytes in its tests for
//!   that reason.
//! * **A rejected token takes one more round trip.** The broker answers a bad
//!   token with a JSON failure and *waits* for the client to send a single
//!   `%x01` before it will finish the exchange. Skip it and the handshake hangs
//!   until the connect deadline, which reads as "the cluster is unreachable"
//!   rather than "your token is expired".
//! * **The failure JSON is the only thing that says why.** `status` separates
//!   an expired token from an insufficient scope from a listener that wanted a
//!   different issuer, and the SASL error code (58) is the same in all three.
//! * **A token source, not a token.** KIP-368 re-authentication re-runs this
//!   exchange on a live socket hours after connect, by which time an access
//!   token captured at construction has expired. So the configuration holds a
//!   [`TokenProvider`] that is asked again each time.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use crate::error::{Error, Result};

/// The `%x01` separator RFC 7628 frames its messages with.
pub(crate) const KVSEP: u8 = 0x01;

/// The gs2 header: no channel binding, no authorization id.
///
/// Kafka's broker compares a non-empty authzid against the token's own
/// principal and rejects a mismatch, so there is nothing to be gained by
/// sending one — the same reason SCRAM's header is fixed.
const GS2_HEADER: &str = "n,,";

/// A future resolving to a bearer token.
///
/// Boxed because [`TokenProvider`] has to stay dyn-compatible: the connection
/// stores one behind an `Arc` and calls it again on every re-authentication.
pub type TokenFuture<'a> = Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>>;

/// Where `OAUTHBEARER` gets its token.
///
/// Implemented for any `Fn() -> impl Future<Output = Result<String>>`, so a
/// closure is usually enough:
///
/// ```no_run
/// # async fn example(vault: std::sync::Arc<()>) -> kafka_conn::Result<()> {
/// use kafka_conn::{ConnectionConfig, SaslConfig};
///
/// # async fn fetch_token(_: &()) -> kafka_conn::Result<String> { Ok(String::new()) }
/// let config = ConnectionConfig::new().with_sasl(SaslConfig::oauth_bearer(move || {
///     let vault = vault.clone();
///     async move { fetch_token(&vault).await }
/// }));
/// # Ok(())
/// # }
/// ```
pub trait TokenProvider: Send + Sync + 'static {
    /// The token to present, as of now.
    ///
    /// Called once per SASL exchange, which means once at connect and again on
    /// every KIP-368 re-authentication — the second of which happens on a
    /// timer this crate owns and the caller never sees. An implementation that
    /// caches is responsible for refreshing here;
    /// [`OidcTokenProvider`](crate::OidcTokenProvider) is one that does.
    ///
    /// Bounded by the connection's connect timeout during connect and by its
    /// request timeout on re-authentication, so a provider that never returns
    /// fails the exchange rather than hanging it. Enforcing your own deadline
    /// is still better: yours can say what it was waiting for.
    fn token(&self) -> TokenFuture<'_>;
}

impl<F, Fut> TokenProvider for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<String>> + Send + 'static,
{
    fn token(&self) -> TokenFuture<'_> {
        Box::pin(self())
    }
}

/// One token, fixed at construction — what
/// [`SaslConfig::oauth_bearer_token`](crate::SaslConfig::oauth_bearer_token)
/// wraps.
pub(crate) struct StaticToken(String);

impl StaticToken {
    pub(crate) fn new(token: String) -> Self {
        Self(token)
    }
}

impl TokenProvider for StaticToken {
    fn token(&self) -> TokenFuture<'_> {
        let token = self.0.clone();
        Box::pin(async move { Ok(token) })
    }
}

/// The initial client response, RFC 7628 §3.1.
///
/// ```text
/// n,,^Aauth=Bearer <token>^A[<key>=<value>^A...]^A
/// ```
///
/// A gs2 header, then `%x01`, then `%x01`-terminated `key=value` pairs, then
/// one more `%x01` to close the list. Note the double separator at the end:
/// the last kvpair's terminator, and then the list's.
pub(crate) fn initial_client_response(
    token: &str,
    extensions: &[(String, String)],
) -> Result<Vec<u8>> {
    validate_token(token)?;

    let mut out = Vec::with_capacity(GS2_HEADER.len() + token.len() + 24);
    out.extend_from_slice(GS2_HEADER.as_bytes());
    out.push(KVSEP);
    out.extend_from_slice(b"auth=Bearer ");
    out.extend_from_slice(token.as_bytes());
    out.push(KVSEP);
    for (key, value) in extensions {
        validate_extension(key, value)?;
        out.extend_from_slice(key.as_bytes());
        out.push(b'=');
        out.extend_from_slice(value.as_bytes());
        out.push(KVSEP);
    }
    out.push(KVSEP);
    Ok(out)
}

/// Reject a token that cannot be framed, before it reaches a socket.
///
/// The character set is RFC 6750's `b64token`, which is what an OAuth 2 bearer
/// token is allowed to contain and a superset of what Kafka's own client
/// accepts. Being strict here turns the most common caller mistake — passing
/// `"Bearer eyJ…"` where a bare token belongs — into a message that says so,
/// instead of a `SaslAuthenticationException` from the broker.
fn validate_token(token: &str) -> Result<()> {
    if token.is_empty() {
        return Err(Error::InvalidRequest(
            "the OAUTHBEARER token source returned an empty token".to_owned(),
        ));
    }
    if let Some(bad) = token.chars().find(|c| {
        !(c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '~' | '+' | '/' | '='))
    }) {
        return Err(Error::InvalidRequest(format!(
            "OAUTHBEARER token contains {bad:?}, which RFC 6750 does not permit in a \
             bearer token; pass the token alone, without the \"Bearer \" scheme"
        )));
    }
    Ok(())
}

/// Reject an extension that would corrupt the message, per RFC 7628 §3.1.
///
/// `auth` is refused because it is the token's own key: allowing a caller to
/// set it a second time would put two `auth` pairs in one message, and which
/// one the broker honours is not something to leave to chance.
fn validate_extension(key: &str, value: &str) -> Result<()> {
    if key.is_empty() || !key.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err(Error::InvalidRequest(format!(
            "SASL extension key {key:?} must be one or more ASCII letters"
        )));
    }
    if key.eq_ignore_ascii_case("auth") {
        return Err(Error::InvalidRequest(
            "the SASL extension key \"auth\" carries the bearer token itself and cannot be set"
                .to_owned(),
        ));
    }
    if value.is_empty()
        || !value
            .chars()
            .all(|c| matches!(c, '\x21'..='\x7e' | ' ' | '\t'))
    {
        return Err(Error::InvalidRequest(format!(
            "SASL extension {key:?} has a value that is empty or contains a character \
             RFC 7628 does not permit"
        )));
    }
    Ok(())
}

/// Why the broker refused the token.
///
/// RFC 7628 §3.2.2: the server's failure challenge is a JSON object carrying
/// `status`, and optionally the `scope` and `openid-configuration` a client
/// would need to get a token that *would* work.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TokenRejection {
    status: Option<String>,
    scope: Option<String>,
    openid_configuration: Option<String>,
    /// The challenge as it arrived, kept only when it did not parse as the
    /// JSON the RFC specifies. A broker that answers with something else is
    /// still telling us something, and discarding it leaves a bare "rejected".
    raw: Option<String>,
}

impl TokenRejection {
    /// Read a failure challenge. Never fails: an unparsable challenge is
    /// reported verbatim rather than replacing the authentication error with a
    /// decode error about the explanation for it.
    pub(crate) fn parse(challenge: &[u8]) -> Self {
        let text = String::from_utf8_lossy(challenge);
        let field = |json: &serde_json::Value, name: &str| {
            json.get(name)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        };
        match serde_json::from_str::<serde_json::Value>(&text) {
            Ok(json) if json.is_object() => Self {
                status: field(&json, "status"),
                scope: field(&json, "scope"),
                openid_configuration: field(&json, "openid-configuration"),
                raw: None,
            },
            _ => Self {
                raw: Some(text.into_owned()),
                ..Self::default()
            },
        }
    }
}

impl fmt::Display for TokenRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts: Vec<String> = Vec::new();
        if let Some(status) = &self.status {
            parts.push(format!("status={status}"));
        }
        if let Some(scope) = &self.scope {
            parts.push(format!("scope={scope}"));
        }
        if let Some(configuration) = &self.openid_configuration {
            parts.push(format!("openid-configuration={configuration}"));
        }
        if let Some(raw) = &self.raw {
            parts.push(format!("challenge={raw:?}"));
        }
        if parts.is_empty() {
            // A `{}` challenge, or one whose only fields we do not name. The
            // status is the *point* of the message, so say that it was absent
            // rather than rendering nothing at all.
            f.write_str("no status in the broker's failure challenge")
        } else {
            f.write_str(&parts.join(", "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(token: &str, extensions: &[(&str, &str)]) -> Result<Vec<u8>> {
        let owned: Vec<(String, String)> = extensions
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        initial_client_response(token, &owned)
    }

    /// The KAFKA-7182 regression, asserted on the exact bytes.
    ///
    /// Kafka's client emitted the gs2 header without the separators for a year
    /// and its broker accepted it, so "it works against my cluster" proved
    /// nothing. The literal below is the RFC's own example shape.
    #[test]
    fn the_initial_response_carries_every_x01_separator() {
        let bytes = response("mF_9.B5f-4.1JqM", &[]).unwrap();
        assert_eq!(bytes, b"n,,\x01auth=Bearer mF_9.B5f-4.1JqM\x01\x01");
        // Two separators at the end, and neither is optional: the first ends
        // the auth kvpair, the second ends the list.
        assert_eq!(&bytes[bytes.len() - 2..], &[KVSEP, KVSEP]);
    }

    #[test]
    fn extensions_are_x01_terminated_kvpairs_before_the_final_separator() {
        let bytes = response(
            "tok",
            &[("logicalCluster", "lkc-42"), ("identityPoolId", "p1")],
        )
        .unwrap();
        assert_eq!(
            bytes,
            b"n,,\x01auth=Bearer tok\x01logicalCluster=lkc-42\x01identityPoolId=p1\x01\x01"
        );
    }

    #[test]
    fn a_token_that_would_break_the_framing_never_reaches_the_socket() {
        for bad in ["", "Bearer tok", "tok\x01en", "tok,en", "tök"] {
            let err = response(bad, &[]).unwrap_err();
            assert!(matches!(err, Error::InvalidRequest(_)), "{bad:?}: {err:?}");
        }
    }

    #[test]
    fn a_jwt_is_a_valid_token() {
        // Three base64url segments and two dots — the shape every OIDC issuer
        // hands out, and the thing a stricter charset check would break.
        assert!(response("eyJhbGciOiJub25lIn0.eyJzdWIiOiJhIn0.", &[]).is_ok());
    }

    #[test]
    fn extension_keys_are_letters_and_never_shadow_auth() {
        assert!(response("tok", &[("logical", "v")]).is_ok());
        for (key, value) in [("auth", "Bearer x"), ("AUTH", "x"), ("k3y", "v"), ("", "v")] {
            assert!(
                response("tok", &[(key, value)]).is_err(),
                "{key:?} should be refused"
            );
        }
        assert!(response("tok", &[("key", "")]).is_err(), "empty value");
        assert!(response("tok", &[("key", "va\x01ue")]).is_err(), "kvsep");
    }

    #[test]
    fn a_failure_challenge_yields_its_status() {
        let rejection = TokenRejection::parse(
            br#"{"status":"invalid_token","scope":"kafka","openid-configuration":"https://idp/.well-known/openid-configuration"}"#,
        );
        let rendered = rejection.to_string();
        assert!(rendered.contains("status=invalid_token"), "{rendered}");
        assert!(rendered.contains("scope=kafka"), "{rendered}");
        assert!(
            rendered.contains("openid-configuration=https://idp/"),
            "{rendered}"
        );
    }

    #[test]
    fn a_status_only_challenge_renders_only_the_status() {
        assert_eq!(
            TokenRejection::parse(br#"{"status":"insufficient_scope"}"#).to_string(),
            "status=insufficient_scope"
        );
    }

    #[test]
    fn an_unparsable_challenge_is_reported_verbatim_not_swallowed() {
        // A broker that is not Kafka, or a Kafka that changed its mind. Either
        // way the bytes are the only evidence available.
        let rendered = TokenRejection::parse(b"not json at all").to_string();
        assert!(rendered.contains("not json at all"), "{rendered}");

        // Valid JSON that is not an object is equally unhelpful, and equally
        // worth printing.
        assert!(
            TokenRejection::parse(b"[1,2]")
                .to_string()
                .contains("[1,2]")
        );
    }

    #[test]
    fn an_empty_object_says_the_status_was_missing() {
        assert!(
            TokenRejection::parse(b"{}")
                .to_string()
                .contains("no status")
        );
    }

    #[tokio::test]
    async fn a_closure_is_a_token_provider_and_is_asked_every_time() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let provider = {
            let calls = calls.clone();
            move || {
                let calls = calls.clone();
                async move { Ok(format!("token-{}", calls.fetch_add(1, Ordering::SeqCst))) }
            }
        };
        assert_eq!(provider.token().await.unwrap(), "token-0");
        assert_eq!(provider.token().await.unwrap(), "token-1");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_static_token_is_handed_out_unchanged() {
        let provider = StaticToken::new("fixed".to_owned());
        assert_eq!(provider.token().await.unwrap(), "fixed");
        assert_eq!(provider.token().await.unwrap(), "fixed");
    }
}
