//! KIP-768: fetch and refresh OAUTHBEARER tokens from an OIDC token endpoint.
//!
//! [`crate::oauth`] takes a token from wherever the caller has one. This is the
//! other half a real deployment needs: `client_credentials` against an issuer,
//! cached, and replaced *before* it expires rather than after — because the
//! moment a long-lived connection re-authenticates (KIP-368) is chosen by the
//! broker's session lifetime, not by the caller, and a token that goes stale in
//! between fails an established connection in a way that reads like a broker
//! fault.
//!
//! Two deliberate non-features:
//!
//! * **No JWT parsing.** The token endpoint's response carries `expires_in`, so
//!   refresh scheduling never needs to look inside the token — and it should
//!   not: to a *client* an access token is opaque, and only the broker is
//!   entitled to an opinion about its claims.
//! * **No authorization-code or device flow.** A Kafka client is a machine. If
//!   a human is in the loop, the token arrives some other way and
//!   [`SaslConfig::oauth_bearer`](crate::SaslConfig::oauth_bearer) takes it.

use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use hyper::{Method, Request, Uri};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use tokio::sync::Mutex;

use crate::error::{Error, Result};
use crate::oauth::{TokenFuture, TokenProvider};
use crate::tls::TlsConfig;

/// Cap on the token response we will buffer.
///
/// A token endpoint answers with a few kilobytes. Reading an unbounded body from
/// a host that may not be the one we think it is turns a misconfigured url into
/// a memory problem.
const MAX_RESPONSE_BYTES: usize = 256 * 1024;

/// Lifetime assumed when the endpoint does not say.
///
/// `expires_in` is optional in RFC 6749 §5.1, and an issuer that omits it has
/// told us nothing — but the token still works, so failing would be worse than
/// refreshing more often than strictly needed.
const ASSUMED_LIFETIME: Duration = Duration::from_secs(300);

type HttpClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// How to reach an OIDC token endpoint.
#[derive(Clone)]
pub struct OidcConfig {
    /// The token endpoint, e.g.
    /// `https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token`.
    pub token_endpoint: String,
    /// The OAuth client id.
    pub client_id: String,
    /// The OAuth client secret.
    pub client_secret: String,
    /// `scope` to request. Entra wants `<client-id>/.default`; Keycloak
    /// usually wants nothing.
    pub scope: Option<String>,
    /// `audience` to request — Auth0 and Keycloak use it, Entra does not.
    pub audience: Option<String>,
    /// Refresh this long before the token expires, over and above the
    /// fraction-of-lifetime schedule. See [`usable_lifetime`].
    pub refresh_margin: Duration,
    /// Deadline for one token fetch.
    pub timeout: Duration,
    /// Send the client id and secret in the form body instead of as HTTP Basic
    /// authentication.
    pub credentials_in_body: bool,
    /// Permit an `http://` token endpoint.
    pub allow_plaintext_endpoint: bool,
    /// TLS settings for the endpoint. `None` means the system trust store,
    /// which is right for a public issuer and wrong for one behind a private CA.
    pub tls: Option<TlsConfig>,
}

impl std::fmt::Debug for OidcConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OidcConfig")
            .field("token_endpoint", &self.token_endpoint)
            .field("client_id", &self.client_id)
            .field("client_secret", &"<redacted>")
            .field("scope", &self.scope)
            .field("audience", &self.audience)
            .field("refresh_margin", &self.refresh_margin)
            .field("timeout", &self.timeout)
            .field("credentials_in_body", &self.credentials_in_body)
            .field("allow_plaintext_endpoint", &self.allow_plaintext_endpoint)
            .field("tls", &self.tls)
            .finish()
    }
}

impl OidcConfig {
    /// Everything a `client_credentials` exchange needs.
    pub fn new(
        token_endpoint: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        Self {
            token_endpoint: token_endpoint.into(),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            scope: None,
            audience: None,
            refresh_margin: Duration::from_secs(60),
            timeout: Duration::from_secs(10),
            credentials_in_body: false,
            allow_plaintext_endpoint: false,
            tls: None,
        }
    }

    /// Request a scope.
    #[must_use]
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    /// Request a scope, or none — for relaying a configuration value.
    #[must_use]
    pub fn with_maybe_scope(mut self, scope: Option<impl Into<String>>) -> Self {
        self.scope = scope.map(Into::into);
        self
    }

    /// Request an audience.
    #[must_use]
    pub fn with_audience(mut self, audience: impl Into<String>) -> Self {
        self.audience = Some(audience.into());
        self
    }

    /// Request an audience, or none.
    #[must_use]
    pub fn with_maybe_audience(mut self, audience: Option<impl Into<String>>) -> Self {
        self.audience = audience.map(Into::into);
        self
    }

    /// Refresh this long before expiry.
    #[must_use]
    pub fn with_refresh_margin(mut self, margin: Duration) -> Self {
        self.refresh_margin = margin;
        self
    }

    /// Set the deadline for one token fetch.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Send the credentials in the form body.
    ///
    /// HTTP Basic is the default because RFC 6749 §2.3.1 prefers it and Kafka's
    /// own client uses it. Some issuers only accept body parameters, and the
    /// symptom is a bare `invalid_client` that says nothing about which of the
    /// two it wanted — hence the switch.
    #[must_use]
    pub fn with_credentials_in_body(mut self) -> Self {
        self.credentials_in_body = true;
        self
    }

    /// Permit an `http://` endpoint.
    ///
    /// Off by default: a `client_credentials` request puts the client secret in
    /// the request itself, so plaintext hands it to anything on the path. A
    /// local test issuer is a reasonable place to say yes.
    #[must_use]
    pub fn with_allow_plaintext_endpoint(mut self) -> Self {
        self.allow_plaintext_endpoint = true;
        self
    }

    /// Trust settings for the endpoint — for an issuer behind a private CA.
    #[must_use]
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }
}

/// A cached token, and the two moments that matter about it.
#[derive(Debug)]
struct Cached {
    token: String,
    /// When to go and get a new one.
    refresh_after: Instant,
    /// When this one stops working. Only consulted when a refresh *failed*.
    expires_at: Instant,
}

/// An OAUTHBEARER token source backed by a `client_credentials` exchange.
///
/// Caches, refreshes ahead of expiry, and is a
/// [`TokenProvider`](crate::TokenProvider), so it plugs straight into the
/// mechanism:
///
/// ```no_run
/// # fn example() -> kafka_conn::Result<()> {
/// use kafka_conn::{ConnectionConfig, OidcConfig, OidcTokenProvider, SaslConfig, TlsConfig};
///
/// let provider = OidcTokenProvider::new(
///     OidcConfig::new(
///         "https://login.microsoftonline.com/tenant/oauth2/v2.0/token",
///         "client-id",
///         std::env::var("OAUTH_CLIENT_SECRET").unwrap_or_default(),
///     )
///     .with_scope("client-id/.default"),
/// )?;
///
/// let config = ConnectionConfig::new()
///     .with_tls(TlsConfig::system())
///     .with_sasl(SaslConfig::oauth_bearer(provider));
/// # Ok(())
/// # }
/// ```
///
/// One provider should be shared by every connection to a cluster — it is what
/// keeps them on one token and one fetch. Cloning is not offered for that
/// reason; wrap it in an `Arc` if you need to hold it in two places, or hand the
/// same [`SaslConfig`](crate::SaslConfig) to the pool, which already shares it.
pub struct OidcTokenProvider {
    config: OidcConfig,
    endpoint: Uri,
    http: HttpClient,
    /// The lock is what makes a refresh single-flight: ten connections
    /// re-authenticating in the same second perform one token fetch, not ten.
    cached: Mutex<Option<Cached>>,
}

impl std::fmt::Debug for OidcTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OidcTokenProvider")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl OidcTokenProvider {
    /// Build a provider. Fetches nothing yet — the first token is fetched when
    /// the first connection authenticates.
    pub fn new(config: OidcConfig) -> Result<Self> {
        let endpoint: Uri = config.token_endpoint.parse().map_err(|e| {
            Error::InvalidRequest(format!(
                "token endpoint {:?} is not a valid uri: {e}",
                config.token_endpoint
            ))
        })?;
        match endpoint.scheme_str() {
            Some("https") => {}
            Some("http") if config.allow_plaintext_endpoint => {
                tracing::warn!(
                    endpoint = %endpoint,
                    "fetching tokens over http; the client secret is on the wire in the clear"
                );
            }
            Some("http") => {
                return Err(Error::InvalidRequest(format!(
                    "token endpoint {endpoint} is http, which would send the client secret \
                     in the clear; use https or opt in with \
                     OidcConfig::with_allow_plaintext_endpoint"
                )));
            }
            other => {
                return Err(Error::InvalidRequest(format!(
                    "token endpoint {endpoint} has scheme {other:?}, which is not http(s)"
                )));
            }
        }

        // The rustls config comes from our own TlsConfig rather than from
        // hyper-rustls's root loaders, for one reason: TlsConfig names the ring
        // provider explicitly. Anything that resolves a process-wide default
        // provider can fail at runtime depending on what else in the binary
        // installed one, which is not a failure mode to inherit for a token
        // fetch.
        let tls = config.tls.clone().unwrap_or_else(TlsConfig::system);
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls.rustls_config()?)
            .https_or_http()
            .enable_http1()
            .build();

        Ok(Self {
            config,
            endpoint,
            http: Client::builder(TokioExecutor::new()).build(connector),
            cached: Mutex::new(None),
        })
    }

    /// The current access token, fetching or refreshing if it is time.
    pub async fn current_token(&self) -> Result<String> {
        let mut cached = self.cached.lock().await;
        if let Some(current) = cached.as_ref()
            && Instant::now() < current.refresh_after
        {
            return Ok(current.token.clone());
        }

        match self.fetch().await {
            Ok(fresh) => {
                let token = fresh.token.clone();
                *cached = Some(fresh);
                Ok(token)
            }
            Err(error) => {
                // The refresh is deliberately early, so a failed one does not
                // mean the token we hold is unusable. Presenting it beats
                // failing a connection over an endpoint blip — and the warning
                // is what makes the eventual hard failure explicable.
                match cached.as_ref().filter(|c| Instant::now() < c.expires_at) {
                    Some(current) => {
                        tracing::warn!(
                            endpoint = %self.endpoint,
                            %error,
                            expires_in_s = current
                                .expires_at
                                .saturating_duration_since(Instant::now())
                                .as_secs(),
                            "token refresh failed; presenting the cached token"
                        );
                        Ok(current.token.clone())
                    }
                    None => Err(error),
                }
            }
        }
    }

    /// The `client_credentials` form body.
    ///
    /// Its own function because `form_urlencoded::Serializer` is not `Send`, and
    /// a live one held across the `await` below would make the whole future
    /// un-spawnable — which shows up as a `TokenProvider` that will not compile
    /// rather than as anything about forms.
    fn form_body(&self) -> String {
        let mut form = form_urlencoded::Serializer::new(String::new());
        form.append_pair("grant_type", "client_credentials");
        if let Some(scope) = &self.config.scope {
            form.append_pair("scope", scope);
        }
        if let Some(audience) = &self.config.audience {
            form.append_pair("audience", audience);
        }
        if self.config.credentials_in_body {
            form.append_pair("client_id", &self.config.client_id);
            form.append_pair("client_secret", &self.config.client_secret);
        }
        form.finish()
    }

    /// One `client_credentials` round trip.
    async fn fetch(&self) -> Result<Cached> {
        let body = self.form_body();

        let mut builder = Request::builder()
            .method(Method::POST)
            .uri(self.endpoint.clone())
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(ACCEPT, "application/json");
        if !self.config.credentials_in_body {
            builder = builder.header(
                AUTHORIZATION,
                format!(
                    "Basic {}",
                    B64.encode(basic_credentials(
                        &self.config.client_id,
                        &self.config.client_secret
                    ))
                ),
            );
        }
        let request = builder
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| self.unreachable(format!("could not build the request: {e}")))?;

        let started = Instant::now();
        let response = tokio::time::timeout(self.config.timeout, self.http.request(request))
            .await
            .map_err(|_| self.unreachable(format!("no response within {:?}", self.config.timeout)))?
            .map_err(|e| self.unreachable(e.to_string()))?;

        let status = response.status();
        let body = Limited::new(response.into_body(), MAX_RESPONSE_BYTES)
            .collect()
            .await
            .map_err(|e| self.unreachable(format!("reading the response body: {e}")))?
            .to_bytes();

        if !status.is_success() {
            return Err(Error::TokenEndpoint {
                endpoint: self.endpoint.to_string(),
                status: Some(status.as_u16()),
                detail: format!("returned HTTP {status}: {}", describe_failure(&body)),
            });
        }

        let (token, lifetime) = parse_token_response(&self.endpoint.to_string(), &body)?;
        let usable = usable_lifetime(lifetime, self.config.refresh_margin);
        tracing::debug!(
            endpoint = %self.endpoint,
            lifetime_s = lifetime.as_secs(),
            refresh_in_s = usable.as_secs(),
            took_ms = started.elapsed().as_millis(),
            "fetched an access token"
        );
        let now = Instant::now();
        Ok(Cached {
            token,
            refresh_after: now + usable,
            expires_at: now + lifetime,
        })
    }

    /// The endpoint could not be reached, or did not answer in time — which is
    /// a different problem from an endpoint that refused, and names no HTTP
    /// status precisely because there was none.
    fn unreachable(&self, detail: String) -> Error {
        Error::TokenEndpoint {
            endpoint: self.endpoint.to_string(),
            status: None,
            detail,
        }
    }
}

impl TokenProvider for OidcTokenProvider {
    fn token(&self) -> TokenFuture<'_> {
        Box::pin(self.current_token())
    }
}

/// The `client_id:client_secret` pair for HTTP Basic, RFC 6749 §2.3.1.
///
/// Both halves are form-urlencoded *before* base64. That is easy to skip and
/// impossible to notice until a rotated secret happens to contain a `+` — at
/// which point authentication fails with `invalid_client` and the secret looks
/// correct everywhere you check it.
fn basic_credentials(client_id: &str, client_secret: &str) -> String {
    let encode = |raw: &str| form_urlencoded::byte_serialize(raw.as_bytes()).collect::<String>();
    format!("{}:{}", encode(client_id), encode(client_secret))
}

/// How much of a token's lifetime to use before refreshing.
///
/// 80% of the window, the same fraction the Java client's
/// `sasl.login.refresh.window.factor` defaults to, and never closer to expiry
/// than the configured margin. The floor at half the lifetime is what stops a
/// margin larger than the token's own lifetime — a 60-second margin against a
/// 30-second test token — from meaning "refresh on every single call".
fn usable_lifetime(lifetime: Duration, margin: Duration) -> Duration {
    let millis = u64::try_from(lifetime.as_millis()).unwrap_or(u64::MAX);
    let by_factor = Duration::from_millis(millis.saturating_mul(8) / 10);
    let by_margin = lifetime.saturating_sub(margin);
    by_factor.min(by_margin).max(lifetime / 2)
}

/// Read `access_token` and `expires_in` out of a token response.
fn parse_token_response(endpoint: &str, body: &[u8]) -> Result<(String, Duration)> {
    let json: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| Error::TokenEndpoint {
            endpoint: endpoint.to_owned(),
            status: Some(200),
            detail: format!("answered with something that is not JSON: {e}"),
        })?;

    let token = json
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| Error::TokenEndpoint {
            endpoint: endpoint.to_owned(),
            status: Some(200),
            detail: "answered without an access_token".to_owned(),
        })?
        .to_owned();

    // A number per RFC 6749, a string in the wild often enough to be worth
    // handling — the alternative is a provider that works for everyone else and
    // refreshes every five minutes for us.
    let lifetime = json
        .get("expires_in")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .map(Duration::from_secs)
        .filter(|d| !d.is_zero())
        .unwrap_or_else(|| {
            tracing::debug!(
                endpoint,
                assumed_s = ASSUMED_LIFETIME.as_secs(),
                "token response carried no usable expires_in"
            );
            ASSUMED_LIFETIME
        });

    Ok((token, lifetime))
}

/// Turn an error response into something an operator can act on.
///
/// RFC 6749 §5.2 puts `error` and `error_description` in the body, and the
/// description is usually the only place the real cause appears — Entra's
/// `AADSTS7000215: Invalid client secret provided`, for one.
fn describe_failure(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
        let field = |name: &str| {
            json.get(name)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        };
        match (field("error"), field("error_description")) {
            (Some(error), Some(description)) => return format!("{error}: {description}"),
            (Some(error), None) => return error,
            (None, Some(description)) => return description,
            (None, None) => {}
        }
    }
    let trimmed = text.trim();
    if trimmed.is_empty() {
        "with an empty body".to_owned()
    } else {
        // Bounded: an html error page from a reverse proxy is not a log line.
        trimmed.chars().take(200).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> OidcConfig {
        OidcConfig::new("https://idp.example/token", "client", "s3cret")
    }

    #[test]
    fn debug_never_prints_the_client_secret() {
        let rendered = format!("{:?}", config());
        assert!(!rendered.contains("s3cret"), "{rendered}");
        let provider = OidcTokenProvider::new(config()).unwrap();
        assert!(!format!("{provider:?}").contains("s3cret"));
    }

    #[test]
    fn an_http_endpoint_is_refused_unless_opted_into() {
        let plaintext = OidcConfig::new("http://idp.example/token", "client", "s3cret");
        let err = OidcTokenProvider::new(plaintext.clone()).unwrap_err();
        assert!(matches!(err, Error::InvalidRequest(_)), "{err:?}");
        assert!(
            OidcTokenProvider::new(plaintext.with_allow_plaintext_endpoint()).is_ok(),
            "a local test issuer is a legitimate reason to ask"
        );
    }

    #[test]
    fn a_nonsense_endpoint_is_refused_at_construction_not_at_first_use() {
        for bad in ["not a uri", "ftp://idp.example/token", "/token"] {
            let err = OidcTokenProvider::new(OidcConfig::new(bad, "c", "s")).unwrap_err();
            assert!(matches!(err, Error::InvalidRequest(_)), "{bad}: {err:?}");
        }
    }

    #[test]
    fn basic_credentials_are_form_encoded_before_base64() {
        // The `+` is the case that silently breaks: base64 of the raw secret
        // authenticates nowhere and looks right in every log.
        assert_eq!(
            basic_credentials("cli ent", "a+b/c=d"),
            "cli+ent:a%2Bb%2Fc%3Dd"
        );
    }

    #[test]
    fn a_token_is_refreshed_at_eighty_percent_of_its_lifetime() {
        let margin = Duration::from_secs(60);
        assert_eq!(
            usable_lifetime(Duration::from_secs(3600), margin),
            Duration::from_secs(2880)
        );
    }

    #[test]
    fn the_margin_wins_when_it_is_tighter_than_the_factor() {
        assert_eq!(
            usable_lifetime(Duration::from_secs(100), Duration::from_secs(30)),
            Duration::from_secs(70)
        );
    }

    #[test]
    fn a_margin_larger_than_the_lifetime_does_not_mean_refresh_every_call() {
        // 10-second tokens against the default 60-second margin: half the
        // lifetime, not zero.
        assert_eq!(
            usable_lifetime(Duration::from_secs(10), Duration::from_secs(60)),
            Duration::from_secs(5)
        );
        assert_eq!(
            usable_lifetime(Duration::ZERO, Duration::from_secs(60)),
            Duration::ZERO
        );
    }

    #[test]
    fn a_token_response_yields_the_token_and_its_lifetime() {
        let (token, lifetime) = parse_token_response(
            "https://idp/token",
            br#"{"token_type":"Bearer","expires_in":3599,"access_token":"eyJ0.eyJz."}"#,
        )
        .unwrap();
        assert_eq!(token, "eyJ0.eyJz.");
        assert_eq!(lifetime, Duration::from_secs(3599));
    }

    #[test]
    fn expires_in_is_accepted_as_a_string_too() {
        let (_, lifetime) =
            parse_token_response("e", br#"{"access_token":"t","expires_in":"120"}"#).unwrap();
        assert_eq!(lifetime, Duration::from_secs(120));
    }

    #[test]
    fn a_response_without_a_lifetime_falls_back_rather_than_failing() {
        let (_, lifetime) = parse_token_response("e", br#"{"access_token":"t"}"#).unwrap();
        assert_eq!(lifetime, ASSUMED_LIFETIME);
    }

    #[test]
    fn a_response_without_a_token_names_the_endpoint() {
        for body in [
            &br#"{"token_type":"Bearer"}"#[..],
            &br#"{"access_token":""}"#[..],
            b"<html>maintenance</html>",
        ] {
            let err = parse_token_response("https://idp/token", body).unwrap_err();
            let Error::TokenEndpoint { endpoint, .. } = &err else {
                panic!("expected a token endpoint error, got {err:?}");
            };
            assert_eq!(endpoint, "https://idp/token");
            // Not a SASL failure: nothing has been said to a broker yet.
            assert!(err.to_string().contains("token endpoint"), "{err}");
        }
    }

    #[test]
    fn an_error_response_surfaces_the_issuers_own_description() {
        assert_eq!(
            describe_failure(
                br#"{"error":"invalid_client","error_description":"AADSTS7000215: Invalid client secret provided"}"#
            ),
            "invalid_client: AADSTS7000215: Invalid client secret provided"
        );
        assert_eq!(
            describe_failure(br#"{"error":"invalid_scope"}"#),
            "invalid_scope"
        );
        assert_eq!(describe_failure(b""), "with an empty body");
        assert!(describe_failure(b"<html>502</html>").contains("502"));
        // An html error page from a proxy is truncated, not logged whole.
        let long = vec![b'x'; 4096];
        assert_eq!(describe_failure(&long).len(), 200);
    }

    #[test]
    fn a_failed_fetch_is_retriable_only_when_retrying_could_help() {
        let unreachable = Error::TokenEndpoint {
            endpoint: "https://idp/token".to_owned(),
            status: None,
            detail: "connection refused".to_owned(),
        };
        assert!(unreachable.retriable(), "the endpoint may come back");

        let refused = Error::TokenEndpoint {
            endpoint: "https://idp/token".to_owned(),
            status: Some(401),
            detail: "returned HTTP 401".to_owned(),
        };
        assert!(!refused.retriable(), "a bad secret stays bad");

        let overloaded = Error::TokenEndpoint {
            endpoint: "https://idp/token".to_owned(),
            status: Some(503),
            detail: "returned HTTP 503".to_owned(),
        };
        assert!(overloaded.retriable());
    }
}
