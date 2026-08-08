//! Issue #13 acceptance: OAUTHBEARER tokens fetched from an OIDC endpoint.
//!
//! `cargo test -p kafka-conn --features oidc --test oauth_oidc -- --ignored`
//!
//! The two halves need different things, so both live here:
//!
//! * The **token endpoint** half needs an issuer and no broker. Those tests are
//!   not `#[ignore]`d — they run in `cargo xtask ci` with no Docker, because a
//!   refresh schedule that is wrong is wrong without a cluster to prove it on.
//! * The **end-to-end** half needs a broker that validates OAUTHBEARER, so it
//!   wears `#[testkit::integration_test]` like every other acceptance test.
//!
//! The issuer is a twenty-line HTTP server rather than Keycloak in a container.
//! That is not a mocked broker — the broker is real and validates a real token;
//! it is a mocked *identity provider*, and what is under test here is our side
//! of `client_credentials`.
#![cfg(feature = "oidc")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use kafka_conn::protocol::messages::MetadataRequest;
use kafka_conn::{Connection, ConnectionConfig, Error, OidcConfig, OidcTokenProvider, SaslConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

/// What the fake issuer should answer with, given how many requests it has
/// already served.
type Responder = Arc<dyn Fn(usize) -> (u16, String) + Send + Sync>;

/// A one-endpoint OAuth issuer on localhost.
struct Issuer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
    served: Arc<AtomicUsize>,
}

impl Issuer {
    async fn start(responder: Responder) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let served = Arc::new(AtomicUsize::new(0));

        {
            let requests = requests.clone();
            let served = served.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let requests = requests.clone();
                    let served = served.clone();
                    let responder = responder.clone();
                    tokio::spawn(async move {
                        serve(stream, requests, served, responder).await;
                    });
                }
            });
        }

        Self {
            addr,
            requests,
            served,
        }
    }

    /// An issuer that always answers with the same token and lifetime.
    async fn always(token: &str, expires_in: u64) -> Self {
        let body = format!(r#"{{"access_token":"{token}","expires_in":{expires_in}}}"#);
        Self::start(Arc::new(move |_| (200, body.clone()))).await
    }

    fn token_endpoint(&self) -> String {
        format!("http://{}/oauth2/token", self.addr)
    }

    fn served(&self) -> usize {
        self.served.load(Ordering::SeqCst)
    }

    async fn requests(&self) -> Vec<String> {
        self.requests.lock().await.clone()
    }

    /// A provider pointed at this issuer. `http` is the whole reason
    /// `with_allow_plaintext_endpoint` exists.
    fn provider(&self) -> OidcTokenProvider {
        self.provider_with(|config| config)
    }

    fn provider_with(&self, adjust: impl FnOnce(OidcConfig) -> OidcConfig) -> OidcTokenProvider {
        let config = OidcConfig::new(self.token_endpoint(), "kaas-lib", "s3cret")
            .with_scope("kafka")
            .with_allow_plaintext_endpoint();
        OidcTokenProvider::new(adjust(config)).expect("a valid provider")
    }
}

async fn serve(
    mut stream: TcpStream,
    requests: Arc<Mutex<Vec<String>>>,
    served: Arc<AtomicUsize>,
    responder: Responder,
) {
    // Read headers, then exactly as much body as Content-Length promises. A
    // token request is small enough that this never needs to be clever, but it
    // does need to be complete, or the client sees a truncated exchange.
    let mut raw = Vec::new();
    let mut buffer = [0u8; 1024];
    loop {
        let read = match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        raw.extend_from_slice(&buffer[..read]);
        let text = String::from_utf8_lossy(&raw).to_lowercase();
        if let Some(headers_end) = text.find("\r\n\r\n") {
            let length: usize = text
                .split("content-length:")
                .nth(1)
                .and_then(|tail| tail.split("\r\n").next())
                .and_then(|value| value.trim().parse().ok())
                .unwrap_or(0);
            if raw.len() >= headers_end + 4 + length {
                break;
            }
        }
    }

    let count = served.fetch_add(1, Ordering::SeqCst);
    requests
        .lock()
        .await
        .push(String::from_utf8_lossy(&raw).into_owned());

    let (status, body) = responder(count);
    let reason = match status {
        200 => "OK",
        401 => "Unauthorized",
        500 => "Internal Server Error",
        _ => "Status",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
    let _ = stream.shutdown().await;
}

#[tokio::test]
async fn a_token_is_fetched_once_and_then_reused() {
    let issuer = Issuer::always("token-a", 3600).await;
    let provider = issuer.provider();

    assert_eq!(provider.current_token().await.unwrap(), "token-a");
    assert_eq!(provider.current_token().await.unwrap(), "token-a");
    assert_eq!(
        issuer.served(),
        1,
        "a cached token was fetched twice; every connection would pay for it"
    );

    let request = &issuer.requests().await[0];
    assert!(request.starts_with("POST /oauth2/token "), "{request}");
    assert!(
        request.contains("grant_type=client_credentials"),
        "{request}"
    );
    assert!(request.contains("scope=kafka"), "{request}");
    // HTTP Basic by default, per RFC 6749 §2.3.1 — so the secret is not in the
    // body where a request log would keep it.
    assert!(
        request.to_lowercase().contains("authorization: basic"),
        "{request}"
    );
    assert!(
        !request.contains("s3cret"),
        "the secret is base64, not literal"
    );
}

#[tokio::test]
async fn credentials_can_go_in_the_body_for_issuers_that_want_them_there() {
    let issuer = Issuer::always("token-a", 3600).await;
    let provider = issuer.provider_with(OidcConfig::with_credentials_in_body);
    provider.current_token().await.unwrap();

    let request = &issuer.requests().await[0];
    assert!(request.contains("client_id=kaas-lib"), "{request}");
    assert!(request.contains("client_secret=s3cret"), "{request}");
    assert!(
        !request.to_lowercase().contains("authorization:"),
        "{request}"
    );
}

/// The point of the whole issue: nobody calls anything to make this happen.
#[tokio::test]
async fn an_expiring_token_is_replaced_without_the_caller_asking() {
    let issuer = Issuer::start(Arc::new(|count| {
        (
            200,
            format!(r#"{{"access_token":"token-{count}","expires_in":2}}"#),
        )
    }))
    .await;
    // No margin: the refresh point is then 80% of the two-second lifetime,
    // which is what keeps this test short rather than what the default would do.
    let provider = issuer.provider_with(|config| config.with_refresh_margin(Duration::ZERO));

    assert_eq!(provider.current_token().await.unwrap(), "token-0");
    tokio::time::sleep(Duration::from_millis(1_800)).await;
    assert_eq!(
        provider.current_token().await.unwrap(),
        "token-1",
        "the token was reused past its refresh point"
    );
    assert_eq!(issuer.served(), 2);
}

/// A refresh is early by design, so a failed one is not yet a problem.
#[tokio::test]
async fn a_dead_endpoint_does_not_invalidate_a_token_that_still_works() {
    let issuer = Issuer::start(Arc::new(|count| match count {
        0 => (
            200,
            r#"{"access_token":"token-a","expires_in":8}"#.to_owned(),
        ),
        _ => (500, r#"{"error":"temporarily_unavailable"}"#.to_owned()),
    }))
    .await;
    let provider = issuer.provider();

    assert_eq!(provider.current_token().await.unwrap(), "token-a");
    // Between the refresh point (half the lifetime at the earliest, so 4s here)
    // and the eight-second expiry. The gap either side is deliberately wide: the
    // assertion below is about *behaviour in that window*, so a runner slow
    // enough to sleep past the expiry would turn a correct library into a red
    // test, which `tests/connection.rs` documents this suite having done once.
    tokio::time::sleep(Duration::from_millis(4_500)).await;
    assert_eq!(
        provider.current_token().await.unwrap(),
        "token-a",
        "failing a connection over an early refresh is worse than using a \
         token that has not expired"
    );
    assert!(issuer.served() >= 2, "the refresh was attempted");
}

#[tokio::test]
async fn an_endpoint_that_refuses_blames_itself_and_not_the_broker() {
    let issuer = Issuer::start(Arc::new(|_| {
        (
            401,
            r#"{"error":"invalid_client","error_description":"bad secret"}"#.to_owned(),
        )
    }))
    .await;
    let provider = issuer.provider();

    let err = provider.current_token().await.unwrap_err();
    let Error::TokenEndpoint {
        endpoint,
        status,
        detail,
    } = &err
    else {
        panic!("expected a token endpoint error, got {err:?}");
    };
    assert_eq!(status, &Some(401));
    assert!(endpoint.contains(&issuer.addr.to_string()), "{endpoint}");
    assert!(detail.contains("invalid_client: bad secret"), "{detail}");
    assert!(
        !err.retriable(),
        "a rejected client secret will be rejected again"
    );
    // And it is not an authentication failure: no broker has been spoken to.
    assert!(!matches!(err, Error::Authentication(_)));
}

#[tokio::test]
async fn an_unreachable_endpoint_is_retriable_and_names_no_status() {
    // Bind and drop, so the port is almost certainly closed.
    let addr = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    let provider = OidcTokenProvider::new(
        OidcConfig::new(format!("http://{addr}/token"), "id", "secret")
            .with_allow_plaintext_endpoint()
            .with_timeout(Duration::from_secs(2)),
    )
    .unwrap();

    let err = provider.current_token().await.unwrap_err();
    let Error::TokenEndpoint { status, .. } = &err else {
        panic!("expected a token endpoint error, got {err:?}");
    };
    assert_eq!(status, &None, "there was no response to have a status");
    assert!(err.retriable(), "the endpoint may come back");
}

/// Ten connections re-authenticating in the same second are one fetch.
#[tokio::test]
async fn concurrent_callers_share_a_single_fetch() {
    let issuer = Issuer::always("token-a", 3600).await;
    let provider = Arc::new(issuer.provider());

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let provider = provider.clone();
        tasks.push(tokio::spawn(async move { provider.current_token().await }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap().unwrap(), "token-a");
    }
    assert_eq!(issuer.served(), 1);
}

#[tokio::test]
async fn an_http_endpoint_needs_an_explicit_opt_in() {
    let err = OidcTokenProvider::new(OidcConfig::new("http://idp.example/token", "id", "secret"))
        .unwrap_err();
    assert!(matches!(err, Error::InvalidRequest(_)), "{err:?}");
}

fn metadata_request() -> MetadataRequest {
    MetadataRequest::default()
        .with_topics(Some(vec![]))
        .with_allow_auto_topic_creation(false)
}

#[testkit::integration_test]
async fn a_fetched_token_authenticates_against_a_real_broker() {
    use testkit::{BrokerConfig, Cluster, Security};

    let issuer = Issuer::start(Arc::new(|_| {
        (
            200,
            format!(
                r#"{{"access_token":"{}","expires_in":120}}"#,
                testkit::unsecured_jws("dana", Duration::from_secs(120))
            ),
        )
    }))
    .await;

    let broker = testkit::single_broker_with(
        BrokerConfig::new()
            .with_security(Security::SaslPlaintext)
            .with_mechanism(testkit::SaslMechanism::OauthBearer),
    )
    .await
    .unwrap();

    let config = ConnectionConfig::new()
        .with_sasl(SaslConfig::oauth_bearer(issuer.provider()).allow_plaintext_password());
    let conn = Connection::connect(&broker.bootstrap()[0], config)
        .await
        .expect("a token fetched from the issuer authenticates");
    conn.send(metadata_request()).await.unwrap();
    assert_eq!(issuer.served(), 1);
}

/// The join between #12 and #13: a re-authentication hours into a connection
/// must present a token fetched *then*, not the one connect used.
#[testkit::integration_test]
async fn reauthentication_presents_a_token_fetched_after_the_first_one() {
    use testkit::{BrokerConfig, Cluster, Security};

    const REAUTH_WINDOW: Duration = Duration::from_secs(10);

    // `expires_in` is two seconds, so the cached token is always past its
    // refresh point by the time the broker's ten-second session expires: the
    // re-authentication has to go back to the issuer. The JWS inside lives for
    // two minutes, which is what the *broker* checks — the two clocks are
    // deliberately different, because that is the case a client gets wrong.
    let issuer = Issuer::start(Arc::new(|_| {
        (
            200,
            format!(
                r#"{{"access_token":"{}","expires_in":2}}"#,
                testkit::unsecured_jws("dana", Duration::from_secs(120))
            ),
        )
    }))
    .await;

    let broker = testkit::single_broker_with(
        BrokerConfig::new()
            .with_security(Security::SaslPlaintext)
            .with_mechanism(testkit::SaslMechanism::OauthBearer)
            .with_property(
                "listener.name.external.connections.max.reauth.ms",
                REAUTH_WINDOW.as_millis().to_string(),
            ),
    )
    .await
    .unwrap();

    let config = ConnectionConfig::new()
        .with_sasl(SaslConfig::oauth_bearer(issuer.provider()).allow_plaintext_password());
    let conn = Connection::connect(&broker.bootstrap()[0], config)
        .await
        .unwrap();

    let started = Instant::now();
    let deadline = started + REAUTH_WINDOW * 2 + Duration::from_secs(5);
    let mut round_trips = 0u32;
    while Instant::now() < deadline {
        conn.send(metadata_request())
            .await
            .unwrap_or_else(|e| panic!("connection died after {round_trips} round trips: {e}"));
        round_trips += 1;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    assert!(!conn.is_closed(), "connection was closed at session expiry");
    assert!(
        issuer.served() > 1,
        "the issuer was asked once in {:?}, so re-authentication replayed the \
         token from connect",
        started.elapsed()
    );
}
