//! The connection actor: typed, concurrent request/response over one socket.
//!
//! # Shape
//!
//! One socket, two tasks, one correlation map. Callers hand an encoded frame to
//! the writer over a channel and wait on a `oneshot`; the reader routes each
//! response frame back by correlation id. Decoding happens on the *calling*
//! task, not in the reader, so a response that fails to parse fails one request
//! instead of taking down every other request sharing the connection.
//!
//! # Cancel safety
//!
//! Rule 5: dropping a `send` future must never leave the socket half-read. It
//! cannot here, because the caller never touches the socket. Drop the future
//! and the `oneshot` receiver goes away; the request is still written, the
//! response is still read, and the reader discards it on finding no waiter. The
//! in-flight permit is released by its guard. The connection stays perfectly
//! consistent — the only cost is one wasted round trip.
//!
//! # Death
//!
//! When the socket dies, every pending caller resolves to
//! [`Error::ConnectionClosed`] and every subsequent send fails immediately. A
//! UI backend that hangs one future per dead broker degrades into a process
//! that appears to work while doing nothing, which is much worse than an error.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use kafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, SaslAuthenticateRequest, SaslHandshakeRequest,
};
use kafka_protocol::protocol::{Decodable, StrBytes};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, mpsc, oneshot, watch};
use tokio_util::codec::Framed;
use tracing::Instrument;

use crate::api_key::ApiKey;
use crate::codec;
use crate::config::ConnectionConfig;
use crate::error::{Error, Result};
use crate::error_code::ErrorCode;
use crate::rpc::Rpc;
use crate::sasl::{AuthOutcome, SaslTransport};
use crate::stats::{ConnectionStats, StatsSnapshot};
use crate::transport::Transport;
use crate::versions::{ApiVersions, our_range};

/// `UNSUPPORTED_VERSION`. Named because the `ApiVersions` bootstrap treats it
/// as data rather than as a failure.
const UNSUPPORTED_VERSION: i16 = 35;

type PendingMap = HashMap<i32, oneshot::Sender<Result<Bytes>>>;

/// Shared correlation state.
struct Pending {
    /// Waiters by correlation id, or `None` once the connection is dead.
    waiters: Mutex<Option<PendingMap>>,
}

impl Pending {
    fn new() -> Self {
        Self {
            waiters: Mutex::new(Some(HashMap::new())),
        }
    }

    /// Register a waiter, or report the connection dead.
    fn register(&self, id: i32, tx: oneshot::Sender<Result<Bytes>>, peer: &str) -> Result<()> {
        let mut guard = self.waiters.lock().map_err(|_| poisoned(peer))?;
        match guard.as_mut() {
            Some(map) => {
                map.insert(id, tx);
                Ok(())
            }
            None => Err(Error::ConnectionClosed {
                peer: peer.to_owned(),
            }),
        }
    }

    fn take(&self, id: i32) -> Option<oneshot::Sender<Result<Bytes>>> {
        let mut guard = self.waiters.lock().ok()?;
        guard.as_mut()?.remove(&id)
    }

    /// Mark dead and resolve every waiter.
    fn close(&self, peer: &str) {
        let drained = match self.waiters.lock() {
            Ok(mut guard) => guard.take(),
            // A poisoned lock means a panic inside the mutex, which cannot
            // happen with this code — but if it somehow did, the honest move is
            // to leak the waiters rather than to panic a second time. They
            // resolve to ConnectionClosed anyway when their sender drops.
            Err(_) => None,
        };
        if let Some(map) = drained {
            for (_, tx) in map {
                let _ = tx.send(Err(Error::ConnectionClosed {
                    peer: peer.to_owned(),
                }));
            }
        }
    }

    fn is_closed(&self) -> bool {
        self.waiters.lock().map(|g| g.is_none()).unwrap_or(true)
    }
}

fn poisoned(peer: &str) -> Error {
    Error::ConnectionClosed {
        peer: peer.to_owned(),
    }
}

/// A live connection to one broker.
///
/// Cheap to clone; every clone shares the socket, the counters and the
/// correlation map. The socket closes when the last clone drops.
#[derive(Debug, Clone)]
pub struct Connection {
    inner: Arc<Inner>,
}

struct Inner {
    peer: String,
    node_id: Option<i32>,
    config: ConnectionConfig,
    versions: ApiVersions,
    stats: Arc<ConnectionStats>,
    commands: mpsc::Sender<BytesMut>,
    pending: Arc<Pending>,
    inflight: Arc<tokio::sync::Semaphore>,
    correlation: AtomicI32,
    /// Held for reading by every request and for writing by re-authentication,
    /// which must not interleave with normal traffic.
    reauth_gate: RwLock<()>,
    shutdown: watch::Sender<bool>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("peer", &self.peer)
            .field("node_id", &self.node_id)
            .field("read_only", &self.config.read_only)
            .field("stats", &self.stats.snapshot())
            .finish()
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

impl Connection {
    /// Open a connection to `addr` and complete the handshake.
    ///
    /// "Handshake" means TLS if configured, then `ApiVersions`, then SASL if
    /// configured — in that order, because each step's framing depends on the
    /// previous one having succeeded.
    pub async fn connect(addr: &str, config: ConnectionConfig) -> Result<Self> {
        Self::connect_as(addr, None, config).await
    }

    /// Open a connection and remember which broker id it belongs to.
    pub async fn connect_as(
        addr: &str,
        node_id: Option<i32>,
        config: ConnectionConfig,
    ) -> Result<Self> {
        let span = tracing::debug_span!("kafka.connect", peer = addr, node_id);
        async move {
            let transport = tokio::time::timeout(config.connect_timeout, open(addr, &config))
                .await
                .map_err(|_| Error::Timeout {
                    api_key: ApiKey::ApiVersions,
                    elapsed: config.connect_timeout,
                })??;

            let encrypted = transport.is_encrypted();
            if let Some(sasl) = &config.sasl {
                // Checked before anything is sent, so a misconfiguration never
                // puts a password on an unencrypted wire even once.
                sasl.check_encryption(encrypted)?;
            }

            let mut framed = Framed::new(transport, codec::frame_codec(config.max_frame_bytes));
            let stats = ConnectionStats::new();

            let mut raw = RawConn {
                framed: &mut framed,
                config: &config,
                stats: &stats,
                correlation: 0,
                versions: ApiVersions::default(),
                peer: addr,
            };
            raw.versions = negotiate_versions(&mut raw).await?;
            let versions = raw.versions.clone();

            let mut session_lifetime_ms = 0;
            if let Some(sasl) = config.sasl.clone() {
                session_lifetime_ms = crate::sasl::authenticate(&sasl, &mut raw).await?;
            }

            let connection = spawn(addr.to_owned(), node_id, config, versions, stats, framed);

            if let Some(delay) = crate::sasl::reauth_delay(session_lifetime_ms) {
                connection.spawn_reauth(delay);
            }
            Ok(connection)
        }
        .instrument(span)
        .await
    }

    /// Send a request and await its response.
    ///
    /// Uses the connection's default deadline. See [`Connection::send_until`]
    /// for a caller-propagated one.
    ///
    /// # On the `Request` bound
    ///
    /// This is the one place in the workspace where a `kafka-protocol` trait
    /// appears in a public signature, and it is deliberate: this crate *is* the
    /// wire boundary, and inventing a parallel request trait would add a layer
    /// that converts protocol types to protocol types. Rule 1 applies with full
    /// force to everything above — no crate that depends on this one may put a
    /// `kafka_protocol` type in its own public API.
    pub async fn send<R: Rpc>(&self, request: R) -> Result<R::Response> {
        let deadline = Instant::now() + self.inner.config.request_timeout;
        self.send_until(request, deadline).await
    }

    /// Send a request with a caller-supplied deadline.
    ///
    /// The deadline is propagated rather than being a constant, so a scan that
    /// is already 200ms from its own budget does not start a 30-second fetch.
    pub async fn send_until<R: Rpc>(&self, request: R, deadline: Instant) -> Result<R::Response> {
        let _gate = self.inner.reauth_gate.read().await;
        self.send_inner(request, deadline).await
    }

    async fn send_inner<R: Rpc>(&self, request: R, deadline: Instant) -> Result<R::Response> {
        let api_key = R::API_KEY;

        // Before the socket, before the semaphore, before anything. M8's
        // property is that a read-only client never emits a mutating byte.
        if self.inner.config.read_only && api_key.is_mutating() {
            return Err(Error::ReadOnly { api_key });
        }

        let version = self.inner.versions.negotiate(api_key)?;
        let frame = self
            .round_trip(api_key, version, &request, deadline)
            .await?;
        codec::decode_response::<R>(api_key, version, frame)
    }

    /// The untyped half of `send`: everything up to and including receiving the
    /// response frame.
    async fn round_trip<R: Rpc>(
        &self,
        api_key: ApiKey,
        version: i16,
        request: &R,
        deadline: Instant,
    ) -> Result<Bytes> {
        let inner = &self.inner;
        let correlation_id = inner.correlation.fetch_add(1, Ordering::Relaxed) & i32::MAX;

        let span = tracing::debug_span!(
            "kafka.rpc",
            peer = %inner.peer,
            node_id = inner.node_id,
            api = %api_key,
            version,
            correlation_id,
        );
        let _entered = span.enter();

        let bytes = codec::encode_request(
            api_key,
            version,
            correlation_id,
            inner.config.client_id.as_deref(),
            request,
        )?;

        // Bounded in-flight. Acquired before registering so a saturated
        // connection applies backpressure instead of growing the map.
        let permit = with_deadline(deadline, api_key, inner.inflight.clone().acquire_owned())
            .await?
            .map_err(|_| Error::ConnectionClosed {
                peer: inner.peer.clone(),
            })?;

        let (tx, rx) = oneshot::channel();
        inner.pending.register(correlation_id, tx, &inner.peer)?;

        if inner.commands.send(bytes).await.is_err() {
            // Writer is gone; do not leave a waiter behind for a request that
            // will never be written.
            let _ = inner.pending.take(correlation_id);
            return Err(Error::ConnectionClosed {
                peer: inner.peer.clone(),
            });
        }

        let started = Instant::now();
        let received = with_deadline(deadline, api_key, rx).await;
        // The permit is released here whether we got a response, timed out, or
        // were cancelled — including when this future is simply dropped.
        drop(permit);

        // Per-api histograms, fed by the same clock the span reports. Recorded
        // for failures too: an api whose *timeouts* are slow is exactly what a
        // latency panel needs to show.
        metrics::histogram!("kafka_rpc_duration_seconds", "api" => api_key.name())
            .record(started.elapsed().as_secs_f64());
        metrics::counter!("kafka_rpc_total", "api" => api_key.name()).increment(1);

        match received {
            Ok(Ok(result)) => result,
            Ok(Err(_recv_error)) => Err(Error::ConnectionClosed {
                peer: inner.peer.clone(),
            }),
            Err(timeout) => {
                // Stop the reader handing a frame to a receiver nobody holds.
                let _ = inner.pending.take(correlation_id);
                Err(timeout)
            }
        }
    }

    /// The broker's advertised versions, intersected with ours.
    pub fn versions(&self) -> &ApiVersions {
        &self.inner.versions
    }

    /// The version this connection would send an api key at.
    ///
    /// Callers need this when a request's *contents* depend on the version —
    /// `MetadataRequest`'s null-versus-empty topic list, for one — and when
    /// deciding whether a newer API is available before falling back to an
    /// older one.
    pub fn negotiated_version(&self, api_key: ApiKey) -> Option<i16> {
        self.inner
            .versions
            .get(api_key)
            .and_then(|e| e.negotiated())
    }

    /// Traffic counters for this connection.
    pub fn stats(&self) -> &Arc<ConnectionStats> {
        &self.inner.stats
    }

    /// A snapshot of the counters.
    pub fn stats_snapshot(&self) -> StatsSnapshot {
        self.inner.stats.snapshot()
    }

    /// The address this connection was opened to.
    pub fn peer(&self) -> &str {
        &self.inner.peer
    }

    /// The broker id, when the caller knew it.
    pub fn node_id(&self) -> Option<i32> {
        self.inner.node_id
    }

    /// Whether this client refuses mutating requests.
    pub fn is_read_only(&self) -> bool {
        self.inner.config.read_only
    }

    /// Whether the socket has died.
    pub fn is_closed(&self) -> bool {
        self.inner.pending.is_closed() || self.inner.commands.is_closed()
    }

    /// Close the connection, failing everything in flight.
    pub fn close(&self) {
        let _ = self.inner.shutdown.send(true);
        self.inner.pending.close(&self.inner.peer);
    }

    /// KIP-368: re-authenticate on the live socket before the broker's session
    /// lifetime runs out.
    fn spawn_reauth(&self, first_delay: Duration) {
        let connection = self.clone();
        let mut shutdown = self.inner.shutdown.subscribe();
        tokio::spawn(async move {
            let mut delay = first_delay;
            loop {
                tokio::select! {
                    _ = shutdown.changed() => return,
                    _ = tokio::time::sleep(delay) => {}
                }

                let Some(sasl) = connection.inner.config.sasl.clone() else {
                    return;
                };

                // The write half of the gate: normal sends already in flight
                // finish first, and new ones queue until this returns.
                let _exclusive = connection.inner.reauth_gate.write().await;
                let mut transport = ReauthTransport {
                    connection: &connection,
                };
                match crate::sasl::authenticate(&sasl, &mut transport).await {
                    Ok(lifetime) => match crate::sasl::reauth_delay(lifetime) {
                        Some(next) => {
                            tracing::debug!(peer = %connection.inner.peer, ?next, "re-authenticated");
                            delay = next;
                        }
                        None => return,
                    },
                    Err(error) => {
                        // The broker is about to close this socket anyway.
                        // Failing now, loudly, beats being killed mid-request
                        // by something that looks like a network fault.
                        tracing::warn!(
                            peer = %connection.inner.peer,
                            %error,
                            "re-authentication failed, closing connection"
                        );
                        connection.close();
                        return;
                    }
                }
            }
        });
    }
}

/// Open the socket and, if configured, wrap it in TLS.
async fn open(addr: &str, config: &ConnectionConfig) -> Result<Transport> {
    let tcp = TcpStream::connect(addr)
        .await
        .map_err(|e| Error::transport("connecting to broker", e))?;
    tcp.set_nodelay(true)
        .map_err(|e| Error::transport("disabling Nagle", e))?;

    match &config.tls {
        None => Ok(Transport::Plain(tcp)),
        Some(tls) => {
            let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);
            let connector = tls.connector()?;
            let server_name = tls.server_name(host)?;
            let stream = connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| Error::transport("TLS handshake", e))?;
            Ok(Transport::Tls(Box::new(stream)))
        }
    }
}

/// Split the framed stream into reader and writer tasks.
fn spawn(
    peer: String,
    node_id: Option<i32>,
    config: ConnectionConfig,
    versions: ApiVersions,
    stats: Arc<ConnectionStats>,
    framed: Framed<Transport, tokio_util::codec::LengthDelimitedCodec>,
) -> Connection {
    let (commands_tx, mut commands_rx) = mpsc::channel::<BytesMut>(config.max_in_flight);
    let (shutdown_tx, _) = watch::channel(false);
    let pending = Arc::new(Pending::new());
    let inflight = Arc::new(tokio::sync::Semaphore::new(config.max_in_flight));

    let (mut sink, mut stream) = framed.split();

    // Writer.
    {
        let stats = stats.clone();
        let pending = pending.clone();
        let peer = peer.clone();
        let mut shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            loop {
                let frame = tokio::select! {
                    _ = shutdown.changed() => break,
                    frame = commands_rx.recv() => match frame {
                        Some(frame) => frame,
                        None => break,
                    },
                };
                let len = frame.len() + 4;
                if let Err(error) = sink.send(frame.freeze()).await {
                    tracing::debug!(%peer, %error, "write failed");
                    break;
                }
                stats.record_sent(len);
            }
            // A dead writer means no request can ever be answered.
            pending.close(&peer);
        });
    }

    // Reader.
    {
        let stats = stats.clone();
        let pending = pending.clone();
        let peer = peer.clone();
        let mut shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            loop {
                let frame = tokio::select! {
                    _ = shutdown.changed() => break,
                    frame = stream.next() => match frame {
                        Some(frame) => frame,
                        None => break,
                    },
                };
                match frame {
                    Ok(frame) => {
                        stats.record_received(frame.len() + 4);
                        let frame = frame.freeze();
                        match codec::peek_correlation_id(&frame) {
                            Ok(correlation_id) => match pending.take(correlation_id) {
                                Some(waiter) => {
                                    // A failed send means the caller was
                                    // cancelled. Expected, not an error.
                                    let _ = waiter.send(Ok(frame));
                                }
                                None => tracing::trace!(
                                    %peer,
                                    correlation_id,
                                    "response for a caller that went away"
                                ),
                            },
                            Err(error) => {
                                tracing::warn!(%peer, %error, "undecodable response frame");
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        tracing::debug!(%peer, %error, "read failed");
                        break;
                    }
                }
            }
            pending.close(&peer);
        });
    }

    Connection {
        inner: Arc::new(Inner {
            peer,
            node_id,
            config,
            versions,
            stats,
            commands: commands_tx,
            pending,
            inflight,
            correlation: AtomicI32::new(1),
            reauth_gate: RwLock::new(()),
            shutdown: shutdown_tx,
        }),
    }
}

/// Await `future` until `deadline`, reporting a typed timeout.
async fn with_deadline<F: Future>(
    deadline: Instant,
    api_key: ApiKey,
    future: F,
) -> Result<F::Output> {
    let started = Instant::now();
    match tokio::time::timeout_at(deadline.into(), future).await {
        Ok(value) => Ok(value),
        Err(_) => Err(Error::Timeout {
            api_key,
            elapsed: started.elapsed(),
        }),
    }
}

/// A request/response pair over the bare framed stream, used before the actor
/// exists.
struct RawConn<'a> {
    framed: &'a mut Framed<Transport, tokio_util::codec::LengthDelimitedCodec>,
    config: &'a ConnectionConfig,
    stats: &'a Arc<ConnectionStats>,
    correlation: i32,
    versions: ApiVersions,
    peer: &'a str,
}

impl RawConn<'_> {
    /// One request, one response frame.
    async fn round_trip<R: Rpc>(
        &mut self,
        api_key: ApiKey,
        version: i16,
        request: &R,
    ) -> Result<Bytes> {
        self.correlation = self.correlation.wrapping_add(1) & i32::MAX;
        let bytes = codec::encode_request(
            api_key,
            version,
            self.correlation,
            self.config.client_id.as_deref(),
            request,
        )?;
        let len = bytes.len() + 4;

        let deadline = Instant::now() + self.config.connect_timeout;
        with_deadline(deadline, api_key, self.framed.send(bytes.freeze()))
            .await?
            .map_err(|e| Error::transport("sending handshake request", e))?;
        self.stats.record_sent(len);

        let frame = with_deadline(deadline, api_key, self.framed.next())
            .await?
            .ok_or_else(|| Error::ConnectionClosed {
                peer: self.peer.to_owned(),
            })?
            .map_err(|e| Error::transport("reading handshake response", e))?;
        self.stats.record_received(frame.len() + 4);

        let frame = frame.freeze();
        let correlation = codec::peek_correlation_id(&frame)?;
        if correlation != self.correlation {
            return Err(Error::decode(
                "handshake response correlation id",
                std::io::Error::other(format!("expected {}, got {correlation}", self.correlation)),
            ));
        }
        Ok(frame)
    }

    /// One request, one decoded response.
    async fn call<R: Rpc>(&mut self, api_key: ApiKey, request: &R) -> Result<R::Response> {
        let version = self.versions.negotiate(api_key)?;
        let frame = self.round_trip(api_key, version, request).await?;
        codec::decode_response::<R>(api_key, version, frame)
    }
}

/// The `ApiVersions` bootstrap.
///
/// A genuine chicken-and-egg: we cannot know what the broker speaks until we
/// ask, and asking requires choosing a version. Send at our maximum, and treat
/// `UNSUPPORTED_VERSION` as data rather than as a failed handshake — the broker
/// still returns its version table in that error response, encoded at v0.
async fn negotiate_versions(raw: &mut RawConn<'_>) -> Result<ApiVersions> {
    let our_max = our_range(ApiKey::ApiVersions)
        .map(|r| r.max)
        .ok_or_else(|| Error::Unsupported("this build cannot encode ApiVersions".to_owned()))?;

    let request = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_string(
            raw.config.client_software_name.clone(),
        ))
        .with_client_software_version(StrBytes::from_string(
            raw.config.client_software_version.clone(),
        ));

    let frame = raw
        .round_trip(ApiKey::ApiVersions, our_max, &request)
        .await?;
    // The response header is v0 at every api version — see codec.rs.
    let body = codec::split_response_body(ApiKey::ApiVersions, our_max, frame)?;
    let error_code = peek_error_code(&body)?;

    let response = if error_code == UNSUPPORTED_VERSION {
        tracing::debug!(
            peer = raw.peer,
            our_max,
            "broker rejected ApiVersions at our maximum; falling back to v0"
        );
        let mut at_v0 = body.clone();
        let downgraded = ApiVersionsResponse::decode(&mut at_v0, 0)
            .map_err(|e| Error::decode("decoding ApiVersions v0 fallback", e))?;
        if downgraded.api_keys.is_empty() {
            // Some brokers answer the version probe with an empty table. Ask
            // again, properly, at v0.
            let frame = raw.round_trip(ApiKey::ApiVersions, 0, &request).await?;
            let mut body = codec::split_response_body(ApiKey::ApiVersions, 0, frame)?;
            ApiVersionsResponse::decode(&mut body, 0)
                .map_err(|e| Error::decode("decoding ApiVersions v0", e))?
        } else {
            downgraded
        }
    } else {
        let mut body = body;
        ApiVersionsResponse::decode(&mut body, our_max)
            .map_err(|e| Error::decode("decoding ApiVersions", e))?
    };

    if let Some(code) = ErrorCode::from_code(response.error_code) {
        return Err(Error::from_code(code, None));
    }

    Ok(ApiVersions::from_triples(
        response
            .api_keys
            .iter()
            .map(|k| (k.api_key, k.min_version, k.max_version)),
    ))
}

/// Every response body in the protocol whose first field is an error code puts
/// it first; `ApiVersions` is one, which is what makes the v0 fallback
/// decidable before we know the body's version.
fn peek_error_code(body: &Bytes) -> Result<i16> {
    if body.len() < 2 {
        return Err(Error::decode(
            "reading ApiVersions error code",
            std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "ApiVersions response body is shorter than its error code",
            ),
        ));
    }
    let mut head = body.get(..2).unwrap_or_default();
    Ok(head.get_i16())
}

impl SaslTransport for RawConn<'_> {
    async fn handshake(&mut self, mechanism: &str) -> Result<Vec<String>> {
        let version = self.versions.negotiate(ApiKey::SaslHandshake)?;
        if version < 1 {
            // v0 puts SASL tokens directly on the socket instead of inside
            // SaslAuthenticate. Kafka has offered v1 since 1.0.0; refusing is
            // better than carrying a second framing path forever.
            return Err(Error::Unsupported(
                "broker only offers SaslHandshake v0, which uses raw token framing".to_owned(),
            ));
        }
        let request = SaslHandshakeRequest::default()
            .with_mechanism(StrBytes::from_string(mechanism.to_owned()));
        let frame = self
            .round_trip(ApiKey::SaslHandshake, version, &request)
            .await?;
        let response =
            codec::decode_response::<SaslHandshakeRequest>(ApiKey::SaslHandshake, version, frame)?;
        let mechanisms: Vec<String> = response
            .mechanisms
            .iter()
            .map(|m| m.as_str().to_owned())
            .collect();
        if let Some(code) = ErrorCode::from_code(response.error_code) {
            return Err(Error::Authentication(format!(
                "broker rejected mechanism {mechanism} ({code}); it enables [{}]",
                mechanisms.join(", ")
            )));
        }
        Ok(mechanisms)
    }

    async fn authenticate(&mut self, token: Vec<u8>) -> Result<AuthOutcome> {
        let request = SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(token));
        let response = self.call(ApiKey::SaslAuthenticate, &request).await?;
        auth_outcome(
            response.error_code,
            response.error_message.map(|m| m.as_str().to_owned()),
            response.auth_bytes,
            response.session_lifetime_ms,
        )
    }
}

/// The same exchange, but over a live connection — KIP-368 re-authentication.
struct ReauthTransport<'a> {
    connection: &'a Connection,
}

impl SaslTransport for ReauthTransport<'_> {
    async fn handshake(&mut self, mechanism: &str) -> Result<Vec<String>> {
        let request = SaslHandshakeRequest::default()
            .with_mechanism(StrBytes::from_string(mechanism.to_owned()));
        let deadline = Instant::now() + self.connection.inner.config.request_timeout;
        // `send_inner`, not `send`: the re-auth task already holds the gate.
        let response = self.connection.send_inner(request, deadline).await?;
        let mechanisms: Vec<String> = response
            .mechanisms
            .iter()
            .map(|m| m.as_str().to_owned())
            .collect();
        if let Some(code) = ErrorCode::from_code(response.error_code) {
            return Err(Error::Authentication(format!(
                "broker rejected mechanism {mechanism} on re-authentication ({code})"
            )));
        }
        Ok(mechanisms)
    }

    async fn authenticate(&mut self, token: Vec<u8>) -> Result<AuthOutcome> {
        let request = SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(token));
        let deadline = Instant::now() + self.connection.inner.config.request_timeout;
        let response = self.connection.send_inner(request, deadline).await?;
        auth_outcome(
            response.error_code,
            response.error_message.map(|m| m.as_str().to_owned()),
            response.auth_bytes,
            response.session_lifetime_ms,
        )
    }
}

fn auth_outcome(
    error_code: i16,
    error_message: Option<String>,
    auth_bytes: Bytes,
    session_lifetime_ms: i64,
) -> Result<AuthOutcome> {
    if let Some(code) = ErrorCode::from_code(error_code) {
        return Err(Error::from_code(code, error_message));
    }
    Ok(AuthOutcome {
        auth_bytes: auth_bytes.to_vec(),
        session_lifetime_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::ResponseHeader;
    use kafka_protocol::protocol::Encodable;

    #[test]
    fn pending_resolves_every_waiter_when_the_socket_dies() {
        let pending = Pending::new();
        let (tx, mut rx) = oneshot::channel();
        pending.register(7, tx, "broker:9092").unwrap();
        pending.close("broker:9092");

        let received = rx.try_recv().expect("resolved, not dropped");
        assert!(matches!(received, Err(Error::ConnectionClosed { .. })));
        assert!(pending.is_closed());
    }

    #[test]
    fn registering_on_a_dead_connection_fails_immediately() {
        let pending = Pending::new();
        pending.close("broker:9092");
        let (tx, _rx) = oneshot::channel();
        let err = pending.register(1, tx, "broker:9092").unwrap_err();
        assert!(matches!(err, Error::ConnectionClosed { .. }));
    }

    #[test]
    fn taking_an_unknown_correlation_id_is_not_an_error() {
        // This is the cancelled-caller path: the frame arrives, nobody wants
        // it, and the reader must carry on.
        let pending = Pending::new();
        assert!(pending.take(99).is_none());
        assert!(!pending.is_closed());
    }

    /// `ApiVersionsResponse` bodies are hand-assembled here: the crate's
    /// response *encoders* are behind `feature = "broker"`, which this
    /// workspace does not build. See lib.rs.
    fn api_versions_body(version: i16, error_code: i16) -> BytesMut {
        let mut body = BytesMut::new();
        body.extend_from_slice(&error_code.to_be_bytes());
        if version >= 3 {
            body.extend_from_slice(&[1]); // compact array, zero entries
            body.extend_from_slice(&0i32.to_be_bytes()); // throttle_time_ms
            body.extend_from_slice(&[0]); // tagged fields
        } else {
            body.extend_from_slice(&0i32.to_be_bytes()); // array, zero entries
            if version >= 1 {
                body.extend_from_slice(&0i32.to_be_bytes()); // throttle_time_ms
            }
        }
        body
    }

    #[test]
    fn an_api_versions_error_body_still_yields_a_readable_error_code() {
        let body = api_versions_body(0, UNSUPPORTED_VERSION);
        assert_eq!(
            peek_error_code(&body.freeze()).unwrap(),
            UNSUPPORTED_VERSION
        );
    }

    #[test]
    fn a_truncated_api_versions_body_is_an_error_not_a_panic() {
        assert!(peek_error_code(&Bytes::from_static(&[0])).is_err());
    }

    #[test]
    fn the_error_code_is_readable_before_the_body_version_is_known() {
        // The whole point of the fallback: a v0-encoded body and a v3-encoded
        // body both start with the error code, so the bootstrap can decide
        // which decoder to use without having decided already.
        for version in [0i16, 3] {
            let mut frame = BytesMut::new();
            ResponseHeader::default()
                .with_correlation_id(1)
                .encode(&mut frame, 0)
                .unwrap();
            frame.extend_from_slice(&api_versions_body(version, UNSUPPORTED_VERSION));
            let body =
                codec::split_response_body(ApiKey::ApiVersions, version, frame.freeze()).unwrap();
            assert_eq!(peek_error_code(&body).unwrap(), UNSUPPORTED_VERSION);
        }
    }

    #[test]
    fn a_v0_fallback_body_decodes_into_a_version_table() {
        // What the broker sends when it rejects our ApiVersions version: an
        // error code *and* its table, encoded at v0.
        let mut body = BytesMut::new();
        body.extend_from_slice(&UNSUPPORTED_VERSION.to_be_bytes());
        body.extend_from_slice(&1i32.to_be_bytes()); // one entry
        body.extend_from_slice(&ApiKey::Metadata.code().to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
        body.extend_from_slice(&13i16.to_be_bytes());

        let mut buf = body.freeze();
        let decoded = ApiVersionsResponse::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.error_code, UNSUPPORTED_VERSION);
        assert_eq!(decoded.api_keys.len(), 1, "the table survives the error");

        let table = ApiVersions::from_triples(
            decoded
                .api_keys
                .iter()
                .map(|k| (k.api_key, k.min_version, k.max_version)),
        );
        assert!(table.supports(ApiKey::Metadata));
    }
}
