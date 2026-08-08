//! The client error type.
//!
//! M5 asks for the failure modes to be distinguishable *at the type level*,
//! because a UI renders them differently: a transport error is "the cluster is
//! unreachable", an authorization error is "ask your admin", a decode failure
//! is "this is our bug". Collapsing them into one string throws that away.
//!
//! The broker's own codes live next door in [`crate::error_code`]; this type is
//! about what happened, that one is about what the broker said.

use std::error::Error as StdError;
use std::time::Duration;

use crate::api_key::ApiKey;
use crate::error_code::ErrorCode;

/// Result alias for this workspace.
pub type Result<T> = std::result::Result<T, Error>;

/// Anything that can go wrong talking to a broker.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The socket failed, or never opened.
    #[error("{context}: {source}")]
    Transport {
        /// What we were doing.
        context: &'static str,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// The connection is gone. Every in-flight request resolves to this rather
    /// than hanging — a UI backend that leaks a hung future per dead broker
    /// stops working long before anyone notices why.
    #[error("connection to {peer} closed")]
    ConnectionClosed {
        /// Which broker, for the log line that follows.
        peer: String,
    },

    /// The caller's deadline passed.
    #[error("{api_key} timed out after {elapsed:?}")]
    Timeout {
        /// The request that ran out of time.
        api_key: ApiKey,
        /// How long it had.
        elapsed: Duration,
    },

    /// The credentials were rejected, or the handshake could not agree.
    #[error("authentication failed: {0}")]
    Authentication(String),

    /// The principal authenticated but is not permitted to do this.
    #[error("not authorized: {0}")]
    Authorization(ErrorCode),

    /// The broker answered with an error code.
    #[error("broker returned {code}{}", .message.as_ref().map(|m| format!(": {m}")).unwrap_or_default())]
    Broker {
        /// The classified code.
        code: ErrorCode,
        /// The broker's own message, when the response carries one.
        message: Option<String>,
    },

    /// A response did not parse.
    ///
    /// Distinct from every other variant because it means *we* are wrong: a
    /// version negotiated badly, or a schema drifted.
    #[error("{context}: {source}")]
    Decode {
        /// What we were decoding.
        context: &'static str,
        /// The underlying decode failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// A read-only client refused a mutating request before touching the
    /// network. See [`ApiKey::is_mutating`].
    #[error("client is read-only and {api_key} mutates cluster state")]
    ReadOnly {
        /// The key that was refused.
        api_key: ApiKey,
    },

    /// No version of this API is speakable by both ends.
    ///
    /// Usually *our* side is the binding one: `kafka-protocol` 0.17 ships
    /// Kafka 4.0 schemas and the broker is newer.
    #[error("no usable version of {api_key}: broker offers {broker:?}, we speak {ours:?}")]
    UnsupportedApi {
        /// The key.
        api_key: ApiKey,
        /// The broker's `(min, max)`, if it advertised the key at all.
        broker: Option<(i16, i16)>,
        /// Our `(min, max)`, if this build knows the key.
        ours: Option<(i16, i16)>,
    },

    /// The caller asked for something the protocol or this build cannot
    /// express — an unnamed API key, a sentinel that needs a schema version we
    /// cannot encode. An honest blocker, not a workaround.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// A request was malformed before it went out.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// An OAuth token endpoint could not be reached, or refused to issue a
    /// token.
    ///
    /// Deliberately not [`Error::Authentication`]: nothing has been said to a
    /// broker yet. "Your identity provider rejected our client secret" and "the
    /// broker rejected the token we got" are different problems with different
    /// owners, and collapsing them sends an operator to the wrong system.
    #[error("token endpoint {endpoint} {detail}")]
    TokenEndpoint {
        /// The endpoint that was asked, so the message names the system at
        /// fault.
        endpoint: String,
        /// The HTTP status, when there was a response at all. `None` means the
        /// endpoint was never reached.
        status: Option<u16>,
        /// What happened, including the issuer's own `error_description` when it
        /// sent one.
        detail: String,
    },
}

impl Error {
    /// Wrap an I/O error with context.
    pub fn transport(context: &'static str, source: std::io::Error) -> Self {
        Error::Transport { context, source }
    }

    /// Wrap a decode failure with context.
    pub fn decode(
        context: &'static str,
        source: impl Into<Box<dyn StdError + Send + Sync>>,
    ) -> Self {
        Error::Decode {
            context,
            source: source.into(),
        }
    }

    /// Build the right variant for a broker error code.
    ///
    /// Authentication and authorization codes are lifted out of
    /// [`Error::Broker`] here rather than at every call site, because a caller
    /// that forgets renders "not authorized" as a generic failure.
    pub fn from_code(code: ErrorCode, message: Option<String>) -> Self {
        if code.is_authentication() {
            Error::Authentication(message.unwrap_or_else(|| code.to_string()))
        } else if code.is_authorization() {
            Error::Authorization(code)
        } else {
            Error::Broker { code, message }
        }
    }

    /// The broker code, when there is one.
    pub fn code(&self) -> Option<ErrorCode> {
        match self {
            Error::Broker { code, .. } | Error::Authorization(code) => Some(*code),
            _ => None,
        }
    }

    /// Whether retrying could plausibly succeed.
    ///
    /// A dead connection counts: the pool will open a new one. A decode failure
    /// does not — retrying a schema mismatch just burns the same bytes again.
    pub fn retriable(&self) -> bool {
        match self {
            Error::Transport { .. } | Error::ConnectionClosed { .. } | Error::Timeout { .. } => {
                true
            }
            Error::Broker { code, .. } => code.retriable(),
            // An endpoint we never reached may come back, and so may one that
            // is overloaded or rate limiting. One that *refused* — a bad client
            // secret, an unknown scope — will refuse again.
            Error::TokenEndpoint { status, .. } => match status {
                None => true,
                Some(code) => *code >= 500 || *code == 429,
            },
            Error::Authorization(_)
            | Error::Authentication(_)
            | Error::Decode { .. }
            | Error::ReadOnly { .. }
            | Error::UnsupportedApi { .. }
            | Error::Unsupported(_)
            | Error::InvalidRequest(_) => false,
        }
    }

    /// Whether handling this error should refresh the metadata snapshot.
    pub fn needs_metadata_refresh(&self) -> bool {
        // A dead or unreachable broker is itself evidence the snapshot is
        // stale, not just evidence that one request failed.
        match self {
            Error::Transport { .. } | Error::ConnectionClosed { .. } => true,
            Error::Broker { code, .. } => code.needs_metadata_refresh(),
            _ => false,
        }
    }

    /// Whether handling this error should invalidate a cached coordinator.
    pub fn needs_coordinator_refresh(&self) -> bool {
        match self {
            Error::Broker { code, .. } => code.needs_coordinator_refresh(),
            _ => false,
        }
    }
}

/// One failure often has to be reported to many callers: every record in a
/// rejected produce batch, every partition in a request whose connection died.
/// Without `Clone` each of those sites has to invent a way to fan an error out,
/// and they invent different ones.
///
/// Two variants cannot be duplicated faithfully and are **reconstructed**:
///
/// * [`Error::Transport`] keeps its [`std::io::ErrorKind`] and its rendering,
///   but a cloned `io::Error` loses the raw OS error code.
/// * [`Error::Decode`] keeps its source's rendering rather than its concrete
///   type, so downcasting the clone will not find the original.
///
/// Everything [`Error::retriable`], [`Error::code`],
/// [`Error::needs_metadata_refresh`] and `Display` read is preserved exactly,
/// which is the whole of what callers branch on. Derived rather than hand-
/// written is not an option — `io::Error` and a boxed source are not `Clone`.
impl Clone for Error {
    fn clone(&self) -> Self {
        match self {
            Error::Transport { context, source } => Error::Transport {
                context,
                source: std::io::Error::new(source.kind(), source.to_string()),
            },
            Error::ConnectionClosed { peer } => Error::ConnectionClosed { peer: peer.clone() },
            Error::Timeout { api_key, elapsed } => Error::Timeout {
                api_key: *api_key,
                elapsed: *elapsed,
            },
            Error::Authentication(message) => Error::Authentication(message.clone()),
            Error::Authorization(code) => Error::Authorization(*code),
            Error::Broker { code, message } => Error::Broker {
                code: *code,
                message: message.clone(),
            },
            Error::Decode { context, source } => Error::Decode {
                context,
                source: source.to_string().into(),
            },
            Error::ReadOnly { api_key } => Error::ReadOnly { api_key: *api_key },
            Error::UnsupportedApi {
                api_key,
                broker,
                ours,
            } => Error::UnsupportedApi {
                api_key: *api_key,
                broker: *broker,
                ours: *ours,
            },
            Error::Unsupported(message) => Error::Unsupported(message.clone()),
            Error::InvalidRequest(message) => Error::InvalidRequest(message.clone()),
            Error::TokenEndpoint {
                endpoint,
                status,
                detail,
            } => Error::TokenEndpoint {
                endpoint: endpoint.clone(),
                status: *status,
                detail: detail.clone(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_codes_do_not_hide_inside_broker() {
        // 58 = SASL_AUTHENTICATION_FAILED, 29 = TOPIC_AUTHORIZATION_FAILED.
        let authn = Error::from_code(ErrorCode::SaslAuthenticationFailed, None);
        assert!(matches!(authn, Error::Authentication(_)));
        let authz = Error::from_code(ErrorCode::TopicAuthorizationFailed, None);
        assert!(matches!(authz, Error::Authorization(_)));
        assert!(!authz.retriable());
    }

    #[test]
    fn decode_failures_are_never_retried() {
        let err = Error::decode("test", std::io::Error::other("boom"));
        assert!(!err.retriable());
        assert!(!err.needs_metadata_refresh());
    }

    #[test]
    fn transport_failures_invalidate_metadata() {
        let err = Error::transport("connect", std::io::Error::other("boom"));
        assert!(err.retriable());
        assert!(err.needs_metadata_refresh());
    }

    #[test]
    fn broker_errors_delegate_both_axes() {
        let err = Error::from_code(ErrorCode::NotLeaderOrFollower, None);
        assert!(err.retriable());
        assert!(err.needs_metadata_refresh());
        assert!(!err.needs_coordinator_refresh());

        let err = Error::from_code(ErrorCode::NotCoordinator, None);
        assert!(err.needs_coordinator_refresh());
        assert!(!err.needs_metadata_refresh());
    }
}
