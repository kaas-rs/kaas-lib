//! Delegation tokens, KIP-48.
//!
//! A delegation token is a short-lived shared secret a broker issues to an
//! already-authenticated principal, so that something else — a Connect worker,
//! a Streams task, a batch job fanned out over a hundred containers — can
//! authenticate *as* that principal without being handed its password or its
//! keytab. The token is a SCRAM credential: the id is the username, the
//! base64-encoded HMAC is the password, and
//! [`SaslConfig::delegation_token`](kafka_conn::SaslConfig::delegation_token)
//! is the other half of this module.
//!
//! Three things about the feature are worth knowing before using it, because
//! each is a broker rule rather than a client one and each fails as a bare
//! error code:
//!
//! * **A token cannot beget a token.** `CreateDelegationToken` is refused on a
//!   connection that authenticated with a delegation token, which is what stops
//!   one leaked token from renewing itself indefinitely into a new identity.
//! * **The feature is off unless the broker has a master key.** Without
//!   `delegation.token.secret.key` every call here comes back
//!   `DELEGATION_TOKEN_AUTH_DISABLED`, and on a cluster with more than one
//!   broker the key must be the *same* on all of them or a token issued by one
//!   fails against another.
//! * **Only the owner and the renewers may renew or expire**, and both are
//!   identified by principal — so a renewer has to be named at creation time.
//!
//! The HMAC is the credential. It is never rendered by `Debug` here, and it is
//! the one field worth being careful with when logging a token elsewhere.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::Bytes;
use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::create_delegation_token_request::CreatableRenewers;
use kafka_conn::protocol::messages::describe_delegation_token_request::DescribeDelegationTokenOwner;
use kafka_conn::protocol::messages::{
    CreateDelegationTokenRequest, DescribeDelegationTokenRequest, ExpireDelegationTokenRequest,
    RenewDelegationTokenRequest,
};
use kafka_conn::{Error, ErrorCode, Result};

use crate::Admin;

/// A Kafka principal: a type and a name.
///
/// Kafka writes one as `User:alice`, and the type is almost always `User` —
/// but it is a separate field on the wire wherever a principal is not an ACL
/// string, so it is a separate field here.
///
/// The name is not necessarily a username. A client authenticated by
/// certificate is the certificate's subject, so `CN=bob,O=example` is a
/// perfectly ordinary principal name, commas and all. That is why
/// [`Principal::parse`] splits on the *first* colon only.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Principal {
    /// The principal type — `User` unless a custom principal builder says
    /// otherwise.
    pub principal_type: String,
    /// The principal name.
    pub name: String,
}

impl Principal {
    /// A `User:` principal.
    pub fn user(name: impl Into<String>) -> Self {
        Self {
            principal_type: "User".to_owned(),
            name: name.into(),
        }
    }

    /// A principal of any type.
    pub fn new(principal_type: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            principal_type: principal_type.into(),
            name: name.into(),
        }
    }

    /// Whether the name is a certificate subject rather than a username.
    ///
    /// Kafka's default `ssl.principal.mapping.rules` is `DEFAULT`, which is to
    /// say "use the whole X.500 subject", so a client authenticated by
    /// certificate is `CN=bob-mtls` and not `bob-mtls` — Strimzi's User
    /// Operator issues exactly that for a `KafkaUser`. Anything rendering "who
    /// is this" or matching an ACL principal has to expect the DN form; a
    /// screen that assumes a bare username shows nothing on a mutual-TLS
    /// cluster and blames the cluster.
    ///
    /// Recognises the shape — a leading `attribute=value` — rather than
    /// validating RFC 4514, because the question being asked is "should this be
    /// read as a name or as a subject", and a broker will not hand us a
    /// malformed one.
    pub fn is_distinguished_name(&self) -> bool {
        let first = self.name.split(',').next().unwrap_or_default().trim();
        match first.split_once('=') {
            Some((attribute, value)) => {
                let attribute = attribute.trim();
                !attribute.is_empty()
                    && !value.trim().is_empty()
                    && attribute
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '.')
            }
            None => false,
        }
    }

    /// Read a `Type:name` string, as an ACL binding spells it.
    ///
    /// Splits on the first colon, so a distinguished name keeps the rest of
    /// its own. A string with no colon at all is taken to be a `User` name,
    /// because that is what someone typing one means.
    pub fn parse(principal: &str) -> Self {
        match principal.split_once(':') {
            Some((principal_type, name)) => Self::new(principal_type, name),
            None => Self::user(principal),
        }
    }
}

impl std::fmt::Display for Principal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.principal_type, self.name)
    }
}

/// What to ask for when creating a token.
#[derive(Debug, Clone, Default)]
pub struct NewDelegationToken {
    owner: Option<Principal>,
    renewers: Vec<Principal>,
    max_lifetime_ms: i64,
}

impl NewDelegationToken {
    /// A token for the calling principal, with the broker's default lifetime.
    pub fn new() -> Self {
        Self {
            owner: None,
            renewers: Vec::new(),
            // -1 is the schema's "use the server side default", which is
            // `delegation.token.max.lifetime.ms`. Not zero: zero is a lifetime.
            max_lifetime_ms: -1,
        }
    }

    /// Issue the token to another principal (KIP-373).
    ///
    /// Needs `CreateDelegationToken` v3, which is Kafka 3.3 and later, and the
    /// caller needs `CreateTokens` on the `User` resource for that principal.
    /// On an older broker this fails with an [`Error::Unsupported`] naming the
    /// version rather than quietly issuing a token to the wrong principal.
    #[must_use]
    pub fn with_owner(mut self, owner: Principal) -> Self {
        self.owner = Some(owner);
        self
    }

    /// Allow `renewer` to renew and expire this token.
    ///
    /// Additive; the owner may always do both and does not need naming here.
    /// Nobody else can be added later — the renewer list is fixed at creation.
    #[must_use]
    pub fn with_renewer(mut self, renewer: Principal) -> Self {
        self.renewers.push(renewer);
        self
    }

    /// Cap the token's lifetime.
    ///
    /// The broker clamps this to its own `delegation.token.max.lifetime.ms`,
    /// so this can shorten a token and never lengthen one.
    #[must_use]
    pub fn with_max_lifetime(mut self, lifetime: std::time::Duration) -> Self {
        self.max_lifetime_ms = i64::try_from(lifetime.as_millis()).unwrap_or(i64::MAX);
        self
    }
}

/// A delegation token.
///
/// [`DelegationToken::hmac`] is the credential half and is what
/// [`DelegationToken::password`] renders for a SASL configuration.
/// `DescribeDelegationToken` returns it too, for the tokens the calling
/// principal owns.
#[derive(Clone, PartialEq, Eq)]
pub struct DelegationToken {
    /// Whose authority the token carries.
    pub owner: Principal,
    /// Who asked for it. The same as the owner unless a KIP-373 request named
    /// somebody else.
    pub requester: Principal,
    /// The token id, which is the SCRAM username.
    pub token_id: String,
    /// The HMAC, which is the SCRAM password once base64-encoded. **Secret.**
    pub hmac: Bytes,
    /// When the token was issued, in epoch milliseconds.
    pub issue_timestamp_ms: i64,
    /// When it expires unless renewed, in epoch milliseconds.
    pub expiry_timestamp_ms: i64,
    /// The furthest it can ever be renewed to, in epoch milliseconds. Renewing
    /// past this is refused, so it is the number that decides when the holder
    /// needs a new token rather than another renewal.
    pub max_timestamp_ms: i64,
    /// Who else may renew it. Empty on a freshly created token: the response
    /// to `CreateDelegationToken` does not echo the list back, so what is known
    /// here is what was asked for, not what the broker recorded — call
    /// [`Admin::describe_delegation_tokens`] for that.
    pub renewers: Vec<Principal>,
}

impl DelegationToken {
    /// The SCRAM password for this token: the HMAC, base64-encoded.
    ///
    /// This is the exact string a Java client puts in its JAAS config, and the
    /// second argument to
    /// [`SaslConfig::delegation_token`](kafka_conn::SaslConfig::delegation_token).
    pub fn password(&self) -> String {
        B64.encode(&self.hmac)
    }
}

impl std::fmt::Debug for DelegationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The whole point of a delegation token is that the HMAC is a usable
        // credential until it expires. A `#[derive(Debug)]` here puts one in
        // every log line that formats an admin result.
        f.debug_struct("DelegationToken")
            .field("owner", &self.owner)
            .field("requester", &self.requester)
            .field("token_id", &self.token_id)
            .field("hmac", &"<redacted>")
            .field("issue_timestamp_ms", &self.issue_timestamp_ms)
            .field("expiry_timestamp_ms", &self.expiry_timestamp_ms)
            .field("max_timestamp_ms", &self.max_timestamp_ms)
            .field("renewers", &self.renewers)
            .finish()
    }
}

impl Admin {
    /// Create a delegation token.
    ///
    /// The connection this goes out on must have authenticated as something
    /// other than a delegation token, and the cluster must have a
    /// `delegation.token.secret.key`; neither is checkable from here, and both
    /// come back as an error code rather than a silent failure.
    pub async fn create_delegation_token(
        &self,
        options: &NewDelegationToken,
    ) -> Result<DelegationToken> {
        let mut request = CreateDelegationTokenRequest::default()
            .with_max_lifetime_ms(options.max_lifetime_ms)
            .with_renewers(
                options
                    .renewers
                    .iter()
                    .map(|renewer| {
                        CreatableRenewers::default()
                            .with_principal_type(StrBytes::from_string(
                                renewer.principal_type.clone(),
                            ))
                            .with_principal_name(StrBytes::from_string(renewer.name.clone()))
                    })
                    .collect(),
            );

        if let Some(owner) = &options.owner {
            // The field exists only in v3. Encoding it at v1 or v2 is not a
            // silent no-op in this codec — it is an encode error — but the
            // message that matters is which broker feature is missing, not
            // which field failed to serialise.
            let version = self
                .negotiated_for::<CreateDelegationTokenRequest>()
                .await?;
            if version < 3 {
                return Err(Error::Unsupported(format!(
                    "issuing a delegation token to another principal needs \
                     CreateDelegationToken v3 (KIP-373); this cluster negotiates v{version}"
                )));
            }
            request = request
                .with_owner_principal_type(Some(StrBytes::from_string(
                    owner.principal_type.clone(),
                )))
                .with_owner_principal_name(Some(StrBytes::from_string(owner.name.clone())));
        }

        let response = self.cluster().send_any(request).await?;
        if let Some(code) = ErrorCode::from_code(response.error_code) {
            return Err(Error::from_code(code, None));
        }

        let owner = Principal::new(
            response.principal_type.to_string(),
            response.principal_name.to_string(),
        );
        // v1 and v2 have no requester fields, so they arrive empty. The owner
        // *is* the requester there — there was no way to ask for anything else
        // before KIP-373 — and reporting an empty principal instead would be a
        // version artefact dressed up as a fact about the token.
        let requester = if response.token_requester_principal_name.is_empty() {
            owner.clone()
        } else {
            Principal::new(
                response.token_requester_principal_type.to_string(),
                response.token_requester_principal_name.to_string(),
            )
        };

        Ok(DelegationToken {
            owner,
            requester,
            token_id: response.token_id.to_string(),
            hmac: response.hmac,
            issue_timestamp_ms: response.issue_timestamp_ms,
            expiry_timestamp_ms: response.expiry_timestamp_ms,
            max_timestamp_ms: response.max_timestamp_ms,
            renewers: options.renewers.clone(),
        })
    }

    /// Renew a token, returning its new expiry in epoch milliseconds.
    ///
    /// `hmac` identifies the token — the token id is not accepted here, which
    /// is deliberate on the broker's part: renewing requires proving you hold
    /// the secret. The caller must be the owner or a named renewer.
    ///
    /// The broker clamps the new expiry to the token's `max_timestamp_ms`, so
    /// a renewal that returns an unchanged expiry means the token has reached
    /// the end of its life and needs replacing rather than renewing.
    pub async fn renew_delegation_token(
        &self,
        hmac: impl Into<Bytes>,
        renew_period: std::time::Duration,
    ) -> Result<i64> {
        let request = RenewDelegationTokenRequest::default()
            .with_hmac(hmac.into())
            .with_renew_period_ms(i64::try_from(renew_period.as_millis()).unwrap_or(i64::MAX));

        let response = self.cluster().send_any(request).await?;
        match ErrorCode::from_code(response.error_code) {
            Some(code) => Err(Error::from_code(code, None)),
            None => Ok(response.expiry_timestamp_ms),
        }
    }

    /// Expire a token, returning the expiry the broker settled on.
    ///
    /// A negative or zero period expires it immediately, which is the
    /// revocation path; a positive one *shortens* its life to now plus that
    /// period. It cannot extend a token — use
    /// [`Admin::renew_delegation_token`] for that.
    pub async fn expire_delegation_token(
        &self,
        hmac: impl Into<Bytes>,
        expiry_period_ms: i64,
    ) -> Result<i64> {
        let request = ExpireDelegationTokenRequest::default()
            .with_hmac(hmac.into())
            .with_expiry_time_period_ms(expiry_period_ms);

        let response = self.cluster().send_any(request).await?;
        match ErrorCode::from_code(response.error_code) {
            Some(code) => Err(Error::from_code(code, None)),
            None => Ok(response.expiry_timestamp_ms),
        }
    }

    /// Describe tokens.
    ///
    /// `owners` of `None` asks for every token the calling principal is
    /// allowed to see — its own, plus every token if it has `Describe` on the
    /// cluster. An empty slice asks for nothing and is answered with nothing,
    /// which is a different question and worth not confusing with the first.
    pub async fn describe_delegation_tokens(
        &self,
        owners: Option<&[Principal]>,
    ) -> Result<Vec<DelegationToken>> {
        let request = DescribeDelegationTokenRequest::default().with_owners(owners.map(|owners| {
            owners
                .iter()
                .map(|owner| {
                    DescribeDelegationTokenOwner::default()
                        .with_principal_type(StrBytes::from_string(owner.principal_type.clone()))
                        .with_principal_name(StrBytes::from_string(owner.name.clone()))
                })
                .collect()
        }));

        let response = self.cluster().send_any(request).await?;
        if let Some(code) = ErrorCode::from_code(response.error_code) {
            return Err(Error::from_code(code, None));
        }

        Ok(response
            .tokens
            .into_iter()
            .map(|token| {
                let owner = Principal::new(
                    token.principal_type.to_string(),
                    token.principal_name.to_string(),
                );
                let requester = if token.token_requester_principal_name.is_empty() {
                    owner.clone()
                } else {
                    Principal::new(
                        token.token_requester_principal_type.to_string(),
                        token.token_requester_principal_name.to_string(),
                    )
                };
                DelegationToken {
                    owner,
                    requester,
                    token_id: token.token_id.to_string(),
                    hmac: token.hmac,
                    issue_timestamp_ms: token.issue_timestamp,
                    expiry_timestamp_ms: token.expiry_timestamp,
                    max_timestamp_ms: token.max_timestamp,
                    renewers: token
                        .renewers
                        .into_iter()
                        .map(|renewer| {
                            Principal::new(
                                renewer.principal_type.to_string(),
                                renewer.principal_name.to_string(),
                            )
                        })
                        .collect(),
                }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_principal_round_trips_through_its_acl_spelling() {
        let principal = Principal::parse("User:alice");
        assert_eq!(principal, Principal::user("alice"));
        assert_eq!(principal.to_string(), "User:alice");
    }

    /// A certificate-authenticated principal is its subject DN, which contains
    /// commas and equals signs and — the part that breaks a naive parser —
    /// nothing that looks like a username.
    #[test]
    fn a_distinguished_name_keeps_its_own_separators() {
        let principal = Principal::parse("User:CN=bob-mtls,O=example");
        assert_eq!(principal.principal_type, "User");
        assert_eq!(principal.name, "CN=bob-mtls,O=example");
        assert_eq!(principal.to_string(), "User:CN=bob-mtls,O=example");
    }

    #[test]
    fn a_subject_is_told_apart_from_a_username() {
        for dn in [
            "User:CN=bob-mtls",
            "User:CN=bob-mtls,O=io.strimzi",
            "User:CN = bob, OU = clients",
            "User:1.2.840.113549=x",
        ] {
            assert!(
                Principal::parse(dn).is_distinguished_name(),
                "{dn} is a subject"
            );
        }
        for name in [
            "User:alice",
            "User:*",
            "User:ANONYMOUS",
            // An equals sign alone does not make a subject; the attribute has
            // to look like one, and neither half may be empty.
            "User:=bob",
            "User:weird name=",
        ] {
            assert!(
                !Principal::parse(name).is_distinguished_name(),
                "{name} is a username"
            );
        }
    }

    #[test]
    fn a_bare_name_is_taken_to_be_a_user() {
        assert_eq!(Principal::parse("alice"), Principal::user("alice"));
    }

    /// The link between the two halves of KIP-48: the string this returns is
    /// the SASL password, and getting the encoding wrong produces a token that
    /// creates cleanly and cannot log in.
    #[test]
    fn the_password_is_the_base64_of_the_hmac() {
        let token = DelegationToken {
            owner: Principal::user("alice"),
            requester: Principal::user("alice"),
            token_id: "id".to_owned(),
            hmac: Bytes::from_static(b"hmac"),
            issue_timestamp_ms: 0,
            expiry_timestamp_ms: 0,
            max_timestamp_ms: 0,
            renewers: Vec::new(),
        };
        assert_eq!(token.password(), "aG1hYw==");
    }

    #[test]
    fn debug_never_renders_the_hmac() {
        let token = DelegationToken {
            owner: Principal::user("alice"),
            requester: Principal::user("alice"),
            token_id: "id".to_owned(),
            hmac: Bytes::from_static(b"SUPER-SECRET"),
            issue_timestamp_ms: 0,
            expiry_timestamp_ms: 0,
            max_timestamp_ms: 0,
            renewers: Vec::new(),
        };
        let rendered = format!("{token:?}");
        assert!(!rendered.contains("SUPER-SECRET"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
    }

    #[test]
    fn a_new_token_asks_for_the_brokers_default_lifetime() {
        assert_eq!(NewDelegationToken::new().max_lifetime_ms, -1);
        assert_eq!(
            NewDelegationToken::new()
                .with_max_lifetime(std::time::Duration::from_secs(3600))
                .max_lifetime_ms,
            3_600_000
        );
    }
}
