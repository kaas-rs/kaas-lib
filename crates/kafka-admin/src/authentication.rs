//! How a cluster's listeners authenticate, and what that implies about one
//! principal.
//!
//! [`principal`](crate::principal) answers "what credentials does the cluster
//! store for X", which is a complete answer when X is a SCRAM user and silence
//! when X is anything else. This module is the other half: what the cluster
//! *accepts*. Crossed with the first half, silence starts saying something —
//! a principal with no stored credential on a cluster whose only client
//! listener is `SASL_SSL` with `OAUTHBEARER` enabled has exactly one way in.
//!
//! Everything here is read from a broker's own configuration, which means two
//! standing caveats:
//!
//! * **It is inference, not proof.** The only record of how a principal
//!   actually authenticated on a given connection is the broker's authorizer
//!   log. [`MechanismVerdict::is_conclusive`] says whether one possibility
//!   survived elimination, not whether the broker agrees.
//! * **The `PLAIN` credential store is invisible.** Its JAAS entries are
//!   sensitive configs, so they arrive with no value at all. That a listener
//!   *enables* `PLAIN` is readable; who can use it is not.

use std::collections::BTreeMap;
use std::fmt;

use kafka_conn::{Error, Result};

use crate::Admin;
use crate::principal::PrincipalDescription;
use crate::security::ScramMechanism;
use crate::types::{ConfigEntry, ConfigResource};

/// Kafka's default `ssl.principal.mapping.rules`: use the whole certificate
/// subject, so a certificate principal is its DN and nothing else.
const DEFAULT_MAPPING_RULES: &str = "DEFAULT";

/// A listener's transport and authentication protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecurityProtocol {
    /// Unencrypted, unauthenticated.
    Plaintext,
    /// TLS, with the principal coming from the client certificate when one is
    /// asked for.
    Ssl,
    /// SASL over an unencrypted socket.
    SaslPlaintext,
    /// SASL over TLS.
    SaslSsl,
    /// A protocol name this build does not know.
    Other(String),
}

impl SecurityProtocol {
    /// Parse a `listener.security.protocol.map` value.
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_uppercase().as_str() {
            "PLAINTEXT" => SecurityProtocol::Plaintext,
            "SSL" => SecurityProtocol::Ssl,
            "SASL_PLAINTEXT" => SecurityProtocol::SaslPlaintext,
            "SASL_SSL" => SecurityProtocol::SaslSsl,
            other => SecurityProtocol::Other(other.to_owned()),
        }
    }

    /// Whether the listener negotiates SASL.
    pub fn uses_sasl(&self) -> bool {
        matches!(
            self,
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
        )
    }

    /// Whether the listener runs TLS, and can therefore see a client
    /// certificate.
    pub fn uses_tls(&self) -> bool {
        matches!(self, SecurityProtocol::Ssl | SecurityProtocol::SaslSsl)
    }
}

impl fmt::Display for SecurityProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SecurityProtocol::Plaintext => f.write_str("PLAINTEXT"),
            SecurityProtocol::Ssl => f.write_str("SSL"),
            SecurityProtocol::SaslPlaintext => f.write_str("SASL_PLAINTEXT"),
            SecurityProtocol::SaslSsl => f.write_str("SASL_SSL"),
            SecurityProtocol::Other(name) => f.write_str(name),
        }
    }
}

/// Whether a TLS listener asks for a client certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientAuth {
    /// No certificate is asked for, so no principal can come from one.
    #[default]
    None,
    /// A certificate is accepted if offered. Both mutual TLS and another
    /// mechanism are possible on the same listener.
    Requested,
    /// A certificate is mandatory.
    Required,
}

impl ClientAuth {
    /// Parse an `ssl.client.auth` value. Anything unrecognised is `None`,
    /// which is also Kafka's default.
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "requested" => ClientAuth::Requested,
            "required" => ClientAuth::Required,
            _ => ClientAuth::None,
        }
    }

    /// Whether a client certificate can produce a principal here.
    pub const fn accepts_certificates(self) -> bool {
        matches!(self, ClientAuth::Requested | ClientAuth::Required)
    }
}

/// A way of authenticating, as this crate names them.
///
/// Wider than [`kafka_conn::SaslMechanism`] on purpose: that enum is what this
/// client can *speak*, and this one is what a cluster can be configured to
/// accept — including mutual TLS, which is not SASL at all, and Kerberos,
/// which this client does not implement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthMechanism {
    /// SCRAM, at one hash.
    Scram(ScramMechanism),
    /// `PLAIN` — a username and password checked against a broker-side store
    /// this client cannot read.
    Plain,
    /// `OAUTHBEARER` — a token issued by something outside the cluster.
    OauthBearer,
    /// `GSSAPI`. Note that this client cannot speak it.
    Kerberos,
    /// A client certificate, with the principal derived from its subject.
    MutualTls,
    /// A delegation token, which authenticates as its owner over SCRAM.
    DelegationToken,
    /// A SASL mechanism name this build does not know — a custom callback
    /// handler, most likely.
    Other(String),
}

impl AuthMechanism {
    /// Parse a name as it appears in `sasl.enabled.mechanisms`.
    pub fn parse_sasl(name: &str) -> Self {
        match name.trim().to_ascii_uppercase().as_str() {
            "SCRAM-SHA-256" => AuthMechanism::Scram(ScramMechanism::Sha256),
            "SCRAM-SHA-512" => AuthMechanism::Scram(ScramMechanism::Sha512),
            "PLAIN" => AuthMechanism::Plain,
            "OAUTHBEARER" => AuthMechanism::OauthBearer,
            "GSSAPI" => AuthMechanism::Kerberos,
            other => AuthMechanism::Other(other.to_owned()),
        }
    }

    /// Whether using this mechanism requires a credential the cluster itself
    /// stores — which is to say, whether its absence from a principal's
    /// description rules it out.
    ///
    /// True for SCRAM and for delegation tokens, which are SCRAM credentials
    /// with an expiry. False for everything else, and that asymmetry is the
    /// whole engine of [`PrincipalDescription::likely_mechanism`].
    pub const fn needs_stored_credential(&self) -> bool {
        matches!(
            self,
            AuthMechanism::Scram(_) | AuthMechanism::DelegationToken
        )
    }
}

impl fmt::Display for AuthMechanism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthMechanism::Scram(ScramMechanism::Sha256) => f.write_str("SCRAM-SHA-256"),
            AuthMechanism::Scram(ScramMechanism::Sha512) => f.write_str("SCRAM-SHA-512"),
            AuthMechanism::Plain => f.write_str("PLAIN"),
            AuthMechanism::OauthBearer => f.write_str("OAUTHBEARER"),
            AuthMechanism::Kerberos => f.write_str("GSSAPI"),
            AuthMechanism::MutualTls => f.write_str("mutual TLS"),
            AuthMechanism::DelegationToken => f.write_str("delegation token"),
            AuthMechanism::Other(name) => f.write_str(name),
        }
    }
}

/// One listener, and what it accepts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenerAuth {
    /// The listener name — `SASL_SSL`, or whatever
    /// `listener.security.protocol.map` calls it.
    pub name: String,
    /// The protocol the name maps to.
    pub protocol: SecurityProtocol,
    /// SASL mechanisms this listener enables, in the order the broker lists
    /// them. Empty on a listener that speaks no SASL.
    pub sasl_mechanisms: Vec<AuthMechanism>,
    /// Whether a client certificate is asked for. Always [`ClientAuth::None`]
    /// on a listener without TLS, which cannot see one.
    pub client_auth: ClientAuth,
    /// Whether this is the inter-broker listener, which clients have no
    /// business connecting to and whose mechanisms therefore say nothing about
    /// how a user authenticates.
    pub is_inter_broker: bool,
    /// Whether this is a KRaft controller listener, named by
    /// `controller.listener.names`.
    ///
    /// A combined broker-and-controller node lists its controller listener in
    /// `listeners` alongside the client ones, so a reader that does not filter
    /// it out attributes the controllers' authentication to users. It is never
    /// advertised and no client ever reaches it.
    pub is_controller: bool,
}

impl ListenerAuth {
    /// Every mechanism a client can present to this listener, mutual TLS
    /// included.
    pub fn mechanisms(&self) -> Vec<AuthMechanism> {
        let mut mechanisms = self.sasl_mechanisms.clone();
        if self.protocol.uses_tls() && self.client_auth.accepts_certificates() {
            mechanisms.push(AuthMechanism::MutualTls);
        }
        mechanisms
    }
}

/// How a cluster's listeners authenticate.
///
/// Read from one broker's configuration. Kafka requires every broker to define
/// the same listener *names*, so this describes the cluster in every case that
/// is not a misconfiguration — but the per-listener mechanism and certificate
/// settings are per-broker, and a cluster mid-rollout can genuinely disagree
/// with itself. [`node_id`](Self::node_id) records which broker answered so a
/// caller chasing drift knows where to compare.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterAuthentication {
    /// The broker whose configuration this is.
    pub node_id: i32,
    /// Every listener the broker defines.
    pub listeners: Vec<ListenerAuth>,
    /// `ssl.principal.mapping.rules`, which decides what a certificate subject
    /// becomes. `None` when the broker did not report it.
    ///
    /// Worth more than it looks: under the default rule a certificate
    /// principal is its whole DN, so a principal whose name is *not* DN-shaped
    /// cannot have come from a certificate. A custom rule removes that
    /// inference entirely, and this field is how
    /// [`PrincipalDescription::likely_mechanism`] knows which world it is in.
    pub principal_mapping_rules: Option<String>,
}

impl ClusterAuthentication {
    /// The listeners a client is meant to use — everything that is neither
    /// inter-broker nor a KRaft controller endpoint.
    pub fn client_listeners(&self) -> impl Iterator<Item = &ListenerAuth> {
        self.listeners
            .iter()
            .filter(|listener| !listener.is_inter_broker && !listener.is_controller)
    }

    /// Every mechanism any client listener accepts, deduplicated.
    pub fn client_mechanisms(&self) -> Vec<AuthMechanism> {
        let mut mechanisms: Vec<AuthMechanism> = Vec::new();
        for listener in self.client_listeners() {
            for mechanism in listener.mechanisms() {
                if !mechanisms.contains(&mechanism) {
                    mechanisms.push(mechanism);
                }
            }
        }
        mechanisms
    }

    /// Whether certificate subjects map to whole DNs, which is Kafka's default
    /// and the assumption that lets a bare username rule mutual TLS out.
    ///
    /// True when the broker reports no rules at all: absent means unset means
    /// default.
    pub fn maps_certificates_to_subjects(&self) -> bool {
        match &self.principal_mapping_rules {
            Some(rules) => rules.trim().eq_ignore_ascii_case(DEFAULT_MAPPING_RULES),
            None => true,
        }
    }
}

/// How a verdict was reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerdictBasis {
    /// The cluster stores a credential for this principal and a client
    /// listener enables it. The strongest thing available, and still only
    /// "can", not "did": a principal with a SCRAM password may hold a
    /// certificate too.
    StoredCredential,
    /// The principal's name is a certificate subject and a client listener
    /// accepts certificates.
    CertificateSubject,
    /// What the listeners enable, minus what the cluster would have had to
    /// store and did not.
    Elimination,
    /// The listener inventory could not be read, or named nothing this build
    /// can reason about.
    Unknown,
}

/// What a principal most likely authenticates with.
///
/// Read [`is_conclusive`](Self::is_conclusive) before quoting this at anyone:
/// a verdict with three candidates is an honest list of possibilities, not an
/// answer, and the `Display` impl renders it as one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MechanismVerdict {
    /// The mechanisms still possible. One entry is a conclusion; several is a
    /// shortlist.
    ///
    /// **Empty is meaningful**: under [`VerdictBasis::Elimination`] it says
    /// the cluster offers this principal no way in at all — a stale principal,
    /// or one that belongs to a listener this broker does not define.
    pub candidates: Vec<AuthMechanism>,
    /// How the candidates were arrived at.
    pub basis: VerdictBasis,
}

impl MechanismVerdict {
    /// Whether exactly one possibility survived.
    pub fn is_conclusive(&self) -> bool {
        self.basis != VerdictBasis::Unknown && self.candidates.len() == 1
    }
}

impl fmt::Display for MechanismVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.basis == VerdictBasis::Unknown {
            return f.write_str("unknown");
        }
        if self.candidates.is_empty() {
            return f.write_str("nothing this cluster offers");
        }

        let names: Vec<String> = self
            .candidates
            .iter()
            .map(|mechanism| mechanism.to_string())
            .collect();
        write!(f, "{}", names.join(" or "))?;
        match self.basis {
            VerdictBasis::StoredCredential => f.write_str(" (stored credential)"),
            VerdictBasis::CertificateSubject => f.write_str(" (certificate subject)"),
            VerdictBasis::Elimination => f.write_str(" (by elimination)"),
            VerdictBasis::Unknown => Ok(()),
        }
    }
}

impl PrincipalDescription {
    /// Cross what the cluster stores about this principal with what its
    /// listeners accept.
    ///
    /// Three rules, in order:
    ///
    /// 1. **A stored credential the cluster also enables wins.** A SCRAM entry
    ///    or a delegation token is positive evidence, and no amount of
    ///    elimination beats it.
    /// 2. **A DN-shaped name on a cluster that accepts certificates is mutual
    ///    TLS.** Kafka's default mapping rules make a certificate principal its
    ///    whole subject, so the name is the evidence.
    /// 3. **Otherwise, eliminate.** SCRAM and delegation tokens need a
    ///    credential the cluster stores, and this principal has none — so
    ///    strike them, strike mutual TLS if the name is not a subject and the
    ///    mapping rules are the default, and report what is left.
    ///
    /// The result is inference. The broker's authorizer log is the only record
    /// of what actually happened on a connection; see the [module docs](self).
    ///
    /// ```no_run
    /// # async fn example() -> kafka_admin::Result<()> {
    /// use kafka_admin::{Admin, Principal};
    /// use kafka_meta::ClusterConfig;
    ///
    /// let admin = Admin::connect(["localhost:9092"], ClusterConfig::default()).await?;
    /// let listeners = admin.describe_authentication().await?;
    /// let alice = admin.describe_principal(&Principal::user("alice")).await?;
    ///
    /// // "SCRAM-SHA-512 (stored credential)", or
    /// // "PLAIN or OAUTHBEARER (by elimination)".
    /// println!("{}", alice.likely_mechanism(&listeners));
    /// # Ok(())
    /// # }
    /// ```
    pub fn likely_mechanism(&self, cluster: &ClusterAuthentication) -> MechanismVerdict {
        let offered = cluster.client_mechanisms();
        if offered.is_empty() {
            return MechanismVerdict {
                candidates: Vec::new(),
                basis: VerdictBasis::Unknown,
            };
        }

        // 1. Stored credentials, kept only where a client listener enables
        //    them. A SCRAM entry for a mechanism no listener offers is a
        //    leftover, and reporting it as the answer would be worse than
        //    saying nothing.
        let mut stored: Vec<AuthMechanism> = self
            .scram_mechanisms()
            .into_iter()
            .map(AuthMechanism::Scram)
            .filter(|mechanism| offered.contains(mechanism))
            .collect();
        // A token authenticates over SCRAM, so it needs some SCRAM mechanism
        // enabled — not the same one as any stored credential, since the token
        // id is its own username.
        let tokens = self.tokens.as_ref().map(Vec::as_slice).unwrap_or_default();
        if !tokens.is_empty()
            && offered
                .iter()
                .any(|mechanism| matches!(mechanism, AuthMechanism::Scram(_)))
        {
            stored.push(AuthMechanism::DelegationToken);
        }
        if !stored.is_empty() {
            return MechanismVerdict {
                candidates: stored,
                basis: VerdictBasis::StoredCredential,
            };
        }

        // 2. A certificate subject is a name only a certificate produces.
        let accepts_certificates = offered.contains(&AuthMechanism::MutualTls);
        if accepts_certificates && self.principal.is_distinguished_name() {
            return MechanismVerdict {
                candidates: vec![AuthMechanism::MutualTls],
                basis: VerdictBasis::CertificateSubject,
            };
        }

        // Elimination rests entirely on "the cluster stores no credential for
        // this principal", so a store that refused to answer collapses the
        // whole argument. Not finding a credential and not being allowed to
        // look produce the same empty list and mean opposite things.
        if !store_answered(&self.scram) || !store_answered(&self.tokens) {
            return MechanismVerdict {
                candidates: Vec::new(),
                basis: VerdictBasis::Unknown,
            };
        }

        // 3. Eliminate. Anything needing a stored credential is out — the
        //    describe above found none — and mutual TLS is out when the name
        //    is not a subject and the rules would have made it one.
        let strike_certificates =
            !self.principal.is_distinguished_name() && cluster.maps_certificates_to_subjects();
        let candidates = offered
            .into_iter()
            .filter(|mechanism| !mechanism.needs_stored_credential())
            .filter(|mechanism| !(strike_certificates && *mechanism == AuthMechanism::MutualTls))
            .collect();

        MechanismVerdict {
            candidates,
            basis: VerdictBasis::Elimination,
        }
    }
}

/// Whether a credential store gave an answer that can be eliminated on.
///
/// `Ok` is an answer, empty or not. So is `DELEGATION_TOKEN_AUTH_DISABLED`,
/// which says the cluster keeps no token store at all and therefore holds no
/// token for anybody. Anything else — an authorization failure, a timeout, an
/// api key the broker does not serve — leaves the store unread, and an unread
/// store cannot rule anything out.
fn store_answered<T>(source: &Result<Vec<T>>) -> bool {
    match source {
        Ok(_) => true,
        Err(error) => error.code() == Some(kafka_conn::ErrorCode::DelegationTokenAuthDisabled),
    }
}

impl Admin {
    /// Describe how this cluster's listeners authenticate clients.
    ///
    /// One `DescribeConfigs` against one broker, which is enough because
    /// listener *names* have to agree cluster-wide; see
    /// [`ClusterAuthentication`] for when that is not enough. Needs
    /// `DescribeConfigs` on the cluster, and is non-mutating, so it works on a
    /// [read-only client](Admin::connect_read_only).
    ///
    /// The `PLAIN` and Kerberos credential stores stay invisible: their JAAS
    /// configuration is sensitive, so the broker returns those entries with no
    /// value. What a listener *enables* is readable; who can use it is not.
    pub async fn describe_authentication(&self) -> Result<ClusterAuthentication> {
        let snapshot = self.cluster().refresh_if_stale().await?;
        // Lowest id rather than "whichever broker we happen to hold a
        // connection to", so two calls describe the same broker and a diff
        // between them is drift rather than routing.
        let node_id = snapshot
            .brokers()
            .iter()
            .map(|broker| broker.node_id)
            .min()
            .ok_or_else(|| Error::Unsupported("no brokers in the metadata snapshot".to_owned()))?;

        let described = self
            .describe_configs([ConfigResource::broker(node_id)])
            .await?;
        let entries = match described.into_iter().next() {
            Some((_, outcome)) => outcome?,
            // Apache Kafka answers one result per requested resource. A
            // reimplementation that answers none has said nothing, and
            // inventing an error code it did not send would be worse.
            None => {
                return Err(Error::Unsupported(format!(
                    "broker {node_id} returned no configuration to describe"
                )));
            }
        };

        Ok(listeners_from_config(node_id, &entries))
    }
}

/// Build the listener inventory from a broker's configuration.
///
/// Split out from the RPC because every interesting case here is a parsing
/// case — a custom listener name, a per-listener mechanism override, an
/// inter-broker listener that must not be counted — and none of them need a
/// broker to test.
fn listeners_from_config(node_id: i32, entries: &[ConfigEntry]) -> ClusterAuthentication {
    let config: BTreeMap<&str, &str> = entries
        .iter()
        .filter_map(|entry| {
            entry
                .value
                .as_deref()
                .map(|value| (entry.name.as_str(), value))
        })
        .collect();
    let get = |key: &str| {
        config
            .get(key)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
    };

    // NAME:PROTOCOL pairs. Absent means the standard names map to themselves.
    let protocol_map: BTreeMap<String, SecurityProtocol> = get("listener.security.protocol.map")
        .map(|value| {
            value
                .split(',')
                .filter_map(|pair| pair.split_once(':'))
                .map(|(name, protocol)| {
                    (
                        name.trim().to_ascii_uppercase(),
                        SecurityProtocol::parse(protocol),
                    )
                })
                .collect()
        })
        .unwrap_or_default();

    // Either the inter-broker listener is named, or it is the one whose
    // protocol matches `security.inter.broker.protocol` — which defaults to
    // PLAINTEXT, and which Kafka forbids setting alongside the name.
    let inter_broker_name = get("inter.broker.listener.name").map(str::to_ascii_uppercase);
    let inter_broker_protocol = inter_broker_name.is_none().then(|| {
        SecurityProtocol::parse(get("security.inter.broker.protocol").unwrap_or("PLAINTEXT"))
    });

    // A combined broker-and-controller node — which is what every fixture and
    // most small clusters run — lists its controller listener here alongside
    // the client ones, and it is never advertised to anybody.
    let controller_names: Vec<String> = get("controller.listener.names")
        .map(|value| {
            value
                .split(',')
                .map(|name| name.trim().to_ascii_uppercase())
                .filter(|name| !name.is_empty())
                .collect()
        })
        .unwrap_or_default();

    let listeners = get("listeners")
        .map(|value| {
            value
                .split(',')
                .filter_map(|listener| listener.split("://").next())
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(|name| {
                    let upper = name.to_ascii_uppercase();
                    let protocol = protocol_map
                        .get(&upper)
                        .cloned()
                        .unwrap_or_else(|| SecurityProtocol::parse(&upper));

                    // Kafka lowercases the listener name in the per-listener
                    // config prefix, whatever case the listener is declared in.
                    let prefix = format!("listener.name.{}", name.to_ascii_lowercase());
                    let sasl_mechanisms = if protocol.uses_sasl() {
                        get(&format!("{prefix}.sasl.enabled.mechanisms"))
                            .or_else(|| get("sasl.enabled.mechanisms"))
                            // Kafka's own default, and the reason a listener
                            // nobody configured advertises Kerberos.
                            .unwrap_or("GSSAPI")
                            .split(',')
                            .map(str::trim)
                            .filter(|mechanism| !mechanism.is_empty())
                            .map(AuthMechanism::parse_sasl)
                            .collect()
                    } else {
                        Vec::new()
                    };
                    let client_auth = if protocol.uses_tls() {
                        ClientAuth::parse(
                            get(&format!("{prefix}.ssl.client.auth"))
                                .or_else(|| get("ssl.client.auth"))
                                .unwrap_or("none"),
                        )
                    } else {
                        ClientAuth::None
                    };
                    let is_inter_broker = match (&inter_broker_name, &inter_broker_protocol) {
                        (Some(named), _) => *named == upper,
                        (None, Some(expected)) => protocol == *expected,
                        (None, None) => false,
                    };

                    ListenerAuth {
                        is_controller: controller_names.contains(&upper),
                        name: upper,
                        protocol,
                        sasl_mechanisms,
                        client_auth,
                        is_inter_broker,
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    ClusterAuthentication {
        node_id,
        listeners,
        principal_mapping_rules: get("ssl.principal.mapping.rules").map(str::to_owned),
    }
}

#[cfg(test)]
mod tests {
    use kafka_conn::ErrorCode;

    use super::*;
    use crate::security::ScramCredentialInfo;
    use crate::tokens::Principal;
    use crate::types::ConfigSource;

    fn entry(name: &str, value: &str) -> ConfigEntry {
        ConfigEntry {
            name: name.to_owned(),
            value: Some(value.to_owned()),
            source: ConfigSource::StaticBrokerConfig,
            is_sensitive: false,
            read_only: true,
            documentation: None,
        }
    }

    fn described(principal: Principal, scram: Vec<ScramMechanism>) -> PrincipalDescription {
        PrincipalDescription {
            principal,
            scram: Ok(scram
                .into_iter()
                .map(|mechanism| ScramCredentialInfo {
                    mechanism,
                    iterations: 4096,
                })
                .collect()),
            tokens: Ok(Vec::new()),
            acls: Ok(Vec::new()),
            quotas: Ok(Vec::new()),
        }
    }

    /// The shape every fixture in this workspace produces: one client listener
    /// speaking SASL, one PLAINTEXT listener for the brokers themselves.
    fn sasl_cluster(mechanisms: &str) -> ClusterAuthentication {
        listeners_from_config(
            1,
            &[
                entry("listeners", "BROKER://:9093,CLIENT://:9092"),
                entry(
                    "listener.security.protocol.map",
                    "BROKER:PLAINTEXT,CLIENT:SASL_SSL",
                ),
                entry("inter.broker.listener.name", "BROKER"),
                entry("listener.name.client.sasl.enabled.mechanisms", mechanisms),
            ],
        )
    }

    #[test]
    fn a_named_listener_takes_its_protocol_from_the_map() {
        let cluster = sasl_cluster("SCRAM-SHA-512");
        assert_eq!(cluster.listeners.len(), 2);

        let client = &cluster.listeners[1];
        assert_eq!(client.name, "CLIENT");
        assert_eq!(client.protocol, SecurityProtocol::SaslSsl);
        assert_eq!(
            client.sasl_mechanisms,
            vec![AuthMechanism::Scram(ScramMechanism::Sha512)]
        );
        assert!(!client.is_inter_broker);
    }

    /// The inter-broker listener's mechanisms say nothing about how a user
    /// authenticates, so counting them would poison every verdict on a cluster
    /// whose brokers talk PLAINTEXT to each other — which is most of them.
    #[test]
    fn the_inter_broker_listener_is_not_a_client_listener() {
        let cluster = sasl_cluster("PLAIN");
        assert!(cluster.listeners[0].is_inter_broker);
        assert_eq!(
            cluster.client_listeners().count(),
            1,
            "{:?}",
            cluster.listeners
        );
        assert_eq!(cluster.client_mechanisms(), vec![AuthMechanism::Plain]);
    }

    /// The exact listener configuration `testkit` renders, which is also the
    /// shape of any combined broker-and-controller node: three listeners, one
    /// of them a controller endpoint no client can reach. Counting that one
    /// would attribute the controllers' authentication to users.
    #[test]
    fn a_kraft_controller_listener_is_not_a_client_listener() {
        let cluster = listeners_from_config(
            1,
            &[
                entry(
                    "listeners",
                    "EXTERNAL://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094",
                ),
                entry(
                    "listener.security.protocol.map",
                    "EXTERNAL:SASL_PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
                ),
                entry("controller.listener.names", "CONTROLLER"),
                entry("inter.broker.listener.name", "BROKER"),
                entry("sasl.enabled.mechanisms", "SCRAM-SHA-512"),
            ],
        );

        let client: Vec<&str> = cluster
            .client_listeners()
            .map(|listener| listener.name.as_str())
            .collect();
        assert_eq!(client, vec!["EXTERNAL"]);
        assert!(cluster.listeners[2].is_controller);
        assert_eq!(
            cluster.client_mechanisms(),
            vec![AuthMechanism::Scram(ScramMechanism::Sha512)]
        );
    }

    /// Without an explicit name the inter-broker listener is the one matching
    /// `security.inter.broker.protocol`, which defaults to PLAINTEXT.
    #[test]
    fn the_inter_broker_listener_can_be_named_by_protocol_instead() {
        let cluster = listeners_from_config(
            1,
            &[
                entry("listeners", "PLAINTEXT://:9092,SASL_SSL://:9094"),
                entry("sasl.enabled.mechanisms", "OAUTHBEARER"),
            ],
        );
        assert!(cluster.listeners[0].is_inter_broker, "{:?}", cluster);
        assert_eq!(
            cluster.client_mechanisms(),
            vec![AuthMechanism::OauthBearer]
        );
    }

    /// A listener nobody configured enables GSSAPI, because that is Kafka's
    /// default — and reporting an empty mechanism list there would make a
    /// SASL listener look like an open one.
    #[test]
    fn an_unconfigured_sasl_listener_defaults_to_kerberos() {
        let cluster = listeners_from_config(1, &[entry("listeners", "SASL_PLAINTEXT://:9092")]);
        assert_eq!(
            cluster.listeners[0].sasl_mechanisms,
            vec![AuthMechanism::Kerberos]
        );
    }

    #[test]
    fn a_tls_listener_reports_whether_it_wants_a_certificate() {
        let cluster = listeners_from_config(
            1,
            &[
                entry("listeners", "SSL://:9094"),
                entry("ssl.client.auth", "required"),
                entry("security.inter.broker.protocol", "SSL"),
            ],
        );
        assert_eq!(cluster.listeners[0].client_auth, ClientAuth::Required);
        // ...but it is the inter-broker listener here, so it offers a client
        // nothing.
        assert!(cluster.client_mechanisms().is_empty());
    }

    /// A per-listener override beats the cluster-wide default, which is how
    /// every real multi-listener cluster is configured.
    #[test]
    fn a_per_listener_override_beats_the_broker_wide_setting() {
        let cluster = listeners_from_config(
            1,
            &[
                entry("listeners", "CLIENT://:9092"),
                entry("listener.security.protocol.map", "CLIENT:SASL_SSL"),
                entry("sasl.enabled.mechanisms", "GSSAPI"),
                entry(
                    "listener.name.client.sasl.enabled.mechanisms",
                    "SCRAM-SHA-256,OAUTHBEARER",
                ),
            ],
        );
        assert_eq!(
            cluster.listeners[0].sasl_mechanisms,
            vec![
                AuthMechanism::Scram(ScramMechanism::Sha256),
                AuthMechanism::OauthBearer
            ]
        );
    }

    #[test]
    fn a_sensitive_config_is_absent_rather_than_null() {
        let cluster = listeners_from_config(
            1,
            &[
                entry("listeners", "SASL_PLAINTEXT://:9092"),
                ConfigEntry {
                    name: "listener.name.sasl_plaintext.plain.sasl.jaas.config".to_owned(),
                    value: None,
                    source: ConfigSource::StaticBrokerConfig,
                    is_sensitive: true,
                    read_only: true,
                    documentation: None,
                },
            ],
        );
        // The JAAS entry contributes nothing, which is the point: who can use
        // PLAIN is not readable, only that it is enabled.
        assert_eq!(cluster.listeners.len(), 1);
    }

    #[test]
    fn a_stored_credential_the_cluster_enables_is_the_answer() {
        let cluster = sasl_cluster("SCRAM-SHA-512,PLAIN");
        let alice = described(Principal::user("alice"), vec![ScramMechanism::Sha512]);

        let verdict = alice.likely_mechanism(&cluster);
        assert_eq!(
            verdict.candidates,
            vec![AuthMechanism::Scram(ScramMechanism::Sha512)]
        );
        assert_eq!(verdict.basis, VerdictBasis::StoredCredential);
        assert!(verdict.is_conclusive());
        assert_eq!(verdict.to_string(), "SCRAM-SHA-512 (stored credential)");
    }

    /// A credential for a mechanism no listener offers is a leftover, not an
    /// answer — and saying "SCRAM-SHA-256" about a cluster that stopped
    /// enabling it is worse than admitting the elimination.
    #[test]
    fn a_credential_no_listener_enables_is_not_the_answer() {
        let cluster = sasl_cluster("OAUTHBEARER");
        let alice = described(Principal::user("alice"), vec![ScramMechanism::Sha256]);

        let verdict = alice.likely_mechanism(&cluster);
        assert_eq!(verdict.candidates, vec![AuthMechanism::OauthBearer]);
        assert_eq!(verdict.basis, VerdictBasis::Elimination);
    }

    #[test]
    fn a_delegation_token_holder_authenticates_over_scram() {
        let cluster = sasl_cluster("SCRAM-SHA-256");
        let mut alice = described(Principal::user("alice"), Vec::new());
        alice.tokens = Ok(vec![crate::principal::PrincipalToken {
            token_id: "token-1".to_owned(),
            requester: Principal::user("alice"),
            issue_timestamp_ms: 0,
            expiry_timestamp_ms: 1,
            max_timestamp_ms: 2,
            renewers: Vec::new(),
        }]);

        let verdict = alice.likely_mechanism(&cluster);
        assert_eq!(verdict.candidates, vec![AuthMechanism::DelegationToken]);
        assert_eq!(verdict.basis, VerdictBasis::StoredCredential);
    }

    /// The mutual-TLS case, which is the one the principal half alone could
    /// not answer at all.
    #[test]
    fn a_certificate_subject_on_a_certificate_cluster_is_mutual_tls() {
        let cluster = listeners_from_config(
            1,
            &[
                entry("listeners", "BROKER://:9093,CLIENT://:9092"),
                entry(
                    "listener.security.protocol.map",
                    "BROKER:PLAINTEXT,CLIENT:SSL",
                ),
                entry("inter.broker.listener.name", "BROKER"),
                entry("listener.name.client.ssl.client.auth", "required"),
            ],
        );
        let bob = described(Principal::user("CN=bob,O=example"), Vec::new());

        let verdict = bob.likely_mechanism(&cluster);
        assert_eq!(verdict.candidates, vec![AuthMechanism::MutualTls]);
        assert_eq!(verdict.basis, VerdictBasis::CertificateSubject);
        assert!(verdict.is_conclusive());
        assert_eq!(verdict.to_string(), "mutual TLS (certificate subject)");
    }

    /// Elimination doing real work: no stored credential rules SCRAM out, and
    /// a bare username under the default mapping rules rules certificates out,
    /// leaving one answer on a listener that offers three ways in.
    #[test]
    fn elimination_names_the_one_mechanism_left() {
        let cluster = listeners_from_config(
            1,
            &[
                entry("listeners", "CLIENT://:9092"),
                entry("listener.security.protocol.map", "CLIENT:SASL_SSL"),
                entry(
                    "listener.name.client.sasl.enabled.mechanisms",
                    "SCRAM-SHA-512,OAUTHBEARER",
                ),
                entry("listener.name.client.ssl.client.auth", "requested"),
            ],
        );
        let alice = described(Principal::user("alice"), Vec::new());

        let verdict = alice.likely_mechanism(&cluster);
        assert_eq!(verdict.candidates, vec![AuthMechanism::OauthBearer]);
        assert_eq!(verdict.basis, VerdictBasis::Elimination);
        assert!(verdict.is_conclusive());
        assert_eq!(verdict.to_string(), "OAUTHBEARER (by elimination)");
    }

    /// Custom mapping rules can turn a subject into a bare username, so the
    /// name stops being evidence and mutual TLS survives elimination.
    #[test]
    fn custom_mapping_rules_keep_certificates_in_play() {
        let mut cluster = listeners_from_config(
            1,
            &[
                entry("listeners", "CLIENT://:9092"),
                entry("listener.security.protocol.map", "CLIENT:SASL_SSL"),
                entry("listener.name.client.sasl.enabled.mechanisms", "PLAIN"),
                entry("listener.name.client.ssl.client.auth", "required"),
            ],
        );
        let alice = described(Principal::user("alice"), Vec::new());
        assert_eq!(
            alice.likely_mechanism(&cluster).candidates,
            vec![AuthMechanism::Plain],
            "default rules make a bare name proof it came from no certificate"
        );

        cluster.principal_mapping_rules = Some("RULE:^CN=(.*?),.*$/$1/L,DEFAULT".to_owned());
        let verdict = alice.likely_mechanism(&cluster);
        assert_eq!(
            verdict.candidates,
            vec![AuthMechanism::Plain, AuthMechanism::MutualTls]
        );
        assert!(!verdict.is_conclusive());
        assert_eq!(verdict.to_string(), "PLAIN or mutual TLS (by elimination)");
    }

    /// A cluster whose only client listener enables SCRAM, for a principal
    /// with no credential: the honest answer is that it cannot get in.
    #[test]
    fn a_principal_with_no_way_in_says_so() {
        let cluster = sasl_cluster("SCRAM-SHA-512");
        let nobody = described(Principal::user("nobody"), Vec::new());

        let verdict = nobody.likely_mechanism(&cluster);
        assert!(verdict.candidates.is_empty());
        assert_eq!(verdict.basis, VerdictBasis::Elimination);
        assert!(!verdict.is_conclusive());
        assert_eq!(verdict.to_string(), "nothing this cluster offers");
    }

    /// An unreadable credential store must not become an elimination: not
    /// finding a credential and not being allowed to look are different, and
    /// only one of them rules SCRAM out.
    #[test]
    fn an_unreadable_credential_store_is_not_an_elimination() {
        let cluster = sasl_cluster("SCRAM-SHA-512,PLAIN");
        let mut alice = described(Principal::user("alice"), Vec::new());
        alice.scram = Err(Error::from_code(
            ErrorCode::ClusterAuthorizationFailed,
            None,
        ));

        let verdict = alice.likely_mechanism(&cluster);
        assert_eq!(verdict.basis, VerdictBasis::Unknown);
        assert_eq!(verdict.to_string(), "unknown");
    }

    #[test]
    fn a_cluster_with_no_readable_listeners_is_unknown() {
        let cluster = listeners_from_config(1, &[]);
        let alice = described(Principal::user("alice"), Vec::new());

        let verdict = alice.likely_mechanism(&cluster);
        assert_eq!(verdict.basis, VerdictBasis::Unknown);
        assert!(!verdict.is_conclusive());
    }
}
