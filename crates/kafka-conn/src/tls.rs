//! TLS configuration.
//!
//! Nothing here reaches for `dangerous()`. A UI backend that silently accepts
//! any certificate is worse than one that fails to connect, because the
//! failure is invisible — so a private CA is configured by handing us the CA,
//! not by turning verification off.
//!
//! Two limits are stated here rather than discovered, because both are silent:
//!
//! * **PEM only.** A PKCS#12 (`.p12`) or JKS keystore is not accepted, and the
//!   Java ecosystem hands those out by default — Strimzi's `KafkaUser` secret
//!   ships `user.p12` beside the PEMs. Converting is one command:
//!
//!   ```sh
//!   openssl pkcs12 -in user.p12 -nodes -clcerts -out client.pem
//!   openssl pkcs12 -in user.p12 -nodes -nocerts -out client.key
//!   ```
//!
//!   Bundling a PKCS#12 reader would mean either a C binding or another
//!   ASN.1 parser in the crate everything else sits on, for a format `openssl`
//!   already converts; the decision is to document the command.
//! * **No revocation checking.** No CRLs are supplied to the verifier and OCSP
//!   stapling is not validated, so a revoked broker certificate is accepted
//!   until it expires. That is what every mainstream Kafka client does, and it
//!   is still worth saying out loud: on a cluster with a compliance
//!   requirement it is the gap. rustls's verifier builder takes CRLs, so
//!   closing it is a feature rather than a redesign — it wants a caller who
//!   needs it, and a decision about where the CRLs come from.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig as RustlsConfig, RootCertStore};
use tokio_rustls::TlsConnector;

use crate::error::{Error, Result};

/// Where to get trust anchors from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrustAnchors {
    /// The operating system's trust store.
    System,
    /// Only the supplied PEM bundle. The right choice for a private CA: it
    /// means a certificate from a public CA will *not* be accepted.
    Pem(Vec<u8>),
    /// The system store plus a PEM bundle.
    SystemAndPem(Vec<u8>),
}

/// A client certificate and its key, both PEM encoded.
#[derive(Clone)]
pub struct ClientCertificate {
    /// The certificate chain, leaf first.
    pub chain_pem: Vec<u8>,
    /// The private key.
    pub key_pem: Vec<u8>,
}

impl std::fmt::Debug for ClientCertificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render the key material, not even in a debug log.
        f.debug_struct("ClientCertificate")
            .field(
                "chain_pem",
                &format_args!("<{} bytes>", self.chain_pem.len()),
            )
            .field("key_pem", &"<redacted>")
            .finish()
    }
}

/// The oldest TLS version a connection may negotiate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MinTlsVersion {
    /// TLS 1.2 and 1.3. rustls has no 1.0 or 1.1 to offer, so this is already
    /// the safe floor and is the default.
    #[default]
    Tls12,
    /// TLS 1.3 only.
    ///
    /// A policy choice rather than a correctness one: 1.2 as rustls configures
    /// it is not broken. Worth having because "TLS 1.3 or fail" is a
    /// requirement some environments hand down, and the alternative to a knob
    /// is a caller who cannot express it.
    Tls13,
}

/// How to negotiate TLS.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Trust anchors.
    pub anchors: TrustAnchors,
    /// Client certificate for mutual TLS.
    pub client_certificate: Option<ClientCertificate>,
    /// Override the name sent in SNI and verified against the certificate, for
    /// every broker.
    ///
    /// Needed whenever brokers advertise names that do not resolve from where
    /// the client runs — a port-forwarded cluster, or a load balancer in
    /// front of the advertised listeners.
    pub server_name_override: Option<String>,
    /// Per-host overrides, keyed by the lowercased advertised host, consulted
    /// before [`TlsConfig::server_name_override`].
    ///
    /// One name for a whole pool is right when every broker is reached through
    /// the same address and wrong the moment they present distinct certificate
    /// names — a cluster behind one load balancer can otherwise be correct for
    /// exactly one broker.
    pub server_names: BTreeMap<String, String>,
    /// The oldest protocol version to negotiate.
    pub min_version: MinTlsVersion,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            anchors: TrustAnchors::System,
            client_certificate: None,
            server_name_override: None,
            server_names: BTreeMap::new(),
            min_version: MinTlsVersion::Tls12,
        }
    }
}

impl TlsConfig {
    /// Trust the system store.
    pub fn system() -> Self {
        Self::default()
    }

    /// Trust only the given PEM bundle.
    pub fn with_ca_pem(pem: impl Into<Vec<u8>>) -> Self {
        Self {
            anchors: TrustAnchors::Pem(pem.into()),
            ..Self::default()
        }
    }

    /// Trust the system store *and* the given PEM bundle.
    ///
    /// The normal shape for a corporate CA that issues the broker certificates
    /// while the same process also talks to public endpoints — an OIDC issuer,
    /// most obviously, which shares this trust configuration. Prefer
    /// [`TlsConfig::with_ca_pem`] where the cluster's CA is the only one that
    /// should ever be accepted: this variant means a public CA can vouch for a
    /// broker too.
    pub fn with_system_and_ca_pem(pem: impl Into<Vec<u8>>) -> Self {
        Self {
            anchors: TrustAnchors::SystemAndPem(pem.into()),
            ..Self::default()
        }
    }

    /// Present a client certificate.
    pub fn with_client_certificate(
        mut self,
        chain_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_certificate = Some(ClientCertificate {
            chain_pem: chain_pem.into(),
            key_pem: key_pem.into(),
        });
        self
    }

    /// Override the name used for SNI and hostname verification, for every
    /// broker.
    #[must_use]
    pub fn with_server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name_override = Some(name.into());
        self
    }

    /// Override the name used for one advertised host.
    ///
    /// Additive: call it once per broker. Consulted before
    /// [`TlsConfig::with_server_name`], so a blanket override can stand as the
    /// default with named exceptions on top of it.
    ///
    /// `host` is matched against the host part of the address the pool dials —
    /// the advertised listener — case-insensitively, and it is *not* the name
    /// being verified. The two being different is the entire point.
    #[must_use]
    pub fn with_server_name_for(
        mut self,
        host: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        self.server_names
            .insert(host.into().to_ascii_lowercase(), name.into());
        self
    }

    /// Require a minimum protocol version.
    #[must_use]
    pub fn with_min_tls_version(mut self, version: MinTlsVersion) -> Self {
        self.min_version = version;
        self
    }

    /// Build a connector.
    pub fn connector(&self) -> Result<TlsConnector> {
        Ok(TlsConnector::from(Arc::new(self.rustls_config()?)))
    }

    /// The underlying rustls configuration.
    ///
    /// Separate from [`TlsConfig::connector`] because the OIDC token fetch needs
    /// the same trust settings for an HTTPS client that is not a Kafka
    /// connection — one place that decides what this process trusts, rather than
    /// two that drift.
    ///
    /// The crypto provider is named explicitly rather than left to
    /// `ClientConfig::builder()`, which resolves a process-wide default and can
    /// fail at runtime depending on what else in the binary installed one.
    pub(crate) fn rustls_config(&self) -> Result<RustlsConfig> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let versions = RustlsConfig::builder_with_provider(provider);
        let versions = match self.min_version {
            MinTlsVersion::Tls12 => versions.with_safe_default_protocol_versions(),
            MinTlsVersion::Tls13 => versions.with_protocol_versions(&[&rustls::version::TLS13]),
        };
        let builder = versions
            .map_err(|e| Error::Unsupported(format!("rustls protocol versions: {e}")))?
            .with_root_certificates(self.root_store()?);

        match &self.client_certificate {
            None => Ok(builder.with_no_client_auth()),
            Some(cert) => {
                let chain = parse_certs(&cert.chain_pem)?;
                let key = parse_key(&cert.key_pem)?;
                builder
                    .with_client_auth_cert(chain, key)
                    .map_err(|e| Error::Unsupported(format!("client certificate rejected: {e}")))
            }
        }
    }

    /// The name to verify the server against for a given host.
    ///
    /// Most specific first: a per-host entry, then the blanket override, then
    /// the host itself.
    pub fn server_name(&self, host: &str) -> Result<ServerName<'static>> {
        let name = self
            .server_names
            .get(&host.to_ascii_lowercase())
            .map(String::as_str)
            .or(self.server_name_override.as_deref())
            .unwrap_or(host);
        ServerName::try_from(name.to_owned())
            .map_err(|e| Error::InvalidRequest(format!("invalid TLS server name {name:?}: {e}")))
    }

    fn root_store(&self) -> Result<RootCertStore> {
        let mut store = RootCertStore::empty();

        let (use_system, pem) = match &self.anchors {
            TrustAnchors::System => (true, None),
            TrustAnchors::Pem(pem) => (false, Some(pem)),
            TrustAnchors::SystemAndPem(pem) => (true, Some(pem)),
        };

        if use_system {
            let loaded = rustls_native_certs::load_native_certs();
            // `load_native_certs` reports per-file errors alongside whatever it
            // did manage to read. Anchors we could not parse are worth a
            // warning, but they are not a reason to fail a connection that the
            // remaining anchors can verify.
            for error in &loaded.errors {
                tracing::warn!(%error, "skipping unreadable system trust anchor");
            }
            for cert in loaded.certs {
                if let Err(error) = store.add(cert) {
                    tracing::warn!(%error, "skipping unusable system trust anchor");
                }
            }
        }

        if let Some(pem) = pem {
            let certs = parse_certs(pem)?;
            if certs.is_empty() {
                return Err(Error::InvalidRequest(
                    "TLS trust anchor PEM contained no certificates".to_owned(),
                ));
            }
            for cert in certs {
                store
                    .add(cert)
                    .map_err(|e| Error::InvalidRequest(format!("invalid CA certificate: {e}")))?;
            }
        }

        if store.is_empty() {
            return Err(Error::InvalidRequest(
                "no usable TLS trust anchors were configured".to_owned(),
            ));
        }
        Ok(store)
    }
}

/// Classify a socket failure that might be the peer refusing our identity.
///
/// A mutual-TLS listener that will not accept us reports it as a TLS *alert*,
/// which arrives here as an ordinary `io::Error` and would otherwise be
/// [`Error::Transport`] — "the cluster is unreachable", which is the wrong
/// screen, the wrong owner and the wrong next step. It is a credential
/// problem, and the taxonomy exists to say so.
///
/// Note where this has to be called from. Under TLS 1.3 the client certificate
/// goes out *after* the server's Finished, so `connect` returns successfully
/// and the rejection lands on the first read — which is the `ApiVersions`
/// handshake, not the TLS one.
pub(crate) fn handshake_error(
    context: &'static str,
    error: io::Error,
    has_client_certificate: bool,
) -> Error {
    use rustls::AlertDescription as Alert;

    let alert = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<rustls::Error>())
        .and_then(|rustls| match rustls {
            rustls::Error::AlertReceived(alert) => Some(*alert),
            _ => None,
        });

    let Some(alert) = alert else {
        return Error::transport(context, error);
    };

    match (alert, has_client_certificate) {
        (Alert::CertificateRequired, false) => Error::Authentication(
            "the broker requires a client certificate and none is configured; supply one \
             with TlsConfig::with_client_certificate"
                .to_owned(),
        ),
        (
            Alert::CertificateRequired
            | Alert::BadCertificate
            | Alert::UnsupportedCertificate
            | Alert::CertificateRevoked
            | Alert::CertificateExpired
            | Alert::CertificateUnknown
            | Alert::UnknownCA
            | Alert::AccessDenied,
            _,
        ) => Error::Authentication(format!(
            "the broker rejected our TLS client certificate: {alert:?}. Check that it is \
             issued by the CA the listener trusts — a broker's *clients* CA is usually not \
             the CA its own certificate chains to"
        )),
        _ => Error::transport(context, error),
    }
}

fn parse_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>> {
    use rustls::pki_types::pem::PemObject as _;
    CertificateDer::pem_slice_iter(pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::InvalidRequest(format!("could not parse certificate PEM: {e}")))
}

fn parse_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>> {
    use rustls::pki_types::pem::{Error as PemError, PemObject as _};
    let parsed = PrivateKeyDer::from_pem_slice(pem);
    // Checked before the underlying result is rendered, because an encrypted
    // key reaches here as either arm depending on which of the two forms it is
    // in, and "your key has a passphrase" is the useful sentence in both.
    if let Some(message) = encrypted_key_message(pem) {
        return Err(Error::InvalidRequest(message));
    }
    parsed.map_err(|e| match e {
        PemError::NoItemsFound => {
            Error::InvalidRequest("private key PEM contained no key".to_owned())
        }
        other => Error::InvalidRequest(format!("could not parse private key PEM: {other}")),
    })
}

/// Say *why* there is no usable key, when the bytes can tell us.
///
/// The PEM parser skips the sections it cannot use and reports the absence,
/// not the reason — so a passphrase-protected key, which is what
/// `openssl genpkey -aes-256-cbc` produces and what a careful operator is most
/// likely to be holding, arrives as "contained no key" and sends them off to
/// look for a missing file.
fn encrypted_key_message(pem: &[u8]) -> Option<String> {
    const ENCRYPTED: &[u8] = b"BEGIN ENCRYPTED PRIVATE KEY";
    // `Proc-Type: 4,ENCRYPTED` is the older, PEM-level encryption openssl
    // still emits for traditional-format keys.
    const LEGACY_ENCRYPTED: &[u8] = b"Proc-Type: 4,ENCRYPTED";

    let contains = |needle: &[u8]| pem.windows(needle.len()).any(|window| window == needle);
    (contains(ENCRYPTED) || contains(LEGACY_ENCRYPTED)).then(|| {
        "the private key PEM is passphrase-encrypted, which this client cannot decrypt; \
         decrypt it first with `openssl pkcs8 -topk8 -nocrypt -in key.pem -out key.pk8.pem`"
            .to_owned()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_pem_bundle_is_rejected_rather_than_trusting_nothing() {
        let cfg = TlsConfig::with_ca_pem(b"not a certificate".to_vec());
        assert!(cfg.connector().is_err());
    }

    /// The other half of the #34 IPv6 fix: once the brackets are stripped,
    /// rustls must actually accept the bare literal as a name to verify.
    #[test]
    fn an_ipv6_literal_is_a_valid_server_name() {
        let cfg = TlsConfig::system();
        let name = cfg.server_name("::1").expect("IPv6 literal");
        assert!(matches!(name, ServerName::IpAddress(_)), "{name:?}");
    }

    #[test]
    fn the_server_name_override_wins() {
        let cfg = TlsConfig::system().with_server_name("broker.internal");
        let name = cfg.server_name("10.0.0.1").expect("valid name");
        assert!(format!("{name:?}").contains("broker.internal"));
    }

    #[test]
    fn without_an_override_the_host_is_used() {
        let cfg = TlsConfig::system();
        assert!(cfg.server_name("broker.example.com").is_ok());
        // An address that is not a valid DNS name or IP is an error, not a
        // silently-skipped verification.
        assert!(cfg.server_name("not a host name").is_err());
    }

    #[test]
    fn a_per_host_name_beats_the_blanket_override() {
        let cfg = TlsConfig::system()
            .with_server_name("broker.internal")
            .with_server_name_for("10.0.0.2", "broker-2.internal");

        let named = cfg.server_name("10.0.0.2").expect("valid name");
        assert!(format!("{named:?}").contains("broker-2.internal"));
        // Everything else still falls through to the blanket override, so the
        // map is exceptions rather than an all-or-nothing switch.
        let other = cfg.server_name("10.0.0.9").expect("valid name");
        assert!(format!("{other:?}").contains("broker.internal"));
    }

    #[test]
    fn a_per_host_name_matches_the_host_case_insensitively() {
        let cfg =
            TlsConfig::system().with_server_name_for("Broker-1.Example.COM", "broker.internal");
        let name = cfg.server_name("broker-1.example.com").expect("valid name");
        assert!(format!("{name:?}").contains("broker.internal"));
    }

    /// The message an operator reads when their key has a passphrase. Naming
    /// the case is the whole fix — the old one sent them looking for a file
    /// that was right there.
    #[test]
    fn an_encrypted_private_key_says_so() {
        let key =
            b"-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIB\n-----END ENCRYPTED PRIVATE KEY-----\n";
        let err = parse_key(key).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("passphrase-encrypted"), "{message}");
        assert!(message.contains("openssl pkcs8"), "{message}");

        // The legacy header openssl still writes for traditional-format keys.
        let legacy = b"-----BEGIN RSA PRIVATE KEY-----\nProc-Type: 4,ENCRYPTED\nDEK-Info: AES-256-CBC,00\n\nAAAA\n-----END RSA PRIVATE KEY-----\n";
        assert!(
            parse_key(legacy)
                .unwrap_err()
                .to_string()
                .contains("passphrase-encrypted")
        );

        // And a PEM that genuinely has no key still says that, rather than
        // blaming a passphrase for every failure.
        let none = b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";
        assert!(
            parse_key(none)
                .unwrap_err()
                .to_string()
                .contains("contained no key")
        );
    }

    #[test]
    fn requiring_tls13_builds_a_connector() {
        // The negotiated version is a broker-facing property and belongs in the
        // acceptance suite; what is decidable here is that the configuration is
        // one rustls accepts rather than one that fails at connect.
        let cfg = TlsConfig::system().with_min_tls_version(MinTlsVersion::Tls13);
        assert_eq!(cfg.min_version, MinTlsVersion::Tls13);
        assert!(cfg.connector().is_ok());
    }

    #[test]
    fn system_and_pem_is_reachable_without_touching_the_field() {
        // The gap this closes: `SystemAndPem` existed as a variant with no
        // constructor, so the documented shape — a corporate CA beside the
        // public ones — was reachable only by assigning to the public field.
        let cfg = TlsConfig::with_system_and_ca_pem(b"not a certificate".to_vec());
        assert!(matches!(cfg.anchors, TrustAnchors::SystemAndPem(_)));
        // Still validated: an unusable bundle fails rather than quietly leaving
        // the system store to carry the connection.
        assert!(cfg.connector().is_err());
    }

    /// A listener that will not accept us is a credential problem, and
    /// [`Error::Transport`] renders it as "the cluster is unreachable" — the
    /// wrong screen, and a wrong answer that a live run took four retries and
    /// two minutes to produce.
    #[test]
    fn a_rejected_certificate_is_an_authentication_failure_not_a_transport_one() {
        let alert = |description| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                rustls::Error::AlertReceived(description),
            )
        };

        // Nothing presented, and the broker wanted one. The message has to name
        // the fix; the alert on its own names only the symptom.
        let err = handshake_error(
            "reading handshake response",
            alert(rustls::AlertDescription::CertificateRequired),
            false,
        );
        let Error::Authentication(message) = &err else {
            panic!("expected an authentication error, got {err:?}");
        };
        assert!(message.contains("with_client_certificate"), "{message}");

        // Presented and refused — most often issued by the wrong CA, which is
        // easy on a cluster with a separate clients CA.
        for description in [
            rustls::AlertDescription::UnknownCA,
            rustls::AlertDescription::BadCertificate,
            rustls::AlertDescription::CertificateExpired,
            rustls::AlertDescription::AccessDenied,
        ] {
            let err = handshake_error("TLS handshake", alert(description), true);
            assert!(
                matches!(err, Error::Authentication(_)),
                "{description:?}: {err:?}"
            );
        }

        // A genuinely broken socket keeps its own variant: a UI that renders a
        // dead network as "check your certificate" is the same mistake in the
        // other direction.
        let err = handshake_error(
            "TLS handshake",
            io::Error::from(io::ErrorKind::ConnectionReset),
            true,
        );
        assert!(matches!(err, Error::Transport { .. }), "{err:?}");

        // And so does an alert that says nothing about identity.
        let err = handshake_error(
            "TLS handshake",
            alert(rustls::AlertDescription::ProtocolVersion),
            true,
        );
        assert!(matches!(err, Error::Transport { .. }), "{err:?}");
    }

    #[test]
    fn client_certificate_debug_never_prints_the_key() {
        let cert = ClientCertificate {
            chain_pem: b"chain".to_vec(),
            key_pem: b"SUPER SECRET".to_vec(),
        };
        let rendered = format!("{cert:?}");
        assert!(!rendered.contains("SUPER SECRET"), "{rendered}");
    }
}
