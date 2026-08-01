//! TLS configuration.
//!
//! Nothing here reaches for `dangerous()`. A UI backend that silently accepts
//! any certificate is worse than one that fails to connect, because the
//! failure is invisible — so a private CA is configured by handing us the CA,
//! not by turning verification off.

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

/// How to negotiate TLS.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Trust anchors.
    pub anchors: TrustAnchors,
    /// Client certificate for mutual TLS.
    pub client_certificate: Option<ClientCertificate>,
    /// Override the name sent in SNI and verified against the certificate.
    ///
    /// Needed whenever brokers advertise names that do not resolve from where
    /// the client runs — a port-forwarded cluster, or a load balancer in
    /// front of the advertised listeners.
    pub server_name_override: Option<String>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            anchors: TrustAnchors::System,
            client_certificate: None,
            server_name_override: None,
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

    /// Override the name used for SNI and hostname verification.
    pub fn with_server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name_override = Some(name.into());
        self
    }

    /// Build a connector.
    ///
    /// The crypto provider is named explicitly rather than left to
    /// `ClientConfig::builder()`, which resolves a process-wide default and can
    /// fail at runtime depending on what else in the binary installed one.
    pub fn connector(&self) -> Result<TlsConnector> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = RustlsConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| Error::Unsupported(format!("rustls protocol versions: {e}")))?
            .with_root_certificates(self.root_store()?);

        let config = match &self.client_certificate {
            None => builder.with_no_client_auth(),
            Some(cert) => {
                let chain = parse_certs(&cert.chain_pem)?;
                let key = parse_key(&cert.key_pem)?;
                builder
                    .with_client_auth_cert(chain, key)
                    .map_err(|e| Error::Unsupported(format!("client certificate rejected: {e}")))?
            }
        };

        Ok(TlsConnector::from(Arc::new(config)))
    }

    /// The name to verify the server against for a given host.
    pub fn server_name(&self, host: &str) -> Result<ServerName<'static>> {
        let name = self.server_name_override.as_deref().unwrap_or(host);
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

fn parse_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>> {
    let mut reader = io::BufReader::new(pem);
    rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::InvalidRequest(format!("could not parse certificate PEM: {e}")))
}

fn parse_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>> {
    let mut reader = io::BufReader::new(pem);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| Error::InvalidRequest(format!("could not parse private key PEM: {e}")))?
        .ok_or_else(|| Error::InvalidRequest("private key PEM contained no key".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_pem_bundle_is_rejected_rather_than_trusting_nothing() {
        let cfg = TlsConfig::with_ca_pem(b"not a certificate".to_vec());
        assert!(cfg.connector().is_err());
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
    fn client_certificate_debug_never_prints_the_key() {
        let cert = ClientCertificate {
            chain_pem: b"chain".to_vec(),
            key_pem: b"SUPER SECRET".to_vec(),
        };
        let rendered = format!("{cert:?}");
        assert!(!rendered.contains("SUPER SECRET"), "{rendered}");
    }
}
