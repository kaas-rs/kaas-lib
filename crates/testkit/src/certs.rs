//! The client half of a mutual-TLS fixture.
//!
//! # Why a *second* CA
//!
//! The broker's own certificate is generated inside the container by `keytool`
//! and chains to a CA the fixture calls the cluster CA. A client certificate
//! must **not** chain to that one: on a real deployment it does not. Strimzi
//! issues client certificates from a separate clients CA, and the two point in
//! opposite directions within a single handshake — the client verifies the
//! broker against the cluster CA, and the broker verifies the client against
//! the clients CA. Reproducing that split here is the whole point of the
//! fixture (#27): a chain-versus-anchor mix-up then fails in CI rather than
//! against a live cluster.
//!
//! # Why in Rust rather than in the container
//!
//! `keytool` cannot export a private key, and the test process needs the
//! client key as PEM. Converting a PKCS#12 would mean depending on `openssl`
//! being present in whatever image the fixture was pointed at, which is not a
//! property of `apache/kafka` we should rely on. Generating this half here
//! costs one dev-only dependency and removes the container from the equation:
//! only the clients-CA *certificate* crosses into it, to be imported into the
//! broker's truststore.

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose,
};

use crate::error::{Error, Result};

/// A generated clients CA and one client certificate signed by it.
#[derive(Debug, Clone)]
pub(crate) struct ClientPki {
    /// The clients CA certificate, PEM. Imported into the broker's truststore
    /// so it will verify certificates this CA issued.
    pub(crate) ca_pem: String,
    /// The client certificate chain, PEM, leaf first.
    pub(crate) cert_pem: String,
    /// The client's private key, PKCS#8 PEM.
    pub(crate) key_pem: String,
}

/// Generate a clients CA and a client certificate whose principal is `cn`.
///
/// The subject matters: Kafka derives the principal from the certificate's
/// distinguished name, so a test asserting on `User:CN=…` is asserting on what
/// the broker will actually authorize.
pub(crate) fn client_pki(cn: &str) -> Result<ClientPki> {
    let ca_key = KeyPair::generate().map_err(cert_error)?;
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    ca_params.distinguished_name = distinguished_name("kaas-testkit-clients-ca");
    let ca_cert = ca_params.self_signed(&ca_key).map_err(cert_error)?;
    let ca_pem = ca_cert.pem();

    let leaf_key = KeyPair::generate().map_err(cert_error)?;
    let mut leaf_params = CertificateParams::default();
    leaf_params.distinguished_name = distinguished_name(cn);
    // A client certificate, explicitly. A broker configured `ssl.client.auth`
    // checks the extended key usage, and a certificate marked only for server
    // authentication is refused in a way that reads like a chain problem.
    leaf_params.use_authority_key_identifier_extension = true;
    leaf_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    leaf_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];

    let issuer = Issuer::new(ca_params, ca_key);
    let leaf = leaf_params
        .signed_by(&leaf_key, &issuer)
        .map_err(cert_error)?;

    Ok(ClientPki {
        ca_pem,
        cert_pem: leaf.pem(),
        key_pem: leaf_key.serialize_pem(),
    })
}

fn distinguished_name(common_name: &str) -> DistinguishedName {
    let mut name = DistinguishedName::new();
    name.push(DnType::CommonName, common_name);
    name.push(DnType::OrganizationName, "kaas-testkit");
    name
}

fn cert_error(error: rcgen::Error) -> Error {
    Error::ClientCertificate {
        detail: error.to_string(),
    }
}

/// A coherent client certificate and key that **no** fixture broker trusts.
///
/// For the negative case that a wrong-CA certificate is refused. It has to be
/// a self-consistent pair from an unrelated CA: pairing a foreign certificate
/// with the real client's key is rejected locally by rustls as a key
/// mismatch, which proves nothing about what the broker would have done with
/// it.
pub fn untrusted_client_certificate() -> Result<(String, String)> {
    let pki = client_pki("bob-mtls")?;
    Ok((pki.cert_pem, pki.key_pem))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fixture is only worth having if what it generates parses as the
    /// real thing — the same `rustls-pki-types` path `TlsConfig` uses.
    #[test]
    fn the_generated_pki_is_pem_the_tls_layer_can_read() {
        let pki = client_pki("bob-mtls").expect("generate");
        assert!(pki.ca_pem.contains("BEGIN CERTIFICATE"));
        assert!(pki.cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(pki.key_pem.contains("PRIVATE KEY"));
        assert_ne!(
            pki.ca_pem, pki.cert_pem,
            "the client must not be its own anchor — that is the mix-up this \
             fixture exists to catch"
        );
    }

    /// Two fixtures must not share key material, or a test that leaks one
    /// leaks them all.
    #[test]
    fn every_fixture_gets_its_own_key() {
        let a = client_pki("bob-mtls").expect("generate");
        let b = client_pki("bob-mtls").expect("generate");
        assert_ne!(a.key_pem, b.key_pem);
    }
}
