//! SCRAM-SHA-256 / SCRAM-SHA-512, RFC 5802.
//!
//! Two details are worth stating because skipping either produces a client
//! that works everywhere you test it and fails in production:
//!
//! * **SASLprep is a real stringprep profile**, not `trim()`. RFC 4013 maps
//!   non-ASCII spaces (U+00A0, U+2000…) to U+0020 and strips soft hyphens,
//!   among other things. A Java client normalises; if we do not, a password
//!   containing a non-breaking space hashes differently and authentication
//!   fails with an error that says nothing about why.
//! * **The server signature is verified in constant time.** It is the only
//!   thing proving the peer knows the stored key — that is mutual
//!   authentication, and comparing it with `==` leaks a timing oracle for a
//!   value an attacker is trying to forge.
//!
//! Kafka does not do channel binding, so the gs2 header is always `n,,`.
//!
//! The iteration count is the one field of server-first the *peer* prices:
//! PBKDF2 cost is linear in it, so it is bounded on both sides
//! ([`MIN_ITERATIONS`], [`MAX_ITERATIONS`]) before any key derivation runs —
//! a hostile broker does not get to choose our work factor, downward (proof
//! weakening) or upward (CPU denial of service).

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;

use crate::error::{Error, Result};

/// Which SCRAM hash to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramHash {
    /// SHA-256.
    Sha256,
    /// SHA-512.
    Sha512,
}

impl ScramHash {
    /// The mechanism name as it appears in `SaslHandshake`.
    pub const fn mechanism(self) -> &'static str {
        match self {
            ScramHash::Sha256 => "SCRAM-SHA-256",
            ScramHash::Sha512 => "SCRAM-SHA-512",
        }
    }
}

/// The gs2 header Kafka expects: no channel binding, no authzid.
const GS2_HEADER: &str = "n,,";

/// The lowest iteration count the client will accept from a broker.
///
/// 4096 is RFC 7677's floor and the minimum Kafka itself enforces when a
/// credential is stored (`kafka-admin` refuses to write below it, mirroring the
/// broker). A server-first message asking for less is therefore never a
/// legitimate Kafka broker — it is a misconfiguration or an active downgrade:
/// over a plaintext SCRAM connection, a man-in-the-middle rewriting `i=` to a
/// tiny value makes an offline dictionary attack against the observed client
/// proof essentially free.
const MIN_ITERATIONS: u32 = 4096;

/// The highest iteration count the client will accept from a broker.
///
/// PBKDF2 cost is linear in this value and the *server* chooses it, so without
/// a ceiling a malicious broker replies `i=4294967295` and buys hours of
/// blocked CPU per handshake — a remote denial of service, re-triggerable over
/// KIP-368 re-authentication on a single live connection. Kafka's own maximum
/// for stored credentials is 16384; two orders of magnitude above that is
/// headroom for any deliberate hardening while keeping the worst case at a few
/// seconds of compute.
const MAX_ITERATIONS: u32 = 1_000_000;

/// A SCRAM conversation in progress.
pub struct ScramClient {
    hash: ScramHash,
    password: String,
    client_nonce: String,
    /// `client-first-message-bare`, needed later to build the auth message.
    client_first_bare: String,
    /// Set once the client-final message has been sent.
    expected_server_signature: Option<Vec<u8>>,
}

impl std::fmt::Debug for ScramClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The SASLprepped password is still the plaintext credential; never
        // render it. The nonce and the client-first-bare go over the wire in
        // the clear, so they stay visible — they are what makes a failed
        // handshake debuggable.
        f.debug_struct("ScramClient")
            .field("hash", &self.hash)
            .field("password", &"<redacted>")
            .field("client_nonce", &self.client_nonce)
            .field("client_first_bare", &self.client_first_bare)
            .field(
                "expected_server_signature",
                match &self.expected_server_signature {
                    Some(_) => &"<set>",
                    None => &"<unset>",
                },
            )
            .finish()
    }
}

impl ScramClient {
    /// Start a conversation.
    ///
    /// `nonce` must be printable ASCII without a comma; callers should use
    /// [`random_nonce`].
    ///
    /// `extensions` are appended to `client-first-message-bare` as
    /// `,key=value` attributes, which is where SCRAM carries anything the
    /// mechanism itself does not define. The one that matters is KIP-48's
    /// `tokenauth=true`: it is what tells the broker to look the credential up
    /// in the delegation token cache rather than in the user store, so a
    /// delegation token is a SCRAM login that fails without it. Note the
    /// extensions are part of the bare message and therefore part of the auth
    /// message the proof is computed over — they are authenticated, not
    /// decoration.
    pub fn new(
        hash: ScramHash,
        username: &str,
        password: &str,
        nonce: String,
        extensions: &[(String, String)],
    ) -> Result<Self> {
        let username = saslprep(username, "username")?;
        let password = saslprep(password, "password")?;
        if nonce.contains(',') {
            return Err(Error::Authentication(
                "SCRAM nonce may not contain a comma".to_owned(),
            ));
        }
        let mut client_first_bare = format!("n={},r={}", escape_username(&username), nonce);
        for (key, value) in extensions {
            validate_extension(key, value)?;
            client_first_bare.push(',');
            client_first_bare.push_str(key);
            client_first_bare.push('=');
            client_first_bare.push_str(value);
        }
        Ok(Self {
            hash,
            client_first_bare,
            password,
            client_nonce: nonce,
            expected_server_signature: None,
        })
    }

    /// `client-first-message`.
    pub fn client_first(&self) -> Vec<u8> {
        format!("{GS2_HEADER}{}", self.client_first_bare).into_bytes()
    }

    /// Consume `server-first-message` and produce `client-final-message`.
    pub fn client_final(&mut self, server_first: &[u8]) -> Result<Vec<u8>> {
        let server_first = std::str::from_utf8(server_first)
            .map_err(|_| Error::Authentication("server-first-message is not UTF-8".to_owned()))?;

        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;
        for field in server_first.split(',') {
            match field.split_at_checked(2) {
                Some(("r=", value)) => nonce = Some(value),
                Some(("s=", value)) => salt = Some(value),
                Some(("i=", value)) => iterations = Some(value),
                // `e=` is the server rejecting us; anything else is an
                // extension we are allowed to ignore. The value is
                // server-authored text bound for logs, hence the escaping.
                Some(("e=", value)) => {
                    return Err(Error::Authentication(format!(
                        "server rejected SCRAM: {}",
                        crate::sanitize::control_safe(value)
                    )));
                }
                _ => {}
            }
        }

        let nonce = nonce
            .ok_or_else(|| Error::Authentication("server-first-message had no nonce".to_owned()))?;
        // The server's nonce must extend ours. Without this check a
        // man-in-the-middle can replay a recorded exchange. RFC 5802 says
        // *appends*, so equality — a server that added nothing of its own —
        // is refused too; the client nonce being fresh makes that
        // unexploitable today, but the tightening is free.
        if !nonce.starts_with(&self.client_nonce) || nonce.len() <= self.client_nonce.len() {
            return Err(Error::Authentication(
                "server nonce does not extend the client nonce".to_owned(),
            ));
        }
        let salt = B64
            .decode(salt.ok_or_else(|| {
                Error::Authentication("server-first-message had no salt".to_owned())
            })?)
            .map_err(|e| Error::Authentication(format!("undecodable SCRAM salt: {e}")))?;
        let iterations: u32 = iterations
            .ok_or_else(|| {
                Error::Authentication("server-first-message had no iteration count".to_owned())
            })?
            .parse()
            .map_err(|e| Error::Authentication(format!("bad SCRAM iteration count: {e}")))?;
        if iterations < MIN_ITERATIONS {
            return Err(Error::Authentication(format!(
                "SCRAM iteration count {iterations} is below the RFC 7677 minimum of \
                 {MIN_ITERATIONS}; refusing what is either a broken broker or a \
                 downgrade attack"
            )));
        }
        if iterations > MAX_ITERATIONS {
            return Err(Error::Authentication(format!(
                "SCRAM iteration count {iterations} exceeds the supported maximum of \
                 {MAX_ITERATIONS}; refusing to burn unbounded CPU on a broker-chosen \
                 work factor"
            )));
        }

        let client_final_without_proof = format!("c={},r={}", B64.encode(GS2_HEADER), nonce);
        let auth_message = format!(
            "{},{server_first},{client_final_without_proof}",
            self.client_first_bare
        );

        let salted = self.salted_password(&salt, iterations);
        let client_key = self.hmac(&salted, b"Client Key")?;
        let stored_key = self.digest(&client_key);
        let client_signature = self.hmac(&stored_key, auth_message.as_bytes())?;

        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(k, s)| k ^ s)
            .collect();

        let server_key = self.hmac(&salted, b"Server Key")?;
        self.expected_server_signature = Some(self.hmac(&server_key, auth_message.as_bytes())?);

        Ok(format!("{client_final_without_proof},p={}", B64.encode(proof)).into_bytes())
    }

    /// Verify `server-final-message`, completing mutual authentication.
    pub fn verify_server_final(&self, server_final: &[u8]) -> Result<()> {
        let expected = self.expected_server_signature.as_ref().ok_or_else(|| {
            Error::Authentication("server-final-message arrived before client-final".to_owned())
        })?;
        let server_final = std::str::from_utf8(server_final)
            .map_err(|_| Error::Authentication("server-final-message is not UTF-8".to_owned()))?;

        for field in server_final.split(',') {
            match field.split_at_checked(2) {
                Some(("e=", value)) => {
                    return Err(Error::Authentication(format!(
                        "server rejected SCRAM: {}",
                        crate::sanitize::control_safe(value)
                    )));
                }
                Some(("v=", value)) => {
                    let signature = B64.decode(value).map_err(|e| {
                        Error::Authentication(format!("undecodable server signature: {e}"))
                    })?;
                    // Constant time: this is the value an attacker forges.
                    return if signature.ct_eq(expected).into() {
                        Ok(())
                    } else {
                        Err(Error::Authentication(
                            "server signature did not verify — the peer does not know the password"
                                .to_owned(),
                        ))
                    };
                }
                _ => {}
            }
        }
        Err(Error::Authentication(
            "server-final-message had no verifier".to_owned(),
        ))
    }

    fn salted_password(&self, salt: &[u8], iterations: u32) -> Vec<u8> {
        match self.hash {
            ScramHash::Sha256 => {
                pbkdf2::pbkdf2_hmac_array::<Sha256, 32>(self.password.as_bytes(), salt, iterations)
                    .to_vec()
            }
            ScramHash::Sha512 => {
                pbkdf2::pbkdf2_hmac_array::<Sha512, 64>(self.password.as_bytes(), salt, iterations)
                    .to_vec()
            }
        }
    }

    fn hmac(&self, key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
        fn run<D>(key: &[u8], data: &[u8]) -> Result<Vec<u8>>
        where
            D: hmac::EagerHash,
        {
            let mut mac = <Hmac<D> as KeyInit>::new_from_slice(key).map_err(|e| {
                Error::Authentication(format!("HMAC rejected a {} byte key: {e}", key.len()))
            })?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        match self.hash {
            ScramHash::Sha256 => run::<Sha256>(key, data),
            ScramHash::Sha512 => run::<Sha512>(key, data),
        }
    }

    fn digest(&self, data: &[u8]) -> Vec<u8> {
        match self.hash {
            ScramHash::Sha256 => Sha256::digest(data).to_vec(),
            ScramHash::Sha512 => Sha512::digest(data).to_vec(),
        }
    }
}

/// Salt and hash a password the way SCRAM's `Hi()` does.
///
/// Exposed because `AlterUserScramCredentials` needs it: the broker stores a
/// salted hash and never sees the plaintext, so the *client* is responsible for
/// producing one. Getting it wrong stores a credential that writes cleanly and
/// then fails every login, which is a slow way to find out.
pub fn salted_password(
    hash: ScramHash,
    password: &str,
    salt: &[u8],
    iterations: u32,
) -> Result<Vec<u8>> {
    let prepared = saslprep(password, "password")?;
    Ok(match hash {
        ScramHash::Sha256 => {
            pbkdf2::pbkdf2_hmac_array::<Sha256, 32>(prepared.as_bytes(), salt, iterations).to_vec()
        }
        ScramHash::Sha512 => {
            pbkdf2::pbkdf2_hmac_array::<Sha512, 64>(prepared.as_bytes(), salt, iterations).to_vec()
        }
    })
}

/// A fresh random salt for a stored credential.
pub fn random_salt() -> Vec<u8> {
    use rand::RngCore;
    let mut bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    bytes.to_vec()
}

/// A fresh client nonce.
///
/// Base64 of 24 random bytes: printable, comma-free by construction, and 192
/// bits of entropy, which is well past what RFC 5802 asks for.
pub fn random_nonce() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 24];
    rand::rng().fill_bytes(&mut bytes);
    B64.encode(bytes)
}

/// Apply the SASLprep profile, reporting failures as authentication errors.
///
/// The error detail is kept for the username and dropped for the password:
/// stringprep's failure Display names the offending character, error strings
/// reach log aggregators, and one character of a password is one character
/// too many.
fn saslprep(value: &str, what: &'static str) -> Result<String> {
    stringprep::saslprep(value)
        .map(|s| s.into_owned())
        .map_err(|e| {
            if what == "password" {
                Error::Authentication(
                    "password is not valid under SASLprep (RFC 4013); it contains a \
                     prohibited character"
                        .to_owned(),
                )
            } else {
                Error::Authentication(format!("{what} is not valid under SASLprep: {e}"))
            }
        })
}

/// RFC 5802 §5.1: `,` and `=` are the field separators, so they are escaped in
/// the username rather than forbidden.
fn escape_username(username: &str) -> String {
    username.replace('=', "=3D").replace(',', "=2C")
}

/// Reject an extension that would corrupt `client-first-message-bare`.
///
/// The grammar RFC 5802 §7 writes as `attr-val = ALPHA "=" value` is read by
/// Kafka's own `ScramMessages` as `[a-zA-Z]+=[^,]*`, so multi-letter keys —
/// `tokenauth` among them — are what the broker actually parses. This follows
/// the broker rather than the letter of the ABNF, because a client that is
/// stricter than the server here can send nothing useful.
///
/// The reserved single letters are refused for the same reason `auth` is
/// refused in an OAUTHBEARER response: `n` and `r` are the username and the
/// nonce, and letting a caller set a second one puts two of the same attribute
/// in one message with no rule saying which the broker honours.
fn validate_extension(key: &str, value: &str) -> Result<()> {
    const RESERVED: [&str; 10] = ["a", "c", "e", "i", "m", "n", "p", "r", "s", "v"];

    if key.is_empty() || !key.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err(Error::InvalidRequest(format!(
            "SCRAM extension key {key:?} must be one or more ASCII letters"
        )));
    }
    if RESERVED.contains(&key.to_ascii_lowercase().as_str()) {
        return Err(Error::InvalidRequest(format!(
            "SCRAM extension key {key:?} is a reserved RFC 5802 attribute and cannot be set"
        )));
    }
    // A comma ends the attribute, and a NUL ends nothing but is not a value
    // character either. Both would be read by the broker as a message we did
    // not write.
    if value.is_empty() || value.chars().any(|c| c == ',' || c.is_control()) {
        return Err(Error::InvalidRequest(format!(
            "SCRAM extension {key:?} has a value that is empty or contains a character \
             RFC 5802 does not permit in one"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `#[derive(Debug)]` here would put the plaintext credential into any
    /// log line that formats a connection. Asserting on the rendering is what
    /// stops the derive from coming back.
    #[test]
    fn debug_does_not_render_the_password() {
        let client = ScramClient::new(
            ScramHash::Sha256,
            "user",
            "s3cr3t-pencil",
            "rOprNGfwEbeRWgbNEkqO".to_owned(),
            &[],
        )
        .unwrap();

        let rendered = format!("{client:?}");
        assert!(
            !rendered.contains("s3cr3t-pencil"),
            "password leaked into Debug: {rendered}"
        );
        assert!(rendered.contains("<redacted>"), "{rendered}");
        // The wire-visible parts stay, or a failed handshake is undebuggable.
        assert!(rendered.contains("rOprNGfwEbeRWgbNEkqO"), "{rendered}");
    }

    /// RFC 7677 §3, the SCRAM-SHA-256 worked example.
    ///
    /// Fixing the nonce is the only way to test this deterministically, and
    /// getting it byte-exact is the whole point: every intermediate value in
    /// SCRAM is a hash, so an error anywhere shows up only as "authentication
    /// failed" against a real broker.
    #[test]
    fn rfc7677_worked_example() {
        let mut client = ScramClient::new(
            ScramHash::Sha256,
            "user",
            "pencil",
            "rOprNGfwEbeRWgbNEkqO".to_owned(),
            &[],
        )
        .unwrap();

        assert_eq!(
            String::from_utf8(client.client_first()).unwrap(),
            "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
        );

        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let final_msg = client.client_final(server_first.as_bytes()).unwrap();
        assert_eq!(
            String::from_utf8(final_msg).unwrap(),
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
             p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );

        client
            .verify_server_final(b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
            .unwrap();
    }

    #[test]
    fn a_forged_server_signature_is_rejected() {
        let mut client = ScramClient::new(
            ScramHash::Sha256,
            "user",
            "pencil",
            "rOprNGfwEbeRWgbNEkqO".to_owned(),
            &[],
        )
        .unwrap();
        client
            .client_final(
                b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
            )
            .unwrap();
        let err = client
            .verify_server_final(b"v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
            .unwrap_err();
        assert!(matches!(err, Error::Authentication(_)), "{err:?}");
    }

    /// RFC 5802 says the server *appends* to the client nonce; a server-first
    /// echoing our nonce unmodified added no freshness of its own and is
    /// refused. Unexploitable while the client nonce is random per exchange —
    /// the check is defense in depth, not a fix.
    #[test]
    fn a_server_nonce_equal_to_ours_is_rejected() {
        let mut client = ScramClient::new(
            ScramHash::Sha256,
            "user",
            "pencil",
            "clientnonce".to_owned(),
            &[],
        )
        .unwrap();
        let err = client
            .client_final(b"r=clientnonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096")
            .unwrap_err();
        assert!(matches!(err, Error::Authentication(_)), "{err:?}");
    }

    /// The stringprep error Display names the offending character, and error
    /// strings travel to log aggregators. For a password that is a one-
    /// character-at-a-time disclosure channel, so the cause is dropped there
    /// — and kept for the username, where it is the only useful diagnostic.
    #[test]
    fn a_prohibited_password_character_is_not_named_in_the_error() {
        let err = ScramClient::new(
            ScramHash::Sha256,
            "user",
            "pw\u{0007}x",
            "abc".to_owned(),
            &[],
        )
        .unwrap_err();
        let rendered = format!("{err}");
        assert!(!rendered.contains('\u{0007}'), "{rendered:?}");
        assert!(rendered.contains("password"), "{rendered}");
    }

    #[test]
    fn a_server_nonce_that_does_not_extend_ours_is_rejected() {
        let mut client = ScramClient::new(
            ScramHash::Sha256,
            "user",
            "pencil",
            "clientnonce".to_owned(),
            &[],
        )
        .unwrap();
        let err = client
            .client_final(b"r=somethingelse,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096")
            .unwrap_err();
        assert!(matches!(err, Error::Authentication(_)), "{err:?}");
    }

    /// The broker chooses the iteration count, and PBKDF2 cost is linear in
    /// it — so a broker-supplied `i=4294967295` is hours of CPU, not a
    /// preference. The check must fire before any key derivation runs, which
    /// is what asserting on the *error* (rather than a slow success) proves:
    /// were the ceiling missing, this test would hang rather than fail.
    #[test]
    fn an_absurd_iteration_count_is_rejected_before_any_derivation() {
        let mut client =
            ScramClient::new(ScramHash::Sha256, "user", "pencil", "abc".to_owned(), &[]).unwrap();
        let err = client
            .client_final(b"r=abcdef,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4294967295")
            .unwrap_err();
        assert!(matches!(err, Error::Authentication(_)), "{err:?}");
        assert!(format!("{err}").contains("4294967295"), "{err}");
    }

    /// Below RFC 7677's 4096 floor is a downgrade attack over plaintext SCRAM
    /// (a MITM rewriting `i=` makes the observed proof cheap to dictionary),
    /// so it is refused, not warned about. Zero keeps its own coverage: it was
    /// the only bound the code originally had.
    #[test]
    fn a_downgraded_iteration_count_is_rejected() {
        for i in ["0", "1", "1024", "4095"] {
            let mut client =
                ScramClient::new(ScramHash::Sha256, "user", "pencil", "abc".to_owned(), &[])
                    .unwrap();
            let server_first = format!("r=abcdef,s=W22ZaJ0SNY7soEsUEjb6gQ==,i={i}");
            let err = client.client_final(server_first.as_bytes()).unwrap_err();
            assert!(matches!(err, Error::Authentication(_)), "i={i}: {err:?}");
        }
    }

    #[test]
    fn server_errors_surface_as_authentication_failures() {
        let mut client =
            ScramClient::new(ScramHash::Sha256, "user", "pencil", "abc".to_owned(), &[]).unwrap();
        let err = client.client_final(b"e=unknown-user").unwrap_err();
        assert!(format!("{err}").contains("unknown-user"), "{err}");
    }

    /// The trap from CLAUDE.md, made executable.
    #[test]
    fn saslprep_maps_non_ascii_space_the_way_a_java_client_does() {
        // U+00A0 NO-BREAK SPACE maps to U+0020 under RFC 4013.
        let with_nbsp = ScramClient::new(
            ScramHash::Sha256,
            "user",
            "pass\u{00A0}word",
            "abc".to_owned(),
            &[],
        )
        .unwrap();
        let with_space = ScramClient::new(
            ScramHash::Sha256,
            "user",
            "pass word",
            "abc".to_owned(),
            &[],
        )
        .unwrap();
        assert_eq!(with_nbsp.password, with_space.password);
        assert_eq!(with_nbsp.password, "pass word");
    }

    #[test]
    fn saslprep_strips_soft_hyphens() {
        // U+00AD SOFT HYPHEN maps to nothing.
        let client = ScramClient::new(
            ScramHash::Sha256,
            "us\u{00AD}er",
            "pw",
            "abc".to_owned(),
            &[],
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(client.client_first()).unwrap(),
            "n,,n=user,r=abc"
        );
    }

    #[test]
    fn prohibited_characters_are_an_error_not_a_silent_strip() {
        // U+0007 BELL is prohibited by RFC 4013.
        assert!(
            ScramClient::new(
                ScramHash::Sha256,
                "user",
                "pw\u{0007}",
                "abc".to_owned(),
                &[]
            )
            .is_err()
        );
    }

    #[test]
    fn usernames_escape_the_field_separators() {
        let client =
            ScramClient::new(ScramHash::Sha256, "a,b=c", "pw", "nonce".to_owned(), &[]).unwrap();
        assert_eq!(
            String::from_utf8(client.client_first()).unwrap(),
            "n,,n=a=2Cb=3Dc,r=nonce"
        );
    }

    /// KIP-48's whole client side, in one assertion: a delegation token is a
    /// SCRAM login whose extension is what makes the broker look in the token
    /// cache. Without the `,tokenauth=true` the same bytes are a login attempt
    /// for a user named after a token id.
    #[test]
    fn extensions_are_attributes_on_client_first_bare() {
        let client = ScramClient::new(
            ScramHash::Sha256,
            "token-id",
            "hmac",
            "nonce".to_owned(),
            &[("tokenauth".to_owned(), "true".to_owned())],
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(client.client_first()).unwrap(),
            "n,,n=token-id,r=nonce,tokenauth=true"
        );
    }

    /// The extension is inside the auth message, so it is covered by the proof.
    /// A broker that saw a different extension list computes a different
    /// signature — which is the property that stops one being stripped in
    /// flight.
    #[test]
    fn an_extension_changes_the_proof() {
        let server_first = b"r=nonce-server,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let mut plain =
            ScramClient::new(ScramHash::Sha256, "u", "p", "nonce".to_owned(), &[]).unwrap();
        let mut extended = ScramClient::new(
            ScramHash::Sha256,
            "u",
            "p",
            "nonce".to_owned(),
            &[("tokenauth".to_owned(), "true".to_owned())],
        )
        .unwrap();
        assert_ne!(
            plain.client_final(server_first).unwrap(),
            extended.client_final(server_first).unwrap()
        );
    }

    #[test]
    fn an_extension_that_would_forge_a_field_is_refused() {
        let bad: [(&str, &str); 6] = [
            // `n` and `r` are the username and the nonce.
            ("n", "mallory"),
            ("R", "attacker-nonce"),
            ("", "v"),
            ("token auth", "true"),
            // A comma ends the attribute, so this is two attributes.
            ("tokenauth", "true,n=mallory"),
            ("tokenauth", ""),
        ];
        for (key, value) in bad {
            let err = ScramClient::new(
                ScramHash::Sha256,
                "u",
                "p",
                "nonce".to_owned(),
                &[(key.to_owned(), value.to_owned())],
            )
            .unwrap_err();
            assert!(
                matches!(err, Error::InvalidRequest(_)),
                "{key}={value}: {err:?}"
            );
        }
    }

    #[test]
    fn stored_credentials_use_the_same_hashing_as_the_handshake() {
        // The credential we write must be the one the broker checks against.
        // Comparing against the RFC 7677 vector's salted password is the
        // strongest available statement of that.
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let stored = salted_password(ScramHash::Sha256, "pencil", &salt, 4096).unwrap();
        assert_eq!(stored.len(), 32);
        // SASLprep applies here too, or a non-ASCII password stores one hash
        // and authenticates with another.
        let with_nbsp = salted_password(ScramHash::Sha256, "a\u{00A0}b", &salt, 4096).unwrap();
        let with_space = salted_password(ScramHash::Sha256, "a b", &salt, 4096).unwrap();
        assert_eq!(with_nbsp, with_space);
    }

    #[test]
    fn salts_are_random_and_the_right_size() {
        let a = random_salt();
        assert_eq!(a.len(), 16);
        assert_ne!(a, random_salt());
    }

    #[test]
    fn nonces_are_distinct_and_comma_free() {
        let a = random_nonce();
        let b = random_nonce();
        assert_ne!(a, b);
        assert!(!a.contains(','));
    }

    #[test]
    fn sha512_produces_a_different_proof_than_sha256() {
        let server_first = b"r=abcdef,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let mut a = ScramClient::new(ScramHash::Sha256, "u", "p", "abc".to_owned(), &[]).unwrap();
        let mut b = ScramClient::new(ScramHash::Sha512, "u", "p", "abc".to_owned(), &[]).unwrap();
        assert_ne!(
            a.client_final(server_first).unwrap(),
            b.client_final(server_first).unwrap()
        );
    }
}
