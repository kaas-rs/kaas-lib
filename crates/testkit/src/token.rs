//! Unsecured JWS minting, for the OAUTHBEARER fixtures.
//!
//! Kafka ships `OAuthBearerUnsecuredValidatorCallbackHandler` for exactly this
//! situation: a broker that has to validate OAUTHBEARER tokens with no OAuth
//! server anywhere. It accepts a JWS whose header says `alg: none` and whose
//! signature is empty, and then checks the claims. So a fixture can mint its
//! own tokens and the no-mocked-brokers rule stays intact — the broker is real
//! and the *issuer* is the part that goes away.
//!
//! What the validator demands, from
//! `clients/src/main/java/org/apache/kafka/common/security/oauthbearer/internals/unsecured/`
//! in the release we test against:
//!
//! * three dot-separated segments, the third empty (so the serialization ends
//!   in a `.`),
//! * `alg` of exactly `none` in the header,
//! * a `sub` claim — the principal the broker will authorize as `User:<sub>`,
//! * an `exp` claim, in seconds, in the future. `iat` is optional; we send it
//!   because `validateTimeConsistency` compares the two when both are present,
//!   and a fixture whose tokens have no issue time cannot exercise that.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;

/// Mint an unsecured JWS naming `subject`, valid for `lifetime`.
///
/// The broker's principal will be `User:<subject>`.
///
/// ```
/// let token = testkit::unsecured_jws("alice", std::time::Duration::from_secs(60));
/// assert!(token.ends_with('.'), "the signature segment is empty");
/// ```
pub fn unsecured_jws(subject: &str, lifetime: Duration) -> String {
    let issued_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let expires_at = issued_at.saturating_add(lifetime.as_secs().max(1));

    // Hand-built rather than serialized: the shape is fixed, the only values
    // are a bare identifier and two integers, and a JSON dependency for this
    // would be the tail wagging the dog. `subject` is quote-escaped anyway,
    // since a token that silently produces malformed JSON would fail as an
    // authentication error and name nothing.
    let header = br#"{"alg":"none"}"#;
    let claims = format!(
        r#"{{"sub":"{}","iat":{issued_at},"exp":{expires_at}}}"#,
        subject.replace('\\', "\\\\").replace('"', "\\\"")
    );

    // Note the trailing dot: `OAuthBearerUnsecuredJws` splits on `.`, and an
    // empty third segment is how "no digital signature" is spelled.
    format!(
        "{}.{}.",
        B64URL.encode(header),
        B64URL.encode(claims.as_bytes())
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(segment: &str) -> String {
        String::from_utf8(B64URL.decode(segment).expect("base64url")).expect("utf-8")
    }

    #[test]
    fn the_serialization_is_three_segments_with_an_empty_signature() {
        let token = unsecured_jws("alice", Duration::from_secs(30));
        let segments: Vec<&str> = token.split('.').collect();
        assert_eq!(segments.len(), 3, "{token}");
        assert!(segments[2].is_empty(), "a signature would be rejected");
        assert_eq!(decode(segments[0]), r#"{"alg":"none"}"#);
    }

    #[test]
    fn the_claims_carry_a_subject_and_an_expiry_in_the_future() {
        let token = unsecured_jws("alice", Duration::from_secs(30));
        let claims = decode(token.split('.').nth(1).expect("claims segment"));
        assert!(claims.contains(r#""sub":"alice""#), "{claims}");

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("after the epoch")
            .as_secs();
        let exp: u64 = claims
            .rsplit_once(':')
            .and_then(|(_, tail)| tail.trim_end_matches('}').parse().ok())
            .expect("an exp claim");
        assert!(exp > now, "exp {exp} is not in the future (now {now})");
        assert!(exp <= now + 30, "exp {exp} is further out than asked for");
    }

    #[test]
    fn base64url_is_unpadded_so_the_dots_stay_the_only_separators() {
        // Padding is `=`, which is legal in base64url output and *not* what
        // Kafka's own client emits. Matching it keeps the fixture honest about
        // what a real client sends.
        let token = unsecured_jws("a-subject-of-some-length", Duration::from_secs(1));
        assert!(!token.contains('='), "{token}");
    }

    #[test]
    fn a_zero_lifetime_still_produces_a_token_that_can_be_validated() {
        // `Duration::ZERO` would otherwise mint `exp == iat`, which the broker
        // reads as already expired — a fixture failing for a reason that has
        // nothing to do with the code under test.
        let token = unsecured_jws("alice", Duration::ZERO);
        let claims = decode(token.split('.').nth(1).expect("claims segment"));
        let (iat, exp) = (
            claims
                .split_once(r#""iat":"#)
                .and_then(|(_, tail)| tail.split(',').next())
                .and_then(|v| v.parse::<u64>().ok())
                .expect("iat"),
            claims
                .rsplit_once(':')
                .and_then(|(_, tail)| tail.trim_end_matches('}').parse::<u64>().ok())
                .expect("exp"),
        );
        assert!(exp > iat, "iat {iat}, exp {exp}");
    }

    #[test]
    fn a_subject_with_a_quote_cannot_break_the_claims_json() {
        let token = unsecured_jws("ali\"ce", Duration::from_secs(5));
        let claims = decode(token.split('.').nth(1).expect("claims segment"));
        assert!(claims.contains(r#""sub":"ali\"ce""#), "{claims}");
    }
}
