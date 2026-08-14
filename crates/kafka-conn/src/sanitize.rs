//! Making untrusted bytes safe to put in a log line.
//!
//! Broker responses carry free-text fields — `error_message`, advertised
//! `host` names, SASL server messages — with no charset constraint anywhere in
//! the protocol, and they end up in [`tracing`] output via error Displays and
//! log fields. `tracing`'s fmt subscriber does not strip ANSI sequences, so a
//! hostile broker that puts `ESC [2J` in an error message is writing to the
//! *operator's terminal* when the log is viewed: cursor games, forged log
//! lines, clipboard escapes on some emulators.
//!
//! The rule this module implements: text the client did not author is passed
//! through [`control_safe`] at the point where it becomes an owned `String` of
//! ours — the [`crate::Error::from_code`] funnel for broker error messages,
//! the SASL exchanges for server-authored rejection text, metadata decoding
//! for advertised host names. Escaping at ingestion rather than at each log
//! site means a field added to a log line next year is already safe.
//!
//! Escaping is visible, not silent: a control character renders as its
//! `\u{..}` escape, so the log shows *that the broker sent it*, which for a
//! hostile peer is precisely the interesting fact.

use std::borrow::Cow;

/// Escape control characters so the text can reach a terminal unfiltered.
///
/// Borrow-through on the overwhelmingly common clean path. Everything in
/// Unicode's `Cc` category — C0 controls (`\n`, `\r`, `\x1b`…), DEL, and the
/// C1 range a raw byte copy can smuggle in — is rewritten to its
/// [`char::escape_default`] form. Printable text of every script passes
/// untouched.
pub fn control_safe(text: &str) -> Cow<'_, str> {
    if !text.chars().any(char::is_control) {
        return Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if c.is_control() {
            out.extend(c.escape_default());
        } else {
            out.push(c);
        }
    }
    Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_text_is_borrowed_untouched() {
        let text = "ordinary message about topic-42, まで unicode";
        assert!(matches!(control_safe(text), Cow::Borrowed(t) if t == text));
    }

    /// The attack from the audit: ANSI escapes in a broker `error_message`
    /// must not reach a terminal as live control bytes.
    #[test]
    fn ansi_escapes_are_rendered_inert_and_visible() {
        let hostile = "ok\x1b[2Jha\x07";
        let safe = control_safe(hostile);
        assert_eq!(safe, "ok\\u{1b}[2Jha\\u{7}");
        assert!(!safe.contains('\x1b'));
    }

    #[test]
    fn newlines_cannot_forge_log_lines() {
        let safe = control_safe("line\nFORGED level=ERROR msg=fake");
        assert!(!safe.contains('\n'), "{safe}");
        assert!(safe.contains("\\n"), "{safe}");
    }

    #[test]
    fn del_and_c1_controls_are_escaped_too() {
        let safe = control_safe("a\u{7f}b\u{9b}c");
        assert!(safe.chars().all(|c| !c.is_control()), "{safe}");
    }
}
