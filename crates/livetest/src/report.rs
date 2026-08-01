//! A deterministic, line-oriented report.
//!
//! Sorted `key = value` lines, so `diff` is the whole analysis tool. That is
//! the point: CLAUDE.md's argument for keeping this codec independent of the
//! `kaas` broker's is that the two implementations then form a conformance
//! harness, and a harness is only useful if the parity check is legible. Two
//! probe runs and a `diff` gives exactly that — every api version, every
//! feature, every error code, side by side.
//!
//! JSON would need a dependency and would diff worse: a one-line change in a
//! nested object shows up as a reformatted block, whereas a flat sorted key
//! space shows up as one line.

use std::collections::BTreeMap;
use std::fmt::Write as _;

/// A collected report.
#[derive(Debug, Default)]
pub struct Report {
    entries: BTreeMap<String, String>,
    notes: Vec<String>,
}

impl Report {
    /// An empty report.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a fact.
    pub fn set(&mut self, key: impl Into<String>, value: impl std::fmt::Display) {
        self.entries.insert(key.into(), value.to_string());
    }

    /// Record a fact only when there is one, rendering absence as `-`.
    ///
    /// Absence has to be visible in the key space rather than by a missing
    /// line, or a diff cannot tell "this cluster does not have it" from "the
    /// probe did not look".
    pub fn set_opt<T: std::fmt::Display>(&mut self, key: impl Into<String>, value: Option<T>) {
        let rendered = value
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_owned());
        self.entries.insert(key.into(), rendered);
    }

    /// Add a free-text note. Notes are printed but never diffed.
    pub fn note(&mut self, note: impl Into<String>) {
        self.notes.push(note.into());
    }

    /// Whether a key was recorded.
    #[cfg(test)]
    pub fn contains(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    /// Look a value up.
    #[cfg(test)]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries.get(key).map(String::as_str)
    }

    /// How many facts were recorded.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// The diffable body: sorted `key = value`, one per line.
    pub fn render(&self) -> String {
        let mut out = String::new();
        for (key, value) in &self.entries {
            let _ = writeln!(out, "{key} = {value}");
        }
        out
    }

    /// The notes, as comment lines.
    pub fn render_notes(&self) -> String {
        let mut out = String::new();
        for note in &self.notes {
            let _ = writeln!(out, "# {note}");
        }
        out
    }
}

/// A report plus whether the run that produced it succeeded.
///
/// The facts collected before a failure are the most useful ones there are —
/// they say how far the run got and what the cluster looked like on the way.
/// Returning `Result<Report>` throws exactly those away at the moment they
/// matter, so commands that assert return this instead and the caller prints
/// the report before propagating.
#[derive(Debug)]
pub struct Outcome {
    /// What was learned.
    pub report: Report,
    /// Whether the run met its assertions.
    pub result: anyhow::Result<()>,
}

impl Outcome {
    /// A successful run.
    pub fn ok(report: Report) -> Self {
        Self {
            report,
            result: Ok(()),
        }
    }

    /// A run that collected facts and then failed.
    pub fn failed(report: Report, error: anyhow::Error) -> Self {
        Self {
            report,
            result: Err(error),
        }
    }
}

/// Escape a value that might contain a newline or a leading space.
///
/// A broker's error message can contain anything, and one stray newline turns
/// a diffable report into a corrupt one.
pub fn one_line(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .trim()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_is_sorted_so_diffs_are_stable() {
        let mut report = Report::new();
        report.set("zeta", 1);
        report.set("alpha", 2);
        report.set("mid", 3);
        assert_eq!(report.render(), "alpha = 2\nmid = 3\nzeta = 1\n");
    }

    #[test]
    fn recording_the_same_key_twice_keeps_the_last_value() {
        let mut report = Report::new();
        report.set("k", "first");
        report.set("k", "second");
        assert_eq!(report.get("k"), Some("second"));
        assert_eq!(report.len(), 1);
    }

    #[test]
    fn absence_is_recorded_rather_than_omitted() {
        let mut report = Report::new();
        report.set_opt("present", Some(7));
        report.set_opt::<i32>("absent", None);
        // Both keys exist, so a diff distinguishes "not supported" from "not
        // probed".
        assert_eq!(report.get("present"), Some("7"));
        assert_eq!(report.get("absent"), Some("-"));
        assert!(report.contains("absent"));
    }

    #[test]
    fn notes_are_separate_from_the_diffable_body() {
        let mut report = Report::new();
        report.set("k", "v");
        report.note("took 3s");
        assert_eq!(report.render(), "k = v\n");
        assert_eq!(report.render_notes(), "# took 3s\n");
    }

    #[test]
    fn a_multiline_broker_message_cannot_corrupt_the_report() {
        assert_eq!(one_line(" a\nb\r\nc "), "a\\nb\\r\\nc");
        assert_eq!(one_line("back\\slash"), "back\\\\slash");
    }
}
