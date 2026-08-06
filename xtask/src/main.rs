//! Repo-wide chores: `ci`, `integration`, `fmt-check`, `fuzz`, `interop`, `docs`.
//!
//! `ci` is the gate CI runs and the one to run locally before pushing —
//! it is deliberately the *unit* gate and needs no Docker daemon, so it
//! stays fast enough to run on every save.
//!
//! `integration` is the separate, slow half: it runs the `#[ignore]`d
//! tests, every one of which boots a real broker in a container. PLAN.md
//! makes these the acceptance criteria for every milestone, so this is
//! the command that actually decides whether a milestone is done. A
//! green `ci` is not evidence.

use std::path::Path;
use std::process::Command;
use std::{env, fs};

use anyhow::{Context, Result, bail};

fn main() -> Result<()> {
    let task = env::args().nth(1).unwrap_or_default();
    match task.as_str() {
        "ci" => ci(),
        "integration" => integration(),
        "fmt-check" => run("cargo", &["fmt", "--check"]),
        "fuzz" => fuzz(),
        "interop" => interop(),
        "docs" => docs(),
        other => {
            bail!(
                "unknown xtask: {other:?}. try: ci | integration | fmt-check | fuzz | interop | docs"
            )
        }
    }
}

/// Refuse hand-written `#[ignore]` in workspace test sources.
///
/// The integration job is `cargo test -- --ignored`, so `#[ignore]` is the
/// door into it — and `#[testkit::integration_test]` is the only thing
/// allowed to write it, because that attribute is where the two-minute
/// per-test deadline lives. A bare `#[ignore]` would put a test in the job
/// with no deadline at all, and it would look exactly like all the others
/// until the day it wedged the suite.
///
/// `crates/interop` is exempt: it is not a workspace member and runs under
/// `cargo xtask interop`, a different job with different rules.
fn enforce_test_deadline() -> Result<()> {
    // Baked in at build time; the workspace root is xtask's parent. Correct
    // regardless of the directory `cargo xtask` is invoked from.
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("xtask has no parent directory")?;

    let mut offenders = Vec::new();
    for crate_dir in fs::read_dir(root.join("crates"))? {
        let crate_dir = crate_dir?.path();
        if crate_dir.file_name().is_some_and(|name| name == "interop") {
            continue;
        }
        let tests = crate_dir.join("tests");
        if !tests.is_dir() {
            continue;
        }
        for entry in fs::read_dir(&tests)? {
            let file = entry?.path();
            if file.extension().is_none_or(|ext| ext != "rs") {
                continue;
            }
            let source =
                fs::read_to_string(&file).with_context(|| format!("reading {}", file.display()))?;
            for (index, line) in source.lines().enumerate() {
                if line.trim_start().starts_with("#[ignore") {
                    offenders.push(format!("  {}:{}", file.display(), index + 1));
                }
            }
        }
    }

    if offenders.is_empty() {
        Ok(())
    } else {
        bail!(
            "hand-written #[ignore] found in workspace tests — use \
             #[testkit::integration_test], which is where the two-minute \
             deadline lives:\n{}",
            offenders.join("\n")
        )
    }
}

/// fmt + clippy + unit tests. No Docker required.
fn ci() -> Result<()> {
    enforce_test_deadline()?;
    run("cargo", &["fmt", "--check"])?;
    run(
        "cargo",
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
    )?;
    run("cargo", &["test", "--workspace"])?;
    Ok(())
}

/// The `#[ignore]`d integration tests. Requires a reachable Docker
/// daemon — `testcontainers` boots `apache/kafka:4.3.1` per fixture.
///
/// Every test in this job wears `#[testkit::integration_test]`, which caps it
/// at two minutes of wall clock, container boot included — enforced by
/// [`enforce_test_deadline`] refusing any other way in. A test that runs
/// longer fails; a suite that wedges becomes one red test instead of a CI job
/// the runner eventually kills.
///
/// `--no-fail-fast` because these are acceptance criteria, not a build gate.
/// Cargo otherwise stops at the first *test binary* that fails, and each one
/// here is a whole milestone: a single broken assertion in `kafka-read`'s
/// forward scan hid whether the leak suite passed at all, on a run that costs
/// minutes and boots a broker per fixture. Finding out which milestones are
/// red is the entire point of the command, so pay for every one of them.
/// The exit status still reflects any failure.
fn integration() -> Result<()> {
    enforce_test_deadline()?;
    run(
        "cargo",
        &[
            "test",
            "--workspace",
            "--all-features",
            "--no-fail-fast",
            "--",
            "--ignored",
            "--nocapture",
        ],
    )
}

/// The `cargo-fuzz` target from M11.
///
/// Its own task rather than part of `ci` because `cargo-fuzz` needs a nightly
/// toolchain, and pinning the workspace to nightly for one target would drag
/// every other crate along with it. Five minutes is PLAN.md's figure; a longer
/// run belongs in a scheduled job.
fn fuzz() -> Result<()> {
    run(
        "cargo",
        &[
            "+nightly",
            "fuzz",
            "run",
            "--fuzz-dir",
            "fuzz",
            "record_batch",
            "--",
            "-max_total_time=300",
        ],
    )
}

/// Cross-client interoperability against `rdkafka`.
///
/// A separate crate outside the workspace: `rdkafka` builds librdkafka from C
/// source and wants cmake, which is a reasonable thing to require of this job
/// and an unreasonable thing to require of `ci`.
fn interop() -> Result<()> {
    run(
        "cargo",
        &[
            "test",
            "--manifest-path",
            "crates/interop/Cargo.toml",
            "--no-fail-fast",
            "--",
            "--ignored",
            "--nocapture",
        ],
    )
}

/// Build the documentation book.
///
/// Needs `mdbook`, `mdbook-mermaid` and `mdbook-linkcheck` on `PATH`; the
/// `docs` job in `.github/workflows/ci.yml` pins all three. `mdbook build`
/// runs the linkcheck backend too, so a broken cross-reference fails here
/// rather than shipping as a 404.
fn docs() -> Result<()> {
    if env::args().nth(2).as_deref() == Some("--serve") {
        run("mdbook", &["serve", "docs"])
    } else {
        run("mdbook", &["build", "docs"])
    }
}

fn run(program: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(program)
        .args(args)
        .status()
        .with_context(|| format!("failed to spawn `{program}`"))?;
    if !status.success() {
        bail!("`{program} {}` failed: {status}", args.join(" "));
    }
    Ok(())
}
