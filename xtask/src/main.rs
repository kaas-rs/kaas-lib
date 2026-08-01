//! Repo-wide chores: `ci`, `integration`, `fmt-check`.
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

use std::env;
use std::process::Command;

use anyhow::{Context, Result, bail};

fn main() -> Result<()> {
    let task = env::args().nth(1).unwrap_or_default();
    match task.as_str() {
        "ci" => ci(),
        "integration" => integration(),
        "fmt-check" => run("cargo", &["fmt", "--check"]),
        "fuzz" => fuzz(),
        "interop" => interop(),
        other => {
            bail!("unknown xtask: {other:?}. try: ci | integration | fmt-check | fuzz | interop")
        }
    }
}

/// fmt + clippy + unit tests. No Docker required.
fn ci() -> Result<()> {
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
fn integration() -> Result<()> {
    run(
        "cargo",
        &[
            "test",
            "--workspace",
            "--all-features",
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
            "--",
            "--ignored",
            "--nocapture",
        ],
    )
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
