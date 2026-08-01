# xtask

Repo-wide chores, as a binary in the workspace rather than a Makefile or a
shell script. `publish = false`, and its only dependency is `anyhow`.

```sh
cargo xtask ci             # fmt + clippy + unit tests, no Docker
cargo xtask integration    # the #[ignore]d acceptance tests, needs Docker
cargo xtask fmt-check      # just the formatting gate
cargo xtask fuzz           # the record-batch fuzz target, needs nightly
cargo xtask interop        # cross-client checks against rdkafka, needs cmake
cargo xtask docs           # build this book (--serve for live reload)
```

## Why these are separate commands

The split is not cosmetic — each one is separated from `ci` for a specific
reason, and the reasons are worth knowing before adding a seventh.

**`ci` is deliberately the *unit* gate.** No Docker daemon, so it stays fast
enough to run on every save, and it is what CI runs on every push. `cargo
build` succeeding is not evidence of anything;
[`integration`](../compat/verification.md) is what decides whether a
milestone is done.

**`integration` is the slow half.** Every test boots a real broker in a
container, so it is minutes rather than seconds. Manual in CI.

**`fuzz` needs nightly.** `cargo-fuzz` does not work on stable, and pinning
the whole workspace to nightly for one target would drag every other crate
along with it. It gets its own toolchain invocation and its own CI job.

**`interop` needs cmake.** `rdkafka` builds librdkafka from C source. That is
a fine requirement for the cross-client job and a terrible one for `ci` — CI
on a minimal runner image has already been red once over exactly this — so
the `interop` crate lives outside the workspace entirely.

**`docs` needs three binaries on `PATH`**: `mdbook`, `mdbook-mermaid` and
`mdbook-linkcheck`. The `docs` job in `.github/workflows/ci.yml` pins all
three. `mdbook build` runs the linkcheck backend too, so a broken
cross-reference fails the build rather than shipping as a 404.

## The version pins

mdbook stays on the **0.4.x** line: mdbook-linkcheck 0.7.7 and mdbook-mermaid
≥ 0.17.0 target different mdbook major lines — 0.17.0 is built against
mdbook 0.5's preprocessor protocol and fails against 0.4 — so 0.16.2 is the
newest mermaid preprocessor that works here.

**Bump all three together** when moving to mdbook 0.5, and keep `ci.yml` and
`docs-publish.yml` in lockstep.

## Adding a task

`main.rs` is a `match` on `env::args().nth(1)` with a `bail!` default that
lists the known tasks. Keep that list current — it is the only help text
there is.
