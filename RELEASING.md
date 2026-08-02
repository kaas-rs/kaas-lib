# Releasing

The five library crates — `kafka-conn`, `kafka-meta`, `kafka-admin`,
`kafka-read`, `kafka-produce` — publish to crates.io **in lockstep** at a
single version held
in `[workspace.package]`. `testkit`, `livetest`, `xtask` and `interop` are
`publish = false`.

`.github/workflows/release.yml` does the work.

> **Publishing is irreversible.** crates.io yanks but never deletes, and a
> version number can never be reused. A mistake is permanent, so the workflow
> is deliberately hard to trigger by accident.

## Before the first release

Three things that are only true once.

**0. The crates.io account needs a verified email address.** Not the token —
the *account* the token belongs to. Without one, every upload is rejected
with:

```
the remote server responded with an error (status 400 Bad Request):
A verified email address is required to publish crates to crates.io.
```

Verify it at <https://crates.io/settings/profile>. Worth doing before
anything else, because nothing local can detect it: the token authenticates
fine, `cargo publish --dry-run` never contacts the registry, and the failure
lands after the gates have spent minutes booting brokers — at the first
upload of the run. Harmless when it happens (the first crate never uploads,
so nothing is claimed and the version is still free), just slow to discover.

**1. The crate names must be available.** As of the last check
`kafka-conn`, `kafka-meta`, `kafka-admin`, `kafka-read` and
`kafka-produce` were all
unclaimed on crates.io. Confirm before releasing — publishing claims them
permanently, and they are generic names in a flat namespace. If that gives
you pause, rename first; it is far cheaper now than after.

**2. The first publish needs an API token.** crates.io cannot attach a
Trusted Publisher to a crate that does not exist yet — unlike PyPI, there is
no "pending publisher". So:

1. Create a token at <https://crates.io/settings/tokens> scoped to
   `publish-new` and `publish-update`.
2. Add it as the repository secret `CARGO_REGISTRY_TOKEN`.
3. Run the release (below).
4. **Then** configure Trusted Publishing for each of the five crates at
   `https://crates.io/crates/<name>/settings` — repository
   `kaas-rs/kaas-lib`, workflow `release.yml`, environment `crates-io`.
5. **Delete the `CARGO_REGISTRY_TOKEN` secret.** The workflow detects its
   absence and switches to OIDC automatically; no stored credential from
   then on.

## Recommended: protect the environment

The `publish` job targets the `crates-io` environment. Add a required
reviewer under **Settings → Environments → crates-io** and every publish
needs a human approval click. Given that publishing cannot be undone, this is
worth the five seconds.

The workflow is deliberately split into `gate` and `publish` so that click is
informed. `environment:` blocks a job before its *first* step, so a single
job carrying both the gates and the upload asks for approval before any
evidence exists — the reviewer clicks blind on the one irreversible action in
the repository. With the gates in their own unprotected job, the prompt only
appears once the tag, the lints, the packaging check and the full acceptance
suite are green.

## Cutting a release

```sh
# 1. Bump BOTH version lines in the root Cargo.toml. They are adjacent.
#      [workspace.package]  version = "0.2.1"
#      [workspace.dependencies]
#        kafka-conn = { path = "...", version = "0.2.1" }
#        kafka-meta = { path = "...", version = "0.2.1" }
$EDITOR Cargo.toml
cargo check                 # refresh Cargo.lock

# 2. Prove it locally.
cargo xtask ci
cargo xtask integration
cargo publish --dry-run --workspace

# 3. Land it.
git commit -am "chore(release): 0.2.1"
git push origin main

# 4. Tag. The tag push is what publishes.
git tag v0.2.1
git push origin v0.2.1
```

The tag must be `v` + the workspace version exactly. The workflow reads the
version cargo actually resolved, asserts all five crates agree on it, and
refuses to run if the tag disagrees.

### Why two version lines and not one

A crate's own version inherits from `[workspace.package]`, but the *dependency
requirement* one published crate states for another does not — that is a
separate string, and crates.io needs it to name a version that exists.

Leaving it stale is uniquely nasty: everything builds locally, because the
path dependency wins; `cargo xtask ci` is green; and the failure lands
partway through a five-crate publish, after some crates are already live and
irreversibly so, as a resolver error about a version nobody has uploaded.
Keeping both lines in the root manifest means one file and two adjacent
edits — `cargo publish --dry-run --workspace` in step 2 is what catches it if
you miss one.

### Dry run first

```
Actions → Release → Run workflow → dry_run ✓
```

Manual runs default to a dry run: it packages and verifies every crate,
compiling each against the *packaged* versions of its dependencies, and
uploads nothing. Only a tag push publishes for real.

## What the workflow gates on

The `gate` job, in order, all blocking:

1. **Tag matches the workspace version**, and all five crates agree on it.
2. **`actionlint`** — the release path is itself a workflow, and a workflow
   expression error fails in zero seconds with no jobs and no logs rather
   than loudly. This file carried one for its entire existence, so every
   push produced a red Release run and the publish path had never executed.
3. **`cargo xtask ci`** — fmt, clippy with `-D warnings`, unit tests.
4. **`cargo publish --workspace --dry-run`** — packaging exactly as crates.io
   will see it. The only gate that catches a stale inter-crate version
   requirement before the upload does, because a path dependency wins every
   local build.
5. **`cargo xtask integration`** — the full acceptance suite against a real
   `apache/kafka:4.3.1` broker. Minutes rather than seconds, which is the
   right trade for an irreversible upload. A green `cargo build` is not
   evidence that a release works.

Only then does `publish` start and pause for the environment's reviewer. It
authenticates and runs `cargo publish --workspace`, which resolves the order
itself (`kafka-conn` → `kafka-meta` →
`kafka-admin`/`kafka-read`/`kafka-produce`) and waits
for each crate to reach the index before publishing its dependents.

## If something goes wrong

**A crate published but a later one failed.** The successful ones are live
and cannot be un-published. Fix the cause, bump to the next patch version,
and release again — `cargo publish` refuses to overwrite an existing
version, so a re-run at the same version will not repair it.

**A bad version is live.** `cargo yank --version X.Y.Z <crate>` stops new
dependents resolving to it. It does **not** delete it, and existing
`Cargo.lock` files keep working. Yank all five together or you strand
consumers on a broken combination.

**Secrets to check** if authentication fails: either `CARGO_REGISTRY_TOKEN`
exists and is valid, or Trusted Publishing is configured for *every* crate
being published. A partial Trusted Publishing setup fails partway through,
which is the worst case — see above.

## Versioning

Pre-1.0, so breaking changes go in the minor position: `0.1.x` → `0.2.0`.

Two things force a version bump that are easy to miss:

- **A `kafka-protocol` upgrade.** Its types are `#[non_exhaustive]` and
  regenerate each Kafka release. The
  [domain boundary](https://kaas-rs.github.io/kaas-lib/architecture/domain-boundary.html)
  is what keeps that from being automatically breaking for our consumers —
  but check it actually held before calling an upgrade non-breaking.
- **A new error-code or api-key variant.** Our enums are `#[non_exhaustive]`
  too, so adding variants is not breaking for matchers with a wildcard arm.
  Changing what an existing variant *means* is.
