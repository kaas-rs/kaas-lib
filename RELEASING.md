# Releasing

The four library crates — `kafka-conn`, `kafka-meta`, `kafka-admin`,
`kafka-read` — publish to crates.io **in lockstep** at a single version held
in `[workspace.package]`. `testkit`, `livetest`, `xtask` and `interop` are
`publish = false`.

`.github/workflows/release.yml` does the work.

> **Publishing is irreversible.** crates.io yanks but never deletes, and a
> version number can never be reused. A mistake is permanent, so the workflow
> is deliberately hard to trigger by accident.

## Before the first release

Two things that are only true once.

**1. The crate names must be available.** As of the last check
`kafka-conn`, `kafka-meta`, `kafka-admin` and `kafka-read` were all
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
4. **Then** configure Trusted Publishing for each of the four crates at
   `https://crates.io/crates/<name>/settings` — repository
   `kaas-rs/kaas-lib`, workflow `release.yml`, environment `crates-io`.
5. **Delete the `CARGO_REGISTRY_TOKEN` secret.** The workflow detects its
   absence and switches to OIDC automatically; no stored credential from
   then on.

## Recommended: protect the environment

The job targets the `crates-io` environment. Add a required reviewer under
**Settings → Environments → crates-io** and every publish needs a human
approval click. Given that publishing cannot be undone, this is worth the
five seconds.

## Cutting a release

```sh
# 1. Bump BOTH version lines in the root Cargo.toml. They are adjacent.
#      [workspace.package]  version = "0.2.0"
#      [workspace.dependencies]
#        kafka-conn = { path = "...", version = "0.2.0" }
#        kafka-meta = { path = "...", version = "0.2.0" }
$EDITOR Cargo.toml
cargo check                 # refresh Cargo.lock

# 2. Prove it locally.
cargo xtask ci
cargo xtask integration
cargo publish --dry-run --workspace

# 3. Land it.
git commit -am "chore(release): 0.2.0"
git push origin main

# 4. Tag. The tag push is what publishes.
git tag v0.2.0
git push origin v0.2.0
```

The tag must be `v` + the workspace version exactly. The workflow reads the
version cargo actually resolved, asserts all four crates agree on it, and
refuses to run if the tag disagrees.

### Why two version lines and not one

A crate's own version inherits from `[workspace.package]`, but the *dependency
requirement* one published crate states for another does not — that is a
separate string, and crates.io needs it to name a version that exists.

Leaving it stale is uniquely nasty: everything builds locally, because the
path dependency wins; `cargo xtask ci` is green; and the failure lands
partway through a four-crate publish, after some crates are already live and
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

In order, all blocking:

1. **Tag matches the workspace version**, and all four crates agree on it.
2. **`cargo xtask ci`** — fmt, clippy with `-D warnings`, unit tests.
3. **`cargo xtask integration`** — the full acceptance suite against a real
   `apache/kafka:4.3.1` broker. Minutes rather than seconds, which is the
   right trade for an irreversible upload. A green `cargo build` is not
   evidence that a release works.

Then it authenticates and runs `cargo publish --workspace`, which resolves
the order itself (`kafka-conn` → `kafka-meta` → `kafka-admin`/`kafka-read`)
and waits for each crate to reach the index before publishing its dependents.

## If something goes wrong

**A crate published but a later one failed.** The successful ones are live
and cannot be un-published. Fix the cause, bump to the next patch version,
and release again — `cargo publish` refuses to overwrite an existing
version, so a re-run at the same version will not repair it.

**A bad version is live.** `cargo yank --version X.Y.Z <crate>` stops new
dependents resolving to it. It does **not** delete it, and existing
`Cargo.lock` files keep working. Yank all four together or you strand
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
