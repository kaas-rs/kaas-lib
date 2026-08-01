# docs/

The kaas-lib documentation book. Published at
**<https://kaas-rs.github.io/kaas-lib/>**, rebuilt from `main` on every push
by `.github/workflows/docs-publish.yml`.

## Layout

| Path | What it is |
|---|---|
| `src/` | the chapters — Parts I–IV, plus `SUMMARY.md` (the table of contents) |
| `book.toml` | mdbook config: `rust` theme, search, fold nav, mermaid + linkcheck backends |
| `mermaid.min.js`, `mermaid-init.js` | committed by `mdbook-mermaid install`; required by the build |
| `book/` | build output, gitignored |

## Building

```sh
cargo xtask docs           # mdbook build (html + linkcheck)
cargo xtask docs --serve   # live-reloading local preview
```

Needs `mdbook`, `mdbook-mermaid` and `mdbook-linkcheck` on `PATH`. CI pins
them in the `docs` job of `.github/workflows/ci.yml`: **mdbook 0.4.52,
mdbook-mermaid 0.16.2, mdbook-linkcheck 0.7.7**.

Keep to the 0.4.x line — mdbook-mermaid ≥ 0.17 targets mdbook 0.5's
preprocessor protocol and fails against 0.4. **Bump all three together**, and
keep `ci.yml` and `docs-publish.yml` in lockstep.

Enabling the linkcheck renderer turns off mdbook's html default, which is why
the site lands in `book/html/` rather than `book/` — the publish workflow
uploads that path.

## The drift gates

Two checks run in the `docs` CI job:

1. **Link check.** `mdbook-linkcheck` runs as a build backend, so a broken
   intra-book link fails `mdbook build` rather than shipping as a 404.
   `follow-web-links = false`, so external URLs are *not* validated — be
   careful adding them, since nothing will catch a wrong one.
2. **Source-path scan.** Every `crates/…*.rs` path cited anywhere in `src/`
   must exist in the tree, so a refactor that moves a file fails CI instead
   of leaving a citation that is confidently wrong.

**Not yet gated: the [API matrix](src/compat/api-matrix.md).** Its rows are
generated from `crates/kafka-conn/src/api_key.rs` (wire codes, `is_mutating`)
and `crates/kafka-meta/src/routing.rs` (routing class), but the check is
manual today. Regenerate the table when either file changes; adding a real
`gen-api-matrix` xtask is worth doing the first time it goes stale.

## Writing conventions

- **Write for a reader who knows Kafka but not kaas-lib.** Do not explain
  what a consumer group is; do explain that there are four kinds and they
  need different RPCs. Locate things in the Kafka mental model, then say what
  this library does differently.
- **Lead with the failure mode.** Nearly every design decision here exists
  because of a specific way of getting it wrong — a silent reorder, a
  topic created by a typo, corruption reported at the end of every fetch.
  Name it. That is the part a reader cannot reconstruct from the source.
- **Tie back to the through-line.** The [introduction](src/introduction.md)
  states three invariants and a constraint; most pages are an instance of
  one of them. Say which.
- **Code is the source of truth.** Where a doc and the source disagree, the
  source wins and the doc gets fixed — including when that means documenting
  a gap. Part II's blocked-upstream entries lead with what is *missing*;
  do not soften them.
- **Cite real paths**, specific enough to be useful — module, not just
  crate. The scan enforces existence.
- **Cross-link instead of duplicating.** Deep architecture lives in Part I,
  compatibility claims in Part II; Part III crate chapters stay short and
  point at both.
- **Rust samples are `rust,no_run`** and are not compiled by `mdbook build`.
  Verify signatures against the source before adding one. The playground Run
  button is disabled globally in `book.toml`, since no sample can resolve
  workspace crates.

## Structure

| Part | Audience |
|---|---|
| Introduction, Getting started | anyone |
| I — Architecture | contributors, and users debugging something odd |
| II — Kafka compatibility | anyone evaluating whether this works on their cluster |
| III — Code tour | contributors |
| IV — Using the library | users |
