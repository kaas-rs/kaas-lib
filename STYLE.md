# STYLE.md — consuming `with_*` builders

How optional configuration is expressed in this codebase, and a convention
portable to any Rust project. It is written to be copied: nothing below is
specific to Kafka or to this workspace.

`CLAUDE.md` rule 7 is the enforcement point here. This file is the reasoning
behind it, the edge cases, and the parts a reviewer will argue about.

## The style in one sentence

Every optional setting is a `#[must_use] pub fn with_x(mut self, …) -> Self`
on the domain type itself; required data goes in `new()`. There is no builder
struct and no terminal `build()`.

```rust
let record = ProducerRecord::new("orders")     // required data
    .with_key("customer-7")                    // everything else optional
    .with_value("{\"total\":42}")
    .with_header("content-type", "application/json");
```

In ecosystem terms this is a *consuming* (or *by-value*) *builder*, and
`with_`-prefixed setters are sometimes called *withers*. What distinguishes
this variant from the classic Builder pattern is that the domain type is its
own builder: `ProducerRecord::new("orders")` is already a `ProducerRecord`,
not a `ProducerRecordBuilder` awaiting a `.build()`.

## The rule

- **Required data goes in `new()`. Everything optional is a `with_*` method.**
  If a value has no sensible default, it is an argument to `new()`, not a
  setter that might be forgotten.
- **The signature is always `#[must_use] pub fn with_x(mut self, …) -> Self`.**
  It consumes and returns `Self`, so chains never need a `let mut`.
- **The type is its own builder.** No `FooBuilder`, no `build()`. Every
  intermediate value is a valid `Foo`.
- **The prefix is `with_`, without exception** — including boolean toggles:
  `with_read_only()`, not `read_only()`.
- **Never `&mut self` setters. Never public mutable fields** for anything a
  caller is expected to set.
- **Setters assign; last call wins.** Anything additive says so in its name
  and its documentation.
- **Take `impl Into<T>` / `impl IntoIterator<Item = T>`** so callers do not
  convert at the call site.

## Why these three properties are load-bearing

**Validity is structural.** Because `new()` takes the required data and every
`with_*` returns a complete value, an invalid instance cannot be expressed.
That is what removes `build() -> Result<T>` — there is no deferred failure to
report, so there is no error path, so callers do not handle one.

**`#[must_use]` is not decoration.** The single failure mode of a consuming
builder is this:

```rust
let mut record = ProducerRecord::new("t");
record.with_key("k");   // compiles, does nothing, silently loses the key
```

`#[must_use]` turns that into a warning, and with `-D warnings` into a build
failure. Apply it to *every* builder or the style has a hole. This workspace
has 58 consuming builders and 58 `#[must_use]` attributes; that ratio is the
invariant, and it is worth grepping for in review.

**Total prefix discipline beats locally-better names.** `with_read_only()`
reads slightly worse than `read_only()`. The trade is that a caller never has
to know which module a type came from to guess what its setters are called.
One convention that is occasionally clumsy beats two that each read well in
isolation.

## Naming: the cases that come up

### Booleans

Prefer a named method over a `bool` parameter where the flag has one
interesting value:

```rust
.with_read_only()                 // good: reads as prose at the call site
.with_read_only(true)             // avoid: `true` means nothing to a reader
```

Use `with_x(bool)` when both values are genuinely expressive and a caller may
be relaying one (`with_auto_commit(enabled)`).

### Relaying a choice rather than making one: `with_maybe_*`

A setter taking `T` is right for code that has *decided*. Code passing a value
*through* — a CLI flag, a config field, a request parameter — holds
`Option<T>`, and without help it has to leave the chain:

```rust
let mut record = ProducerRecord::new("t").with_value(payload);
if let Some(partition) = configured {
    record = record.with_partition(partition);   // the `let mut` is the tell
}
```

Add an `Option`-taking sibling so relaying reads like deciding:

```rust
let record = ProducerRecord::new("t")
    .with_value(payload)
    .with_maybe_partition(configured);
```

Two rules for these. **Assign, do not merge**: `with_maybe_x(None)` clears a
value set earlier in the chain, exactly as a second `with_x` overwrites the
first. Ignoring `None` would make the last call in a chain conditional on the
ones before it, which is a rule nobody remembers at the call site. And **add
rather than widen** once published: changing `with_x(T)` to
`with_x(impl Into<Option<T>>)` needs no new surface and still infers for
`with_x(5)`, but it changes the signature of a live method, which is a semver
decision rather than a convenience fix.

### Additive setters

Most setters replace. Where one appends, the name must say so and the
documentation must confirm it:

```rust
.with_header("trace", "a")        // appends; two calls give two headers
.with_headers([…])                // would replace; plural implies the whole set
```

### Collections

Take `impl IntoIterator<Item = T>` and replace wholesale. If both replace and
append are genuinely needed, name them `with_xs` and `with_x`.

## Where this is the wrong tool

Reach for something else when:

- **Many interdependent required fields need validating together.** Use a real
  builder with `build() -> Result<T>`. This style has nowhere to put the check,
  and faking it with a panicking constructor is worse than the pattern it
  replaces.
- **The value must be mutated after construction.** A consuming builder gives
  no way to adjust something already handed out. Add explicit domain methods
  for that; do not bolt `&mut` setters onto the same surface, or callers will
  not know which half to use.
- **The type is large and moved in a hot loop.** The moves usually vanish under
  optimisation — measure before assuming they do not.

## A note for reviewers porting this elsewhere

**Bare-name builders are the more common Rust idiom.** `ClientBuilder::timeout()`,
`runtime::Builder::worker_threads()`, `Command::arg()` — the standard library
and most of the ecosystem omit the prefix. Adopting `with_` universally trades
that familiarity for total predictability, and it is a defensible trade, but
present it as a choice. Someone will ask, and "it is what everyone does" is
not the answer, because it is not.

**Decide before 1.0.** The prefix rule costs nothing on day one and is
expensive afterwards: applying it to a single type here meant renaming seven
published methods and roughly forty call sites, and it broke every downstream
caller. Renaming is also the one part of this that cannot be done gradually
without breaking people twice.

**Rename from the compiler, not from a text search.** `.key(`, `.value(`,
`.timestamp(` and `.partition(` are all live method or field names on other
types in a typical workspace — including third-party ones sitting in the same
file. Rename the definitions first and let `rustc`'s E0599 spans drive every
call site; a `sed` over the tree will silently maul unrelated code.

## Checklist

- [ ] Required data is in `new()`, not a setter
- [ ] Every setter is `#[must_use] pub fn with_x(mut self, …) -> Self`
- [ ] Every setter starts with `with_`, booleans included
- [ ] No `&mut self` setters, no public settable fields
- [ ] Additive setters are named and documented as additive
- [ ] Arguments are `impl Into<T>` / `impl IntoIterator<Item = T>`
- [ ] Settings a caller might relay have a `with_maybe_*` sibling
