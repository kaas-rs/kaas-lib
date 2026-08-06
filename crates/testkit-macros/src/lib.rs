//! The attribute every integration test in this workspace wears.
//!
//! [`integration_test`] replaces the `#[tokio::test]` + `#[ignore]` pair and
//! adds the one thing the pair cannot: a **hard two-minute deadline** on the
//! whole test, container boot included. The integration job is
//! `cargo test -- --ignored`, so `#[ignore]` is the door into it — putting the
//! deadline inside the attribute that writes `#[ignore]` is what makes "every
//! test in the job is bounded" a property of the build rather than a
//! convention. `cargo xtask` refuses hand-written `#[ignore]` in workspace
//! test sources for the same reason.
//!
//! Two minutes is a per-test budget, not a target: a fixture boots in tens of
//! seconds and the poll loops inside the tests keep their own, shorter
//! deadlines so that when something hangs, the failure names the condition
//! that was being waited for. This deadline is the backstop that turns "the
//! suite wedged in CI until the runner killed it" into a single red test.

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input};

/// Marks an async fn as an integration test with a two-minute deadline.
///
/// Expands to `#[tokio::test]` + `#[ignore = "needs Docker"]`, with the body
/// wrapped in `tokio::time::timeout(120s)`. On expiry the test panics and
/// fails; the body future is dropped, which rule 5 (every public async fn is
/// cancel-safe) makes a clean shutdown rather than a leak.
///
/// Takes no arguments — the deadline is deliberately not configurable, so no
/// test can quietly opt back out of the budget.
#[proc_macro_attribute]
pub fn integration_test(args: TokenStream, item: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[testkit::integration_test] takes no arguments; \
             the two-minute deadline is not configurable",
        )
        .to_compile_error()
        .into();
    }

    let function = parse_macro_input!(item as ItemFn);
    if function.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            function.sig.fn_token,
            "#[testkit::integration_test] requires an async fn",
        )
        .to_compile_error()
        .into();
    }

    let attrs = &function.attrs;
    let vis = &function.vis;
    let sig = &function.sig;
    let block = &function.block;

    quote! {
        #(#attrs)*
        #[tokio::test]
        #[ignore = "needs Docker"]
        #[allow(clippy::panic)]
        #vis #sig {
            let deadline = ::core::time::Duration::from_secs(120);
            match ::tokio::time::timeout(deadline, async move #block).await {
                ::core::result::Result::Ok(output) => output,
                ::core::result::Result::Err(_) => ::core::panic!(
                    "integration test exceeded its two-minute deadline"
                ),
            }
        }
    }
    .into()
}
