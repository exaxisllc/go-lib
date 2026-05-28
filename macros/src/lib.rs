// SPDX-License-Identifier: Apache-2.0
//! Procedural macros for go-lib.
//!
//! Exported through `go_lib` — use as `#[go_lib::run]`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, ReturnType};

/// Attribute macro that wraps a function body in [`go_lib::run`].
///
/// The macro rewrites
///
/// ```rust,ignore
/// #[go_lib::run]
/// fn main() {
///     /* body */
/// }
/// ```
///
/// into
///
/// ```rust,ignore
/// fn main() {
///     go_lib::run(move || {
///         /* body */
///     })
/// }
/// ```
///
/// When the function has a return type the closure is annotated with the same
/// type so that `?` and explicit `return` expressions work as expected:
///
/// ```rust,ignore
/// #[go_lib::run]
/// fn main() -> Result<(), MyError> {
///     do_work()?;
///     Ok(())
/// }
/// // expands to:
/// fn main() -> Result<(), MyError> {
///     go_lib::run(move || -> Result<(), MyError> {
///         do_work()?;
///         Ok(())
///     })
/// }
/// ```
///
/// Function parameters (if any) are captured by the `move` closure, so the
/// macro also works on non-`main` entry points or helper functions.
///
/// # Errors
///
/// Emits a compile error if the function is `async` (go-lib provides its own
/// concurrency model and does not interact with an async executor).
#[proc_macro_attribute]
pub fn run(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut func = parse_macro_input!(item as ItemFn);

    // Reject async functions — go-lib's scheduler is not an async executor.
    if let Some(async_token) = &func.sig.asyncness {
        return syn::Error::new_spanned(
            async_token,
            "#[go_lib::run] does not support async functions; \
             go-lib provides its own M:N scheduler",
        )
        .to_compile_error()
        .into();
    }

    let return_type = func.sig.output.clone();
    let body        = &*func.block;

    // Build `go_lib::run(move || [-> ReturnType] { body })`.
    // The `move` ensures function parameters are captured into the closure.
    let run_call = match &return_type {
        ReturnType::Default => quote! {
            go_lib::run(move || #body)
        },
        ReturnType::Type(_, ty) => quote! {
            go_lib::run(move || -> #ty #body)
        },
    };

    // Replace the function body with the single `run` call.
    func.block = syn::parse_quote! { { #run_call } };

    quote! { #func }.into()
}
