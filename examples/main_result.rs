// SPDX-License-Identifier: Apache-2.0
//! main_result — return `Result<(), E>` from a `#[go_lib::main]` entry point.
//!
//! Pattern: the entry function returns `Result`; the attribute forwards that
//! return type to the first goroutine.  The `?` operator works naturally in
//! the body, and Rust's built-in `Termination` trait prints the error and
//! sets exit code 1 on `Err`.
//!
//! `go_lib::scope` parses every string concurrently.  Each goroutine borrows its
//! `&str` directly from the `inputs` slice — no channel or `Arc` required.
//! `h.join().unwrap()?` unwraps the panic wrapper and then propagates any
//! `ParseIntError` with `?`.
//!
//! ```sh
//! cargo run --example main_result
//! ```

use std::num::ParseIntError;

#[go_lib::main]
fn main() -> Result<(), ParseIntError> {
    let inputs = ["3", "1", "4", "1", "5", "9"];

    // Parse each string concurrently; goroutines borrow `inputs` directly.
    let sum: i64 = go_lib::scope(|scope| -> Result<i64, ParseIntError> {
        let handles: Vec<_> = inputs
            .iter()
            .map(|s| scope.go(move || s.parse::<i64>()))
            .collect();

        // Fold results: h.join().unwrap() strips the panic wrapper;
        // `?` propagates the first ParseIntError out of scope's return value.
        handles
            .into_iter()
            .try_fold(0_i64, |acc, h| Ok(acc + h.join().unwrap()?))
    })?;

    println!("sum = {sum}"); // sum = 23
    Ok(())
}
