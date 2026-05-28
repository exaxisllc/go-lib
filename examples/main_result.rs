// SPDX-License-Identifier: Apache-2.0
//! main_result — return `Result<(), E>` from `main` via `go_lib::run`.
//!
//! Pattern: the closure returns `Result`; `main` returns the same `Result`.
//! The `?` operator works naturally inside the closure, and Rust's built-in
//! `Termination` trait prints the error and sets exit code 1 on `Err`.
//!
//! ```
//! cargo run --example main_result
//! ```

use std::num::ParseIntError;

use go_lib::{chan::chan, go};

fn main() -> Result<(), ParseIntError> {
    // go_lib::run returns Result<(), ParseIntError> — main returns it directly.
    go_lib::run(|| -> Result<(), ParseIntError> {
        let inputs = ["3", "1", "4", "1", "5", "9"];
        let (tx, rx) = chan::<Result<i64, ParseIntError>>(inputs.len());

        // Parse each string concurrently.
        for s in inputs {
            let tx = tx.clone();
            go!(move || tx.send(s.parse::<i64>()));
        }

        // Collect results, propagating the first parse error with `?`.
        let mut sum = 0_i64;
        for _ in 0..inputs.len() {
            sum += rx.recv().unwrap()?;
        }
        println!("sum = {sum}");   // sum = 23
        Ok(())
    })
}
