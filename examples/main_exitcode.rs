// SPDX-License-Identifier: Apache-2.0
//! main_exitcode — return `ExitCode` from a `#[go_lib::main]` entry point.
//!
//! Pattern: the attribute forwards the entry function's return type to the
//! first goroutine.  Here the body returns `ExitCode::SUCCESS` or
//! `ExitCode::FAILURE` and the process exits with the appropriate status code.
//!
//! `go_lib::scope` runs the N tasks concurrently and collects their results
//! without any `Arc`, `WaitGroup`, or channel — each goroutine borrows `id`
//! by value and returns its `(id, bool)` result through the join handle.
//!
//! ```sh
//! cargo run --example main_exitcode
//! echo $?   # 1 — some tasks fail (by design in this demo)
//! ```

use std::process::ExitCode;

#[go_lib::main]
fn main() -> ExitCode {
    const N: usize = 5;

    // Spawn N tasks concurrently; collect (id, ok) from each join handle.
    let results = go_lib::scope(|s| {
        let handles: Vec<_> = (0..N)
            .map(|id| s.go(move || (id, run_task(id))))
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("task goroutine panicked"))
            .collect::<Vec<_>>()
    });

    let mut failures = 0_usize;
    for (id, ok) in results {
        println!("  task {id}: {}", if ok { "ok" } else { "FAIL" });
        if !ok {
            failures += 1;
        }
    }
    println!("{}/{N} tasks passed", N - failures);

    // Map the result to a process exit code.
    if failures == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Placeholder workload: even-numbered tasks succeed, odd ones fail.
fn run_task(id: usize) -> bool {
    id.is_multiple_of(2)
}
