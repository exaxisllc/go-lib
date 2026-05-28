// SPDX-License-Identifier: Apache-2.0
//! main_exitcode — return `ExitCode` from `main` via `go_lib::run`.
//!
//! Pattern: `go_lib::run` returns whatever its closure returns.  Here the
//! closure returns a `bool`; `main` converts that to `ExitCode::SUCCESS` or
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

fn main() -> ExitCode {
    // go_lib::run returns the closure's return value directly.
    let all_passed = go_lib::run(|| {
        const N: usize = 5;

        // Spawn N tasks concurrently; collect (id, ok) from each join handle.
        let results = go_lib::scope(|s| {
            let handles: Vec<_> = (0..N)
                .map(|id| s.spawn(move || (id, run_task(id))))
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
        failures == 0 // ← this bool is the return value of run()
    });

    // Map the boolean result to a process exit code.
    if all_passed {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Placeholder workload: even-numbered tasks succeed, odd ones fail.
fn run_task(id: usize) -> bool {
    id.is_multiple_of(2)
}
