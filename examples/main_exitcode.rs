// SPDX-License-Identifier: Apache-2.0
//! main_exitcode — return `ExitCode` from `main` via `go_lib::run`.
//!
//! Pattern: `go_lib::run` returns whatever its closure returns.  Here the
//! closure returns a `bool`; `main` converts that to `ExitCode::SUCCESS` or
//! `ExitCode::FAILURE` and the process exits with the appropriate status code.
//!
//! ```
//! cargo run --example main_exitcode
//! echo $?   # 1 — some tasks fail (by design in this demo)
//! ```

use std::process::ExitCode;

use go_lib::{chan::chan, go, sync::WaitGroup};
use std::sync::Arc;

fn main() -> ExitCode {
    // go_lib::run returns the closure's return value directly.
    let all_passed = go_lib::run(|| {
        const N: usize = 5;
        let (tx, rx) = chan::<(usize, bool)>(N);
        let wg = Arc::new(WaitGroup::new());

        // Spawn N tasks concurrently; each reports (id, success).
        for id in 0..N {
            let tx  = tx.clone();
            let wg2 = Arc::clone(&wg);
            wg.add(1);
            go!(move || {
                let ok = run_task(id);
                tx.send((id, ok));
                wg2.done();
            });
        }

        // Wait for all tasks, then read their results.
        wg.wait();
        let mut failures = 0_usize;
        for _ in 0..N {
            if let Some((id, ok)) = rx.recv() {
                println!("  task {id}: {}", if ok { "ok" } else { "FAIL" });
                if !ok { failures += 1; }
            }
        }
        println!("{}/{N} tasks passed", N - failures);
        failures == 0   // ← this bool is the return value of run()
    });

    // Map the boolean result to a process exit code.
    if all_passed { ExitCode::SUCCESS } else { ExitCode::FAILURE }
}

/// Placeholder workload: even-numbered tasks succeed, odd ones fail.
fn run_task(id: usize) -> bool {
    id % 2 == 0
}
