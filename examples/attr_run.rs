// SPDX-License-Identifier: Apache-2.0
//! attr_main — three ways to use `#[go_lib::run]`.
//!
//! The attribute macro rewrites the function body into
//! `go_lib::run(move || { … })`, eliminating the manual wrapping boilerplate.
//!
//! Run with:
//!   cargo run --example attr_main

use std::num::ParseIntError;
use std::process::ExitCode;

use go_lib::{chan::chan, go, sync::WaitGroup};
use std::sync::Arc;

// ── 1. Plain main() — no return value ───────────────────────────────────────

/// `#[go_lib::run]` wraps the body in `go_lib::run(move || { … })`.
/// The scheduler is initialised automatically; no manual `run` call needed.
#[allow(dead_code)]
fn plain() {
    #[go_lib::run]
    fn _inner() {
        let (tx, rx) = chan::<&str>(0);
        go!(move || tx.send("hello from #[go_lib::run]"));
        println!("{}", rx.recv().unwrap());
    }
    _inner();
}

// ── 2. main() -> ExitCode ────────────────────────────────────────────────────

#[allow(dead_code)]
fn with_exitcode() -> ExitCode {
    #[go_lib::run]
    fn _inner() -> ExitCode {
        const N: usize = 4;
        let (tx, rx) = chan::<bool>(N);
        let wg = Arc::new(WaitGroup::new());

        for id in 0..N {
            let (tx, wg2) = (tx.clone(), Arc::clone(&wg));
            wg.add(1);
            go!(move || {
                tx.send(id % 2 == 0); // even tasks succeed
                wg2.done();
            });
        }

        wg.wait();
        let failures = (0..N)
            .filter(|_| rx.recv() == Some(false))
            .count();
        println!("{}/{N} tasks passed", N - failures);
        if failures == 0 { ExitCode::SUCCESS } else { ExitCode::FAILURE }
    }
    _inner()
}

// ── 3. main() -> Result<(), E> ───────────────────────────────────────────────

#[allow(dead_code)]
fn with_result() -> Result<(), ParseIntError> {
    #[go_lib::run]
    fn _inner() -> Result<(), ParseIntError> {
        let inputs = ["1", "2", "3", "4"];
        let (tx, rx) = chan::<Result<i64, ParseIntError>>(inputs.len());

        for s in inputs {
            let tx = tx.clone();
            go!(move || tx.send(s.parse::<i64>()));
        }

        let mut sum = 0_i64;
        for _ in 0..inputs.len() {
            sum += rx.recv().unwrap()?; // ? propagates ParseIntError
        }
        println!("sum = {sum}"); // sum = 10
        Ok(())
    }
    _inner()
}

// ── driver ───────────────────────────────────────────────────────────────────

fn main() {
    println!("── plain:");
    plain();

    println!("── with ExitCode:");
    let code = with_exitcode();
    println!("   ExitCode = {code:?}");

    println!("── with Result:");
    match with_result() {
        Ok(()) => println!("   Ok(())"),
        Err(e) => println!("   Err({e})"),
    }
}
