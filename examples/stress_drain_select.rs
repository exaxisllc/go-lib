// SPDX-License-Identifier: Apache-2.0
//! Stress test for select + unbuffered-channel handoff churn under
//! short-lived scheduler invocations.
//!
//! Historically this was the canary for a "select waiter drained mid-handoff"
//! hazard: a channel send/recv writing or reading a **select** waiter's
//! `sudog.elem` — which points into that goroutine's `selectgo` stack frame —
//! at the same instant an exit drain munmapped that stack.  The exit drain (and
//! the `DrainSync`/`RcuGuard` apparatus that guarded it) has since been
//! deleted: goroutines are now process-global and never force-reclaimed, so
//! that stack stays mapped for the process lifetime and the hazard cannot
//! arise.  The example is kept as a high-churn select/channel stress test.
//!
//! Scenario per `run_impl` invocation:
//!  * a set of UNBUFFERED channels (every transfer is a direct handoff that
//!    touches a parked peer's stack);
//!  * "selector" goroutines blocked in `select!` recv across several of those
//!    channels (so their elem points into their own stack frame);
//!  * "sender" goroutines hammering those channels (each match writes into a
//!    selector's stack);
//!  * the main goroutine returns at a randomised sub-millisecond instant, so it
//!    exits while selectors are parked and senders are mid-handoff.
//!
//! Run with (uncapped, 15 s):
//!   cargo run --release --example stress_drain_select 15 100000000
//!
//! A correct runtime prints "stress completed without crash".
//!
//! Each worker OS thread drives its own bootstrap, so this harness calls the
//! internal `go_lib::__main_entry` (what `#[go_lib::main]` expands to) directly
//! rather than the entry attribute, which only fits a single `main`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use go_lib::{chan::chan, go, select};

static ITERS: AtomicU64 = AtomicU64::new(0);
static STOP: AtomicBool = AtomicBool::new(false);
static WORKERS_DONE: AtomicU64 = AtomicU64::new(0);

/// One worker thread: repeatedly run a short-lived scheduler invocation that
/// parks selectors and exits while handoffs are in flight.
fn select_rt(seed: u64, max_iters: u64) {
    let mut x = seed | 1;
    let mut iters = 0u64;
    while !STOP.load(Ordering::Relaxed) && iters < max_iters {
        iters += 1;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        // Randomise how long main runs before returning:
        // 50–550 µs keeps the return point inside the window where selectors
        // are parking and senders are handing off.
        let wait_us = 50 + (x % 500);

        go_lib::__main_entry(move || {
            // Three unbuffered channels; selectors multiplex across them.
            let (a_tx, a_rx) = chan::<u64>(0);
            let (b_tx, b_rx) = chan::<u64>(0);
            let (c_tx, c_rx) = chan::<u64>(0);

            // Selectors: each blocks in select! recv across all three
            // channels, looping so many are parked at drain time.
            for _ in 0..6 {
                let a_rx = a_rx.clone();
                let b_rx = b_rx.clone();
                let c_rx = c_rx.clone();
                go!(move || {
                    for _ in 0..64 {
                        let got = select! {
                            recv(a_rx) -> v => { v }
                            recv(b_rx) -> v => { v }
                            recv(c_rx) -> v => { v }
                        };
                        if got.is_none() {
                            break; // a channel closed
                        }
                    }
                });
            }

            // Senders: hammer each channel so handoffs into selector stacks are
            // in flight when main returns.
            for (k, tx) in [a_tx, b_tx, c_tx].into_iter().enumerate() {
                go!(move || {
                    for i in 0..64u64 {
                        tx.send((k as u64) << 32 | i);
                    }
                });
            }

            // Return around the handoff storm so main exits mid-handoff.
            let t0 = Instant::now();
            while t0.elapsed() < Duration::from_micros(wait_us) {
                std::hint::spin_loop();
            }
        });
        ITERS.fetch_add(1, Ordering::Relaxed);
    }
    WORKERS_DONE.fetch_add(1, Ordering::Relaxed);
}

fn main() {
    let secs: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let max_iters: u64 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);

    const N_WORKERS: u64 = 8;
    let mut handles = Vec::new();
    for i in 0..N_WORKERS {
        handles.push(std::thread::spawn(move || {
            select_rt(0x9e3779b9 ^ (i + 1), max_iters)
        }));
    }

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(secs) {
        std::thread::sleep(Duration::from_secs(1));
        let elapsed = start.elapsed().as_secs();
        let done = WORKERS_DONE.load(Ordering::Relaxed);
        println!("[{elapsed:>3}s] iters: {}, workers done: {done}",
                 ITERS.load(Ordering::Relaxed));
        if done == N_WORKERS {
            break;
        }
    }
    STOP.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    println!("stress completed without crash");
}
