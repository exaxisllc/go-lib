// SPDX-License-Identifier: Apache-2.0
//! Stress reproducer for the cross-Rt timer-wake use-after-free.
//!
//! Scenario under test: `run_impl` exits (Phase 2b drain frees GWAITING
//! goroutines, including sleepers) at the same moment the global timer
//! thread has popped those sleepers' expired entries off `TIMER_HEAP` but
//! has not yet entered its RCU read-side critical section.  The drain then
//! frees the `Box<G>`; the timer thread dereferences the freed pointer and
//! can spuriously `goready` whatever new goroutine reuses the allocation —
//! typically one parked on a channel, which then resumes with
//! `(*gp).param == null` and dies with a NULL dereference in `chanrecv`.
//!
//! Two thread pools:
//!  * sleeper pool — run_impls that spawn short sleepers and return right
//!    around their expiry, so the drain races the in-flight timer pop;
//!  * churn pool — run_impls doing unbuffered channel ping-pong, recycling
//!    freed G allocations into channel-parked goroutines.
//!
//! Run with: cargo run --release --example stress_drain_timer [seconds] [iters-per-worker]
//!
//! With the singleton scheduler, each `run_impl` invocation leaks only a
//! 16-byte `InvState` (the per-call Rt/M-thread leak is gone), so memory
//! stays flat even at tens of thousands of iterations per run.  The
//! per-worker iteration cap exists only to bound runtime on very fast
//! machines; the drain/wake races this canary guards reproduce within ~10
//! iterations on a buggy runtime.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

static ITERS_SLEEP: AtomicU64 = AtomicU64::new(0);
static ITERS_CHURN: AtomicU64 = AtomicU64::new(0);
static STOP: AtomicBool = AtomicBool::new(false);
static WORKERS_DONE: AtomicU64 = AtomicU64::new(0);

fn sleeper_rt(seed: u64, max_iters: u64) {
    let mut x = seed | 1;
    let mut iters = 0u64;
    while !STOP.load(Ordering::Relaxed) && iters < max_iters {
        iters += 1;
        // Cheap xorshift to vary the overlap between expiry and drain.
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let sleep_us = 200 + (x % 600); // 200–800 µs
        let wait_us = 100 + (x % 1200); // main returns 100–1300 µs in

        go_lib::run(move || {
            for _ in 0..8 {
                go_lib::go!(move || {
                    go_lib::sleep(Duration::from_micros(sleep_us));
                });
            }
            // Busy-wait so the main goroutine returns (and the drain runs)
            // right around the sleepers' expiry instant.
            let t0 = Instant::now();
            while t0.elapsed() < Duration::from_micros(wait_us) {
                std::hint::spin_loop();
            }
        });
        ITERS_SLEEP.fetch_add(1, Ordering::Relaxed);
    }
    WORKERS_DONE.fetch_add(1, Ordering::Relaxed);
}

fn churn_rt(max_iters: u64) {
    let mut iters = 0u64;
    while !STOP.load(Ordering::Relaxed) && iters < max_iters {
        iters += 1;
        go_lib::run(|| {
            const PAIRS: usize = 8;
            let (done_tx, done_rx) = go_lib::chan::chan::<()>(PAIRS);
            for _ in 0..PAIRS {
                let (tx, rx) = go_lib::chan::chan::<i32>(0);
                let dtx = done_tx.clone();
                go_lib::go!(move || {
                    // Parks in chanrecv with param == null until the sender
                    // arrives — the crash site if a stale wake lands here.
                    for _ in 0..4 {
                        let v = rx.recv();
                        assert!(v.is_some());
                    }
                    dtx.send(());
                });
                go_lib::go!(move || {
                    for i in 0..4 {
                        tx.send(i);
                    }
                });
            }
            for _ in 0..PAIRS {
                done_rx.recv();
            }
        });
        ITERS_CHURN.fetch_add(1, Ordering::Relaxed);
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
    for i in 0..4u64 {
        handles.push(std::thread::spawn(move || {
            sleeper_rt(0x9e3779b9 ^ (i + 1), max_iters)
        }));
    }
    for _ in 0..4 {
        handles.push(std::thread::spawn(move || churn_rt(max_iters)));
    }

    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(secs)
        && WORKERS_DONE.load(Ordering::Relaxed) < N_WORKERS
    {
        std::thread::sleep(Duration::from_secs(1));
        eprintln!(
            "[{:>3}s] sleeper iters: {}, churn iters: {}, workers done: {}",
            t0.elapsed().as_secs(),
            ITERS_SLEEP.load(Ordering::Relaxed),
            ITERS_CHURN.load(Ordering::Relaxed),
            WORKERS_DONE.load(Ordering::Relaxed),
        );
    }
    STOP.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    eprintln!("stress completed without crash");
}
