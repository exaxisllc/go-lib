// SPDX-License-Identifier: Apache-2.0
//! stress_stopm_strand — decisive repro for the issue-#5 lost wakeup.
//!
//! One round: the main goroutine spawns a detached "hog" and a scoped
//! "target", then blocks waiting for the target's result.  The hog wakes the
//! target (channel send → `goready` → `runqput` onto the hog's local P +
//! `startm`) and then spins on the CPU *forever*, never returning to the
//! scheduler.  If that `startm` dropped the wake (no idle M yet) and an idle M
//! parks in `stopm` after re-checking only the global queue, the target is
//! stranded on the hog's local run queue with the hog monopolising that P and
//! every other M asleep — a permanent deadlock (issue #5).  A scheduler whose
//! `stopm` re-checks every P's local queue steals the target and the round
//! finishes immediately.
//!
//! Each `run()` is one independent sample of the race window.  The window is
//! narrow, so drive it in a loop under a timeout; on a fixed scheduler every
//! sample completes promptly, on the buggy one some deadlock outright:
//!
//!   for i in $(seq 1 200); do
//!     GOMAXPROCS=2 GOLIB_ASYNCPREEMPT_OFF=1 \
//!       perl -e 'alarm 6; exec @ARGV' \
//!       ./target/release/examples/stress_stopm_strand || echo "HANG #$i";
//!   done

use go_lib::{chan::chan, go};

#[go_lib::main]
fn main() {
    let (start_tx, start_rx) = chan::<u64>(0);
    let (result_tx, result_rx) = chan::<u64>(0);

    // Detached hog: wait for the go-ahead, wake the target, then spin
    // forever holding its P (never cycles back to the scheduler).
    go!(move || {
        let _ = start_rx.recv();
        result_tx.send(42);          // <-- the wake that can be lost
        let mut x = 0u64;
        loop {
            x = x.wrapping_mul(2862933555777941757).wrapping_add(1);
            std::hint::black_box(x);
        }
    });

    // Hand the round to the hog, then block on the target's result.  If the
    // wake is stranded, this recv never returns and the entry hangs forever.
    let r: u64 = go_lib::scope(|s| {
        let h = s.go(move || {
            start_tx.send(0);        // release the hog
            result_rx.recv().unwrap_or(0)
        });
        h.join().unwrap_or(0)
    });

    println!("strand ok: result={r}");
}
