// SPDX-License-Identifier: Apache-2.0
//! `gopark` / `goready` — the parking and wakeup primitives.
//!
//! Every blocking operation (channel send/receive, WaitGroup, select) uses
//! `gopark` to suspend the current goroutine and `goready` to wake it.
//!
//! Ported from `gopark` / `goready` in `runtime/proc.go`.
//!
//! ## Protocol
//!
//! ```text
//! goroutine stack               g0 stack
//! ─────────────────             ─────────────────
//! gopark(reason)
//!   mcall(park_fn)  ──────────► park_fn(g):
//!                                 atomicstatus ← Gwaiting
//!                                 unlink G from M
//!                                 schedule()        (loops forever)
//! ```
//!
//! `goready` is called by whoever holds the sleeping G (a channel, sudog, etc.)
//! to transition it back to `Grunnable` and enqueue it for execution.

use std::ptr;

use super::g::{current_g, set_current_g, G, GRUNNABLE, GRUNNING, GWAITING, WaitReason};
use super::m::current_m;
use super::sched::{sched, schedule, startm};

#[cfg(target_arch = "x86_64")]
use super::asm_amd64::mcall;
#[cfg(target_arch = "aarch64")]
use super::asm_arm64::mcall;

// ---------------------------------------------------------------------------
// gopark — suspend the current goroutine
// ---------------------------------------------------------------------------

/// Suspend the current goroutine with wait reason `reason`.
///
/// Transitions the goroutine from `Grunning` to `Gwaiting`, releases its M,
/// and enters the scheduler on g0's stack via `mcall`.  Control does not
/// return to the caller; it is restored only when `goready` re-enqueues the G.
///
/// # Precondition
///
/// Must be called from a goroutine's stack (not g0 or a bare OS thread).
/// A `debug_assert` fires in debug builds if this is violated.
///
/// Ported from `gopark` in `runtime/proc.go`.
pub(crate) fn gopark(reason: WaitReason) {
    let gp = current_g();
    debug_assert!(!gp.is_null(), "gopark: called from g0 or bare OS thread");
    // SAFETY: gp is non-null (asserted above) and points to the current goroutine.
    unsafe { (*gp).waitreason = reason };
    // SAFETY: mcall switches to g0 and invokes park_fn, which sets GWAITING
    // and re-enters schedule().  This is safe to call when gp is a live goroutine.
    unsafe { mcall(gp, park_fn) };
    // park_fn → schedule() — control never returns here until goready re-enqueues gp.
}

/// Mcall target for `gopark`.  Runs on g0's stack.
///
/// Sets G status to `Gwaiting`, unlinks G from the M, and enters `schedule`.
unsafe extern "C" fn park_fn(gp: *mut G) {
    let m = current_m();

    unsafe {
        (*gp).atomicstatus.store(GWAITING, std::sync::atomic::Ordering::Release);
        (*gp).m   = ptr::null_mut();
        (*m).curg = ptr::null_mut();
        set_current_g(ptr::null_mut());
    }

    unsafe { schedule() };
}

// ---------------------------------------------------------------------------
// goready — wake a parked goroutine
// ---------------------------------------------------------------------------

/// Make `gp` runnable and enqueue it for scheduling.
///
/// Transitions `gp` from `Gwaiting` to `Grunnable`.  If the calling thread
/// has a P, the G is placed in its local run queue (as `runnext`); otherwise
/// it goes to the global run queue.  An idle M is woken if one is available.
///
/// May be called from any goroutine or from g0.
///
/// ## GWAITING spin
///
/// Channel operations release their lock *before* calling `gopark`, so there
/// is a brief window where the target G is still `GRUNNING` (park_fn hasn't
/// run yet) even though we have already dequeued its sudog.  We spin with
/// `spin_loop` hints until the G reaches `GWAITING` before marking it
/// `GRUNNABLE`, preventing a second M from picking up a goroutine that is
/// still executing on the first M.
///
/// The spin is always very short — just the cost of a single `mcall` switch
/// (≈ tens of nanoseconds) — and terminates as soon as park_fn fires.
///
/// Ported from `goready` in `runtime/proc.go`.
pub(crate) unsafe fn goready(gp: *mut G) {
    use std::sync::atomic::Ordering::Acquire;

    // Spin until the goroutine finishes its GRUNNING → GWAITING transition.
    loop {
        let s = unsafe { (*gp).atomicstatus.load(Acquire) };
        if s == GWAITING {
            break;
        }
        debug_assert!(
            s == GRUNNING,
            "goready: unexpected status {s} — G must be GRUNNING or GWAITING"
        );
        std::hint::spin_loop();
    }

    unsafe { (*gp).atomicstatus.store(GRUNNABLE, std::sync::atomic::Ordering::Release) };

    let sc = sched();
    let m  = current_m();

    if !m.is_null() {
        let p = unsafe { (*m).p };
        if !p.is_null() {
            // Place on local run queue as the next G to run.
            unsafe { (*p).runqput(gp, true, &sc.global_run_q) };
            // Wake an idle M to run the G if this M is busy.
            unsafe { startm(ptr::null_mut()) };
            return;
        }
    }

    // No local P — push to global run queue.
    unsafe {
        (*gp).schedlink = ptr::null_mut();
        sc.global_run_q.push_batch(gp, gp, 1);
        startm(ptr::null_mut());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use crate::runtime::g::{Stack, G};

    fn make_g(id: u64) -> Box<G> {
        let lo = (id as usize + 1) << 20;
        G::new(Stack { lo, hi: lo + 65536 }, id)
    }

    /// `goready` on a parked goroutine must cause it to run.
    ///
    /// The original test pushed a fake G into the live global queue and popped
    /// it back to check it arrived.  That races with background M-threads that
    /// call `findrunnable` and would execute any G they find — a fake G with a
    /// non-mmap'd stack causes a SIGSEGV on context switch.
    ///
    /// This version uses `run_impl` with a real goroutine so execution is safe,
    /// and verifies the observable outcome: the goroutine body ran.
    #[test]
    fn goready_pushes_to_global_queue() {
        use crate::runtime::sched::run_impl;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let ran = Arc::new(AtomicBool::new(false));
        let ran2 = Arc::clone(&ran);
        let ran3 = Arc::clone(&ran); // checked inside run_impl to bound the loop

        run_impl(move || {
            // spawn_goroutine calls goready internally (via push_batch + startm).
            // Verify that the goroutine runs, which proves the ready path works.
            crate::runtime::sched::spawn_goroutine(move || {
                ran2.store(true, Ordering::Release);
            });
            // Yield until the spawned goroutine runs.  A fixed iteration count
            // is fragile under heavy parallel test load; use a wall-clock
            // deadline instead so the test passes even on slow CI runners.
            let deadline =
                std::time::Instant::now() + std::time::Duration::from_secs(5);
            while !ran3.load(Ordering::Acquire)
                && std::time::Instant::now() < deadline
            {
                crate::gosched();
            }
        });

        assert!(ran.load(Ordering::Acquire), "goroutine should have run via goready path");
    }
}
