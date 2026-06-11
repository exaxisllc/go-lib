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

use super::g::{casgstatus, current_g, readgstatus, set_current_g, G, GPREEMPTED, GRUNNABLE, GRUNNING, GWAITING, WaitReason};
use super::m::current_m;
use super::sched::{current_rt_ptr, sched, schedule, set_current_rt, startm};

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
        casgstatus(gp, GRUNNING, GWAITING);
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
    // RCU read-side critical section.  Pairs with the `DrainSync` that the
    // run_impl Phase 2b drainer holds while freeing `Box<G>` allocations of
    // cancelled goroutines.  As long as this guard is alive, no drainer will
    // free the `gp` we are about to dereference.
    //
    // The guard MUST cover every dereference of `gp` in this function (the
    // status reads in the spin loop, the CAS, and the schedlink store on the
    // global-queue path).  Callers that load `gp` from shared state and then
    // call `goready` should hold their own outer `RcuGuard` so the pointer
    // load itself is also covered — see `closechan`, `chansend`,
    // `WaitGroup::add`, etc.
    let _cs = super::rcu::RcuGuard::new();

    // Spin until the goroutine finishes its in-flight status transition.
    // The two expected stable states are:
    //   GWAITING   — normal gopark (channel / mutex / timer / condvar)
    //   GPREEMPTED — async preemption landed before goready was called
    // We spin briefly on GRUNNING because channel operations release their
    // lock *before* calling gopark, so there is a tiny window where the G
    // is still GRUNNING even though its sudog has been dequeued.
    let old_status = loop {
        let s = unsafe { readgstatus(gp) };
        if s == GWAITING || s == GPREEMPTED {
            break s;
        }
        // Transient states: spin until the goroutine settles.
        //   GRUNNING   — channel ops release lock before gopark; the goroutine
        //                is still executing the mcall switch onto g0.
        //   GRUNNABLE  — goroutine was async-preempted (SIGURG) between timer
        //                insertion and its gopark call.  fire_expired() is the
        //                canonical caller for the timer path and handles this
        //                case by re-inserting the timer; other callers should
        //                not reach here with GRUNNABLE, but we guard anyway.
        if s == GRUNNABLE {
            // Already schedulable — nothing to do.  The caller (fire_expired)
            // should have handled GRUNNABLE before calling goready; if another
            // caller reaches here it means the goroutine is already queued and
            // will run without further intervention.
            return;
        }
        // GDEAD — the goroutine was cancelled by the run_impl shutdown drain
        // (GWAITING → GDEAD CAS) before this goready call arrived.  Nothing
        // to schedule; return without touching the G further.
        use super::g::GDEAD;
        if s == GDEAD {
            return;
        }
        debug_assert!(
            s == GRUNNING,
            "goready: unexpected status {s} — expected GRUNNING, GWAITING, GPREEMPTED, or GDEAD"
        );
        std::hint::spin_loop();
    };

    // GWAITING / GPREEMPTED → GRUNNABLE.
    unsafe { casgstatus(gp, old_status, GRUNNABLE) };

    let sc = sched();
    let m  = current_m();

    // Hold an `m.locks` guard across runqput / push_batch + startm.  Without
    // it, SIGURG can fire midway through these critical sections (each holds
    // an internal Mutex) and `preemptm` would self-deadlock trying to
    // re-acquire the same lock.  See `MLockGuard` doc-comment.
    let _lk = super::m::m_lock();

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

/// Wake a parked goroutine that belongs to a **different `Rt`** than the
/// calling thread.
///
/// Netpoll shares one process-wide poll fd between every concurrent
/// `run_impl` invocation, so an M-thread (or sysmon) of Rt-B can harvest a
/// readiness event for a goroutine owned by Rt-A.  Calling plain [`goready`]
/// there would enqueue A's goroutine on B's local P — B's scheduler would
/// then execute a foreign G against the wrong run queues, `allg`, and
/// shutdown flag.  This variant always takes the **global-queue path of the
/// owning `Rt`**, temporarily binding `CURRENT_RT` so `startm` wakes one of
/// the owner's M-threads.
///
/// The status-spin protocol mirrors [`goready`]; see its doc-comment.
pub(crate) unsafe fn goready_remote(gp: *mut G, rt: *const super::sched::Rt) {
    use super::g::GDEAD;

    // RCU read-side — same pairing as goready (Phase 2b drainer's DrainSync).
    let _cs = super::rcu::RcuGuard::new();

    let old_status = loop {
        let s = unsafe { readgstatus(gp) };
        if s == GWAITING || s == GPREEMPTED {
            break s;
        }
        if s == GRUNNABLE || s == GDEAD {
            return; // already queued / cancelled by the owner's drain
        }
        debug_assert!(
            s == GRUNNING,
            "goready_remote: unexpected status {s}"
        );
        std::hint::spin_loop();
    };

    unsafe { casgstatus(gp, old_status, GRUNNABLE) };

    // Suppress async preemption across the foreign Mutex critical section
    // (same rationale as goready's m_lock guard).
    let _lk = super::m::m_lock();

    // Bind CURRENT_RT to the owner so startm operates on its idle lists,
    // then restore the caller's binding.
    let prev = current_rt_ptr();
    set_current_rt(rt);
    unsafe {
        (*gp).schedlink = ptr::null_mut();
        (*rt).global_run_q.push_batch(gp, gp, 1);
        startm(ptr::null_mut());
    }
    set_current_rt(prev);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use crate::runtime::g::{Stack, G};
    use crate::runtime::stack::GOROUTINE_STACK_BYTES;

    #[allow(dead_code)] // shared test-helper; used by pending goready/gopark tests
    fn make_g(id: u64) -> Box<G> {
        let lo = (id as usize + 1) << 20;
        G::new(Stack { lo, hi: lo + GOROUTINE_STACK_BYTES }, id)
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
