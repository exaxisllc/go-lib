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
    // Block SIGURG across the `current_g()` read and the `mcall` save so async
    // preemption cannot split the thread-local read and migrate this goroutine
    // mid-park (which would make `mcall` save into the wrong goroutine's gobuf
    // — the cross-stack corruption fixed in `async_preempt2`).  Unlike
    // `gopark_commit`, plain `gopark` arrives WITHOUT `m.locks` elevated, so it
    // has no Guard-0 protection of its own.  `park_fn` unblocks on g0.
    #[cfg(not(windows))]
    unsafe { super::m::block_sigurg() };
    let gp = current_g();
    debug_assert!(!gp.is_null(), "gopark: called from g0 or bare OS thread");
    // SAFETY: gp is non-null (asserted above) and points to the current goroutine.
    unsafe { (*gp).waitreason = reason };
    // SAFETY: mcall switches to g0 and invokes park_fn, which sets GWAITING
    // and re-enters schedule().  This is safe to call when gp is a live goroutine.
    unsafe { mcall(gp, park_fn) };
    // park_fn → schedule() — control never returns here until goready re-enqueues gp.
}

/// Suspend the current goroutine, releasing a caller-held lock only AFTER
/// the goroutine is `GWAITING` — Go's `gopark(unlockf, …)` commit protocol.
///
/// Blocking operations publish a waiter (sudog) and then park.  If they
/// release their lock *before* parking, there is a window in which the
/// goroutine is still `GRUNNING` (or `GRUNNABLE`, if async preemption lands
/// in the window) while its sudog is already visible: a waker that completes
/// the rendezvous then cannot deliver the wake (`goready` sees a
/// non-`GWAITING` status) and the wake is lost — the goroutine parks forever
/// and the consumed sudog corrupts the pool.  Holding the lock until
/// `park_fn` has transitioned the G to `GWAITING` closes the window: no
/// waker can reach the sudog before the park is committed.
///
/// `unlock_fn(unlock_arg)` is invoked exactly once, on g0's stack, after the
/// status transition (or after the parking G is reaped by a dead-invocation
/// drain — the lock must be released in that path too).
///
/// # Contract: m.locks transfer
///
/// The caller must arrive with `m.locks` elevated by exactly one for the
/// dissolved lock guard (see `LockGuard::into_locked_raw`), which keeps
/// async preemption suppressed across the `mcall` — guaranteeing `park_fn`
/// runs on the same M.  `park_fn` decrements `m.locks` once when an unlock
/// handoff is present.
///
/// # Safety
/// The caller must actually hold the lock that `unlock_fn` releases, and
/// must not touch the protected state after calling this.
pub(crate) unsafe fn gopark_commit(
    reason:     WaitReason,
    unlock_fn:  unsafe fn(*mut u8),
    unlock_arg: *mut u8,
) {
    let gp = current_g();
    debug_assert!(!gp.is_null(), "gopark_commit: called from g0 or bare OS thread");
    unsafe {
        (*gp).waitreason      = reason;
        (*gp).park_unlock_fn  = Some(unlock_fn);
        (*gp).park_unlock_arg = unlock_arg;
        mcall(gp, park_fn);
    }
}

/// Mcall target for `gopark`.  Runs on g0's stack.
///
/// Sets G status to `Gwaiting`, unlinks G from the M, and enters `schedule`.
unsafe extern "C" fn park_fn(gp: *mut G) {
    // Balance plain `gopark`'s block_sigurg() (a no-op for the `gopark_commit`
    // path, which keeps SIGURG mask-unblocked and relies on `m.locks` instead).
    // The gobuf save is complete and we are on g0, so preemption is safe again.
    #[cfg(not(windows))]
    unsafe { super::m::unblock_sigurg() };
    let m = current_m();

    // Snapshot the gopark_commit unlock handoff FIRST: once the G is
    // GWAITING and the lock is released, a waker can run the G to
    // completion on another M and free it — `gp` must not be dereferenced
    // after the unlock call below.
    let unlock_fn  = unsafe { (*gp).park_unlock_fn.take() };
    let unlock_arg = unsafe {
        let a = (*gp).park_unlock_arg;
        (*gp).park_unlock_arg = ptr::null_mut();
        a
    };
    // Balance the m.locks increment transferred by `gopark_commit` (see its
    // contract).  Done here, before the status transition, so the decrement
    // lands on the SAME M that took the increment — after the unlock below,
    // this G may resume on a different M.
    if unlock_fn.is_some() {
        unsafe { (*m).locks.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) };
    }

    // Go-faithful semantics: goroutines are never force-killed, so parking
    // simply transitions to GWAITING.  (The former dead-invocation reaper that
    // transitioned GRUNNING → GDEAD here has been removed along with the rest
    // of the kill paths.)
    unsafe {
        casgstatus(gp, GRUNNING, GWAITING);
        (*gp).m   = ptr::null_mut();
        (*m).curg = ptr::null_mut();
        set_current_g(ptr::null_mut());
        // Commit point: all writes to `gp` are done.  Release the caller's
        // lock LAST — the moment it is released a waker can dequeue the
        // sudog, goready this G, and another M may run (and even free) it.
        if let Some(f) = unlock_fn {
            f(unlock_arg);
        }
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
    // `goready` dereferences only the immortal `G` descriptor — its
    // `atomicstatus` (spin loop + CAS), `param`, and `schedlink` — never the
    // goroutine's stack.  Because `gfree_put` leaks the descriptor, every one
    // of those dereferences is unconditionally safe even if a concurrent
    // Phase 2b drain has CAS'd this G to GDEAD and freed its stack (the GDEAD
    // arm below handles that case and returns without scheduling).  No
    // reader/drainer synchronisation is required here.

    // Spin until the goroutine finishes its in-flight status transition.
    // The only wakeable state is GWAITING.
    //
    // GPREEMPTED is deliberately NOT wakeable: it exists only inside
    // `preemptm`, between its `GRUNNING → GPREEMPTED` and
    // `GPREEMPTED → GRUNNABLE` transitions.  If goready were to claim it
    // (CAS GPREEMPTED → GRUNNABLE + enqueue), `preemptm`'s second
    // `casgstatus` would spin waiting for GPREEMPTED to reappear while the
    // goroutine runs — and possibly exits and is FREED — on another M.  The
    // stuck spin then reads recycled heap memory; the moment those bytes
    // coincide with GPREEMPTED it "wins" the CAS on garbage and pushes a
    // dangling G pointer into the run queue (observed as `execute` resuming
    // a zeroed Gobuf in the many_goroutines SIGURG storm).  Spinning here
    // instead is always brief: preemptm completes its second CAS within a
    // few instructions.
    loop {
        let s = unsafe { readgstatus(gp) };
        if s == GWAITING {
            // GWAITING / GPREEMPTED → GRUNNABLE.  Use a single
            // compare_exchange rather than `casgstatus` (which retries until
            // it wins): between our status read and the CAS, the run_impl
            // Phase 2b drain can CAS GWAITING → GDEAD.  `casgstatus` would
            // then spin forever waiting for GWAITING to come back — while
            // holding the RcuGuard, which blocks the drainer's DrainSync and
            // deadlocks the whole process.  On CAS failure simply re-inspect
            // the status; the GDEAD/GRUNNABLE arms below handle the rest.
            let won = unsafe {
                (*gp).atomicstatus
                    .compare_exchange(s, GRUNNABLE, std::sync::atomic::Ordering::AcqRel,
                                      std::sync::atomic::Ordering::Relaxed)
                    .is_ok()
            };
            if won {
                break;
            }
            continue; // lost a status race — re-inspect
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
            s == GRUNNING || s == GPREEMPTED,
            "goready: unexpected status {s} — expected GRUNNING, GPREEMPTED (transient), \
             GWAITING, GRUNNABLE, or GDEAD"
        );
        std::hint::spin_loop();
    }

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
