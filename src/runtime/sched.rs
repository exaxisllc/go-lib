//! Scheduler core — `schedule`, `findrunnable`, `execute`, `goexit0`, `gosched`.
//!
//! Ported from `runtime/proc.go`.
//!
//! ## Execution model
//!
//! Every M runs `schedule()` on its g0 stack.  `schedule` picks a runnable G
//! via `findrunnable`, then calls `execute` which does a `gogo` context switch
//! into that G — `execute` never returns.  When the G finishes, the `goexit`
//! trampoline (wired during goroutine creation in step 9) calls `goexit0` back
//! on g0, which cleans up the G and re-enters `schedule`.
//!
//! ## Global state
//!
//! `SCHED` is a process-wide singleton initialised by `schedinit` (step 9).
//! It holds the global run queue, idle P/M lists, and `allp` (all Ps).  The
//! parts that need serialisation are guarded by `Mutex<SchedInner>`; the global
//! run queue carries its own internal lock.

use std::cell::Cell;
use std::ptr;
use std::sync::atomic::{AtomicI32, Ordering::*};
use std::sync::{Mutex, OnceLock};

use super::g::{current_g, set_current_g, G, GDEAD, GRUNNABLE, GRUNNING, WaitReason};
use super::m::M;
use super::p::{GlobalRunQueue, P, PIDLE, PRUNNING};

#[cfg(target_arch = "x86_64")]
use super::asm_amd64::{gogo, mcall};
#[cfg(target_arch = "aarch64")]
use super::asm_arm64::{gogo, mcall};

// ---------------------------------------------------------------------------
// Thread-local: current M
// ---------------------------------------------------------------------------

thread_local! {
    /// The M (OS thread) currently executing on this thread.
    /// Set by `M::start` (step 6 / step 9) before the scheduler loop begins.
    pub(crate) static CURRENT_M: Cell<*mut M> = const { Cell::new(ptr::null_mut()) };
}

/// Return the M running on the current OS thread, or null before initialisation.
#[inline]
pub(crate) fn current_m() -> *mut M {
    CURRENT_M.with(|c| c.get())
}

/// Record `m` as the M for the current OS thread.
///
/// # Safety
/// Must be called exactly once per OS thread, from `M::start` before any
/// scheduler function is invoked.
#[inline]
pub(crate) unsafe fn set_current_m(m: *mut M) {
    CURRENT_M.with(|c| c.set(m));
}

// ---------------------------------------------------------------------------
// Global scheduler state
// ---------------------------------------------------------------------------

/// The parts of `sched` that need mutual exclusion.
pub(crate) struct SchedInner {
    /// Head of the idle-P singly-linked list (linked via `P.link`).
    pub idle_p:    *mut P,
    /// Head of the idle-M singly-linked list (linked via `M.schedlink`).
    pub idle_m:    *mut M,
    /// Count of idle Ms (length of `idle_m` list).
    pub nmidle:    i32,
    /// All Ps — populated by `schedinit` (step 9).  Raw pointers into
    /// `Box<P>` allocations that are leaked for the lifetime of the process.
    pub allp:      Vec<*mut P>,
    /// GOMAXPROCS — set once by `schedinit`.
    pub gomaxprocs: i32,
}

// SAFETY: All raw pointer access inside SchedInner is guarded by the Mutex.
unsafe impl Send for SchedInner {}

/// Process-global scheduler state, equivalent to Go's `sched` variable.
pub(crate) struct Sched {
    /// Global run queue — goroutines that are runnable but not yet on any P.
    pub global_run_q: GlobalRunQueue,
    /// Number of Ms currently spinning (looking for work in `findrunnable`).
    pub nmspinning:   AtomicI32,
    /// Locked parts of scheduler state.
    pub inner:        Mutex<SchedInner>,
}

// SAFETY: Sched is a process-global singleton; individual fields carry their
// own synchronisation (Mutex / AtomicI32 / GlobalRunQueue's internal Mutex).
unsafe impl Sync for Sched {}

static SCHED: OnceLock<Sched> = OnceLock::new();

/// Return a reference to the global scheduler, initialising it on first call.
pub(crate) fn sched() -> &'static Sched {
    SCHED.get_or_init(|| Sched {
        global_run_q: GlobalRunQueue::new(),
        nmspinning:   AtomicI32::new(0),
        inner: Mutex::new(SchedInner {
            idle_p:     ptr::null_mut(),
            idle_m:     ptr::null_mut(),
            nmidle:     0,
            allp:       Vec::new(),
            gomaxprocs: 1,
        }),
    })
}

// ---------------------------------------------------------------------------
// schedule — main scheduler loop (runs on g0)
// ---------------------------------------------------------------------------

/// Main scheduling loop.  Runs on g0's stack; never returns.
///
/// Picks a runnable goroutine via `findrunnable` and transfers control to it
/// via `execute`.  Called initially from `M::start` and re-entered via
/// `goexit0` and `gosched_m`.
///
/// Ported from `schedule` in `runtime/proc.go`.
pub(crate) unsafe fn schedule() -> ! {
    let m = unsafe { current_m() };
    debug_assert!(!m.is_null(), "schedule: CURRENT_M is null — call set_current_m first");

    loop {
        let p = unsafe { (*m).p };
        debug_assert!(!p.is_null(), "schedule: M has no P attached");

        // Every 61 ticks drain one G from the global queue to prevent starvation.
        // 61 is a prime chosen by Go so the check is not aligned to any bursty
        // producer period.
        let tick = unsafe { (*p).schedtick.load(Relaxed) };
        if tick % 61 == 0 && unsafe { sched().global_run_q.len() > 0 } {
            let gp = unsafe { sched().global_run_q.pop() };
            if !gp.is_null() {
                unsafe { execute(gp) }; // -> !
            }
        }

        // Try local run queue first — no lock needed.
        let (gp, _inherit) = unsafe { (*p).runqget() };

        let gp = if !gp.is_null() {
            gp
        } else {
            // Local queue empty; find work elsewhere.
            unsafe { findrunnable() } // may park; always returns a G
        };

        unsafe { execute(gp) }; // -> !
    }
}

// ---------------------------------------------------------------------------
// findrunnable — find a runnable G, parking if there is none
// ---------------------------------------------------------------------------

/// Find and return the next runnable goroutine.
///
/// Search order (matches Go):
/// 1. Local P run queue.
/// 2. Global run queue.
/// 3. Work-steal from a random P (4 attempts).
/// 4. If none, surrender the P and park the M.  On wakeup, loop.
///
/// Always returns a non-null `*mut G`.  Parks the calling M indefinitely if
/// there is truly no work.
///
/// Ported from `findrunnable` in `runtime/proc.go` (trimmed for v1).
pub(crate) unsafe fn findrunnable() -> *mut G {
    let m  = unsafe { current_m() };
    let sc = sched();

    loop {
        // ── 1. Local run queue ────────────────────────────────────────────
        let p = unsafe { (*m).p };
        if !p.is_null() {
            let (gp, _) = unsafe { (*p).runqget() };
            if !gp.is_null() {
                return gp;
            }
        }

        // ── 2. Global run queue ───────────────────────────────────────────
        {
            let gp = unsafe { sc.global_run_q.pop() };
            if !gp.is_null() {
                return gp;
            }
        }

        // ── 3. Work-steal from a random P ─────────────────────────────────
        {
            let inner = sc.inner.lock().unwrap();
            let allp  = &inner.allp;
            let np    = allp.len();
            if np > 1 && !p.is_null() {
                // Simple deterministic pseudo-random start derived from M id.
                let start = (unsafe { (*m).id as usize }).wrapping_mul(0x9e3779b9) % np;
                let victim_ptrs: Vec<*mut P> = (0..4)
                    .map(|i| allp[(start.wrapping_add(i)) % np])
                    .collect();
                drop(inner);

                for (i, victim_ptr) in victim_ptrs.iter().enumerate() {
                    if *victim_ptr == p {
                        continue;
                    }
                    // Steal runnext on the last attempt.
                    let stolen = unsafe {
                        (*p).runqsteal(&**victim_ptr, i == 3)
                    };
                    if !stolen.is_null() {
                        return stolen;
                    }
                }
            }
        }

        // ── 4. No work found — surrender P and park ───────────────────────
        unsafe { stopm() };
        // Woken up by startm/goready; P has been (re-)attached.  Try again.
    }
}

// ---------------------------------------------------------------------------
// stopm — surrender P and park M until startm wakes it
// ---------------------------------------------------------------------------

/// Surrender the current M's P and block until another thread calls `startm`.
///
/// On return, the M's `p` field has been restored to a runnable P by the
/// thread that woke it.
///
/// Ported from `stopm` in `runtime/proc.go`.
unsafe fn stopm() {
    let m  = unsafe { current_m() };
    let sc = sched();

    // Surrender P under the scheduler lock.
    {
        let mut inner = sc.inner.lock().unwrap();
        let p = unsafe { (*m).p };
        if !p.is_null() {
            unsafe {
                (*m).p = ptr::null_mut();
                (*p).status.store(PIDLE, Release);
                (*p).link = inner.idle_p;
                inner.idle_p = p;
            }
        }
        // Enqueue M on idle list.
        unsafe {
            (*m).schedlink = inner.idle_m;
            inner.idle_m   = m;
            inner.nmidle  += 1;
        }
    } // release lock before blocking

    unsafe { (*m).park_m() }; // blocks until unpark()

    // Woken by startm — it has already:
    //   * removed us from idle_m
    //   * decremented nmidle
    //   * set (*m).p to a runnable P
    // Nothing to do here except return.
}

// ---------------------------------------------------------------------------
// startm — wake an idle M and hand it a P
// ---------------------------------------------------------------------------

/// Pop an idle M, give it `p` (or an idle P), and unpark it.
///
/// If `p` is null a P from the idle list is used.  If no idle M or no P is
/// available, `p` is placed on the idle-P list and the function returns.
///
/// Ported from `startm` in `runtime/proc.go`.
pub(crate) unsafe fn startm(p: *mut P) {
    let sc = sched();
    let mut inner = sc.inner.lock().unwrap();

    // Pop an idle M.
    let m = inner.idle_m;
    if m.is_null() {
        // No idle M — park the P.
        if !p.is_null() {
            unsafe {
                (*p).status.store(PIDLE, Release);
                (*p).link = inner.idle_p;
                inner.idle_p = p;
            }
        }
        return;
    }

    // Remove M from idle list.
    unsafe {
        inner.idle_m    = (*m).schedlink;
        (*m).schedlink  = ptr::null_mut();
        inner.nmidle   -= 1;
    }

    // Determine which P to give the M.
    let use_p = if !p.is_null() {
        p
    } else if !inner.idle_p.is_null() {
        let p2 = inner.idle_p;
        unsafe {
            inner.idle_p = (*p2).link;
            (*p2).link   = ptr::null_mut();
        }
        p2
    } else {
        // No P available — put M back on idle list.
        unsafe {
            (*m).schedlink = inner.idle_m;
            inner.idle_m   = m;
            inner.nmidle  += 1;
        }
        return;
    };

    unsafe {
        (*use_p).status.store(PRUNNING, Release);
        (*m).p = use_p;
    }
    drop(inner);

    unsafe { (*m).unpark() };
}

// ---------------------------------------------------------------------------
// execute — run a goroutine (never returns)
// ---------------------------------------------------------------------------

/// Transition `gp` to `Grunning` and context-switch into it via `gogo`.
/// Runs on g0; never returns.
///
/// Ported from `execute` in `runtime/proc.go`.
pub(crate) unsafe fn execute(gp: *mut G) -> ! {
    let m = unsafe { current_m() };

    unsafe {
        (*m).curg  = gp;
        (*gp).m    = m;
        (*gp).atomicstatus.store(GRUNNING, Release);
    }

    // Bump the scheduling tick on the attached P.
    let p = unsafe { (*m).p };
    if !p.is_null() {
        unsafe { (*p).schedtick.fetch_add(1, Relaxed) };
    }

    // Switch into the goroutine's stack.  Never returns.
    unsafe {
        set_current_g(gp);
        gogo(gp)
    }
}

// ---------------------------------------------------------------------------
// goexit0 — G teardown after the goroutine function returns (runs on g0)
// ---------------------------------------------------------------------------

/// Clean up a finished goroutine and re-enter the scheduler.
///
/// Called via the `goexit` trampoline that is wired onto every goroutine's
/// initial stack frame by the spawner (step 9).  Runs on g0.
///
/// Ported from `goexit0` in `runtime/proc.go`.
pub(crate) unsafe extern "C" fn goexit0(gp: *mut G) {
    let m = unsafe { current_m() };

    unsafe {
        (*gp).atomicstatus.store(GDEAD, Release);
        (*gp).m   = ptr::null_mut();
        (*m).curg = ptr::null_mut();
        set_current_g(ptr::null_mut());
    }

    // Re-enter the scheduler on g0's stack.
    unsafe { schedule() }
}

// ---------------------------------------------------------------------------
// gosched — cooperative yield
// ---------------------------------------------------------------------------

/// Yield the current goroutine: move it to the global run queue and reschedule.
///
/// CPU-bound goroutines should call this periodically; v1 has no async
/// preemption signal.
///
/// Ported from `Gosched` / `gosched_m` in `runtime/proc.go`.
pub(crate) unsafe fn gosched() {
    let gp = unsafe { current_g() };
    debug_assert!(!gp.is_null(), "gosched: called from g0 or uninitialised thread");
    unsafe { mcall(gp, gosched_m) };
}

/// Mcall target for `gosched`.  Runs on g0's stack.
unsafe extern "C" fn gosched_m(gp: *mut G) {
    let m = unsafe { current_m() };

    unsafe {
        (*gp).atomicstatus.store(GRUNNABLE, Release);
        (*gp).m   = ptr::null_mut();
        (*m).curg = ptr::null_mut();
        set_current_g(ptr::null_mut());
    }

    // Push to global run queue (single element — schedlink already null from G::new).
    unsafe {
        (*gp).schedlink = ptr::null_mut();
        sched().global_run_q.push_batch(gp, gp, 1);
    }

    unsafe { schedule() };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sched_initialises_once() {
        let s1 = sched() as *const Sched;
        let s2 = sched() as *const Sched;
        assert_eq!(s1, s2, "sched() must return the same singleton");
    }

    #[test]
    fn sched_initial_state() {
        let s = sched();
        let inner = s.inner.lock().unwrap();
        assert_eq!(inner.nmidle, 0);
        assert_eq!(inner.gomaxprocs, 1);
        assert!(inner.idle_p.is_null());
        assert!(inner.idle_m.is_null());
        assert!(inner.allp.is_empty());
        assert_eq!(s.nmspinning.load(Relaxed), 0);
    }

    #[test]
    fn global_run_queue_round_trip() {
        use crate::runtime::g::{Stack, G};
        use std::sync::atomic::Ordering::Relaxed;

        let s = sched();
        let lo = 0x100000usize;
        let g1 = G::new(Stack { lo, hi: lo + 65536 }, 42);
        let g1_ptr = Box::into_raw(g1);

        unsafe {
            (*g1_ptr).schedlink = ptr::null_mut();
            s.global_run_q.push_batch(g1_ptr, g1_ptr, 1);
            assert_eq!(s.global_run_q.len(), 1);

            let got = s.global_run_q.pop();
            assert_eq!(got, g1_ptr);
            assert_eq!(s.global_run_q.len(), 0);

            let _ = Box::from_raw(g1_ptr);
        }
    }
}
