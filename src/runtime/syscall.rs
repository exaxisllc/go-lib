// SPDX-License-Identifier: Apache-2.0
//! Syscall handoff shim — ported from `runtime/proc.go`
//! (`entersyscall` / `exitsyscall`).
//!
//! ## Purpose
//!
//! Any operation that may block an OS thread (a real `syscall`, `std::fs`,
//! `std::net`, sleeping on a `std::sync::Mutex` that is heavily contended,
//! etc.) would deadlock the scheduler if the goroutine's P sat idle while
//! every M was blocked in the kernel.  The solution matches Go's:
//!
//! 1. **`entersyscall`** — before the blocking call:
//!    - Transition P from `PRUNNING` → `PSYSCALL`.
//!    - Save P in `M.oldp` so `exitsyscall` can try to reacquire it.
//!    - Detach M from P (`M.p = null`) so `sysmon` can hand the P to another
//!      M without racing with the syscall M.
//!
//! 2. **`exitsyscall`** — after the blocking call returns:
//!    - Fast path: try to re-attach `M.oldp` (CAS `PSYSCALL` → `PRUNNING`).
//!    - Slow path: `exitsyscall0` — acquire a different idle P, or park.
//!
//! ## Relationship with sysmon
//!
//! `sysmon` (`retake`) watches Ps in `PSYSCALL`.  After `FORCE_RETAKE_NS`
//! (20 ms by default), it CAS's `PSYSCALL → PIDLE`, bumps `syscalltick`, and
//! hands the P to a waiting M via `startm`.  When `exitsyscall` then runs and
//! finds `M.oldp` no longer in `PSYSCALL`, it takes the slow path.
//!
//! ## Wrapping blocking std ops
//!
//! Use the [`with_syscall`] helper (also available at the crate root as
//! [`go_lib::with_syscall`]):
//!
//! ```no_run
//! # fn do_io() {}
//! let result = go_lib::with_syscall(|| do_io());
//! ```
//!
//! `with_syscall` is a no-op when called from outside a goroutine (e.g. from
//! the test thread before `run_impl`).
//!
//! Ported from `entersyscall`, `exitsyscall`, `exitsyscall0` in
//! `runtime/proc.go`.

use std::ptr;
use std::sync::atomic::Ordering::*;

use super::g::{casgstatus, current_g, set_current_g, G, GRUNNABLE, GRUNNING, GSYSCALL};
use super::m::current_m;
use super::p::{PRUNNING, PSYSCALL};
use super::sched::{execute, sched, schedule, startm};

#[cfg(target_arch = "x86_64")]
use super::asm_amd64::mcall;
#[cfg(target_arch = "aarch64")]
use super::asm_arm64::mcall;

// ---------------------------------------------------------------------------
// entersyscall
// ---------------------------------------------------------------------------

/// Mark the current goroutine as being in a syscall.
///
/// Transitions P: `PRUNNING` → `PSYSCALL`, stores P in `M.oldp`, and
/// detaches M from P so `sysmon` can retake the P if the syscall takes too
/// long.
///
/// **No-op** when called from outside a goroutine (null `CURRENT_M`).
///
/// # Safety
/// Must be called on a goroutine stack, not g0.
pub(crate) unsafe fn entersyscall() {
    let m = current_m();
    if m.is_null() { return; }

    let p = unsafe { (*m).p };
    if p.is_null() { return; }

    // Transition P to PSYSCALL.  Relaxed is sufficient here — sysmon reads
    // with Acquire, and the release fence below ensures visibility.
    let old = unsafe {
        (*p).status.compare_exchange(PRUNNING, PSYSCALL, AcqRel, Relaxed)
    };
    if old.is_err() { return; } // already in syscall or not running

    // Bump syscalltick so sysmon can detect a re-entry.
    unsafe { (*p).syscalltick.fetch_add(1, Relaxed) };

    // Save P for exitsyscall and detach from M.
    unsafe {
        (*m).oldp = p;
        (*m).p    = ptr::null_mut();
        // P.m still points at m so exitsyscall can re-claim it via CAS.
    }

    // Transition G: GRUNNING → GSYSCALL (mirrors Go's entersyscall).
    let gp = current_g();
    if !gp.is_null() {
        unsafe { casgstatus(gp, GRUNNING, GSYSCALL) };
    }
}

// ---------------------------------------------------------------------------
// exitsyscall
// ---------------------------------------------------------------------------

/// Restore the current goroutine from syscall state.
///
/// Fast path: re-attach `M.oldp` if it is still in `PSYSCALL`.
/// Slow path: acquire any idle P, or park this M.
///
/// **No-op** when called from outside a goroutine (null `CURRENT_M`).
///
/// # Safety
/// Must be called from the same goroutine that called `entersyscall`.
pub(crate) unsafe fn exitsyscall() {
    let m = current_m();
    if m.is_null() { return; }

    // Fast path: try to re-attach the same P.
    let oldp = unsafe { (*m).oldp };
    if !oldp.is_null() {
        let ok = unsafe {
            (*oldp)
                .status
                .compare_exchange(PSYSCALL, PRUNNING, AcqRel, Relaxed)
                .is_ok()
        };
        if ok {
            unsafe {
                (*m).p     = oldp;
                (*m).oldp  = ptr::null_mut();
                (*oldp).m  = m;
            }
            // G: GSYSCALL → GRUNNING (fast path).
            let gp = current_g();
            if !gp.is_null() {
                unsafe { casgstatus(gp, GSYSCALL, GRUNNING) };
            }
            return; // fast path: back to running with our old P
        }
        unsafe { (*m).oldp = ptr::null_mut() };
    }

    // Slow path: P was stolen by sysmon.  Switch to g0 via mcall so that
    // gp.sched captures the correct resume point *before* the goroutine is
    // placed on the global run queue.  Without mcall, another M could resume
    // the goroutine (via execute → gogo(gp.sched)) while this M is still
    // executing on the goroutine's stack inside exitsyscall0 → park_m_no_p,
    // causing two threads to share the same stack → STATUS_ACCESS_VIOLATION.
    let gp = current_g();
    unsafe { mcall(gp, exitsyscall0_mcall) };
    // When gp is resumed (execute → gogo → mcall epilogue), execution returns
    // here and unwinds normally through exitsyscall → with_syscall.
}

/// Slow-path mcall target for `exitsyscall`.  Runs on g0's stack.
///
/// `mcall` has already saved the goroutine's current PC and SP into
/// `gp.sched`, so any M that later calls `execute(gp) → gogo(gp.sched)`
/// will correctly resume the goroutine at the `mcall` return point inside
/// `exitsyscall` — not restart it from `goroutine_entry`.
///
/// Ported from `exitsyscall0` in `runtime/proc.go`.
unsafe extern "C" fn exitsyscall0_mcall(gp: *mut G) {
    let m  = current_m();
    let sc = sched();

    // Detach the goroutine from this M.  We are on g0's stack, so there is
    // no conflict with the goroutine's stack even after another M picks up gp.
    // G: GSYSCALL → GRUNNABLE (slow path — P was stolen by sysmon).
    unsafe {
        casgstatus(gp, GSYSCALL, GRUNNABLE);
        (*gp).m   = ptr::null_mut();
        (*m).curg = ptr::null_mut();
        set_current_g(ptr::null_mut());
    }

    // Try to grab an idle P.
    let p = {
        let mut inner = sc.inner.lock().unwrap();
        let p = inner.idle_p;
        if !p.is_null() {
            inner.idle_p = unsafe { (*p).link };
            unsafe { (*p).link = ptr::null_mut() };
        }
        p
    };

    if !p.is_null() {
        // Got an idle P — run gp immediately on this M.
        // execute() transitions gp back to GRUNNING and calls gogo(gp.sched),
        // which resumes the goroutine at the mcall return inside exitsyscall.
        unsafe {
            (*p).status.store(PRUNNING, Release);
            (*p).m  = m;
            (*m).p  = p;
        }
        unsafe { execute(gp) };  // → gogo(gp.sched) → never returns here
    }

    // No idle P — enqueue gp and wake another M to run it.
    unsafe {
        (*gp).schedlink = ptr::null_mut();
        sc.global_run_q.push_batch(gp, gp, 1);
        startm(ptr::null_mut());
    }

    // Park this M (no P).  When startm hands us a P and wakes us, re-enter
    // the scheduler loop to pick up goroutines.
    unsafe { park_m_no_p(m) };
    unsafe { schedule() };  // never returns
}

/// Park an M that has no P.
///
/// Adds M to the idle list and blocks.  On wakeup (from `startm`), the P
/// has already been attached to `M.p`.
unsafe fn park_m_no_p(m: *mut super::m::M) {
    let sc = sched();
    {
        let mut inner = sc.inner.lock().unwrap();
        unsafe {
            (*m).schedlink = inner.idle_m;
            inner.idle_m   = m;
            inner.nmidle  += 1;
        }
    }
    unsafe { (*m).park_m() }; // blocks until startm wakes us
    // On return, (*m).p has been set by startm.
}

// ---------------------------------------------------------------------------
// with_syscall — convenience wrapper
// ---------------------------------------------------------------------------

/// Run `f` as a "blocking syscall": calls `entersyscall` before `f`, then
/// `exitsyscall` after `f` returns, and passes through `f`'s return value.
///
/// This is a no-op shim when called from outside the go-lib scheduler (e.g.
/// from regular `main` or a Rust test thread that hasn't called `run`).
///
/// ## Example
///
/// ```no_run
/// let n = go_lib::with_syscall(|| std::fs::read("file.txt"));
/// ```
pub fn with_syscall<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    unsafe { entersyscall() };
    let r = f();
    unsafe { exitsyscall() };
    r
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::runtime::p::{P, PIDLE, PRUNNING, PSYSCALL};
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Arc;

    // Helper: box a P and set it to PRUNNING with a dummy M attached.
    unsafe fn make_running_p() -> (*mut super::super::p::P, *mut super::super::m::M) {
        use crate::runtime::m::M;
        let m = Box::into_raw(unsafe { M::new(999) });
        let p = Box::into_raw(P::new(0i32));
        unsafe {
            (*p).status.store(PRUNNING, Release);
            (*p).m = m;
            (*m).p = p;
        }
        (p, m)
    }

    /// entersyscall transitions P to PSYSCALL and detaches M.
    #[test]
    fn entersyscall_transitions_p() {
        use crate::runtime::m::set_current_m;

        let (p, m) = unsafe { make_running_p() };
        unsafe { set_current_m(m) };

        unsafe { entersyscall() };

        assert_eq!(
            unsafe { (*p).status.load(Relaxed) },
            PSYSCALL,
            "P must be PSYSCALL after entersyscall"
        );
        assert!(
            unsafe { (*m).p.is_null() },
            "M.p must be null after entersyscall"
        );
        assert_eq!(
            unsafe { (*m).oldp },
            p,
            "M.oldp must point to the old P"
        );

        // Clean up — restore M to avoid polluting other tests.
        unsafe {
            (*p).status.store(PRUNNING, Release);
            (*m).p    = p;
            (*m).oldp = std::ptr::null_mut();
            set_current_m(std::ptr::null_mut());
            let _ = Box::from_raw(p);
            let _ = Box::from_raw(m);
        }
    }

    /// exitsyscall fast path: re-attaches M.oldp when it is still PSYSCALL.
    #[test]
    fn exitsyscall_fast_path() {
        use crate::runtime::m::set_current_m;

        let (p, m) = unsafe { make_running_p() };
        unsafe {
            set_current_m(m);
            // Manually put M/P into post-entersyscall state.
            (*p).status.store(PSYSCALL, Release);
            (*m).oldp = p;
            (*m).p    = std::ptr::null_mut();
        }

        unsafe { exitsyscall() };

        assert_eq!(
            unsafe { (*p).status.load(Relaxed) },
            PRUNNING,
            "P must be PRUNNING after exitsyscall fast path"
        );
        assert_eq!(
            unsafe { (*m).p },
            p,
            "M.p must be re-attached after exitsyscall fast path"
        );
        assert!(
            unsafe { (*m).oldp.is_null() },
            "M.oldp must be cleared"
        );

        // Clean up.
        unsafe {
            set_current_m(std::ptr::null_mut());
            let _ = Box::from_raw(p);
            let _ = Box::from_raw(m);
        }
    }

    /// with_syscall is transparent from the goroutine's perspective.
    #[test]
    fn with_syscall_transparent() {
        let result = with_syscall(|| 42_i32);
        assert_eq!(result, 42);
    }

    /// with_syscall inside a goroutine: P transitions through PSYSCALL and back.
    #[test]
    #[go_lib::main]
    fn with_syscall_in_goroutine() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let saw_psyscall = Arc::new(AtomicU32::new(0));
        let saw2 = Arc::clone(&saw_psyscall);

        let saw_gsyscall = Arc::new(AtomicU32::new(0));
        let saw_gsyscall2 = Arc::clone(&saw_gsyscall);

        // Capture P and G status during the "syscall".
        let status_during = with_syscall(|| {
            // Peek at our P's status from inside the syscall.
            let m = current_m();
            if m.is_null() { return PIDLE; }
            // After entersyscall, M.p is null; oldp has the P.
            let p = unsafe { (*m).oldp };
            if p.is_null() { return PIDLE; }

            // Also check G status: should be GSYSCALL.
            let gp = crate::runtime::g::current_g();
            if !gp.is_null() {
                saw_gsyscall2.store(
                    unsafe { (*gp).atomicstatus.load(Ordering::Acquire) },
                    Ordering::Relaxed,
                );
            }

            unsafe { (*p).status.load(Ordering::Acquire) }
        });
        saw2.store(status_during, Ordering::Relaxed);

        assert_eq!(
            saw_psyscall.load(std::sync::atomic::Ordering::Acquire),
            PSYSCALL,
            "P must be in PSYSCALL during with_syscall"
        );
        assert_eq!(
            saw_gsyscall.load(std::sync::atomic::Ordering::Acquire),
            crate::runtime::g::GSYSCALL,
            "G must be in GSYSCALL during with_syscall"
        );
    }
}
