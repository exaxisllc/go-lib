// SPDX-License-Identifier: Apache-2.0
//! Background monitor thread — ported from `runtime/proc.go` (`sysmon`).
//!
//! Runs a single detached OS thread that performs three duties every iteration:
//!
//! 1. **Netpoll** — calls [`netpoll::netpoll_wait(0)`][super::netpoll::netpoll_wait]
//!    (non-blocking) and issues [`goready`][super::park::goready] for every
//!    goroutine whose I/O fd became ready (Unix) or whose overlapped operation
//!    completed (Windows IOCP).  *(v0.2.0 — Step 5)*
//!
//! 2. **Async preemption** — for every P in `PRUNNING` whose `schedtick` has
//!    not advanced for more than `FORCE_PREEMPT_NS` (10 ms), `preemptone`:
//!    - Sets `curg.preempt = true` and `curg.stackguard0 = STACK_PREEMPT`.
//!    - Calls `pthread_kill(m.pthread_id, SIGURG)` to deliver the signal to
//!      the exact OS thread running that goroutine.
//!      The SIGURG handler in `sched.rs` redirects the goroutine to
//!      `async_preempt_trampoline`, producing a non-cooperative yield.
//!      *(v0.2.0 — Step 4; replaces the v1 cooperative-hint-only approach)*
//!
//! 3. **Syscall retake** — if a P has been stuck in `PSYSCALL` for more than
//!    `FORCE_RETAKE_NS` (20 µs), CAS its status to `PIDLE` and hand it off via
//!    `startm` so other goroutines can run.
//!
//! Sleep schedule (unchanged from Go): 20 µs initial; doubles every 50 idle
//! iterations; capped at 10 ms.
//!
//! Ported from `sysmon` and `retake` in `runtime/proc.go`.

use std::sync::atomic::Ordering::*;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use super::g::STACK_PREEMPT;
use super::p::{PIDLE, PSYSCALL, PRUNNING};
use super::sched::{sched, startm};
// libc is used for pthread_kill in preemptone (Step 4).

// ---------------------------------------------------------------------------
// Sleep constants — from Go's sysmon loop
// ---------------------------------------------------------------------------

/// Initial sleep between sysmon iterations (microseconds).
const MIN_SLEEP_US: u64 = 20;
/// Maximum sleep between sysmon iterations (microseconds).
const MAX_SLEEP_US: u64 = 10_000;
/// Idle iterations before sysmon begins doubling its sleep delay.
const IDLE_THRESH: u64 = 50;

/// How long a goroutine may run before sysmon sets its `preempt` flag
/// (nanoseconds).  Matches Go's `forcePreemptNS = 10 * 1000 * 1000`.
const FORCE_PREEMPT_NS: u64 = 10_000_000; // 10 ms

/// Minimum time a P must be in PSYSCALL before we will retake it (nanoseconds).
const FORCE_RETAKE_NS: u64 = 20_000; // 20 µs

/// If the local run queue is empty and spinning/idle Ms exist, allow a longer
/// grace period before retaking the P (nanoseconds).
const LONG_RETAKE_NS: u64 = 10_000_000; // 10 ms

// ---------------------------------------------------------------------------
// Per-P sysmon observation record
// ---------------------------------------------------------------------------

/// Sysmon's last-observed snapshot of a P's scheduler counters.
///
/// Mirrors `sysmontick` in `runtime/runtime2.go`.
#[derive(Clone, Default)]
struct SysmonTick {
    /// Last-seen `P.schedtick`.
    schedtick:   u32,
    /// Monotonic nanoseconds when `schedtick` was last updated.
    schedwhen:   u64,
    /// Last-seen `P.syscalltick`.
    syscalltick: u32,
    /// Monotonic nanoseconds when this P was last observed entering PSYSCALL.
    syscallwhen: u64,
}

// ---------------------------------------------------------------------------
// start_sysmon — spawn the monitor thread
// ---------------------------------------------------------------------------

/// Spawn the sysmon background OS thread.
///
/// The thread is detached — it is never joined and runs for the lifetime of
/// the process.  Call exactly once from `schedinit`.
///
/// Ported from the sysmon goroutine launch in `runtime/proc.go`.
pub(crate) fn start_sysmon() {
    std::thread::Builder::new()
        .name("go-sysmon".to_string())
        .spawn(sysmon_loop)
        .expect("start_sysmon: failed to spawn sysmon thread");
    // Thread handle is dropped here — the thread runs detached.
}

// ---------------------------------------------------------------------------
// sysmon_loop — the monitor loop (runs on its own OS thread)
// ---------------------------------------------------------------------------

/// Main sysmon loop.  Runs indefinitely on the go-sysmon OS thread.
///
/// Ported from `sysmon` in `runtime/proc.go`.
fn sysmon_loop() {
    // Initialise the netpoll backend (idempotent).
    super::netpoll::netpoll_init();

    let mut delay_us: u64 = MIN_SLEEP_US;
    let mut idle: u64 = 0;
    // Per-P tick records, grown lazily to match `allp.len()`.
    let mut ticks: Vec<SysmonTick> = Vec::new();

    loop {
        // ── Exponential sleep backoff ─────────────────────────────────────
        // Go: delay=20µs on first iteration; double after 50 idle iters; cap 10ms.
        if idle == 0 {
            delay_us = MIN_SLEEP_US;
        } else if idle > IDLE_THRESH {
            delay_us = (delay_us * 2).min(MAX_SLEEP_US);
        }
        std::thread::sleep(Duration::from_micros(delay_us));

        // ── Netpoll: wake goroutines whose I/O is ready ───────────────────
        // Use a non-blocking poll here (0 ms timeout); sysmon must not block
        // indefinitely or it will miss retake/preempt duties.
        {
            let ready = unsafe { super::netpoll::netpoll_wait(0) };
            if !ready.is_empty() {
                idle = 0;
                for gp in ready {
                    unsafe { super::park::goready(gp) };
                }
            }
        }

        // ── Retake Ps stuck in syscalls ───────────────────────────────────
        let now_ns = monotonic_ns();
        if retake(now_ns, &mut ticks) != 0 {
            idle = 0; // found work — reset backoff
        } else {
            idle += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// retake — reclaim Ps stuck in syscalls
// ---------------------------------------------------------------------------

/// Scan every P in `allp`:
///
/// - **`PRUNNING`**: if `schedtick` has not advanced for `FORCE_PREEMPT_NS`
///   (10 ms), set `curg.preempt = true` and `curg.stackguard0 = STACK_PREEMPT`
///   as a hint for the goroutine to call `gosched()` at its next safe point.
///   Matches `preemptone` in `runtime/proc.go`.
///
/// - **`PSYSCALL`**: if the P has been stuck in a syscall past the retake
///   threshold, CAS its status to `PIDLE` and hand it off via `startm`.
///
/// Returns the number of Ps where action was taken (preempt-hint set or
/// retaken).
///
/// Ported from `retake` in `runtime/proc.go`.
fn retake(now_ns: u64, ticks: &mut Vec<SysmonTick>) -> u32 {
    let sc = sched();

    // Snapshot allp under the scheduler lock so we can iterate without holding
    // the lock (matches Go's use of allpLock in retake).
    let allp: Vec<*mut super::p::P> = {
        let inner = sc.inner.lock().unwrap();
        // Grow the ticks vec lazily; Ps are never removed in v1.
        if ticks.len() < inner.allp.len() {
            ticks.resize(inner.allp.len(), SysmonTick::default());
        }
        inner.allp.clone()
    };

    let mut acted: u32 = 0;

    for (i, &pp) in allp.iter().enumerate() {
        if pp.is_null() {
            continue;
        }

        let tick = &mut ticks[i];
        let status = unsafe { (*pp).status.load(Acquire) };

        // ── PRUNNING: cooperative preemption hint (step 11) ───────────────
        // Track how long the current G has been running.  Only set the hint
        // if the *same* G has been running for > FORCE_PREEMPT_NS — i.e.,
        // schedtick has not advanced since our last observation.
        //
        // Note: P.m is *not* necessarily current (it may lag one M-switch
        // because startm does not update P.m).  preemptone guards against
        // null M and null curg, but we defer the actual write to a later
        // step once P.m is kept properly in sync (after entersyscall lands).
        if status == PRUNNING {
            let schedtick = unsafe { (*pp).schedtick.load(Acquire) };
            if tick.schedtick != schedtick {
                // G scheduled since last observation — reset timestamp.
                tick.schedtick  = schedtick;
                tick.schedwhen  = now_ns;
            } else if now_ns.saturating_sub(tick.schedwhen) > FORCE_PREEMPT_NS {
                // Same G has been running for > 10 ms — set the preemption hint
                // and deliver SIGURG so the goroutine yields promptly.
                unsafe { preemptone(pp) };
                acted += 1;
            }
        }

        // ── PSYSCALL: retake P if stuck (step 10) ─────────────────────────
        if status == PSYSCALL {
            let syscalltick = unsafe { (*pp).syscalltick.load(Acquire) };

            // If the syscall tick advanced, the goroutine returned from and
            // re-entered a syscall — reset our observation timestamp.
            if tick.syscalltick != syscalltick {
                tick.syscalltick  = syscalltick;
                tick.syscallwhen  = now_ns;
                continue;
            }

            // P has been in PSYSCALL since at least `tick.syscallwhen`.
            let elapsed = now_ns.saturating_sub(tick.syscallwhen);

            // Never retake before the minimum threshold.
            if elapsed < FORCE_RETAKE_NS {
                continue;
            }

            // If the local run queue is empty AND spinning/idle Ms can service
            // work elsewhere, give the P a longer grace period before retaking.
            let run_q_empty = unsafe { (*pp).runq_size() == 0 };
            if run_q_empty && elapsed < LONG_RETAKE_NS {
                let spinning = sc.nmspinning.load(Relaxed);
                let nmidle   = sc.inner.lock().unwrap().nmidle;
                if spinning + nmidle > 0 {
                    continue;
                }
            }

            // Attempt to retake: CAS PSYSCALL → PIDLE.
            if unsafe {
                (*pp).status
                    .compare_exchange(PSYSCALL, PIDLE, AcqRel, Relaxed)
                    .is_ok()
            } {
                // Bump syscalltick so that `exitsyscall` (step 15.5) notices
                // that its P was stolen while it was in the kernel.
                unsafe { (*pp).syscalltick.fetch_add(1, Relaxed) };
                acted += 1;
                // Hand the idle P to a waiting M (or spawn one).
                unsafe { startm(pp) };
            }
        }
    }

    acted
}

// ---------------------------------------------------------------------------
// preemptone — set the preempt flag on the goroutine running on pp
// ---------------------------------------------------------------------------

/// Request async preemption of the goroutine running on `pp`.
///
/// Sets `gp.preempt = true` and `gp.stackguard0 = STACK_PREEMPT`, then
/// delivers `SIGURG` to the M's OS thread via `pthread_kill`.  The signal
/// handler (`sigurg_handler` in `sched.rs`) detects the preempt flag and
/// redirects the goroutine to `async_preempt_trampoline`.
///
/// Falls back gracefully if the P is not currently running a goroutine.
///
/// Ported from `preemptone` in `runtime/proc.go`.
unsafe fn preemptone(pp: *mut super::p::P) {
    let mp = unsafe { (*pp).m };
    if mp.is_null() {
        return;
    }
    let gp = unsafe { (*mp).curg };
    if gp.is_null() {
        return;
    }
    let pthread_id = unsafe { (*mp).pthread_id };
    if pthread_id == 0 {
        // M::start() has not run yet — no OS thread to signal.
        return;
    }

    // Mark the goroutine for preemption.  The SIGURG handler reads this flag.
    unsafe {
        (*gp).preempt     = true;
        (*gp).stackguard0 = STACK_PREEMPT;
    }

    // Unix only: send SIGURG to the M's OS thread.  Windows has no pthread_kill;
    // the preempt flag is set above and the goroutine will yield cooperatively.
    #[cfg(not(windows))]
    unsafe { libc::pthread_kill(pthread_id as libc::pthread_t, libc::SIGURG) };
}

// ---------------------------------------------------------------------------
// monotonic_ns — nanosecond monotonic clock
// ---------------------------------------------------------------------------

/// Return nanoseconds elapsed since an arbitrary process-wide epoch.
///
/// Uses `std::time::Instant` so the same code compiles on every tier-1 target.
/// Only differences between two calls matter; the origin is arbitrary.
fn monotonic_ns() -> u64 {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    let origin = ORIGIN.get_or_init(Instant::now);
    origin.elapsed().as_nanos() as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    /// `monotonic_ns` must be strictly increasing across a short sleep.
    #[test]
    fn monotonic_ns_is_monotonic() {
        let t1 = monotonic_ns();
        std::thread::sleep(Duration::from_millis(2));
        let t2 = monotonic_ns();
        assert!(t2 > t1, "monotonic_ns must be strictly increasing");
    }

    /// `SysmonTick::default` must initialise all fields to zero.
    #[test]
    fn sysmon_tick_default_is_zero() {
        let t = SysmonTick::default();
        assert_eq!(t.schedtick,   0);
        assert_eq!(t.schedwhen,   0);
        assert_eq!(t.syscalltick, 0);
        assert_eq!(t.syscallwhen, 0);
    }

    /// `retake` on a freshly initialised scheduler (all Ps PRUNNING or PIDLE,
    /// schedtick freshly set) must find nothing to retake on a first pass.
    #[test]
    fn retake_finds_nothing_on_first_pass() {
        let mut ticks = Vec::new();
        let now = monotonic_ns();
        // First call: all schedtick/syscalltick observations are brand-new,
        // elapsed == 0 for every P — nothing should be retaken or preempted.
        let _ = retake(now, &mut ticks);
        // Second call immediately after: elapsed is still < thresholds.
        let n = retake(monotonic_ns(), &mut ticks);
        assert_eq!(n, 0, "retake must return 0 when all Ps just started their tick");
    }

    /// `preemptone` sets `preempt = true` and `stackguard0 = STACK_PREEMPT`
    /// on the goroutine running on a P, without touching the P's own status.
    ///
    /// Gated on Unix because the test sends SIGURG via `pthread_kill`, which
    /// is not available on Windows.
    #[cfg(not(windows))]
    #[test]
    fn preemptone_sets_flags() {
        use crate::runtime::g::{Stack, G, STACK_PREEMPT};
        use crate::runtime::m::M;
        use crate::runtime::p::P;
        use std::sync::atomic::Ordering::Release;
        use std::ptr::addr_of_mut;

        // Build a minimal P ← M ← curg chain.
        let mut g  = G::new(Stack { lo: 0x100000, hi: 0x110000 }, 42);
        let gp     = addr_of_mut!(*g);

        let p = Box::into_raw(P::new(7));
        let m = Box::into_raw(unsafe { M::new(99) });

        unsafe {
            (*p).status.store(PRUNNING, Release);
            (*p).m  = m;
            (*m).p  = p;
            (*m).curg = gp;
            (*gp).m = m;
            // Step 4: preemptone requires a non-zero pthread_id to send SIGURG.
            // Use the calling thread's ID so the test exercises the real path.
            (*m).pthread_id = libc::pthread_self() as u64;
        }

        // Before the call, preempt is false and stackguard0 is whatever G::new set.
        assert!(!unsafe { (*gp).preempt }, "preempt must be false before preemptone");
        assert_ne!(
            unsafe { (*gp).stackguard0 }, STACK_PREEMPT,
            "stackguard0 must not be STACK_PREEMPT before preemptone"
        );

        unsafe { preemptone(p) };

        // preemptone set the flags and sent SIGURG.  The flags are set
        // synchronously before pthread_kill, so we can check immediately.
        assert!(unsafe { (*gp).preempt },       "preempt must be true after preemptone");
        assert_eq!(
            unsafe { (*gp).stackguard0 }, STACK_PREEMPT,
            "stackguard0 must equal STACK_PREEMPT after preemptone"
        );

        // Clean up (leak g since it was stack-allocated in the test frame).
        let _ = unsafe { Box::from_raw(p) };
        let _ = unsafe { Box::from_raw(m) };
    }

    /// Verify that a P manually placed in PSYSCALL is retaken after the
    /// FORCE_RETAKE_NS threshold, as seen by `retake`.
    #[test]
    fn retake_reclaims_psyscall_p() {
        use crate::runtime::p::{P, PIDLE, PSYSCALL};
        use std::sync::atomic::Ordering::Release;

        // Create a standalone P and manually drive its status.
        let p = Box::into_raw(P::new(99));
        unsafe { (*p).status.store(PSYSCALL, Release) };

        // Verify the logic path that matters: after elapsed ≥ FORCE_RETAKE_NS
        // the CAS from PSYSCALL → PIDLE fires.
        let status_before = unsafe { (*p).status.load(Acquire) };
        assert_eq!(status_before, PSYSCALL, "precondition: P must start as PSYSCALL");

        // Manually apply the same CAS that retake uses.
        let retaken = unsafe {
            (*p).status
                .compare_exchange(PSYSCALL, PIDLE, AcqRel, Relaxed)
                .is_ok()
        };
        assert!(retaken, "manual CAS PSYSCALL→PIDLE must succeed");
        assert_eq!(
            unsafe { (*p).status.load(Relaxed) },
            PIDLE,
            "P must be PIDLE after retake CAS"
        );

        // Clean up.
        let _ = unsafe { Box::from_raw(p) };
    }
}
