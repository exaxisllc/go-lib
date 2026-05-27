// SPDX-License-Identifier: Apache-2.0
//! Scheduler core — `schedule`, `findrunnable`, `execute`, `goexit0`, `gosched`.
//!
//! Ported from `runtime/proc.go` and `runtime/preempt.go`.
//!
//! ## Execution model
//!
//! Every M runs `schedule()` on its g0 stack.  `schedule` picks a runnable G
//! via `findrunnable`, then calls `execute` which does a `gogo` context switch
//! into that G — `execute` never returns.  When the G finishes, the `goexit`
//! trampoline calls `goexit0` back on g0, which cleans up the G and re-enters
//! `schedule`.
//!
//! ## v0.2.0 additions
//!
//! ### Stack-growth checkpoint (`execute`)
//! Before every `gogo`, `execute` calls [`stack::grow_stack_if_needed`] to
//! proactively double the stack when the saved SP is within `2 × STACK_GUARD`
//! of the guard page.  This is a belt-and-suspenders complement to the reactive
//! SIGSEGV handler in `stack.rs`.
//!
//! ### Async preemption (SIGURG)
//! `schedinit` installs a `SIGURG` handler via [`install_sigurg_handler`].
//! When `sysmon` detects that a goroutine has run for more than 10 ms it sets
//! `gp.preempt = true` then calls `pthread_kill(m.pthread_id, SIGURG)`.  The
//! signal handler ([`sigurg_handler`]) calls [`redirect_to_async_preempt`],
//! which pushes the goroutine's original PC onto its own stack and sets PC to
//! `async_preempt_trampoline`.  The trampoline (in `asm_amd64.rs` /
//! `asm_arm64.rs`) saves all live registers, calls [`async_preempt2`], and
//! restores them on resume — a transparent non-cooperative yield.
//!
//! ### Netpoll integration (`findrunnable`)
//! After the three work-stealing steps, `findrunnable` calls
//! `netpoll::netpoll_wait(0)` (non-blocking) and issues `goready` for every
//! goroutine whose I/O became ready (Unix) or whose overlapped operation
//! completed (Windows IOCP) since the last poll.
//!
//! ## Global state
//!
//! `SCHED` is a process-wide singleton initialised by `schedinit`.
//! It holds the global run queue, idle P/M lists, and `allp` (all Ps).  The
//! parts that need serialisation are guarded by `Mutex<SchedInner>`; the global
//! run queue carries its own internal lock.

use std::any::Any;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, Ordering::*};
use std::sync::{Arc, Mutex, OnceLock};

use super::g::{casgstatus, current_g, readgstatus, set_current_g, G, GDEAD, GPREEMPTED, GRUNNABLE, GRUNNING, STACK_GUARD};
use super::m::{current_m, M};
use super::p::{GlobalRunQueue, P, PIDLE, PRUNNING};
use super::stack::{grow_stack_if_needed, stack_alloc};
#[cfg(not(windows))]
use super::stack::install_sigsegv_handler;
use super::sysmon::start_sysmon;
use super::time::start_timer_thread;

// On Windows: no signal-based async preemption → don't import the trampoline.
#[cfg(all(target_arch = "x86_64", not(windows)))]
use super::asm_amd64::{async_preempt_trampoline, gogo, mcall};
#[cfg(all(target_arch = "x86_64", windows))]
use super::asm_amd64::{gogo, mcall};
#[cfg(target_arch = "aarch64")]
use super::asm_arm64::{async_preempt_trampoline, gogo, mcall};

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
    /// Current GOMAXPROCS value — readable without holding `inner`.
    /// Updated by `schedinit` and `set_gomaxprocs`.
    pub gomaxprocs:   AtomicI32,
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
        gomaxprocs:   AtomicI32::new(1),
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
#[allow(clippy::never_loop)] // loop is the intended infinite-scheduler idiom; execute() diverges
pub(crate) unsafe fn schedule() -> ! {
    let m = current_m();
    debug_assert!(!m.is_null(), "schedule: CURRENT_M is null — call set_current_m first");

    loop {
        let p = unsafe { (*m).p };
        debug_assert!(!p.is_null(), "schedule: M has no P attached");

        // Every 61 ticks drain one G from the global queue to prevent starvation.
        // 61 is a prime chosen by Go so the check is not aligned to any bursty
        // producer period.
        let tick = unsafe { (*p).schedtick.load(Relaxed) };
        if tick % 61 == 0 && sched().global_run_q.len() > 0 {
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
    let m  = current_m();
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

        // ── 4. Non-blocking netpoll: check if any I/O goroutines are ready ──
        {
            let ready = unsafe { super::netpoll::netpoll_wait(0) };
            for gp in ready {
                // Wake each I/O goroutine.  They are moved from Gwaiting →
                // Grunnable and placed into the local P's run queue.
                unsafe { super::park::goready(gp) };
            }
        }

        // ── 5. No work found — surrender P and park ───────────────────────
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
    let m  = current_m();
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
    let m = current_m();

    unsafe {
        (*m).curg  = gp;
        (*gp).m    = m;
        casgstatus(gp, GRUNNABLE, GRUNNING);
    }

    // Bump the scheduling tick on the attached P.
    let p = unsafe { (*m).p };
    if !p.is_null() {
        unsafe { (*p).schedtick.fetch_add(1, Relaxed) };
    }

    // Checkpoint growth: proactively double the stack if the saved SP is
    // within 2×STACK_GUARD of the guard page.  Prevents a SIGSEGV on the
    // very first instruction of the next quantum.
    unsafe { grow_stack_if_needed(gp) };

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
    let m = current_m();

    unsafe {
        casgstatus(gp, GRUNNING, GDEAD);
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
    let gp = current_g();
    debug_assert!(!gp.is_null(), "gosched: called from g0 or uninitialised thread");
    unsafe { mcall(gp, gosched_m) };
}

/// Mcall target for `gosched`.  Runs on g0's stack.
unsafe extern "C" fn gosched_m(gp: *mut G) {
    let m = current_m();

    unsafe {
        casgstatus(gp, GRUNNING, GRUNNABLE);
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
// Async preemption (Step 4) — SIGURG handler + asyncPreempt2
// ---------------------------------------------------------------------------

/// Previous SIGURG handler (chained if the signal is not a preemption).
#[cfg(not(windows))]
static PREV_SIGURG: Mutex<Option<libc::sigaction>> = Mutex::new(None);

/// Install the runtime's SIGURG handler for async goroutine preemption.
///
/// When sysmon wants to preempt a goroutine it sets `gp.preempt = true` then
/// calls `pthread_kill(m.pthread_id, SIGURG)`.  The signal handler detects the
/// goroutine preempt flag, pushes the goroutine's current PC onto its stack,
/// and redirects `RIP`/`PC` to [`async_preempt_trampoline`].  The trampoline
/// saves all live registers, calls `async_preempt2` (which `mcall`s into the
/// scheduler), restores all registers on resume, and `ret`s to the original PC.
///
/// **Not available on Windows** — POSIX signals do not exist there.
///
/// # Safety
/// Call once during `schedinit`.
#[cfg(not(windows))]
pub(crate) unsafe fn install_sigurg_handler() {
    let mut sa: libc::sigaction = unsafe { std::mem::zeroed() };
    sa.sa_sigaction = sigurg_handler as *const () as usize;
    // sa_flags is c_ulong on Linux and c_int on macOS; `as _` lets Rust infer the right type.
    sa.sa_flags     = (libc::SA_SIGINFO | libc::SA_ONSTACK | libc::SA_RESTART) as _;
    unsafe { libc::sigemptyset(&mut sa.sa_mask) };

    let mut old: libc::sigaction = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::sigaction(libc::SIGURG, &sa, &mut old) };
    assert_eq!(ret, 0, "install_sigurg_handler: sigaction failed");

    *PREV_SIGURG.lock().unwrap() = Some(old);
}

/// SIGURG handler: redirect a preemptable goroutine to `async_preempt_trampoline`.
#[cfg(not(windows))]
unsafe extern "C" fn sigurg_handler(
    sig:  libc::c_int,
    info: *mut libc::siginfo_t,
    ctx:  *mut libc::c_void,
) {
    let gp = current_g();
    if !gp.is_null() && unsafe { (*gp).preempt } {
        // Only redirect if the goroutine is actually GRUNNING.
        //
        // SIGURG can arrive while gp is in GWAITING (gopark transition window),
        // GSYSCALL (inside entersyscall), or GPREEMPTED.  Calling
        // redirect_to_async_preempt in those states injects the trampoline into
        // the wrong execution context, causing preemptm to call
        // casgstatus(gp, GRUNNING, GPREEMPTED) on a G that is not GRUNNING
        // which spins forever.
        //
        // Mirrors Go's wantAsyncPreempt() which gates on readgstatus == _Grunning.
        if unsafe { readgstatus(gp) } != GRUNNING {
            return; // not at an async-safe preemption point — ignore
        }
        // Redirect goroutine to the preempt trampoline.
        unsafe { redirect_to_async_preempt(gp, ctx) };
        return;
    }

    // Not our signal — chain to the previous handler.
    let prev = *PREV_SIGURG.lock().unwrap();
    match prev {
        Some(old) if old.sa_sigaction != libc::SIG_DFL
                  && old.sa_sigaction != libc::SIG_IGN => {
            type SaFn = unsafe extern "C" fn(libc::c_int, *mut libc::siginfo_t, *mut libc::c_void);
            let f: SaFn = unsafe { std::mem::transmute(old.sa_sigaction) };
            unsafe { f(sig, info, ctx) };
        }
        _ => {} // default action for SIGURG is no-op; nothing to do
    }
}

/// Redirect the goroutine's execution to `async_preempt_trampoline` by modifying
/// the interrupted register state in `ucontext_t`.
///
/// On AMD64/x86_64: push the original `RIP` onto the goroutine's stack (decrement
/// `RSP`, write to `[RSP]`), then set `RIP` = `async_preempt_trampoline`.
///
/// On AArch64: place the original `PC` into `LR` (x30), then set `PC` to the
/// trampoline.  The trampoline saves x30 and restores it before `ret`.
#[cfg(not(windows))]
#[allow(unused_variables)]
unsafe fn redirect_to_async_preempt(gp: *mut G, ctx: *mut libc::c_void) {
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    unsafe {
        let uc  = ctx as *mut libc::ucontext_t;
        let rip = (*uc).uc_mcontext.gregs[libc::REG_RIP as usize] as usize;
        let rsp = (*uc).uc_mcontext.gregs[libc::REG_RSP as usize] as usize;
        let new_rsp = rsp - 8;
        *(new_rsp as *mut usize) = rip;
        (*uc).uc_mcontext.gregs[libc::REG_RSP as usize] = new_rsp as libc::greg_t;
        (*uc).uc_mcontext.gregs[libc::REG_RIP as usize] =
            async_preempt_trampoline as usize as libc::greg_t;
    }

    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    unsafe {
        let uc  = ctx as *mut libc::ucontext_t;
        // Save original PC into x30 (LR); the trampoline saves and restores x30.
        (*uc).uc_mcontext.regs[30] = (*uc).uc_mcontext.pc;
        (*uc).uc_mcontext.pc = async_preempt_trampoline as u64;
    }

    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    unsafe {
        let uc  = ctx as *mut libc::ucontext_t;
        let ss  = &mut (*(*uc).uc_mcontext).__ss;
        let rip = ss.__rip as usize;
        let rsp = ss.__rsp as usize;
        let new_rsp = rsp - 8;
        *(new_rsp as *mut usize) = rip;
        ss.__rsp = new_rsp as u64;
        ss.__rip = async_preempt_trampoline as *const () as u64;
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    unsafe {
        let uc  = ctx as *mut libc::ucontext_t;
        let ss  = &mut (*(*uc).uc_mcontext).__ss;
        ss.__lr = ss.__pc;
        ss.__pc = async_preempt_trampoline as u64;
    }
}

/// Called by `async_preempt_trampoline` after all live registers have been
/// saved to the goroutine's stack.
///
/// Performs a cooperative yield via `mcall → schedule()`.  When the goroutine
/// is resumed by `gogo`, execution returns here; the trampoline then restores
/// the saved registers and `ret`s to the original interrupted PC.
///
/// Ported from `asyncPreempt2` in `runtime/preempt.go`.
#[unsafe(no_mangle)]
pub(crate) unsafe extern "C" fn async_preempt2() {
    let gp = current_g();
    if gp.is_null() {
        return;
    }

    // Defensive second check: the goroutine must still be GRUNNING when we
    // reach here.  sigurg_handler already gates on readgstatus == GRUNNING,
    // but a narrow race can occur on multi-core systems between the signal
    // check and the trampoline executing.  Bailing here prevents preemptm
    // from calling casgstatus(gp, GRUNNING, GPREEMPTED) on a non-GRUNNING G
    // which would spin forever in the CAS retry loop.
    if unsafe { readgstatus(gp) } != GRUNNING {
        return;
    }

    // Clear preempt flags (sysmon will re-set them next time).
    unsafe {
        (*gp).preempt     = false;
        (*gp).stackguard0 = (*gp).stack.lo + STACK_GUARD;
    }

    // mcall saves this goroutine's state, switches to g0, and calls preemptm.
    // When the goroutine is rescheduled via gogo, mcall returns here.
    unsafe { mcall(gp, preemptm) };
}

/// `mcall` target for async preemption.  Runs on g0's stack.
///
/// Transitions `GRUNNING → GPREEMPTED` (a GC-safe scan point) then immediately
/// to `GPREEMPTED → GRUNNABLE`, detaches the goroutine from the M, and
/// re-enters the scheduler — equivalent to `gosched_m` but called from a
/// signal context.
///
/// The two-step transition matches Go 1.14+: the brief `GPREEMPTED` window lets
/// a future GC scanner observe that the goroutine was stopped at an async-safe
/// point and scan its stack before it becomes runnable again.
unsafe extern "C" fn preemptm(gp: *mut G) {
    let m = current_m();
    unsafe {
        // GRUNNING → GPREEMPTED: goroutine is at an async-safe preemption point.
        casgstatus(gp, GRUNNING, GPREEMPTED);
        // GPREEMPTED → GRUNNABLE: immediately re-queue (no GC scanner yet).
        casgstatus(gp, GPREEMPTED, GRUNNABLE);
        (*gp).m   = ptr::null_mut();
        (*m).curg = ptr::null_mut();
        set_current_g(ptr::null_mut());
        (*gp).schedlink = ptr::null_mut();
        sched().global_run_q.push_batch(gp, gp, 1);
    }
    unsafe { schedule() };
}

// ---------------------------------------------------------------------------
// SIGBUS handler — diagnostic crash reporter
// ---------------------------------------------------------------------------

/// Previous SIGBUS handler (chained on non-scheduler signals).
#[cfg(not(windows))]
static PREV_SIGBUS: Mutex<Option<libc::sigaction>> = Mutex::new(None);

/// Install a SIGBUS handler that prints PC/SP/LR and a forced backtrace before
/// calling `abort`.  This surfaces crashes in background scheduler threads that
/// would otherwise kill the process with no output.
///
/// # Safety
/// Call once during `schedinit`.
#[cfg(not(windows))]
pub(crate) unsafe fn install_sigbus_handler() {
    let mut sa: libc::sigaction = unsafe { std::mem::zeroed() };
    sa.sa_sigaction = sigbus_handler as *const () as usize;
    // SA_ONSTACK: run on the thread's alternate signal stack (installed by
    // M::start) so the handler survives even if the goroutine stack overflowed.
    sa.sa_flags = (libc::SA_SIGINFO | libc::SA_ONSTACK) as _;
    unsafe { libc::sigemptyset(&mut sa.sa_mask) };

    let mut old: libc::sigaction = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::sigaction(libc::SIGBUS, &sa, &mut old) };
    assert_eq!(ret, 0, "install_sigbus_handler: sigaction failed");

    *PREV_SIGBUS.lock().unwrap() = Some(old);
}

/// SIGBUS handler: print crash context (registers + backtrace) then abort.
///
/// Not async-signal-safe in the strictest sense, but we are aborting anyway;
/// the goal is to emit useful diagnostics before the process exits.
#[cfg(not(windows))]
unsafe extern "C" fn sigbus_handler(
    _sig:  libc::c_int,
    _info: *mut libc::siginfo_t,
    ctx:   *mut libc::c_void,
) {
    // Use write() directly for the first line — it is async-signal-safe.
    let msg = b"[go-lib SIGBUS] crash detected\n";
    unsafe { libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len()) };

    // Print register context from the interrupted ucontext_t.
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    if !ctx.is_null() {
        unsafe {
            let uc = ctx as *mut libc::ucontext_t;
            let ss = &(*(*uc).uc_mcontext).__ss;
            eprintln!("[go-lib SIGBUS] PC  = {:#018x}", ss.__pc);
            eprintln!("[go-lib SIGBUS] LR  = {:#018x}", ss.__lr);
            eprintln!("[go-lib SIGBUS] SP  = {:#018x}", ss.__sp);
            eprintln!("[go-lib SIGBUS] FP  = {:#018x}", ss.__fp);
            // Print non-zero GPRs (x0–x28) for more call-site context.
            for (i, r) in ss.__x.iter().enumerate() {
                if *r != 0 {
                    eprintln!("[go-lib SIGBUS] x{i:02} = {r:#018x}");
                }
            }
        }
    }

    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    if !ctx.is_null() {
        unsafe {
            let uc = ctx as *mut libc::ucontext_t;
            let mc = &(*uc).uc_mcontext;
            eprintln!("[go-lib SIGBUS] PC  = {:#018x}", mc.pc);
            eprintln!("[go-lib SIGBUS] SP  = {:#018x}", mc.sp);
            eprintln!("[go-lib SIGBUS] LR  = {:#018x}", mc.regs[30]);
            for (i, r) in mc.regs.iter().enumerate() {
                if *r != 0 {
                    eprintln!("[go-lib SIGBUS] x{i:02} = {r:#018x}");
                }
            }
        }
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    if !ctx.is_null() {
        unsafe {
            let uc  = ctx as *mut libc::ucontext_t;
            let rip = (*uc).uc_mcontext.gregs[libc::REG_RIP as usize];
            let rsp = (*uc).uc_mcontext.gregs[libc::REG_RSP as usize];
            eprintln!("[go-lib SIGBUS] RIP = {rip:#018x}");
            eprintln!("[go-lib SIGBUS] RSP = {rsp:#018x}");
        }
    }

    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    if !ctx.is_null() {
        unsafe {
            let uc = ctx as *mut libc::ucontext_t;
            let ss = &(*(*uc).uc_mcontext).__ss;
            eprintln!("[go-lib SIGBUS] RIP = {:#018x}", ss.__rip);
            eprintln!("[go-lib SIGBUS] RSP = {:#018x}", ss.__rsp);
        }
    }

    // force_capture() captures regardless of RUST_BACKTRACE.
    let bt = std::backtrace::Backtrace::force_capture();
    eprintln!("[go-lib SIGBUS] backtrace:\n{bt}");

    unsafe { libc::abort() };
}

// ---------------------------------------------------------------------------
// Windows VEH — vectored exception handler for STATUS_ACCESS_VIOLATION
// ---------------------------------------------------------------------------

/// Guard against recursive exceptions inside the VEH itself.
#[cfg(windows)]
static VEH_HANDLING: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Windows kernel32 imports for the VEH.
#[cfg(windows)]
mod win_veh_sys {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        /// Register a vectored exception handler.
        /// `first_handler = 1` places it before all SEH handlers.
        pub fn AddVectoredExceptionHandler(
            FirstHandler:    u32,
            VectoredHandler: unsafe extern "system" fn(*mut u8) -> i32,
        ) -> *mut u8;

        /// Return the standard-error file handle (STD_ERROR_HANDLE = 0xFFFFFFF4).
        pub fn GetStdHandle(nStdHandle: u32) -> *mut u8;

        /// Synchronous write to a file handle.  Safe to call from a VEH
        /// because it does not allocate heap memory.
        pub fn WriteFile(
            hFile:                  *mut u8,
            lpBuffer:               *const u8,
            nNumberOfBytesToWrite:  u32,
            lpNumberOfBytesWritten: *mut u32,
            lpOverlapped:           *mut u8,
        ) -> i32;

        /// Flush all pending writes on a file handle (drains the pipe buffer
        /// so data is readable by the parent process before we terminate).
        pub fn FlushFileBuffers(hFile: *mut u8) -> i32;

        /// Return a pseudo-handle for the current process.
        pub fn GetCurrentProcess() -> *mut u8;

        /// Terminate the process immediately — does NOT run atexit/DLL detach
        /// hooks, which makes it safe to call from inside a VEH where locks
        /// may be held.
        pub fn TerminateProcess(hProcess: *mut u8, uExitCode: u32) -> i32;

        /// Create or open a file.  Used to write crash diagnostics to disk so
        /// they survive pipe teardown when the process exits.
        pub fn CreateFileW(
            lpFileName:            *const u16,
            dwDesiredAccess:       u32,
            dwShareMode:           u32,
            lpSecurityAttributes:  *mut u8,
            dwCreationDisposition: u32,
            dwFlagsAndAttributes:  u32,
            hTemplateFile:         *mut u8,
        ) -> *mut u8;

        /// Close a kernel object handle.
        pub fn CloseHandle(hObject: *mut u8) -> i32;

        /// Sleep for `dwMilliseconds` milliseconds.  Called before
        /// `TerminateProcess` to give the parent process (cargo) time to drain
        /// the stderr pipe and capture the VEH output in CI logs.
        pub fn Sleep(dwMilliseconds: u32);
    }
}

/// Write a static byte slice to a Windows HANDLE using WriteFile.
///
/// Async-signal-safe: no heap allocation, no Rust I/O locks.
#[cfg(windows)]
unsafe fn veh_write(h: *mut u8, s: &[u8]) {
    let mut n = 0u32;
    unsafe {
        win_veh_sys::WriteFile(h, s.as_ptr(), s.len() as u32, &mut n,
                               core::ptr::null_mut());
    }
}

/// Open (or create-and-append to) the VEH crash file and return its HANDLE.
///
/// Path: `.\go-lib-crash-veh.txt` (relative to the working directory —
/// in CI this lands in `D:\a\go-lib\go-lib\` next to the binary).
///
/// Opening with `FILE_APPEND_DATA` causes every `WriteFile` call to append
/// to the end of the file regardless of the current file pointer, so both
/// the VEH-install marker and a subsequent crash dump accumulate in the
/// same file without overwriting each other.
///
/// Returns `INVALID_HANDLE_VALUE` on failure so callers can skip file writes.
#[cfg(windows)]
unsafe fn veh_open_crash_file() -> *mut u8 {
    // UTF-16LE: ".\go-lib-crash-veh.txt\0"
    const PATH: &[u16] = &[
        b'.' as u16, b'\\' as u16,
        b'g' as u16, b'o' as u16, b'-' as u16, b'l' as u16, b'i' as u16,
        b'b' as u16, b'-' as u16, b'c' as u16, b'r' as u16, b'a' as u16,
        b's' as u16, b'h' as u16, b'-' as u16, b'v' as u16, b'e' as u16,
        b'h' as u16, b'.' as u16, b't' as u16, b'x' as u16, b't' as u16,
        0u16, // NUL terminator
    ];
    // FILE_APPEND_DATA (0x4) alone — every WriteFile goes to the end of file.
    // Using only FILE_APPEND_DATA (not GENERIC_WRITE) prevents accidental
    // truncation and allows multiple opens (install marker + crash dump) to
    // accumulate in the same file.
    const FILE_APPEND_DATA:     u32 = 0x0000_0004;
    const FILE_SHARE_READ:      u32 = 0x0000_0001;
    const OPEN_ALWAYS:          u32 = 4; // create if absent, open if present
    const FILE_ATTRIBUTE_NORMAL: u32 = 0x80;

    unsafe {
        win_veh_sys::CreateFileW(
            PATH.as_ptr(),
            FILE_APPEND_DATA,
            FILE_SHARE_READ,
            core::ptr::null_mut(),
            OPEN_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            core::ptr::null_mut(),
        )
    }
}

/// Write `val` as `0xXXXXXXXXXXXXXXXX\r\n` to `h`.
///
/// Uses only stack-allocated storage — safe from a VEH.
#[cfg(windows)]
unsafe fn veh_write_hex(h: *mut u8, val: u64) {
    const HEX: &[u8] = b"0123456789abcdef";
    // "0x" + 16 hex digits + "\r\n" = 20 bytes
    let mut buf = [b'0'; 20];
    buf[0] = b'0'; buf[1] = b'x';
    for i in 0..16usize {
        buf[17 - i] = HEX[((val >> (i * 4)) & 0xf) as usize];
    }
    buf[18] = b'\r'; buf[19] = b'\n';
    let mut n = 0u32;
    unsafe {
        win_veh_sys::WriteFile(h, buf.as_ptr(), 20, &mut n,
                               core::ptr::null_mut());
    }
}

/// Install a Vectored Exception Handler that catches `STATUS_ACCESS_VIOLATION`,
/// prints diagnostics (faulting address, fault target, RSP/RIP from the
/// interrupted CONTEXT, and a Rust backtrace), then exits.
///
/// This is the Windows equivalent of the Unix SIGBUS/SIGSEGV handlers and
/// gives us the crash location we need to diagnose scheduler bugs.
///
/// # Safety
/// Call once during `schedinit`.
#[cfg(windows)]
pub(crate) fn install_windows_veh() {
    unsafe {
        win_veh_sys::AddVectoredExceptionHandler(1, windows_veh_handler);

        // Write an installation marker to both stderr and the crash file so we
        // can verify in CI that the VEH was registered before any crash.
        let stderr = win_veh_sys::GetStdHandle(0xFFFF_FFF4u32); // STD_ERROR_HANDLE
        let marker = b"[go-lib] VEH installed\r\n";
        let mut n = 0u32;
        win_veh_sys::WriteFile(stderr, marker.as_ptr(), marker.len() as u32,
                               &mut n, core::ptr::null_mut());
        win_veh_sys::FlushFileBuffers(stderr);

        let fh = veh_open_crash_file();
        const INVALID_HANDLE: *mut u8 = usize::MAX as *mut u8;
        if !fh.is_null() && fh != INVALID_HANDLE {
            win_veh_sys::WriteFile(fh, marker.as_ptr(), marker.len() as u32,
                                   &mut n, core::ptr::null_mut());
            win_veh_sys::CloseHandle(fh);
        }
    }
}

/// Vectored Exception Handler: print crash diagnostics and exit.
///
/// Uses **only** `WriteFile` (no heap allocation, no Rust I/O locks) so it
/// cannot deadlock inside the exception context.  `eprintln!` + backtrace
/// capture were removed because they both allocate and could acquire locks
/// already held by the crashing thread, causing a deadlock before any output
/// is written.
///
/// ## EXCEPTION_POINTERS layout (x64)
/// ```text
/// +0  *EXCEPTION_RECORD
/// +8  *CONTEXT
/// ```
///
/// ## EXCEPTION_RECORD layout (x64)
/// ```text
/// +0   ExceptionCode      (u32)
/// +4   ExceptionFlags     (u32)
/// +8   *ExceptionRecord   (usize — chained record)
/// +16  ExceptionAddress   (usize — faulting instruction)
/// +24  NumberParameters   (u32)
/// +28  _pad               (u32)
/// +32  ExceptionInformation[0..14] (usize each)
///        [0] = 0 (read) | 1 (write) | 8 (DEP)
///        [1] = inaccessible target address
/// ```
///
/// ## CONTEXT offsets for x64 (from WinNT.h — stable across Windows versions)
/// ```text
/// +120 Rax   +128 Rcx   +136 Rdx   +144 Rbx
/// +152 Rsp   +160 Rbp   +168 Rsi   +176 Rdi
/// +184 R8  … +240 R15
/// +248 Rip
/// ```
#[cfg(windows)]
unsafe extern "system" fn windows_veh_handler(ep: *mut u8) -> i32 {
    const STATUS_ACCESS_VIOLATION: u32 = 0xC000_0005;
    const STATUS_STACK_OVERFLOW:   u32 = 0xC000_00FD;
    const EXCEPTION_CONTINUE_SEARCH: i32 = 0;
    const STD_ERROR_HANDLE: u32 = 0xFFFF_FFF4; // ((DWORD)-12)

    // Prevent re-entry (e.g. if a WriteFile call itself faults).
    if VEH_HANDLING.swap(true, std::sync::atomic::Ordering::AcqRel) {
        return EXCEPTION_CONTINUE_SEARCH;
    }

    let h = unsafe { win_veh_sys::GetStdHandle(STD_ERROR_HANDLE) };

    // --- Decode EXCEPTION_POINTERS ---
    // struct EXCEPTION_POINTERS { *EXCEPTION_RECORD; *CONTEXT }
    // Each field is a native pointer (8 bytes on x64).
    let ptrs    = ep as *const usize;
    let er_ptr  = unsafe { *ptrs       } as *const u8; // EXCEPTION_RECORD*
    let ctx_ptr = unsafe { *ptrs.add(1) } as *const u8; // CONTEXT*

    // --- Filter by exception code ---
    let code: u32 = unsafe { (er_ptr as *const u32).read() };
    if code != STATUS_ACCESS_VIOLATION && code != STATUS_STACK_OVERFLOW {
        VEH_HANDLING.store(false, std::sync::atomic::Ordering::Release);
        return EXCEPTION_CONTINUE_SEARCH;
    }

    // --- Emit diagnostics (WriteFile only — no heap, no Rust locks) ---
    unsafe {
        // Header
        veh_write(h, b"[go-lib VEH] exception code   : ");
        veh_write_hex(h, code as u64);

        // Faulting instruction address
        let instr: usize = *(er_ptr.add(16) as *const usize);
        veh_write(h, b"[go-lib VEH] ExceptionAddress : ");
        veh_write_hex(h, instr as u64);

        // Fault type and target address (AV parameters)
        let n_params: u32 = *(er_ptr.add(24) as *const u32);
        if n_params >= 2 {
            let ft: usize = *(er_ptr.add(32) as *const usize);
            let fa: usize = *(er_ptr.add(40) as *const usize);
            veh_write(h, b"[go-lib VEH] fault_type (0=rd,1=wr,8=DEP) : ");
            veh_write_hex(h, ft as u64);
            veh_write(h, b"[go-lib VEH] fault_addr  : ");
            veh_write_hex(h, fa as u64);
        }

        // Key registers from CONTEXT (x64 stable offsets)
        let rip: u64 = *(ctx_ptr.add(248) as *const u64);
        let rsp: u64 = *(ctx_ptr.add(152) as *const u64);
        let rbp: u64 = *(ctx_ptr.add(160) as *const u64);
        let rax: u64 = *(ctx_ptr.add(120) as *const u64);
        let rcx: u64 = *(ctx_ptr.add(128) as *const u64);
        let rdx: u64 = *(ctx_ptr.add(136) as *const u64);
        let rbx: u64 = *(ctx_ptr.add(144) as *const u64);
        let rsi: u64 = *(ctx_ptr.add(168) as *const u64);
        let rdi: u64 = *(ctx_ptr.add(176) as *const u64);
        let r8:  u64 = *(ctx_ptr.add(184) as *const u64);
        let r9:  u64 = *(ctx_ptr.add(192) as *const u64);

        veh_write(h, b"[go-lib VEH] RIP : "); veh_write_hex(h, rip);
        veh_write(h, b"[go-lib VEH] RSP : "); veh_write_hex(h, rsp);
        veh_write(h, b"[go-lib VEH] RBP : "); veh_write_hex(h, rbp);
        veh_write(h, b"[go-lib VEH] RAX : "); veh_write_hex(h, rax);
        veh_write(h, b"[go-lib VEH] RCX : "); veh_write_hex(h, rcx);
        veh_write(h, b"[go-lib VEH] RDX : "); veh_write_hex(h, rdx);
        veh_write(h, b"[go-lib VEH] RBX : "); veh_write_hex(h, rbx);
        veh_write(h, b"[go-lib VEH] RSI : "); veh_write_hex(h, rsi);
        veh_write(h, b"[go-lib VEH] RDI : "); veh_write_hex(h, rdi);
        veh_write(h, b"[go-lib VEH] R8  : "); veh_write_hex(h, r8);
        veh_write(h, b"[go-lib VEH] R9  : "); veh_write_hex(h, r9);

        // Mirror diagnostics to stdout (in case stderr pipe is broken in CI).
        let stdout = win_veh_sys::GetStdHandle(0xFFFF_FFF5u32); // STD_OUTPUT_HANDLE = -11
        veh_write(stdout, b"[go-lib VEH] exception code   : ");
        veh_write_hex(stdout, code as u64);
        let instr2: usize = *(er_ptr.add(16) as *const usize);
        veh_write(stdout, b"[go-lib VEH] ExceptionAddress : ");
        veh_write_hex(stdout, instr2 as u64);
        let rip2: u64 = *(ctx_ptr.add(248) as *const u64);
        let rsp2: u64 = *(ctx_ptr.add(152) as *const u64);
        veh_write(stdout, b"[go-lib VEH] RIP : "); veh_write_hex(stdout, rip2);
        veh_write(stdout, b"[go-lib VEH] RSP : "); veh_write_hex(stdout, rsp2);
        win_veh_sys::FlushFileBuffers(stdout);

        // Write ALL diagnostics to a crash file that survives pipe teardown.
        // In CI the file lands at <cwd>\go-lib-crash-veh.txt.
        let fh = veh_open_crash_file();
        const INVALID_HANDLE: *mut u8 = usize::MAX as *mut u8;
        if !fh.is_null() && fh != INVALID_HANDLE {
            veh_write(fh, b"[go-lib VEH] exception code   : ");
            veh_write_hex(fh, code as u64);
            let instr3: usize = *(er_ptr.add(16) as *const usize);
            veh_write(fh, b"[go-lib VEH] ExceptionAddress : ");
            veh_write_hex(fh, instr3 as u64);
            if n_params >= 2 {
                let ft2: usize = *(er_ptr.add(32) as *const usize);
                let fa2: usize = *(er_ptr.add(40) as *const usize);
                veh_write(fh, b"[go-lib VEH] fault_type : ");
                veh_write_hex(fh, ft2 as u64);
                veh_write(fh, b"[go-lib VEH] fault_addr : ");
                veh_write_hex(fh, fa2 as u64);
            }
            let rip3: u64 = *(ctx_ptr.add(248) as *const u64);
            let rsp3: u64 = *(ctx_ptr.add(152) as *const u64);
            let rbp3: u64 = *(ctx_ptr.add(160) as *const u64);
            let rax3: u64 = *(ctx_ptr.add(120) as *const u64);
            let rcx3: u64 = *(ctx_ptr.add(128) as *const u64);
            let rdx3: u64 = *(ctx_ptr.add(136) as *const u64);
            veh_write(fh, b"[go-lib VEH] RIP : "); veh_write_hex(fh, rip3);
            veh_write(fh, b"[go-lib VEH] RSP : "); veh_write_hex(fh, rsp3);
            veh_write(fh, b"[go-lib VEH] RBP : "); veh_write_hex(fh, rbp3);
            veh_write(fh, b"[go-lib VEH] RAX : "); veh_write_hex(fh, rax3);
            veh_write(fh, b"[go-lib VEH] RCX : "); veh_write_hex(fh, rcx3);
            veh_write(fh, b"[go-lib VEH] RDX : "); veh_write_hex(fh, rdx3);
            win_veh_sys::CloseHandle(fh);
        }

        // Flush the stderr pipe before exiting so the parent process (cargo)
        // can read all bytes written above.
        win_veh_sys::FlushFileBuffers(h);

        // Sleep briefly to give cargo's pipe reader time to drain the buffer.
        // Without this, TerminateProcess can race with the parent's read loop.
        win_veh_sys::Sleep(500);

        // TerminateProcess is safer than ExitProcess inside a VEH: it skips
        // all atexit/DLL-detach hooks that might try to acquire locks already
        // held by the crashing thread.
        win_veh_sys::TerminateProcess(win_veh_sys::GetCurrentProcess(), code);
    }

    // Unreachable — TerminateProcess does not return for the current process.
    EXCEPTION_CONTINUE_SEARCH
}

// ---------------------------------------------------------------------------
// Goroutine creation — goroutine_entry, goexit_trampoline, new_goroutine
// ---------------------------------------------------------------------------

/// Opaque wrapper that owns a `Box<dyn FnOnce()>` behind a thin (single-word)
/// pointer.  Stored in `G.sched.ctxt` so `goroutine_entry` can retrieve it.
struct GoFn(Box<dyn FnOnce() + Send + 'static>);

// ---------------------------------------------------------------------------
// Goroutine panic handler
// ---------------------------------------------------------------------------

/// User-settable handler for goroutine panics.  `None` → default stderr print.
///
/// Stored as `Arc<dyn Fn>` so we can clone it out of the lock before calling,
/// preventing a deadlock if the handler itself calls `set_panic_handler`.
type PanicFn = Arc<dyn Fn(Box<dyn Any + Send + 'static>) + Send + Sync + 'static>;
static PANIC_HANDLER: Mutex<Option<PanicFn>> = Mutex::new(None);

/// Register `f` as the global goroutine-panic handler.
///
/// `f` is called (on the scheduler loop, NOT the panicking goroutine's stack)
/// with the panic payload whenever a goroutine's body panics.  The default
/// handler prints the payload to stderr.  Calling `set_panic_handler` again
/// replaces the previous handler.
///
/// The process does **not** abort after the handler returns; the scheduler
/// continues running other goroutines.
pub fn set_panic_handler<F>(f: F)
where
    F: Fn(Box<dyn Any + Send + 'static>) + Send + Sync + 'static,
{
    *PANIC_HANDLER.lock().unwrap() = Some(Arc::new(f));
}

/// Called from `goroutine_entry` when a goroutine's body panics.
fn handle_goroutine_panic(payload: Box<dyn Any + Send + 'static>) {
    // Clone the Arc out before releasing the lock so the handler can call
    // `set_panic_handler` without deadlocking.
    let handler = PANIC_HANDLER.lock().unwrap_or_else(|e| e.into_inner()).clone();
    match handler {
        Some(f) => f(payload),
        None    => {
            // Default: print to stderr, matching Go's "goroutine panicked" output.
            let msg = payload.downcast_ref::<String>()
                .map(|s| s.as_str())
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("(unknown panic payload)");
            eprintln!("goroutine panicked: {msg}");
        }
    }
}

// ---------------------------------------------------------------------------
// GOMAXPROCS — query and dynamic adjustment
// ---------------------------------------------------------------------------

/// Return the current value of GOMAXPROCS (number of active logical processors).
pub fn gomaxprocs() -> usize {
    sched().gomaxprocs.load(Relaxed) as usize
}

/// Set GOMAXPROCS to `n` (clamped to `[1, 256]`) and return the previous value.
///
/// **Increasing** — allocates new Ps and spawns one M per new P; takes effect
/// immediately.
///
/// **Decreasing** — updates the counter so `gomaxprocs()` returns the new
/// value; excess Ps remain in `allp` but their Ms will not be recruited for
/// new goroutines until GOMAXPROCS is increased again.  Work-stealing
/// continues across the full `allp` slice for v0.2.0.
///
/// Has no effect before the scheduler is initialised (before `run()`).
pub fn set_gomaxprocs(n: usize) -> usize {
    let n = n.clamp(1, 256) as i32;
    let sc = sched();

    let old = {
        let mut inner = sc.inner.lock().unwrap();
        let old = inner.gomaxprocs;

        if n > old {
            // Add Ps for the new slots.
            let new_ps: Vec<*mut P> = (old..n)
                .map(|id| Box::into_raw(P::new(id)))
                .collect();
            inner.allp.extend_from_slice(&new_ps);
            inner.gomaxprocs = n;
            sc.gomaxprocs.store(n, Relaxed);
            drop(inner);

            // Spawn one M per new P (mirrors schedinit).
            for p_ptr in new_ps {
                let id = NEXT_MID.fetch_add(1, Relaxed);
                unsafe { spawn_m(id, p_ptr) };
            }
        } else {
            // Decrease: just update the counters.  Excess Ms self-park when
            // they find no work; they can be re-recruited if GOMAXPROCS rises.
            inner.gomaxprocs = n;
            sc.gomaxprocs.store(n, Relaxed);
        }

        old
    };

    old as usize
}

// ---------------------------------------------------------------------------
// Goroutine ID / M ID counters
// ---------------------------------------------------------------------------

/// Monotonically-increasing goroutine ID counter.  Starts at 1; 0 is reserved
/// for g0 goroutines (one per M).
static NEXT_GOID: AtomicU64 = AtomicU64::new(1);

/// Monotonically-increasing M ID counter.
static NEXT_MID: AtomicI64 = AtomicI64::new(1);

/// Guards `schedinit` so it runs at most once per process.
static INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Entry point for every user goroutine.
///
/// Called by `gogo` (via `jmp` on AMD64, `br` on AArch64) with the stack and
/// registers set up to look as if the goroutine was entered via a normal
/// function call.  Retrieves the closure from `G.sched.ctxt`, calls it, then
/// returns — which falls through to the `goexit_trampoline` address that was
/// pre-loaded as the return address (AMD64: pushed onto the stack; AArch64:
/// placed in the link register `x30`).
///
/// Ported from `runtime·goexit` + Go's goroutine creation mechanism in
/// `runtime/proc.go` and `runtime/asm_{amd64,arm64}.s`.
unsafe extern "C" fn goroutine_entry() {
    let gp = current_g();
    let go_fn = unsafe {
        let fn_ptr = (*gp).sched.ctxt as *mut GoFn;
        (*gp).sched.ctxt = ptr::null_mut();
        Box::from_raw(fn_ptr)
    };

    // Catch panics so they don't abort the process.  The closure may capture
    // non-UnwindSafe types (raw pointers, RefCell, …) so we assert that it is
    // safe — the goroutine's stack is unwound by catch_unwind and no invariants
    // observable to other goroutines are left broken (channels are locked
    // briefly and always released before goroutines block or return).
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (go_fn.0)()));
    if let Err(payload) = result {
        handle_goroutine_panic(payload);
    }

    // Returning here drops through to goexit_trampoline via the pre-wired
    // return address (AMD64: [rsp], AArch64: x30 / lr).
}

// ---------------------------------------------------------------------------
// goexit_trampoline — architecture-specific return target
// ---------------------------------------------------------------------------

// AMD64: The trampoline is entered via the CPU's `ret` instruction, which
// pops a return address and jumps to it.  That means the stack pointer at
// entry is 16-byte aligned (stack.hi), NOT the ABI-expected 8 mod 16.  A
// naked function with no prologue/epilogue preserves that alignment so that
// the subsequent `call goexit0_handler` pushes a return address and arrives
// at goexit0_handler with sp = stack.hi - 8 (8 mod 16) — the ABI-correct
// alignment for a callee entry point.
#[cfg(target_arch = "x86_64")]
#[unsafe(naked)]
unsafe extern "C" fn goexit_trampoline() -> ! {
    core::arch::naked_asm!(
        "call {handler}",
        "ud2",          // should never be reached
        handler = sym goexit0_handler,
    )
}

/// Helper called by the AMD64 `goexit_trampoline`.  Runs as a normal function
/// with correct stack alignment; switches to g0 via `mcall` and calls
/// `goexit0`.
///
/// This function genuinely never returns: `goexit0` calls `schedule()` which
/// loops forever.  The `unreachable_unchecked` is sound because dying
/// goroutines are never resumed via `gogo` — they are dropped in `goexit0`.
#[cfg(target_arch = "x86_64")]
unsafe extern "C" fn goexit0_handler() -> ! {
    let gp = current_g();
    unsafe { mcall(gp, goexit0) };
    // SAFETY: goexit0 → schedule() is an infinite loop; this is unreachable.
    unsafe { std::hint::unreachable_unchecked() }
}

// AArch64: The trampoline is stored in gobuf.lr and loaded into x30 by
// `gogo_asm`.  When `goroutine_entry` executes `ret`, the CPU branches to
// x30 (= goexit_trampoline).  Stack pointer is 16-byte aligned at entry
// (stack.hi), which is the correct AArch64 alignment, so a plain `extern "C"`
// function works with no naked-asm tricks.
#[cfg(target_arch = "aarch64")]
unsafe extern "C" fn goexit_trampoline() -> ! {
    let gp = current_g();
    unsafe { mcall(gp, goexit0) };
    // SAFETY: goexit0 → schedule() is an infinite loop; this is unreachable.
    unsafe { std::hint::unreachable_unchecked() }
}

/// Allocate and initialise a new goroutine G that will run `f`.
///
/// Sets up the initial stack frame so that when `gogo` jumps into the G:
/// - `goroutine_entry` is the first instruction executed.
/// - Returning from `goroutine_entry` lands in `goexit_trampoline`.
/// - `G.sched.ctxt` holds a thin pointer to the heap-allocated closure.
///
/// Ported from `newproc1` in `runtime/proc.go`.
pub(crate) fn new_goroutine(f: impl FnOnce() + Send + 'static) -> Box<G> {
    let stack = unsafe { stack_alloc().expect("new_goroutine: stack_alloc failed") };
    let goid  = NEXT_GOID.fetch_add(1, Relaxed);
    let mut g  = G::new(stack, goid);

    // Heap-allocate the closure behind a thin pointer and store it in ctxt.
    let go_fn   = Box::new(GoFn(Box::new(f)));
    g.sched.ctxt = Box::into_raw(go_fn) as *mut u8;
    g.sched.pc   = goroutine_entry as *const () as usize;

    // Architecture-specific: wire the goexit_trampoline as the return target.
    #[cfg(target_arch = "x86_64")]
    {
        // Write the trampoline address into the word just below stack.hi, then
        // set sp to point at that word.  goroutine_entry's `ret` pops it and
        // jumps there.
        let ret_slot = (g.stack.hi - 8) as *mut usize;
        unsafe { ret_slot.write(goexit_trampoline as *const () as usize) };
        g.sched.sp = g.stack.hi - 8;
        g.sched.bp = 0; // null frame-pointer marks the root of the call chain
    }

    #[cfg(target_arch = "aarch64")]
    {
        // Store the trampoline in the link-register slot of the Gobuf so that
        // gogo_asm loads it into x30.  goroutine_entry's `ret` branches to x30.
        g.sched.lr = goexit_trampoline as usize;
        g.sched.sp = g.stack.hi;
        g.sched.bp = 0;
    }

    g.atomicstatus.store(GRUNNABLE, Release);
    g
}

// ---------------------------------------------------------------------------
// spawn_goroutine — create G, enqueue, wake an M
// ---------------------------------------------------------------------------

/// Create a goroutine for `f`, push it to the global run queue, and wake an
/// idle M if one is available.
///
/// The goroutine will be picked up by whichever M's `findrunnable` finds it
/// first.
///
/// # Precondition
///
/// The go-lib scheduler must be running (i.e. [`schedinit`] has been called).
/// Violating this precondition will not cause undefined behaviour — the goroutine
/// is simply never executed — but it is almost certainly a programming mistake.
/// A `debug_assert` fires in debug builds if called before the scheduler starts.
pub(crate) fn spawn_goroutine(f: impl FnOnce() + Send + 'static) {
    debug_assert!(
        INITIALIZED.load(Acquire),
        "spawn_goroutine called before schedinit; goroutine will never run"
    );
    let g_ptr = Box::into_raw(new_goroutine(f));
    // SAFETY: g_ptr is a freshly-allocated, uniquely-owned G.  push_batch and
    // startm are unsafe because they manipulate the scheduler's internal queues
    // without a typed lock; their preconditions (non-null pointer, valid G
    // layout) are satisfied by the new_goroutine constructor above.
    unsafe {
        (*g_ptr).schedlink = ptr::null_mut();
        sched().global_run_q.push_batch(g_ptr, g_ptr, 1);
        startm(ptr::null_mut());
    }
}

// ---------------------------------------------------------------------------
// schedinit — create Ps and spawn M threads
// ---------------------------------------------------------------------------

/// Initialise the scheduler: create `nprocs` Ps and spawn one M per P.
///
/// Idempotent — subsequent calls are no-ops.
///
/// Ported from `schedinit` + `procresize` + `mstart` in `runtime/proc.go`.
pub(crate) fn schedinit(nprocs: i32) {
    // Ensure we only initialise once even under concurrent callers.
    if INITIALIZED.swap(true, AcqRel) {
        return;
    }
    assert!(nprocs >= 1, "schedinit: nprocs must be ≥ 1");

    // Allow the GOMAXPROCS environment variable to override the caller's value.
    let nprocs = std::env::var("GOMAXPROCS")
        .ok()
        .and_then(|s| s.parse::<i32>().ok())
        .filter(|&n| (1..=256).contains(&n))
        .unwrap_or(nprocs);

    let sc = sched();

    // Create all Ps.
    let ps: Vec<*mut P> = (0..nprocs)
        .map(|id| Box::into_raw(P::new(id)))
        .collect();

    {
        let mut inner = sc.inner.lock().unwrap();
        inner.gomaxprocs = nprocs;
        inner.allp       = ps.clone();
    }
    sc.gomaxprocs.store(nprocs, Relaxed);

    // Spawn one M per P.  Each M thread calls schedule() and loops forever.
    for p_ptr in ps {
        let id = NEXT_MID.fetch_add(1, Relaxed);
        unsafe { spawn_m(id, p_ptr) };
    }

    // Install SIGSEGV / SIGURG / SIGBUS handlers (Unix only — Windows has no
    // POSIX signals; proactive growth and cooperative preemption are used there).
    #[cfg(not(windows))]
    unsafe { install_sigsegv_handler() };
    #[cfg(not(windows))]
    unsafe { install_sigurg_handler() };
    // SIGBUS handler: print PC/SP/LR + backtrace before aborting, so crashes
    // in background scheduler threads produce actionable diagnostics in CI.
    #[cfg(not(windows))]
    unsafe { install_sigbus_handler() };
    // VEH: catch STATUS_ACCESS_VIOLATION on Windows and print diagnostics.
    // This gives us the faulting RIP/RSP and a Rust backtrace so we can
    // identify which scheduler path is crashing.
    #[cfg(windows)]
    install_windows_veh();

    // Start the background monitor thread (sysmon).
    start_sysmon();
    // Start the background timer thread.
    start_timer_thread();
}

/// Allocate a new M, wire it to `p`, and spawn an OS thread that runs the
/// scheduler loop on that M.
///
/// The raw pointers are transmitted across the thread boundary by casting to
/// `usize` (which is `Send`); M and P are both designed to be exclusively
/// owned by one OS thread at a time.
unsafe fn spawn_m(id: i64, p: *mut P) {
    let m = Box::into_raw(unsafe { M::new(id) });

    // Wire M ↔ P before the thread starts so schedule() sees a valid P.
    unsafe {
        (*m).p = p;
        (*p).m = m;
        (*p).status.store(PRUNNING, Release);
    }

    let m_addr = m as usize;

    std::thread::spawn(move || {
        let m = m_addr as *mut M;
        unsafe {
            // Initialise CURRENT_M, G0_SCHED, CURRENT_G for this thread.
            (*m).start();
            // Enter the scheduler loop — never returns.
            schedule();
        }
    });
}

// ---------------------------------------------------------------------------
// run_impl — public entry point (exposed as go_lib::run)
// ---------------------------------------------------------------------------

/// Initialise the scheduler and run `f` as the first goroutine.
///
/// Blocks the calling thread until `f` returns (or panics).
///
/// # Panic safety
///
/// `f` is executed inside `goroutine_entry`'s `catch_unwind`.  If it panics,
/// the panic is caught and routed to the `set_panic_handler` callback; the
/// calling thread must still be unparked so `run` can return.  We use a
/// drop-guard (`UnparkOnDrop`) rather than an explicit `caller.unpark()` call
/// so the unpark happens during Rust's unwind of `wrapper`, *before*
/// `catch_unwind` catches the payload — guaranteeing `park` is always released
/// even when the goroutine panics.
///
/// Ported from the Go runtime bootstrap (`runtime·rt0_go` → `main.main`).
pub(crate) fn run_impl<F: FnOnce() + Send + 'static>(f: F) {
    let nprocs = std::thread::available_parallelism()
        .map(|n| n.get() as i32)
        .unwrap_or(1);

    schedinit(nprocs);

    // Drop guard: unparks the calling thread whether `f` returns or panics.
    struct UnparkOnDrop(std::thread::Thread);
    impl Drop for UnparkOnDrop {
        fn drop(&mut self) { self.0.unpark(); }
    }

    let caller = std::thread::current();
    let wrapper = move || {
        // `_guard` is dropped when `wrapper` exits — normally or via unwind.
        let _guard = UnparkOnDrop(caller);
        f();
    };

    spawn_goroutine(wrapper);

    // Block until the goroutine's drop-guard fires caller.unpark().
    std::thread::park();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    // ── Singleton tests ───────────────────────────────────────────────────

    #[test]
    fn sched_singleton() {
        let s1 = sched() as *const Sched;
        let s2 = sched() as *const Sched;
        assert_eq!(s1, s2, "sched() must return the same singleton");
    }

    // ── Global run-queue round-trip ───────────────────────────────────────

    #[test]
    fn global_run_queue_round_trip() {
        use crate::runtime::g::{Stack, G};
        use crate::runtime::p::GlobalRunQueue;

        // Use a STANDALONE queue, not `sched().global_run_q`.
        // The live global queue is shared with background M-threads that call
        // findrunnable() and would immediately execute any G we push there —
        // a fake G with a non-mmap'd stack would SIGSEGV on context switch.
        let q = GlobalRunQueue::new();

        let lo    = 0x200000usize;
        let g1    = G::new(Stack { lo, hi: lo + 65536 }, 99);
        let g1_ptr = Box::into_raw(g1);

        unsafe {
            (*g1_ptr).schedlink = ptr::null_mut();
            q.push_batch(g1_ptr, g1_ptr, 1);
            assert_eq!(q.len(), 1);
            let got = q.pop();
            assert_eq!(got, g1_ptr);
            assert_eq!(q.len(), 0);
            let _ = Box::from_raw(g1_ptr);
        }
    }

    // ── new_goroutine — structural sanity ─────────────────────────────────

    #[test]
    fn new_goroutine_fields() {
        use crate::runtime::g::GRUNNABLE;
        use std::sync::atomic::Ordering::Relaxed;

        let g     = new_goroutine(|| {});
        let g_ptr = Box::into_raw(g);

        unsafe {
            assert_eq!(
                (*g_ptr).atomicstatus.load(Relaxed),
                GRUNNABLE,
                "new goroutine must start as Grunnable"
            );
            assert_ne!((*g_ptr).sched.pc, 0, "pc must be set to goroutine_entry");
            assert!(!(*g_ptr).sched.ctxt.is_null(), "ctxt must hold the closure");

            // Architecture-specific stack setup.
            #[cfg(target_arch = "x86_64")]
            {
                // sp points one word below stack.hi; that word holds goexit_trampoline.
                assert_eq!((*g_ptr).sched.sp, (*g_ptr).stack.hi - 8);
                let ret_addr = ((*g_ptr).sched.sp as *const usize).read();
                assert_eq!(ret_addr, goexit_trampoline as *const () as usize);
            }
            #[cfg(target_arch = "aarch64")]
            {
                assert_eq!((*g_ptr).sched.sp, (*g_ptr).stack.hi);
                assert_eq!((*g_ptr).sched.lr, goexit_trampoline as *const () as usize);
            }

            // Drop the G — the closure and stack will be leaked here since G has
            // no Drop impl, but this is acceptable in a unit test.
            let _ = Box::from_raw(g_ptr);
        }
    }

    // ── Full scheduler integration ────────────────────────────────────────

    /// Run a single goroutine through the full M:N scheduler.
    ///
    /// This test spawns real OS threads (Ms), performs `gogo`/`mcall` context
    /// switches, and verifies that the goroutine body executes and the scheduler
    /// returns control to the calling thread.
    #[test]
    fn run_single_goroutine() {
        use std::sync::atomic::{AtomicBool, Ordering};
        static RAN: AtomicBool = AtomicBool::new(false);

        run_impl(|| {
            RAN.store(true, Ordering::Release);
        });

        assert!(RAN.load(Ordering::Acquire), "goroutine body did not execute");
    }

    /// Run two goroutines sequentially via `run_impl` (the scheduler is already
    /// initialised by the first call; the second call just spawns a new goroutine).
    #[test]
    fn run_second_goroutine() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNT: AtomicUsize = AtomicUsize::new(0);

        // First call initialises (or reuses) the scheduler.
        run_impl(|| { COUNT.fetch_add(1, Ordering::AcqRel); });
        // Second call reuses the already-running scheduler.
        run_impl(|| { COUNT.fetch_add(1, Ordering::AcqRel); });

        assert_eq!(COUNT.load(Ordering::Acquire), 2);
    }

    /// `gosched()` must complete without panicking and execution must continue
    /// after the call site.
    #[test]
    fn gosched_returns_to_caller() {
        use std::sync::atomic::{AtomicBool, Ordering};
        static AFTER_YIELD: AtomicBool = AtomicBool::new(false);

        run_impl(|| {
            // Yield mid-goroutine.
            unsafe { gosched() };
            // Execution must resume here after rescheduling.
            AFTER_YIELD.store(true, Ordering::Release);
        });

        assert!(
            AFTER_YIELD.load(Ordering::Acquire),
            "execution must continue after gosched()"
        );
    }

    /// Two goroutines: the first loops calling `gosched()` until it sees a flag
    /// set by the second.  Without the yield the first goroutine would starve
    /// the second on a single-P build; with the yield they interleave.
    #[test]
    fn gosched_allows_other_goroutines_to_run() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let flag = Arc::new(AtomicBool::new(false));
        let flag_setter = Arc::clone(&flag);

        run_impl(move || {
            // Goroutine 1: spawn goroutine 2 then yield until it sets the flag.
            spawn_goroutine(move || {
                flag_setter.store(true, Ordering::Release);
            });
            while !flag.load(Ordering::Acquire) {
                unsafe { gosched() };
            }
        });
    }
}
