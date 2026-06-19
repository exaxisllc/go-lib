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
//! proactively double the stack when the saved SP is within `STACK_GUARD`
//! (928 B) of the guard page — matching Go's `stackGuard`.  This is a
//! belt-and-suspenders complement to the reactive SIGSEGV/SIGBUS handler in
//! `stack.rs`.
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
use std::cell::Cell;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, AtomicU8, Ordering::*};
use std::sync::{Arc, Mutex, OnceLock};

use super::g::{casgstatus, current_g, readgstatus, set_current_g, Stack, G, GDEAD, GPREEMPTED, GRUNNABLE, GRUNNING, STACK_GUARD};
use super::m::{current_m, M};
use super::p::{GlobalRunQueue, P, PIDLE, PRUNNING};
use super::stack::{grow_stack_if_needed, stack_alloc, stack_pool_free, GOROUTINE_STACK_BYTES};
#[cfg(not(windows))]
use super::stack::{install_sigsegv_handler, try_grow_stack_from_signal};
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

/// The scheduler.  A single instance is created (and leaked) by the first
/// `run_impl` call and serves the whole process — mirroring Go, where one
/// runtime instance is never torn down.  Concurrent `run_impl` invocations
/// share its run queues, M-threads, and Ps; goroutines are process-global and
/// never force-reclaimed, so an invocation's goroutines simply persist (Go's
/// leaked-goroutine semantics) rather than being isolated to that call.
pub(crate) struct Rt {
    /// Global run queue — goroutines that are runnable but not yet on any P.
    pub global_run_q: GlobalRunQueue,
    /// Number of Ms currently spinning (looking for work in `findrunnable`).
    pub nmspinning:   AtomicI32,
    /// Current GOMAXPROCS value — readable without holding `inner`.
    pub gomaxprocs:   AtomicI32,
    /// Locked parts of scheduler state.
    pub inner:        Mutex<SchedInner>,
    /// Registry of every live goroutine (matches Go's `allgs`).
    pub allg:         Mutex<Vec<*mut G>>,
    /// Never set in singleton mode — the process-wide scheduler is not torn
    /// down (M-threads live for the process lifetime, like Go's).  The flag
    /// and the M-exit paths that check it are kept as infrastructure for a
    /// potential future full-teardown mode.
    pub shutdown:     AtomicBool,
}

// SAFETY: Rt is created once per process and leaked for its lifetime.  All
// fields carry their own synchronisation.  Raw-pointer access through allg and
// the run queues is serialised by the scheduler's ownership invariants.
unsafe impl Sync for Rt {}
unsafe impl Send for Rt {}

// ---------------------------------------------------------------------------
// CURRENT_RT — per-thread pointer to the owning Rt
// ---------------------------------------------------------------------------

thread_local! {
    /// Set on every OS thread that participates in an `Rt`: M-threads (via
    /// `spawn_m`), the sysmon thread (via `start_sysmon`), and the
    /// `run_impl` calling thread (via `schedinit`).  Never set on bare OS
    /// threads that have no scheduler role.
    static CURRENT_RT: Cell<*const Rt> = Cell::new(ptr::null());
}

/// The process-wide singleton scheduler.  Initialised by the first
/// `run_impl` call (which runs `schedinit`); every later call attaches to
/// it by binding `CURRENT_RT`.  Mirrors Go, where one runtime instance
/// serves the whole process and is never torn down.
static GLOBAL_RT: OnceLock<&'static Rt> = OnceLock::new();

/// Raw pointer to the singleton `Rt`, or null before the first `run_impl`.
/// Used by process-shared threads (the timer thread) to bind `CURRENT_RT`
/// lazily.
#[inline]
pub(crate) fn global_rt_ptr() -> *const Rt {
    GLOBAL_RT.get().map_or(ptr::null(), |rt| *rt as *const Rt)
}

/// Return a reference to the calling thread's `Rt`.
///
/// # Panics (debug builds)
/// Panics if `CURRENT_RT` has not been set on this thread.
#[inline]
pub(crate) fn current_rt() -> &'static Rt {
    let p = CURRENT_RT.with(|c| c.get());
    debug_assert!(!p.is_null(), "current_rt: CURRENT_RT is not set on this thread");
    // SAFETY: p was set by set_current_rt to a Box::leak'd Rt which is valid
    // for the process lifetime.
    unsafe { &*p }
}

/// Return the raw `*const Rt` for this thread (null if not set).
#[inline]
pub(crate) fn current_rt_ptr() -> *const Rt {
    CURRENT_RT.with(|c| c.get())
}

/// Return the `P` attached to the current OS thread's M, or null when there
/// is no M (bare threads: the `run_impl` caller, sysmon, the timer thread) or
/// the M currently holds no P.
///
/// Used only to pick *which* per-P cache (sudog free list) to use; the result
/// need not be "our" P for correctness, since each P's cache carries its own
/// lock and Ps live for the whole process (never freed), so any non-null
/// pointer this returns is valid to dereference.
#[inline]
pub(crate) fn current_p() -> *mut P {
    let m = super::m::current_m();
    if m.is_null() {
        ptr::null_mut()
    } else {
        unsafe { (*m).p }
    }
}

/// Bind `rt` as the `Rt` for the current OS thread.
#[inline]
pub(crate) fn set_current_rt(rt: *const Rt) {
    CURRENT_RT.with(|c| c.set(rt));
}

/// Return the scheduler for the current thread.  All internal scheduler
/// functions call this instead of a global singleton.
#[inline]
pub(crate) fn sched() -> &'static Rt {
    current_rt()
}

// ---------------------------------------------------------------------------
// schedule — main scheduler loop (runs on g0)
// ---------------------------------------------------------------------------

/// Pick the next runnable goroutine and transfer control to it.  Runs on g0's
/// stack.
///
/// Picks a runnable goroutine via `findrunnable` and hands off to it via
/// `execute` (which never returns — it `gogo`s into the goroutine).  Called
/// initially from `M::start` and re-entered via mcall targets (`goexit0`,
/// `gosched_m`, `park_fn`, `preemptm`, etc.), so the "loop" is the chain of
/// `execute → … → mcall → schedule` re-entries, not a back-edge in this body:
/// every path here either diverges through `execute` or returns.
///
/// Returns `()` only when `findrunnable` reports the owning `Rt` is shutting
/// down.  After returning, the caller (mcall target or spawn_m's thread
/// closure) terminates the OS thread — mcall_asm does this by calling
/// `m_thread_exit` instead of `ud2`.
///
/// Ported from `schedule` in `runtime/proc.go`.
pub(crate) unsafe fn schedule() {
    let m = current_m();
    debug_assert!(!m.is_null(), "schedule: CURRENT_M is null — call set_current_m first");

    let p = unsafe { (*m).p };
    debug_assert!(!p.is_null(), "schedule: M has no P attached");

    // Every 61 ticks drain one G from the global queue to prevent starvation.
    let tick = unsafe { (*p).schedtick.load(Relaxed) };
    if tick % 61 == 0 && sched().global_run_q.len() > 0 {
        let gp = unsafe { sched().global_run_q.pop() };
        if !gp.is_null() {
            unsafe { execute(gp) }; // -> !, never returns
        }
    }

    // Try local run queue first — no lock needed.
    let (gp, _inherit) = unsafe { (*p).runqget() };

    let gp = if !gp.is_null() {
        gp
    } else {
        // Local queue empty; find work elsewhere.
        match unsafe { findrunnable() } {
            Some(gp) => gp,
            None => return, // Rt is shutting down — caller will exit the thread
        }
    };

    unsafe { execute(gp) }; // -> !, never returns
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
/// 4. Non-blocking netpoll.
/// 5. Surrender the P and park the M.  On wakeup, loop.
///
/// Returns `Some(gp)` with a non-null goroutine, or `None` when the `Rt`
/// is shutting down and there is no work.
///
/// Ported from `findrunnable` in `runtime/proc.go` (trimmed for v1).
pub(crate) unsafe fn findrunnable() -> Option<*mut G> {
    let m  = current_m();
    let sc = sched();

    loop {
        // ── 1. Local run queue ────────────────────────────────────────────
        let p = unsafe { (*m).p };
        if !p.is_null() {
            let (gp, _) = unsafe { (*p).runqget() };
            if !gp.is_null() {
                return Some(gp);
            }
        }

        // ── 2. Global run queue ───────────────────────────────────────────
        {
            let gp = unsafe { sc.global_run_q.pop() };
            if !gp.is_null() {
                return Some(gp);
            }
        }

        // ── 3. Work-steal from every other P ──────────────────────────────
        // Four passes over *all* Ps (in a per-M rotated order), stealing the
        // victim's `runnext` only on the final pass.  Covering every P — not a
        // fixed sample of four — is load-bearing at low GOMAXPROCS: a goroutine
        // woken via `goready` lands in its waker's P `runnext`, and if that
        // waker then monopolises its P (e.g. a long CPU-bound goroutine with
        // async preemption off) the only way the G ever runs is for another M
        // to steal it.  Stealing `runnext` solely on the last pass mirrors Go
        // and avoids tugging a G away from a P that is about to run it.  See
        // issue #5: the old fixed 4-victim sample could omit the P holding the
        // stranded `runnext`, livelocking after `stopm`'s re-check kept us awake.
        {
            let inner = sc.inner.lock().unwrap();
            let np    = inner.allp.len();
            if np > 1 && !p.is_null() {
                let start = (unsafe { (*m).id as usize }).wrapping_mul(0x9e3779b9) % np;
                let victim_ptrs: Vec<*mut P> = (0..np)
                    .map(|i| inner.allp[(start.wrapping_add(i)) % np])
                    .collect();
                drop(inner);

                for pass in 0..4 {
                    let steal_run_next = pass == 3;
                    for &victim_ptr in &victim_ptrs {
                        if victim_ptr == p || victim_ptr.is_null() {
                            continue;
                        }
                        let stolen = unsafe {
                            (*p).runqsteal(&*victim_ptr, steal_run_next)
                        };
                        if !stolen.is_null() {
                            return Some(stolen);
                        }
                    }
                }
            }
        }

        // ── 4. Non-blocking netpoll: check if any I/O goroutines are ready ──
        {
            // The harvested entries are `*mut G` descriptors that stay live
            // while parked, and we wake each only via `goready`, which never
            // touches the goroutine's stack.  An entry whose goroutine has
            // already exited is GDEAD by the time `goready` runs and is dropped
            // by `goready`'s GDEAD arm.
            let ready = unsafe { super::netpoll::netpoll_wait(0) };
            // Singleton scheduler: every harvested goroutine belongs to this
            // Rt, so plain goready is always correct (goready's GDEAD check
            // handles entries whose goroutine has already exited).
            for gp in ready {
                unsafe { super::park::goready(gp) };
            }
        }

        // ── 5. No work found — surrender P and park ───────────────────────
        unsafe { stopm() };
        // After stopm() returns: either startm woke us with work, or the Rt
        // is shutting down.  Check shutdown before looping.
        if sc.shutdown.load(Acquire) {
            return None;
        }
        // Woken up by startm; P has been (re-)attached.  Try again.
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
/// ## Lost-wakeup avoidance
///
/// There is a classic lost-wakeup race between `findrunnable`'s queue checks
/// and `stopm`'s park:
///
/// ```text
///   M:   findrunnable: local empty, global empty → stopm
///   T:   goready(gp): push gp to global queue
///   T:   startm(null): pop idle_m — empty (M hasn't added itself yet) → return
///   M:   stopm: take inner.lock; add self to idle_m; drop lock; park
///        ← M parks forever; queue has gp but no one will wake M
/// ```
///
/// `T` can be any thread: sysmon's netpoll, the timer thread, another M's
/// channel send.  Even with `GOMAXPROCS=1` (no other Ms), the timer and
/// sysmon threads can hit this race.
///
/// The fix matches Go's `stopm` / `mPark` pattern: after adding M to the
/// idle list (still under `inner.lock`), re-check the run queues.  If work
/// appeared, pop ourselves back out of idle_m, take a P, and return without
/// parking.  Holding `inner.lock` across the re-check serialises us with any
/// concurrent `startm`, which also takes `inner.lock` and pops idle_m there —
/// if work was queued after our park, `startm` is guaranteed to find us on
/// idle_m.
///
/// The re-check must cover **every** P's local run queue, not just the global
/// one: a `goready` running on an M-with-P enqueues onto its *local* P (via
/// `runqput(.., next=true)`) before calling `startm`, so a wake dropped by
/// `startm` (no idle M yet) leaves the G on a local queue.  Re-checking only
/// the global queue missed that and stranded the G with every M asleep —
/// issue #5.  See the scan in the body for the full happens-before argument.
///
/// Ported from `stopm` in `runtime/proc.go`.
unsafe fn stopm() {
    let m  = current_m();
    let sc = sched();

    // Surrender P under the scheduler lock and atomically re-check for work.
    {
        let mut inner = sc.inner.lock().unwrap();
        let surrendered_p = unsafe { (*m).p };

        // ── Pre-surrender check: any goroutines on M's local P queue? ─────
        if !surrendered_p.is_null() && unsafe { (*surrendered_p).runq_size() } > 0 {
            return; // skip park — goto top of findrunnable
        }

        // ── Shutdown check (while holding lock) ───────────────────────────
        // If the Rt is already shutting down, return without adding ourselves
        // to the idle list.  This prevents the lost-wakeup scenario where
        // shutdown fires between the two lock acquisitions.
        if sc.shutdown.load(Acquire) {
            return;
        }

        if !surrendered_p.is_null() {
            unsafe {
                (*m).p = ptr::null_mut();
                (*surrendered_p).status.store(PIDLE, Release);
                (*surrendered_p).link = inner.idle_p;
                inner.idle_p = surrendered_p;
            }
        }
        // Enqueue M on idle list.
        unsafe {
            (*m).schedlink = inner.idle_m;
            inner.idle_m   = m;
            inner.nmidle  += 1;
        }

        // ── Re-check ALL run queues (lost-wakeup avoidance) ───────────────
        // Still holding `inner.lock`.  If a `goready` raced with our queue
        // checks and enqueued work between `findrunnable` and here, we would
        // otherwise park forever.
        //
        // It is NOT enough to re-check only the global queue.  `goready`, when
        // called from an M that owns a P, does `runqput(gp, next=true)` onto
        // that M's *local* P and then `startm(null)`.  If that `startm` runs in
        // the narrow window *before* we add ourselves to `idle_m` (above), it
        // finds no idle M and drops the wake — and the woken G is sitting on a
        // local run queue the old global-only re-check never inspected.  With a
        // low `GOMAXPROCS` and async preemption off, the P's owner can stay on a
        // long CPU-bound goroutine and nobody steals the G, so it strands while
        // every other M sleeps here (issue #5).  Scanning every P closes it.
        //
        // The scan is race-free against a concurrent `runqput` *because* of the
        // `inner.lock` handoff: any `goready` that enqueues work also calls
        // `startm`, which must take `inner.lock`.  Either that `startm` acquired
        // the lock before us — in which case the release→acquire edge makes its
        // prior `runqput` visible to this scan — or it acquires the lock after
        // us, by which point we are already on `idle_m` and `startm` will wake
        // us.  One of the two always holds, so no wake is lost.
        let global_work = sc.global_run_q.len() > 0;
        let local_work  = inner.allp.iter().any(|&p| {
            !p.is_null() && unsafe { (*p).runq_size() } > 0
        });
        if global_work || local_work {
            // Try to reclaim a P so the resumed `findrunnable` can actually
            // act on the work: local work must be *stolen* (needs a P); global
            // work can be popped P-lessly, so bail on it even without a P.
            let mut p2 = ptr::null_mut();
            if !inner.idle_p.is_null() {
                p2 = inner.idle_p;
                unsafe {
                    inner.idle_p = (*p2).link;
                    (*p2).link   = ptr::null_mut();
                }
            }
            if !p2.is_null() || global_work {
                // Pop ourselves back off idle_m and return to findrunnable.
                unsafe {
                    inner.idle_m  = (*m).schedlink;
                    (*m).schedlink = ptr::null_mut();
                    inner.nmidle -= 1;
                }
                if !p2.is_null() {
                    unsafe {
                        (*p2).status.store(PRUNNING, Release);
                        (*m).p = p2;
                    }
                }
                return;
            }
            // Local work only and no idle P to steal it with: leave it for the
            // owning P's M to run, put the (null) P back, and park normally.
        }

        // ── Second shutdown check (still under lock) ──────────────────────
        // If shutdown fired after we added ourselves to idle_m, remove from
        // idle_m and return so we don't park indefinitely.
        if sc.shutdown.load(Acquire) {
            unsafe {
                inner.idle_m  = (*m).schedlink;
                (*m).schedlink = ptr::null_mut();
                inner.nmidle -= 1;
            }
            return;
        }
    } // release lock before blocking

    unsafe { (*m).park_m() }; // blocks until startm or shutdown unparks us

    // Woken by startm (which set (*m).p) or by the shutdown sequence (which
    // cleared idle_m).  Either way, findrunnable() will check sc.shutdown
    // after we return and act accordingly.
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

    // Debug-build sanity check: catch a corrupted Gobuf at resume time with
    // context, rather than letting gogo() restore garbage and detonate at an
    // arbitrary point downstream.  `sched.sp`/`bp` must point into the G's own
    // stack and `sp` must be 8-byte aligned; `pc` must be set.
    #[cfg(debug_assertions)]
    unsafe {
        let sp = (*gp).sched.sp;
        let bp = (*gp).sched.bp;
        let pc = (*gp).sched.pc;
        let (lo, hi) = ((*gp).stack.lo, (*gp).stack.hi);
        let sp_ok = sp >= lo && sp <= hi && sp & 7 == 0;
        let bp_ok = bp == 0 || (bp >= lo && bp <= hi);
        if !sp_ok || !bp_ok || pc == 0 {
            let status = (*gp).atomicstatus.load(std::sync::atomic::Ordering::Relaxed);
            eprintln!(
                "execute: corrupt gobuf: gp={gp:p} goid={} status={status} sp={sp:#x} \
                 bp={bp:#x} pc={pc:#x} stack=[{lo:#x},{hi:#x}]",
                (*gp).goid,
            );
            std::process::abort();
        }
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
    // Balance the block_sigurg() taken by goexit_trampoline / goexit0_handler:
    // the goroutine's gobuf save is complete and we are on g0, so SIGURG may be
    // delivered to this M again.  (The matching `m.locks -= 1` below balances
    // the raw counter bump.)
    #[cfg(not(windows))]
    unsafe { super::m::unblock_sigurg() };

    let m = current_m();

    unsafe {
        casgstatus(gp, GRUNNING, GDEAD);
        (*gp).m   = ptr::null_mut();
        (*m).curg = ptr::null_mut();
        set_current_g(ptr::null_mut());
    }

    // Balance the raw `locks += 1` taken by goexit_trampoline /
    // goexit0_handler.  mcall never returns there, so the trampoline cannot
    // release the count itself; an RAII guard would leak one increment per
    // goroutine exit and permanently disable async preemption on this M
    // (sigurg_handler Guard 0 skips whenever m.locks > 0).  Decrementing is
    // safe here: the G is GDEAD and current_g is null, so sigurg_handler
    // cannot start a preemption on this M anyway.
    unsafe { (*m).locks.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) };

    // Remove from the live-goroutine registry before retiring the descriptor
    // to the gFree pool, so the registry never lists a recycled descriptor.
    {
        let mut allg = sched().allg.lock().unwrap();
        if let Some(pos) = allg.iter().position(|&p| p == gp) {
            allg.swap_remove(pos);
        }
    }

    // Retire the G descriptor to the gFree pool, recycling it (and its stack)
    // for a future `new_goroutine`.  `gfree_put` keeps the stack mapped (it is
    // pooled with the descriptor) — so, unlike before, goexit0 does NOT
    // `stack_free` here.  Safe to recycle: this G ran to completion while
    // GRUNNING, was never parked, and is now GDEAD and off every queue, so no
    // peer holds a reference to it.  (Under `GOLIB_GPOOL_OFF=1`, `gfree_put`
    // frees the stack and leaks the descriptor instead.)
    unsafe { gfree_put(gp) };

    // Re-enter the scheduler on g0's stack.  Returns only on Rt shutdown, in
    // which case mcall_asm's m_thread_exit call terminates the OS thread.
    unsafe { schedule() };
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
    // Block SIGURG across the `current_g()` read and the `mcall` save.  In a
    // CPU-bound loop (the canonical `gosched` caller) async preemption is
    // already imminent; if SIGURG split the thread-local read here and migrated
    // the goroutine, `mcall` would save this stack into the wrong goroutine's
    // gobuf (the same cross-stack corruption fixed in `async_preempt2`).
    // `gosched_m` unblocks once the save has completed on g0.
    #[cfg(not(windows))]
    unsafe { super::m::block_sigurg() };
    let gp = current_g();
    debug_assert!(!gp.is_null(), "gosched: called from g0 or uninitialised thread");
    unsafe { mcall(gp, gosched_m) };
}

/// Mcall target for `gosched`.  Runs on g0's stack.
unsafe extern "C" fn gosched_m(gp: *mut G) {
    // Balance `gosched`'s block_sigurg(): the gobuf save is done (we are on g0).
    #[cfg(not(windows))]
    unsafe { super::m::unblock_sigurg() };
    let m = current_m();

    unsafe {
        casgstatus(gp, GRUNNING, GRUNNABLE);
        (*gp).m   = ptr::null_mut();
        (*m).curg = ptr::null_mut();
        set_current_g(ptr::null_mut());
    }

    unsafe {
        (*gp).schedlink = ptr::null_mut();
        sched().global_run_q.push_batch(gp, gp, 1);
    }

    unsafe { schedule() };
    // schedule() returns only on Rt shutdown; mcall_asm calls m_thread_exit
    // after we return, so the OS thread exits cleanly.
}

// ---------------------------------------------------------------------------
// Async preemption (Step 4) — SIGURG handler + asyncPreempt2
// ---------------------------------------------------------------------------

/// Previous SIGURG handler (chained if the signal is not a preemption).
#[cfg(not(windows))]
static PREV_SIGURG: Mutex<Option<libc::sigaction>> = Mutex::new(None);

/// Minimum free stack (bytes) below the interrupted SP required before
/// `sigurg_handler` will inject async preemption (Guard 2.5).
///
/// Budget for everything the preempt path pushes onto the goroutine stack
/// before `mcall_asm` switches to g0:
///
/// - `redirect_to_async_preempt`: 8 B (x86-64 resume-PC push) or 16 B
///   (AArch64 LR spill, 16-byte aligned)
/// - `async_preempt_trampoline`: 392 B register-save frame (x86-64) /
///   ~784 B (AArch64 GPRs + d0–d31) + the `call`/`bl` return slot
/// - `async_preempt2` + `mcall`: ordinary Rust frames, several hundred
///   bytes each in unoptimised debug builds
///
/// 4096 covers all of that with margin while still being well under the
/// smallest stack we ever run on (16 KiB Linux debug).  The equivalent of
/// Go's `asyncPreemptStack` used by `isAsyncSafePoint`.
#[cfg(not(windows))]
const ASYNC_PREEMPT_HEADROOM: usize = 4096;

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

    // Capture a TEXT-segment anchor so `sigurg_handler` can tell our code
    // apart from foreign (system-library) code.  `goroutine_entry` is in our
    // binary and never moves; its address acts as a fixed reference point.
    OUR_TEXT_ANCHOR.store(
        goroutine_entry as *const () as usize,
        std::sync::atomic::Ordering::Relaxed,
    );
}

/// Extract the interrupted stack pointer from a signal `ucontext_t`.
///
/// Used by `sigurg_handler` to verify the signal arrived while the goroutine
/// was executing on its own stack rather than on g0's stack (e.g. inside the
/// `mcall_asm` SP-switch window).
///
/// Returns 0 on platforms where we don't have a definition — the caller
/// treats 0 as "out of bounds", so preemption is conservatively skipped.
#[cfg(not(windows))]
#[inline]
fn ucontext_sp(ctx: *mut libc::c_void) -> usize {
    let uc = ctx as *mut libc::ucontext_t;
    unsafe {
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        return (*uc).uc_mcontext.gregs[libc::REG_RSP as usize] as usize;

        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        return (*uc).uc_mcontext.sp as usize;

        #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
        return (*(*uc).uc_mcontext).__ss.__rsp as usize;

        #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
        return (*(*uc).uc_mcontext).__ss.__sp as usize;

        // Fallback: unknown platform.  Return 0 so the caller skips
        // preemption rather than potentially corrupting a foreign stack.
        #[allow(unreachable_code)]
        0_usize
    }
}

/// Extract the interrupted program counter from a signal `ucontext_t`.
///
/// Used by `sigurg_handler` to skip async preemption when the goroutine was
/// interrupted inside a foreign (system-library) function such as
/// `libsystem_malloc.dylib!free_tiny`.  Preempting there can leave a
/// non-reentrant lock held (e.g., the tiny-allocator's `os_unfair_lock`);
/// when the scheduler then runs another goroutine on the same thread, the
/// next allocation tries to re-acquire that lock and the kernel detects the
/// recursion and aborts with `EXC_BAD_INSTRUCTION` from
/// `_os_unfair_lock_recursive_abort`.
///
/// Returns 0 on platforms where we don't have a definition — the caller
/// treats 0 as "out of our text", so preemption is conservatively skipped.
#[cfg(not(windows))]
#[inline]
fn ucontext_pc(ctx: *mut libc::c_void) -> usize {
    let uc = ctx as *mut libc::ucontext_t;
    unsafe {
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        return (*uc).uc_mcontext.gregs[libc::REG_RIP as usize] as usize;

        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        return (*uc).uc_mcontext.pc as usize;

        #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
        return (*(*uc).uc_mcontext).__ss.__rip as usize;

        #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
        return (*(*uc).uc_mcontext).__ss.__pc as usize;

        #[allow(unreachable_code)]
        0_usize
    }
}

/// Bounds of our own binary's executable text segment, captured once at
/// `schedinit` time.  Used by `sigurg_handler` to decide whether the
/// interrupted instruction is in our code (safe to preempt) or in a
/// dynamically-linked system library (unsafe — see `ucontext_pc` doc).
///
/// Stored as two ranges: `[lo, hi)` — `lo` is a known function address
/// (`goroutine_entry`); `hi` is `lo + TEXT_RANGE_BYTES`.  We don't need
/// an exact match: system libraries on macOS sit at virtual addresses
/// roughly 256 GiB above the main executable's TEXT segment, so any
/// liberal bound that contains "our binary + bundled Rust std" but
/// excludes system libraries works.
///
/// 256 MiB is far larger than any plausible Rust binary's TEXT and far
/// smaller than the gap to system-library VM space.
#[cfg(not(windows))]
const TEXT_RANGE_BYTES: usize = 256 * 1024 * 1024;

/// One-time capture of a known address in our binary's TEXT segment.
/// Initialised by `install_sigurg_handler` to `goroutine_entry as usize`.
#[cfg(not(windows))]
static OUR_TEXT_ANCHOR: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Return `true` if `pc` looks like it points into our own binary's TEXT,
/// `false` if it is in a dynamically-linked library.
///
/// Heuristic: `|pc − anchor| < TEXT_RANGE_BYTES` where `anchor` is the
/// address of `goroutine_entry`.  This works because the linker lays out
/// all of the main executable's code (Rust std, our crate, the test
/// harness) contiguously, while macOS's dyld shared cache places system
/// libraries hundreds of GiB away.
#[cfg(not(windows))]
#[inline]
fn pc_in_our_text(pc: usize) -> bool {
    let anchor = OUR_TEXT_ANCHOR.load(std::sync::atomic::Ordering::Relaxed);
    if anchor == 0 {
        return true; // not yet initialised — fail-open
    }
    let diff = if pc > anchor { pc - anchor } else { anchor - pc };
    diff < TEXT_RANGE_BYTES
}

/// Return `true` if `pc` falls inside one of our scheduler asm trampolines —
/// `gogo_asm`, `mcall_asm`, or `async_preempt_trampoline`.
///
/// These functions are naked asm that switches stacks and restores registers
/// out of band of the saved `g.sched` state.  Async-preempting inside any of
/// them corrupts the goroutine's state on resume because the second
/// `mcall_asm` overwrites `g.sched.regs[]` with whatever the trampoline path
/// was carrying instead of the worker's saved values.
///
/// The heuristic is the same as `pc_in_our_text`: assume each function is at
/// most 4 KiB (these are short asm fns) and check `|pc − fn_start| < 4096`.
#[cfg(not(windows))]
#[inline]
fn pc_in_scheduler_asm(pc: usize) -> bool {
    /// Maximum reasonable size of a scheduler asm trampoline (bytes).
    const ASM_FN_MAX_BYTES: usize = 4096;

    #[inline]
    fn near(pc: usize, fn_addr: usize) -> bool {
        let diff = if pc > fn_addr { pc - fn_addr } else { fn_addr - pc };
        diff < ASM_FN_MAX_BYTES
    }

    #[cfg(target_arch = "x86_64")]
    let bases: [usize; 10] = [
        super::asm_amd64::async_preempt_trampoline as *const () as usize,
        super::asm_amd64::gogo                     as *const () as usize,
        super::asm_amd64::mcall                    as *const () as usize,
        super::asm_amd64::gogo_asm                 as *const () as usize,
        super::asm_amd64::mcall_asm                as *const () as usize,
        // goexit path: RSP is still on the goroutine stack while m.locks is
        // being acquired (TLS accessor has 3 instructions before incl 96(%rax)).
        // Guard 0 (m.locks > 0) is not yet active in that window, so we protect
        // both the naked trampoline and the handler with Guard 3.  (This path
        // bumps m.locks raw, outside m_lock, and relies on block_sigurg + this
        // whitelist rather than m_lock's re-validation.)
        goexit_trampoline                          as *const () as usize,
        goexit0_handler                            as *const () as usize,
        // current_m reads the CURRENT_M TLS slot via libstd's LocalKey glue; a
        // migration between the slot-address fetch and the load would return a
        // stale M.  Kept whitelisted as defense-in-depth: for read-only callers
        // a stale read is benign, and m_lock no longer depends on it (it now
        // re-validates current_m after the bump — see m_lock's doc-comment), so
        // m_lock itself is intentionally NOT in this list.
        super::m::current_m                        as *const () as usize,
        // async_preempt2's prologue (before it blocks SIGURG) must not itself
        // be nested-preempted, or the goroutine could migrate before the block
        // takes effect.  See async_preempt2's doc-comment.
        async_preempt2                             as *const () as usize,
        // goroutine_entry prologue, before it blocks SIGURG — same rationale.
        goroutine_entry                            as *const () as usize,
    ];
    #[cfg(target_arch = "aarch64")]
    let bases: [usize; 9] = [
        super::asm_arm64::async_preempt_trampoline as *const () as usize,
        super::asm_arm64::gogo                     as *const () as usize,
        super::asm_arm64::mcall                    as *const () as usize,
        // The naked context-switch bodies must be covered explicitly — the
        // linker may place them more than ASM_FN_MAX_BYTES away from their
        // Rust wrappers, leaving a window where SIGURG could redirect
        // mid-switch with half-saved/half-restored register state.  The
        // x86-64 list above includes gogo_asm/mcall_asm for the same reason.
        super::asm_arm64::gogo_asm                 as *const () as usize,
        super::asm_arm64::mcall_asm                as *const () as usize,
        // Same pre-lock window as AMD64: goexit_trampoline bumps m.locks raw
        // (outside m_lock) and relies on block_sigurg + this whitelist.
        goexit_trampoline                          as *const () as usize,
        // current_m's TLS read window, defense-in-depth — see x86-64 list and
        // current_m's doc.  m_lock is intentionally NOT listed (it re-validates).
        super::m::current_m                        as *const () as usize,
        // async_preempt2 prologue — see x86-64 list and async_preempt2's doc.
        async_preempt2                             as *const () as usize,
        // goroutine_entry prologue — see x86-64 list.
        goroutine_entry                            as *const () as usize,
    ];

    for b in bases {
        if near(pc, b) { return true; }
    }
    false
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
        // Guard 0: M must not be holding any internal scheduler lock.
        //
        // `spawn_goroutine`, `goready`, `gosched_m`, etc. take the global run
        // queue's `Mutex` to enqueue a G.  If SIGURG arrives midway through
        // that critical section, `redirect_to_async_preempt` would forward
        // execution to `async_preempt_trampoline → async_preempt2 → mcall →
        // preemptm`, and `preemptm` would itself call `push_batch`, which
        // tries to re-acquire the same `Mutex` on the same thread — a hard
        // self-deadlock (the M parks in `pthread_mutex_firstfit_lock_wait`
        // waiting for a lock it already owns).
        //
        // The fix is the same idea Go uses (`m.locks > 0` ⇒ no async preempt):
        // any code that takes a non-reentrant scheduler-internal `Mutex`
        // wraps the critical section in `m_lock()`, which increments
        // `(*m).locks`.  This guard checks the counter and skips preemption
        // when it is non-zero.
        let mp = super::m::current_m();
        if !mp.is_null()
            && unsafe { (*mp).locks.load(std::sync::atomic::Ordering::Relaxed) } > 0
        {
            return; // holding a scheduler-internal lock — skip preemption
        }

        // Guard 0.5: interrupted PC must be in OUR binary's TEXT segment.
        //
        // If we were interrupted inside a dynamically-linked system library
        // (libsystem_malloc, libpthread, etc.), preempting and switching to
        // another goroutine on the same thread will eventually re-enter the
        // same library — which may try to re-acquire a non-reentrant lock
        // it already holds (e.g., the tiny-allocator's `os_unfair_lock`).
        // macOS detects the recursion and crashes with
        // `_os_unfair_lock_recursive_abort` (EXC_BAD_INSTRUCTION / SIGILL).
        //
        // Concrete failure observed before this guard:
        //   Box::drop → dealloc → free_tiny (holds malloc lock)
        //   ←SIGURG redirects to preemptm → schedule() → another goroutine
        //   other goroutine: Box::new → free_tiny → os_unfair_lock_lock_slow
        //     ← recursive owner detected → abort
        //
        // Defer preemption to when we naturally exit the library back into
        // our own code; sysmon will re-set `gp.preempt` and we'll get
        // another SIGURG attempt soon enough.
        let pc = ucontext_pc(ctx);
        if !pc_in_our_text(pc) {
            return; // interrupted in a system library — defer preemption
        }

        // Guard 1: goroutine must be GRUNNING.
        //
        // SIGURG can arrive while gp is in GWAITING (gopark transition window),
        // GSYSCALL (inside entersyscall), or GPREEMPTED.  Redirecting in those
        // states causes preemptm to call casgstatus(gp, GRUNNING, GPREEMPTED)
        // on a non-GRUNNING G, which spins the CAS loop forever.
        //
        // Mirrors Go's wantAsyncPreempt() which gates on readgstatus == _Grunning.
        if unsafe { readgstatus(gp) } != GRUNNING {
            return; // not at an async-safe preemption point — ignore
        }

        // Guard 2: interrupted SP must be within the goroutine's own stack.
        //
        // `mcall_asm` first saves the goroutine's registers to g.sched and
        // then switches SP to g0's stack — but the G status stays GRUNNING
        // until the mcall target (park_fn / gosched_m) calls casgstatus.
        // There is therefore a window after the SP switch where Guard 1 passes
        // but the CPU is actually executing on g0's stack.  Redirecting in that
        // window makes async_preempt_trampoline write its register-save frame
        // onto g0's stack instead of the goroutine's stack, corrupting both
        // stacks and eventually causing a hang or memory corruption.
        //
        // Checking that the interrupted SP is within [gp.stack.lo, gp.stack.hi]
        // closes that window: if we are on g0's stack the SP is outside those
        // bounds and we skip preemption.
        //
        // This mirrors Go's sigctxt.inUserCode() / stackBounds checks.
        let sp      = ucontext_sp(ctx);
        let (lo, hi) = unsafe { ((*gp).stack.lo, (*gp).stack.hi) };
        if sp < lo || sp > hi {
            return; // not on goroutine's stack — skip preemption
        }

        // Guard 2.5: there must be enough free stack BELOW the interrupted SP
        // for the entire preemption machinery to run without touching the
        // guard page.
        //
        // The preempt path consumes goroutine stack out of band of any normal
        // call: `redirect_to_async_preempt` pushes the resume PC (x86-64) or
        // the original LR (AArch64), `async_preempt_trampoline` then builds a
        // ~400 B register-save frame, and `async_preempt2` + `mcall` push
        // ordinary (debug: wide) Rust frames before `mcall_asm` finally
        // switches to g0's stack.
        //
        // If any of those writes lands in the guard page, the SIGSEGV/SIGBUS
        // growth handler fires NESTED inside the preemption machinery — and
        // that growth is unrecoverable: `update_sp_in_context` adjusts only
        // RSP/RBP in the usable-stack range (heap-false-positive fix, PR #23),
        // so live user register values that point into the old usable stack
        // stay stale, get pushed into the trampoline's register-save frame
        // AFTER copystack's conservative scan already ran, and are popped
        // back into the resumed goroutine pointing at the freed (and quickly
        // remapped, zero-filled) old stack.  Observed as `many_goroutines`
        // resuming the spawner with zeroed callee-saved registers in debug
        // builds (SIGSEGV on Linux, SIGABRT via the trampoline's RFLAGS
        // corruption path on macOS).
        //
        // Mirrors Go's `isAsyncSafePoint` stack check (`asyncPreemptStack`):
        // when headroom is insufficient, simply skip — sysmon keeps
        // `gp.preempt` set and retries; if the goroutine stays deep it will
        // touch the guard page through a NORMAL memory access soon enough,
        // which the growth handler recovers from cleanly, and the next SIGURG
        // then finds a doubled stack with plenty of headroom.
        if sp < lo + ASYNC_PREEMPT_HEADROOM {
            return; // too close to the guard page — defer preemption
        }

        // Guard 2.6: the interrupted SP must be 8-byte aligned before we use
        // it as a `*mut usize` to push the resume PC in
        // `redirect_to_async_preempt`.  An async signal can land at any
        // instruction boundary, including inside an unoptimised debug
        // prologue that has only partially adjusted RSP (rustc emits
        // `sub rsp, 2`-style frames for byte-sized locals), leaving RSP at an
        // odd or 2/4/6-mod-8 value.  Pushing a usize there is an unaligned
        // store (UB in Rust, and the debug-build alignment check turns it
        // into a `panic_nounwind` *inside the signal handler* → process
        // abort).  Skipping is safe and cheap: sysmon keeps `gp.preempt` set
        // and retries; RSP is realigned within a few instructions.  Go does
        // not hit this because it controls its own register-safe points.
        if sp & 7 != 0 {
            return; // unaligned SP — not a safe preemption point, defer
        }

        // Guard 3: interrupted PC must not be inside our scheduler's asm
        // trampolines — `gogo_asm`, `mcall_asm`, or `async_preempt_trampoline`
        // itself.
        //
        // These trampolines do **stack switching and register restoration
        // out of band** of the saved `g.sched` state.  If SIGURG fires inside
        // them, redirecting to `async_preempt_trampoline` makes the new
        // trampoline run with half-restored / out-of-sync state and the
        // worker's iterator / loop locals on the stack get clobbered when
        // the second `mcall_asm` overwrites `g.sched.regs[]` with whatever
        // the trampoline path was carrying.  Mirrors Go's `sigctxt.sigpc`
        // check against `runtime.gogo` / `runtime.mcall` address ranges.
        if pc_in_scheduler_asm(pc) {
            return; // inside gogo_asm / mcall_asm / preempt trampoline
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
/// On AMD64/x86_64: store the original `RIP` at `rsp − 136` (8 bytes for the
/// push plus 128 bytes to hop over the System V red zone, which an interrupted
/// leaf function may be using for locals), set `RSP` to that slot, then set
/// `RIP` = `async_preempt_trampoline`.  The trampoline's `ret 128` restores
/// the original `RSP` exactly.
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
        // Skip the System V AMD64 red zone: the 128 bytes BELOW the
        // interrupted RSP are scratch space that a leaf function may be using
        // for its locals without adjusting RSP.  Pushing the resume PC at
        // rsp−8 (and letting the trampoline build its 392-byte register-save
        // frame below that) overwrites those locals; the leaf function then
        // resumes with garbage in its red-zone slots — observed as arbitrary
        // downstream corruption (wild pointers, double-frees) in debug
        // builds.  Go does not need this because the Go ABI has no red zone.
        // The trampoline compensates with `ret 128`, restoring the original
        // RSP exactly.  136 ≡ 8 (mod 16) keeps the trampoline's stack-
        // alignment math identical to a plain rsp−8 push.
        let new_rsp = rsp - 128 - 8;
        *(new_rsp as *mut usize) = rip;
        (*uc).uc_mcontext.gregs[libc::REG_RSP as usize] = new_rsp as libc::greg_t;
        (*uc).uc_mcontext.gregs[libc::REG_RIP as usize] =
            async_preempt_trampoline as *const () as usize as libc::greg_t;
    }

    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    unsafe {
        let uc  = ctx as *mut libc::ucontext_t;
        // Push the ORIGINAL x30 (LR) onto the goroutine stack before
        // clobbering it.  If the interrupted code is a leaf function (or an
        // epilogue after reloading x30), its return address lives only in
        // x30; overwriting it without saving would make the eventual `ret`
        // jump back to the interrupted PC — an infinite loop.  Mirrors Go's
        // sigctxt.pushCall.  The trampoline restores x30 from this slot and
        // pops it before branching to the resume PC.
        let sp = ((*uc).uc_mcontext.sp - 16) as *mut u64; // keep 16-byte alignment
        *sp = (*uc).uc_mcontext.regs[30];
        (*uc).uc_mcontext.sp = sp as u64;
        // x30 = resume PC; the trampoline saves it and branches there at exit.
        (*uc).uc_mcontext.regs[30] = (*uc).uc_mcontext.pc;
        (*uc).uc_mcontext.pc = async_preempt_trampoline as u64;
    }

    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    unsafe {
        let uc  = ctx as *mut libc::ucontext_t;
        let ss  = &mut (*(*uc).uc_mcontext).__ss;
        let rip = ss.__rip as usize;
        let rsp = ss.__rsp as usize;
        // Skip the System V AMD64 red zone — see the Linux x86-64 branch
        // above for the full rationale.  The trampoline's `ret 128` undoes
        // the extra displacement.
        let new_rsp = rsp - 128 - 8;
        *(new_rsp as *mut usize) = rip;
        ss.__rsp = new_rsp as u64;
        ss.__rip = async_preempt_trampoline as *const () as u64;
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    unsafe {
        let uc  = ctx as *mut libc::ucontext_t;
        let ss  = &mut (*(*uc).uc_mcontext).__ss;
        // Push the ORIGINAL x30 (LR) onto the goroutine stack before
        // clobbering it — see the Linux aarch64 branch above for the full
        // rationale (leaf functions keep their return address only in x30;
        // losing it loops the goroutine forever on its next `ret`).
        let sp = (ss.__sp - 16) as *mut u64; // keep 16-byte alignment
        *sp = ss.__lr;
        ss.__sp = sp as u64;
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
    // Block SIGURG for the whole body.  This trampoline-invoked function runs
    // with SIGURG unblocked (the signal mask was restored on sigreturn), so a
    // *nested* SIGURG could otherwise fire while we read `current_g()` below
    // and migrate this goroutine to a different M mid-read.  The thread-local
    // read dispatches through libstd's `LocalKey` glue, so a migration there
    // returns the goroutine that the *old* M scheduled next — and `mcall`
    // would then save THIS stack's registers into THAT goroutine's gobuf,
    // leaving its `sched.sp`/`bp` pointing into a different goroutine's stack
    // (the residual cross-stack `many_goroutines` corruption, pinpointed via a
    // write-side check in `preemptm`).  Go likewise never async-preempts the
    // async-preempt machinery.  `preemptm` unblocks once the save is done on
    // g0; the bail paths below unblock before returning to the trampoline.
    #[cfg(not(windows))]
    unsafe { super::m::block_sigurg() };

    let gp = current_g();
    if gp.is_null() {
        #[cfg(not(windows))]
        unsafe { super::m::unblock_sigurg() };
        return;
    }

    // Defensive second check: the goroutine must still be GRUNNING when we
    // reach here.  sigurg_handler already gates on readgstatus == GRUNNING,
    // but a narrow race can occur on multi-core systems between the signal
    // check and the trampoline executing.  Bailing here prevents preemptm
    // from calling casgstatus(gp, GRUNNING, GPREEMPTED) on a non-GRUNNING G
    // which would spin forever in the CAS retry loop.
    if unsafe { readgstatus(gp) } != GRUNNING {
        #[cfg(not(windows))]
        unsafe { super::m::unblock_sigurg() };
        return;
    }

    // Clear preempt flags (sysmon will re-set them next time).
    unsafe {
        (*gp).preempt     = false;
        (*gp).stackguard0 = (*gp).stack.lo + STACK_GUARD;
    }

    // mcall saves this goroutine's state, switches to g0, and calls preemptm
    // (which unblocks SIGURG once the save has completed on g0).  When the
    // goroutine is rescheduled via gogo, mcall returns here — on whatever M
    // resumes it, whose signal mask already has SIGURG unblocked.
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
    // Balance `async_preempt2`'s `block_sigurg()`: the goroutine's gobuf has
    // now been saved on g0 (no longer on the goroutine's own stack), so it is
    // safe to let SIGURG preempt this M again.  Done first so the rest of the
    // scheduler (schedule/findrunnable) runs preemptable as usual.
    #[cfg(not(windows))]
    unsafe { super::m::unblock_sigurg() };

    let m = current_m();
    unsafe {
        casgstatus(gp, GRUNNING, GPREEMPTED);
        casgstatus(gp, GPREEMPTED, GRUNNABLE);
        (*gp).m   = ptr::null_mut();
        (*m).curg = ptr::null_mut();
        set_current_g(ptr::null_mut());
        (*gp).schedlink = ptr::null_mut();
        sched().global_run_q.push_batch(gp, gp, 1);
    }
    unsafe { schedule() };
    // Returns only on shutdown; mcall_asm exits the thread.
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

/// SIGBUS handler: grow goroutine stack on guard-page fault, or print diagnostics
/// and abort for all other bus errors.
///
/// On macOS, `mprotect(PROT_NONE)` guard-page violations raise `SIGBUS` rather
/// than `SIGSEGV` (Linux convention).  We therefore check for a goroutine
/// guard-page fault first and, if found, grow the stack exactly as the SIGSEGV
/// handler does.  Any other SIGBUS (misaligned access, hardware error, etc.)
/// falls through to the diagnostic print + abort path.
///
/// Not async-signal-safe in the diagnostic path, but we are aborting anyway;
/// the goal is to emit useful output before the process exits.
#[cfg(not(windows))]
unsafe extern "C" fn sigbus_handler(
    _sig:  libc::c_int,
    info:  *mut libc::siginfo_t,
    ctx:   *mut libc::c_void,
) {
    // ── Guard-page fault? Grow the goroutine stack and retry. ────────────────
    if !info.is_null() {
        let fault_addr = unsafe { (*info).si_addr() } as usize;
        if unsafe { try_grow_stack_from_signal(fault_addr, ctx) } {
            return; // SP updated; OS will retry the faulting instruction
        }
    }

    // ── Not a stack fault — print diagnostics (async-signal-safe) and abort. ──
    //
    // ALL output uses write(2) directly.  eprintln!, format!, and
    // Backtrace::force_capture() acquire locks (I/O, symbol resolver, malloc)
    // that may already be held by another thread, causing an unrecoverable
    // deadlock inside the signal handler.  write(2) is listed in POSIX as
    // async-signal-safe and never acquires user-space locks.
    #[inline(always)]
    unsafe fn sig_write(msg: &[u8]) {
        unsafe { libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len()) };
    }
    #[inline(always)]
    unsafe fn sig_hex(label: &[u8], val: u64) {
        unsafe { sig_write(label) };
        const H: &[u8] = b"0123456789abcdef";
        let mut buf = [b'0'; 19]; // "0x" + 16 hex digits + "\n"
        buf[0] = b'0'; buf[1] = b'x';
        for i in 0..16usize { buf[17 - i] = H[((val >> (i * 4)) & 0xf) as usize]; }
        buf[18] = b'\n';
        unsafe { sig_write(&buf) };
    }

    // Also print fault address and goroutine stack bounds for root-cause analysis.
    if !info.is_null() {
        let fault_addr = unsafe { (*info).si_addr() } as u64;
        unsafe { sig_hex(b"[go-lib SIGBUS] fault_addr = ", fault_addr) };
    }
    let gp = super::g::current_g();
    if !gp.is_null() {
        let lo = unsafe { (*gp).stack.lo } as u64;
        let hi = unsafe { (*gp).stack.hi } as u64;
        unsafe { sig_hex(b"[go-lib SIGBUS] stack.lo = ", lo) };
        unsafe { sig_hex(b"[go-lib SIGBUS] stack.hi = ", hi) };
    }
    unsafe { sig_write(b"[go-lib SIGBUS] crash (non-stack fault)\n") };

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    if !ctx.is_null() {
        unsafe {
            let uc = ctx as *mut libc::ucontext_t;
            let ss = &(*(*uc).uc_mcontext).__ss;
            sig_hex(b"[go-lib SIGBUS] PC = ", ss.__pc);
            sig_hex(b"[go-lib SIGBUS] LR = ", ss.__lr);
            sig_hex(b"[go-lib SIGBUS] SP = ", ss.__sp);
            sig_hex(b"[go-lib SIGBUS] FP = ", ss.__fp);
        }
    }

    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    if !ctx.is_null() {
        unsafe {
            let uc = ctx as *mut libc::ucontext_t;
            let mc = &(*uc).uc_mcontext;
            sig_hex(b"[go-lib SIGBUS] PC = ", mc.pc);
            sig_hex(b"[go-lib SIGBUS] SP = ", mc.sp);
            sig_hex(b"[go-lib SIGBUS] LR = ", mc.regs[30]);
        }
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    if !ctx.is_null() {
        unsafe {
            let uc  = ctx as *mut libc::ucontext_t;
            sig_hex(b"[go-lib SIGBUS] RIP = ", (*uc).uc_mcontext.gregs[libc::REG_RIP as usize] as u64);
            sig_hex(b"[go-lib SIGBUS] RSP = ", (*uc).uc_mcontext.gregs[libc::REG_RSP as usize] as u64);
        }
    }

    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    if !ctx.is_null() {
        unsafe {
            let uc = ctx as *mut libc::ucontext_t;
            let ss = &(*(*uc).uc_mcontext).__ss;
            sig_hex(b"[go-lib SIGBUS] RIP = ", ss.__rip);
            sig_hex(b"[go-lib SIGBUS] RSP = ", ss.__rsp);
            sig_hex(b"[go-lib SIGBUS] RAX = ", ss.__rax);
            sig_hex(b"[go-lib SIGBUS] RBX = ", ss.__rbx);
            sig_hex(b"[go-lib SIGBUS] RCX = ", ss.__rcx);
            sig_hex(b"[go-lib SIGBUS] RDX = ", ss.__rdx);
            sig_hex(b"[go-lib SIGBUS] RSI = ", ss.__rsi);
            sig_hex(b"[go-lib SIGBUS] RDI = ", ss.__rdi);
            sig_hex(b"[go-lib SIGBUS] RBP = ", ss.__rbp);
            sig_hex(b"[go-lib SIGBUS] R12 = ", ss.__r12);
            sig_hex(b"[go-lib SIGBUS] R13 = ", ss.__r13);
            sig_hex(b"[go-lib SIGBUS] R14 = ", ss.__r14);
            sig_hex(b"[go-lib SIGBUS] R15 = ", ss.__r15);
            // [RSP-8]: the word the CPU would have popped in a `ret` just
            // before the fault.  If this equals RIP, the crash is a `ret`
            // through a corrupted return address.
            let rsp = ss.__rsp as usize;
            if rsp >= 8 {
                let below = *((rsp - 8) as *const u64);
                sig_hex(b"[go-lib SIGBUS] [RSP-8] = ", below);
            }
        }
    }

    // Print g0 stack bounds and current M pointer for context.
    let mp = super::m::current_m();
    if !mp.is_null() {
        let g0 = unsafe { (*mp).g0 };
        if !g0.is_null() {
            let g0lo = unsafe { (*g0).stack.lo } as u64;
            let g0hi = unsafe { (*g0).stack.hi } as u64;
            unsafe { sig_hex(b"[go-lib SIGBUS] g0.stack.lo = ", g0lo) };
            unsafe { sig_hex(b"[go-lib SIGBUS] g0.stack.hi = ", g0hi) };

            // Scan the g0 stack from [RSP-8] up to g0.hi.  This reveals
            // goexit0's entire saved-register frame and the slot below RSP,
            // which is key to distinguishing a `ret`-through-corruption from
            // a `jmpq *reg` crash.
            #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
            if !ctx.is_null() {
                unsafe {
                    let uc  = ctx as *mut libc::ucontext_t;
                    let ss  = &(*(*uc).uc_mcontext).__ss;
                    let rsp = ss.__rsp as usize;
                    let hi  = g0hi as usize;
                    let start = if rsp >= 8 { rsp - 8 } else { rsp };
                    let end   = hi.min(start + 20 * 8);
                    let mut addr = start;
                    sig_write(b"[go-lib SIGBUS] --- g0 stack scan (RSP-8 .. g0.hi) ---\n");
                    while addr < end {
                        sig_hex(b"[go-lib SIGBUS]  @", addr as u64);
                        let val = *(addr as *const u64);
                        sig_hex(b"[go-lib SIGBUS]  = ", val);
                        addr += 8;
                    }
                }
            }
        }
    } else {
        unsafe { sig_write(b"[go-lib SIGBUS] current_m = NULL\n") };
    }

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
// m_thread_exit — OS-thread termination for M-threads on Rt shutdown
// ---------------------------------------------------------------------------

/// Terminate the current OS thread.
///
/// Called by `mcall_asm` after an mcall target (e.g. `goexit0`, `gosched_m`,
/// `park_fn`) returns — which only happens when the owning `Rt` has set
/// `shutdown = true` and `schedule()` returned `()` instead of looping.
///
/// The `ud2` / `brk #1` that previously followed `call rcx` / `blr x3` in
/// `mcall_asm` is replaced by `call m_thread_exit` / `bl m_thread_exit` so
/// M-threads exit cleanly instead of crashing with SIGILL.
///
/// # Safety
/// Must only be called when the Rt's shutdown flag is set and the M-thread
/// has no more work to do.  The call terminates the OS thread immediately
/// without unwinding the Rust stack; any `Drop` impls on live stack variables
/// in the mcall-target frame are skipped.  This is safe because:
/// * Any goroutines still parked at shutdown are process-lifetime allocations
///   (Go-faithful: goroutines are never force-reclaimed), so nothing that
///   required `Drop` is abandoned by terminating this thread.
/// * g0's stack variables at exit time are POD types with no meaningful `Drop`.
/// * The `Box::leak`'d `Rt` and mmap'd g0 stack are process-lifetime allocations.
#[unsafe(no_mangle)]
pub(crate) unsafe extern "C" fn m_thread_exit() -> ! {
    #[cfg(not(windows))]
    unsafe {
        libc::pthread_exit(core::ptr::null_mut());
    }
    #[cfg(windows)]
    unsafe {
        m_thread_exit_sys::ExitThread(0);
    }
    #[allow(unreachable_code)]
    loop {}
}

#[cfg(windows)]
mod m_thread_exit_sys {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        pub fn ExitThread(dwExitCode: u32) -> !;
    }
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
            //
            // Unwrap up to 4 levels of `Box<dyn Any + Send>` nesting.  Without
            // this, `scope::scope`'s re-panic via `std::panic::panic_any(payload)`
            // (where `payload` is itself `Box<dyn Any>`) hides the real message:
            // the outer payload's runtime type becomes `Box<dyn Any + Send>`, so
            // a direct `downcast::<String>()` on the outer Box always fails and
            // we'd otherwise print the unhelpful "(unknown panic payload)".
            let msg = extract_panic_msg(&payload);
            eprintln!("goroutine panicked: {msg}");
        }
    }
}

/// Best-effort string extraction from a panic payload, recursing through up to
/// `MAX_DEPTH` levels of `Box<dyn Any + Send>` wrapping (see comment in
/// `handle_goroutine_panic`).
fn extract_panic_msg(payload: &(dyn Any + Send)) -> String {
    const MAX_DEPTH: u32 = 4;
    fn recurse(p: &(dyn Any + Send), depth: u32) -> Option<String> {
        if let Some(s) = p.downcast_ref::<String>() {
            return Some(s.clone());
        }
        if let Some(s) = p.downcast_ref::<&str>() {
            return Some((*s).to_string());
        }
        if depth < MAX_DEPTH {
            if let Some(inner) = p.downcast_ref::<Box<dyn Any + Send + 'static>>() {
                return recurse(inner.as_ref(), depth + 1);
            }
        }
        None
    }
    recurse(payload, 0).unwrap_or_else(|| "(unknown panic payload)".to_string())
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
                unsafe { spawn_m(sc, id, p_ptr) };
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

/// Free pool of retired G descriptors — the port of Go's `gfree` lists.
///
/// G allocations are IMMORTAL: once created, a `G` struct's heap allocation is
/// never returned to the heap, only recycled through this pool.  Immortality is
/// load-bearing, not an optimisation: `sysmon`'s `preemptone` dereferences
/// `m.curg` and writes `preempt` / `stackguard0` through it with no
/// synchronisation against goroutine exit (mirroring Go).  If the Box were
/// freed, that write would land in unmapped/recycled heap memory.  With the
/// pool the memory stays valid, so the worst case is a stray `preempt = true`
/// or `stackguard0` write on a dormant or reused G, which is overwritten by the
/// `G::value` reinit on reuse and otherwise handled by the SIGURG guards.
///
/// Reuse is safe ONLY because the kill paths are gone: every descriptor that
/// reaches this pool was retired by `goexit0`, i.e. its goroutine ran to
/// completion while `GRUNNING` and was never parked on a channel — so no peer
/// ever held a reference to it (no leaked `sudog.g`, no pending `gp.param`
/// write).  The earlier reuse attempt that hit a double-free recycled
/// force-killed (parked) descriptors, which DID have live external references;
/// those paths no longer exist.
///
/// A pooled descriptor keeps its mmap'd stack ONLY when that stack is still the
/// default initial size, so a reused G also skips a `stack_alloc`/`stack_free`
/// pair.  A descriptor whose goroutine *grew* its stack parks stackless: the
/// oversized stack is handed to the size-classed stack pool (`stack_pool_free`)
/// and re-acquired at default size on reuse.  This mirrors Go's `gfput`/`gfget`
/// (`stksize != startingStackSize` → `stackfree`) and is what stops grown
/// stacks from accumulating in this unbounded pool — the RSS bound lives in the
/// stack pool's cap, not here.  Stored as `usize` addresses because `*mut G` is
/// not `Send`.
static G_FREE: Mutex<Vec<usize>> = Mutex::new(Vec::new());

/// Whether descriptor reuse is disabled (`GOLIB_GPOOL_OFF=1`).  Ablation lever
/// for A/B-bisecting any reuse-related regression: when set, retired
/// descriptors free their stack and drop the Box (the historical leak-only
/// behaviour) and `new_goroutine` always allocates fresh.  0 = reuse on (default),
/// 1 = off, `u8::MAX` sentinel = not yet read.
static GPOOL_OFF: AtomicU8 = AtomicU8::new(u8::MAX);

#[inline]
fn gpool_off() -> bool {
    let mut v = GPOOL_OFF.load(Relaxed);
    if v == u8::MAX {
        v = match std::env::var("GOLIB_GPOOL_OFF") {
            Ok(s) if s == "1" => 1,
            _ => 0,
        };
        GPOOL_OFF.store(v, Relaxed);
    }
    v == 1
}

/// Retire a dead G's descriptor, recycling it (and its stack) through [`G_FREE`].
///
/// The caller (`goexit0`) must already have transitioned the G to `GDEAD`,
/// cleared `m.curg` / `current_g`, and removed it from `allg` and every queue.
/// The G's stack is retained for reuse — the caller must NOT `stack_free` it.
///
/// With `GOLIB_GPOOL_OFF=1`, frees the stack and drops the owning Box instead
/// (leak-only ablation: the Box is dropped, so the descriptor IS freed here —
/// acceptable because the no-kill-path guarantee means no stale reference can
/// reach it; the immortality requirement is purely about reuse correctness, and
/// the historical leak avoided the freed-descriptor sysmon UAF by never freeing).
unsafe fn gfree_put(gp: *mut G) {
    if gpool_off() {
        // Ablation: behave like the historical leak path — release the stack
        // (to the stack pool, or unmapped; independent of this lever) and leak
        // the descriptor (never recycle, never free) so sysmon's blind write
        // can never hit freed memory.
        let stack = unsafe { (*gp).stack };
        unsafe { stack_pool_free(&stack) };
        // Leak the descriptor on purpose (immortal); do not drop the Box.
        return;
    }
    // Reuse path.  If the goroutine grew its stack beyond the default size,
    // hand that oversized stack to the size-classed stack pool and park the
    // descriptor stackless (Go's `gfput`: `stksize != startingStackSize` →
    // `stackfree`).  This bounds the memory the unbounded `G_FREE` pool can
    // retain; the descriptor re-acquires a default stack on reuse.
    let stack = unsafe { (*gp).stack };
    if stack.hi - stack.lo != GOROUTINE_STACK_BYTES {
        unsafe { stack_pool_free(&stack) };
        unsafe { (*gp).stack = Stack { lo: 0, hi: 0 } };
    }
    // `m_lock` pins this M so SIGURG cannot migrate us mid-`MutexGuard` (a std
    // MutexGuard must unlock on the thread that locked it).
    let _lk = super::m::m_lock();
    G_FREE.lock().unwrap().push(gp as usize);
}

/// Pop a retired descriptor from [`G_FREE`], or `None` if the pool is empty.
///
/// The returned descriptor still owns its pooled stack UNLESS its goroutine grew
/// the stack (then it parked stackless — `stack.lo == 0` — and the caller must
/// allocate a fresh default stack).  Either way the caller reinitialises the `G`
/// in place (via `G::value`) for the new goroutine.
unsafe fn gfree_get() -> Option<*mut G> {
    if gpool_off() {
        return None;
    }
    let _lk = super::m::m_lock();
    G_FREE.lock().unwrap().pop().map(|a| a as *mut G)
}

/// Monotonically-increasing M ID counter.
static NEXT_MID: AtomicI64 = AtomicI64::new(1);

/// Guards signal handler installation so it happens at most once per process.
static SIGNALS_INSTALLED: AtomicBool = AtomicBool::new(false);

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
    // Block SIGURG across the `current_g()` read and the closure-pointer fetch.
    // `gogo` set `current_g` to this fresh goroutine just before jumping here,
    // but if async preemption splits the thread-local read below and migrates
    // the goroutine, `current_g()` returns the goroutine the old M scheduled
    // next — whose `sched.ctxt` we would then consume (it is null after that
    // goroutine's own entry → `Box::from_raw(null)` aborts; observed as
    // "goroutine_entry: NULL ctxt").  We unblock before running the user
    // closure so the closure body remains preemptable.  (goroutine_entry is
    // also in `pc_in_scheduler_asm`, covering the prologue before this block.)
    #[cfg(not(windows))]
    unsafe { super::m::block_sigurg() };
    let gp = current_g();
    let go_fn = unsafe {
        let fn_ptr = (*gp).sched.ctxt as *mut GoFn;
        (*gp).sched.ctxt = ptr::null_mut();
        Box::from_raw(fn_ptr)
    };

    // The closure pointer is now safely in hand; allow preemption again before
    // running (potentially long-lived) user code.
    #[cfg(not(windows))]
    unsafe { super::m::unblock_sigurg() };

    // Catch panics so they don't abort the process.  The closure may capture
    // non-UnwindSafe types (raw pointers, RefCell, …) so we assert that it is
    // safe — the goroutine's stack is unwound by catch_unwind and no invariants
    // observable to other goroutines are left broken (channels are locked
    // briefly and always released before goroutines block or return).
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (go_fn.0)()));
    if let Err(payload) = result {
        // Wrap `handle_goroutine_panic` in a second `catch_unwind`.  Without
        // this, any panic from the user-supplied panic handler (or even from
        // `eprintln!` writing to a broken stderr — common in CI with output
        // capture) propagates out of `goroutine_entry` — an `extern "C" fn` —
        // and Rust aborts with the dreaded "thread caused non-unwinding panic.
        // aborting." (SIGABRT).  That used to abort the whole test process
        // when a single goroutine's panic-reporting machinery failed.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            handle_goroutine_panic(payload);
        }));
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
/// This function genuinely never returns: in the normal path `goexit0` calls
/// `schedule()` which loops forever; on Rt shutdown `schedule()` returns but
/// mcall_asm's `call m_thread_exit` terminates the OS thread before returning
/// here.  The `unreachable_unchecked` is sound in both cases.
///
/// ## Why we bump `m.locks` here
///
/// Incrementing `(*m).locks` causes `sigurg_handler` (Guard 0) to skip async
/// preemption.  Without this, SIGURG can fire between `goroutine_entry`'s
/// `ret` and `goexit0`'s `casgstatus` → `schedule()`.
///
/// If preemption fires inside the `mcall(gp, goexit0)` call here, the async
/// preempt's own `mcall` OVERWRITES `gp.sched.pc` with the trampoline's
/// resume address.  When `gogo` later resumes the goroutine it jumps to that
/// resume point — which is the instruction immediately after
/// `unsafe { mcall(gp, goexit0) }` below — and hits `unreachable_unchecked`,
/// aborting the process with "thread caused non-unwinding panic. aborting."
#[cfg(target_arch = "x86_64")]
unsafe extern "C" fn goexit0_handler() -> ! {
    // Block SIGURG across the locks bump, the `current_g()` read and the mcall
    // save.  Both thread-local reads here are otherwise split-prone: a SIGURG
    // that migrated this goroutine mid-read would land the `locks` bump on the
    // wrong M and/or make `mcall` save into the wrong goroutine's gobuf (the
    // cross-stack corruption fixed in `async_preempt2`).  `goexit0` unblocks on
    // g0.  (goexit0_handler is also in `pc_in_scheduler_asm`, covering the
    // prologue before this block takes effect.)
    #[cfg(not(windows))]
    unsafe { super::m::block_sigurg() };
    // Raw `locks += 1` (not an MLockGuard): mcall never returns here, so a
    // guard's Drop would never run and the count would stay elevated for the
    // rest of the M's life, permanently disabling async preemption on this M.
    // goexit0 decrements once the G is GDEAD and current_g is cleared.
    unsafe { (*current_m()).locks.fetch_add(1, std::sync::atomic::Ordering::Relaxed) };
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
    // Block SIGURG across the bump + current_g() read + mcall save — see the
    // x86-64 goexit0_handler for the rationale.  goexit0 unblocks on g0.
    #[cfg(not(windows))]
    unsafe { super::m::block_sigurg() };
    // Raw `locks += 1` for the entire goexit path; goexit0 decrements once
    // the G is dead — see goexit0_handler's doc comment for the full
    // explanation of why an RAII guard must not be used here.
    unsafe { (*current_m()).locks.fetch_add(1, std::sync::atomic::Ordering::Relaxed) };
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
/// Returns a raw pointer to an immortal `G` — either a freshly heap-allocated
/// one or a descriptor recycled from the gFree pool (see [`gfree_get`]).  The
/// pointer is owned by the scheduler from here on and is retired via
/// [`gfree_put`] at goroutine exit.
///
/// Ported from `newproc1` + `gfget` in `runtime/proc.go`.
pub(crate) fn new_goroutine(f: impl FnOnce() + Send + 'static) -> *mut G {
    let goid = NEXT_GOID.fetch_add(1, Relaxed);

    // Reuse a retired descriptor (and its pooled stack) when one is available;
    // otherwise allocate a fresh stack + `Box<G>`.  Reuse is sound because the
    // pool only ever holds descriptors retired by `goexit0` — goroutines that
    // ran to completion while GRUNNING and were never parked, so no stale
    // external reference (a leaked `sudog.g`, a pending `gp.param` write) can
    // exist.  Reinitialising in place via `G::value` overwrites every field
    // (`param`, `waiting_*`, `preempt`, `atomicstatus`, …), clearing any stray
    // sysmon write the dormant descriptor may have received.
    let gp: *mut G = match unsafe { gfree_get() } {
        Some(gp) => {
            // Recycle: reuse the pooled stack when the descriptor kept one
            // (default-sized); if it parked stackless (its goroutine had grown
            // the stack, which `gfree_put` returned to the stack pool), acquire
            // a fresh default stack — itself likely served from the stack pool.
            let mut stack = unsafe { (*gp).stack };
            if stack.lo == 0 {
                stack = unsafe {
                    stack_alloc().expect("new_goroutine: stack_alloc failed (reuse)")
                };
            }
            // Reset all fields in place, then re-point the self-referential
            // `sched.g` at this allocation (the reuse contract on `G::value`).
            unsafe { ptr::write(gp, G::value(stack, goid)) };
            unsafe { (*gp).sched.g = gp };
            gp
        }
        None => {
            let stack = unsafe { stack_alloc().expect("new_goroutine: stack_alloc failed") };
            Box::into_raw(G::new(stack, goid))
        }
    };
    let g: &mut G = unsafe { &mut *gp };

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
    gp
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
        !current_rt_ptr().is_null(),
        "spawn_goroutine called without an active Rt; call run_impl first"
    );
    let g_ptr = new_goroutine(f);
    // SAFETY: g_ptr is a freshly-allocated, uniquely-owned G.  push_batch and
    // startm are unsafe because they manipulate the scheduler's internal queues
    // without a typed lock; their preconditions (non-null pointer, valid G
    // layout) are satisfied by the new_goroutine constructor above.
    //
    // The `_lk` guard increments `(*current_m).locks` for the duration of the
    // push_batch + startm critical section.  Without this, SIGURG can fire
    // while we hold the global-queue mutex inside push_batch, redirect to
    // preemptm, and self-deadlock when preemptm calls push_batch again.
    let _lk = super::m::m_lock();
    unsafe {
        // Register in allg before the G becomes visible via the run queue.
        // Ordering: allg insert → push_batch → startm, so the live-goroutine
        // registry is never a subset of what M-threads can find in queues.
        sched().allg.lock().unwrap().push(g_ptr);
        (*g_ptr).schedlink = ptr::null_mut();
        sched().global_run_q.push_batch(g_ptr, g_ptr, 1);
        startm(ptr::null_mut());
    }
}

// ---------------------------------------------------------------------------
// schedinit — create Ps and spawn M threads
// ---------------------------------------------------------------------------

/// Create the process-wide singleton `Rt` and return a `'static` reference
/// to it.  Called exactly once, by the first `run_impl`, via `GLOBAL_RT`'s
/// `get_or_init`.  The `Rt` is heap-allocated and intentionally leaked; it
/// is valid for the remainder of the process.  GOMAXPROCS is fixed here by
/// the first caller (or the `GOMAXPROCS` env var).
///
/// Sets `CURRENT_RT` on the calling thread so that `spawn_goroutine` (called
/// immediately after) can find the scheduler.
///
/// Ported from `schedinit` + `procresize` + `mstart` in `runtime/proc.go`.
fn schedinit(nprocs: i32) -> &'static Rt {
    assert!(nprocs >= 1, "schedinit: nprocs must be ≥ 1");

    // Install a panic hook that suppresses panics originating from goroutine
    // threads — but only once per process so we don't stack hooks.
    //
    // Goroutine panics are caught by `goroutine_entry`'s `catch_unwind` which
    // always runs on the same OS thread as the goroutine (no cross-thread
    // unwind), so landing-pad lookup is always correct.  Forwarding them to
    // an external hook first would trigger a spurious test failure before
    // `catch_unwind` has had a chance to route the payload to the caller.
    {
        static PANIC_HOOK_INSTALLED: AtomicBool = AtomicBool::new(false);
        if !PANIC_HOOK_INSTALLED.swap(true, AcqRel) {
            let prev_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                if current_g().is_null() {
                    prev_hook(info);
                }
            }));
        }
    }

    // Allow the GOMAXPROCS environment variable to override the caller's value.
    let nprocs = std::env::var("GOMAXPROCS")
        .ok()
        .and_then(|s| s.parse::<i32>().ok())
        .filter(|&n| (1..=256).contains(&n))
        .unwrap_or(nprocs);

    // Allocate the process-wide Rt and leak it so that M-threads (which
    // outlive any single run_impl stack frame) can hold `&'static Rt`
    // references.
    let rt: &'static Rt = Box::leak(Box::new(Rt {
        global_run_q: GlobalRunQueue::new(),
        nmspinning:   AtomicI32::new(0),
        gomaxprocs:   AtomicI32::new(nprocs),
        inner: Mutex::new(SchedInner {
            idle_p:     ptr::null_mut(),
            idle_m:     ptr::null_mut(),
            nmidle:     0,
            allp:       Vec::new(),
            gomaxprocs: nprocs,
        }),
        allg:     Mutex::new(Vec::new()),
        shutdown: AtomicBool::new(false),
    }));

    // Bind this Rt to the calling thread so spawn_goroutine can find it.
    set_current_rt(rt as *const Rt);

    // Create all Ps.
    let ps: Vec<*mut P> = (0..nprocs)
        .map(|id| Box::into_raw(P::new(id)))
        .collect();

    {
        let mut inner = rt.inner.lock().unwrap();
        inner.allp = ps.clone();
    }

    // Spawn one M per P.  Each M thread sets CURRENT_RT = rt on its thread.
    for p_ptr in ps {
        let id = NEXT_MID.fetch_add(1, Relaxed);
        unsafe { spawn_m(rt, id, p_ptr) };
    }

    // Install signal handlers once per process.
    if !SIGNALS_INSTALLED.swap(true, AcqRel) {
        #[cfg(not(windows))]
        unsafe { install_sigsegv_handler() };
        #[cfg(not(windows))]
        unsafe { install_sigurg_handler() };
        #[cfg(not(windows))]
        unsafe { install_sigbus_handler() };
        #[cfg(windows)]
        install_windows_veh();
    }

    // Initialise the netpoll backend synchronously before any goroutines start.
    super::netpoll::netpoll_init();

    // Start per-Rt background threads.
    start_sysmon(rt);
    start_timer_thread();

    rt
}

/// Allocate a new M, wire it to `p`, and spawn an OS thread that runs the
/// scheduler loop on that M.
///
/// The raw pointers are transmitted across the thread boundary by casting to
/// `usize` (which is `Send`); M, P, and Rt are all valid for the process
/// lifetime once created.
unsafe fn spawn_m(rt: &'static Rt, id: i64, p: *mut P) {
    let m = Box::into_raw(unsafe { M::new(id) });

    // Wire M ↔ P before the thread starts so schedule() sees a valid P.
    unsafe {
        (*m).p = p;
        (*p).m = m;
        (*p).status.store(PRUNNING, Release);
    }

    let m_addr  = m as usize;
    let rt_addr = rt as *const Rt as usize;

    std::thread::spawn(move || {
        let m  = m_addr  as *mut M;
        let rt = rt_addr as *const Rt;
        unsafe {
            // Bind this thread to its Rt so sched() works.
            set_current_rt(rt);
            // Initialise CURRENT_M, G0_SCHED, CURRENT_G for this thread.
            (*m).start();
            // Enter the scheduler loop; returns when Rt signals shutdown.
            schedule();
            // schedule() returned — Rt is shutting down.  The OS thread exits
            // naturally as the closure returns.
        }
    });
}


// ---------------------------------------------------------------------------
// run_impl — public entry point (exposed as go_lib::run)
// ---------------------------------------------------------------------------

/// Initialise the scheduler and run `f` as the first goroutine, returning
/// whatever `f` returns.
///
/// Blocks the calling thread until `f` returns (or panics).
///
/// # Return value
///
/// The value returned by `f` is shuttled back to the calling thread via an
/// `Arc<Mutex<Option<R>>>` slot.  The slot is filled *before* the drop-guard
/// fires, so the calling thread always sees the value as soon as `park()`
/// returns.
///
/// If `f` panics before producing a return value the slot stays `None` and
/// this function panics with a clear message.
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
pub(crate) fn run_impl<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let nprocs = std::thread::available_parallelism()
        .map(|n| n.get() as i32)
        .unwrap_or(1);

    // Attach to the process-wide singleton Rt, creating it on first use.
    // GOMAXPROCS is fixed by the first caller (or the env var); later calls
    // share the same Ps and M-threads.  Goroutines are process-global and are
    // never force-reclaimed — `run()` returning only unblocks the caller (see
    // the Go-faithful semantics note after `park()` below).
    let rt: &'static Rt = *GLOBAL_RT.get_or_init(|| schedinit(nprocs));
    set_current_rt(rt as *const Rt);

    // Drop guard: unparks the calling thread whether `f` returns or panics.
    struct UnparkOnDrop(std::thread::Thread);
    impl Drop for UnparkOnDrop {
        fn drop(&mut self) { self.0.unpark(); }
    }

    // Slot shuttles either the return value or the panic payload from the
    // goroutine back to the caller.
    type Slot<R> = Result<R, Box<dyn std::any::Any + Send + 'static>>;
    let slot: Arc<Mutex<Option<Slot<R>>>> = Arc::new(Mutex::new(None));
    let slot2 = Arc::clone(&slot);

    let caller = std::thread::current();
    let wrapper = move || {
        let _guard = UnparkOnDrop(caller);
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        *slot2.lock().unwrap() = Some(result);
    };

    spawn_goroutine(wrapper);

    // Block until the goroutine's drop-guard fires caller.unpark().
    std::thread::park();

    // Go-faithful semantics: goroutines are process-global and are NEVER
    // force-reclaimed.  When the top-level `f` returns we simply unblock the
    // caller and return its value; any goroutine `f` left blocked persists for
    // the process lifetime, exactly like a leaked goroutine in Go.  Use
    // `scope()` for structured, deterministic cleanup.  (The former Phase 2b
    // drain + reapers — which force-killed parked goroutines mid-rendezvous and
    // made their descriptors unsafe to recycle — have been removed.)

    // The singleton Rt, its Ps, and its M-threads stay alive for the
    // process lifetime (mirroring Go's never-torn-down runtime); there is
    // no shutdown signal.

    match slot.lock().unwrap().take() {
        Some(Ok(v))        => v,
        Some(Err(payload)) => {
            let msg = extract_panic_msg(payload.as_ref());
            std::panic::panic_any(format!("goroutine panicked: {msg}"))
        }
        None => panic!("go_lib::run: first goroutine exited without storing a result"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    // ── Global run-queue round-trip ───────────────────────────────────────

    #[test]
    fn global_run_queue_round_trip() {
        use crate::runtime::g::{Stack, G};
        use crate::runtime::p::GlobalRunQueue;
        use crate::runtime::stack::GOROUTINE_STACK_BYTES;

        // Use a STANDALONE queue, not `sched().global_run_q`.
        // The live global queue is shared with background M-threads that call
        // findrunnable() and would immediately execute any G we push there —
        // a fake G with a non-mmap'd stack would SIGSEGV on context switch.
        let q = GlobalRunQueue::new();

        let lo    = 0x200000usize;
        let g1    = G::new(Stack { lo, hi: lo + GOROUTINE_STACK_BYTES }, 99);
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

        let g_ptr = new_goroutine(|| {});

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

            // Retire the descriptor (immortal — leaked, never freed); the
            // closure and stack leak too, which is acceptable in a unit test.
            gfree_put(g_ptr);
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

    /// Regression test: retiring a goroutine must not leave `m.locks`
    /// elevated.
    ///
    /// `goexit_trampoline` / `goexit0_handler` bump `m.locks` before
    /// `mcall(gp, goexit0)`.  Since mcall never returns there, the count must
    /// be released inside `goexit0` itself; if it leaked, every M that ever
    /// retired a goroutine would keep `locks > 0` forever, permanently
    /// disabling SIGURG async preemption on that M (sigurg_handler Guard 0).
    ///
    /// Strategy: retire a batch of goroutines (each exit runs the goexit path
    /// on some M), then run a second batch of checkers, each recording the
    /// `locks` value of the M it starts on.  At goroutine entry no MLockGuard
    /// is held, so every observed value must be 0.  With the leak, any
    /// checker scheduled onto an M that retired a first-batch goroutine
    /// observes `locks >= 1`.
    #[test]
    fn goroutine_exit_releases_m_locks() {
        use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
        use std::sync::Arc;

        const N: usize = 64;

        let exited     = Arc::new(AtomicUsize::new(0));
        let checked    = Arc::new(AtomicUsize::new(0));
        // Maximum `m.locks` observed at goroutine entry, per batch.  Batch 1
        // samples BEFORE any goroutine has exited (a nonzero value there
        // means the stray count predates the goexit path — spawn/ready side);
        // batch 2 samples after 64 exits (nonzero only here ⇒ exit-path leak).
        let max_locks1 = Arc::new(AtomicI32::new(0));
        let max_locks2 = Arc::new(AtomicI32::new(0));

        let exited_w     = Arc::clone(&exited);
        let checked_w    = Arc::clone(&checked);
        let max_locks1_w = Arc::clone(&max_locks1);
        let max_locks2_w = Arc::clone(&max_locks2);

        run_impl(move || {
            // Batch 1: N goroutines that record their M's `locks` and exit.
            for _ in 0..N {
                let exited     = Arc::clone(&exited_w);
                let max_locks1 = Arc::clone(&max_locks1_w);
                spawn_goroutine(move || {
                    let locks = unsafe { (*current_m()).locks.load(Ordering::Relaxed) };
                    max_locks1.fetch_max(locks, Ordering::AcqRel);
                    exited.fetch_add(1, Ordering::AcqRel);
                });
            }
            while exited_w.load(Ordering::Acquire) < N {
                unsafe { gosched() };
            }

            // Batch 2: N checkers recording their M's `locks` at entry,
            // after every batch-1 goroutine has run the goexit path.
            for _ in 0..N {
                let checked    = Arc::clone(&checked_w);
                let max_locks2 = Arc::clone(&max_locks2_w);
                spawn_goroutine(move || {
                    let locks = unsafe { (*current_m()).locks.load(Ordering::Relaxed) };
                    max_locks2.fetch_max(locks, Ordering::AcqRel);
                    checked.fetch_add(1, Ordering::AcqRel);
                });
            }
            while checked_w.load(Ordering::Acquire) < N {
                unsafe { gosched() };
            }
        });

        let m1 = max_locks1.load(Ordering::Acquire);
        let m2 = max_locks2.load(Ordering::Acquire);
        assert!(
            m1 == 0 && m2 == 0,
            "m.locks nonzero at goroutine entry (batch1 max = {m1}, batch2 \
             max = {m2}) — batch1 > 0 means the stray count predates any \
             goroutine exit (spawn/ready side); batch2-only means the goexit \
             path leaked an increment"
        );
    }

    /// Migration detector for the optimistic `m_lock` pin (replaces the old
    /// `pthread_sigmask` window-blocking).  Once `m_lock` returns, `m.locks`
    /// is > 0, so sigurg_handler Guard 0 must suppress async preemption for the
    /// guard's whole lifetime — meaning `current_m()` cannot change while the
    /// guard is held.  This depends on Guard 0 observing the atomic `locks`
    /// bump; if that read/write pairing were wrong, a goroutine could be
    /// migrated mid-pin and `current_m()` would change under it.
    ///
    /// Each of many goroutines hammers `m_lock`/release in a tight loop, spins
    /// while pinned (giving SIGURG a chance to wrongly migrate), and yields
    /// between iterations so it actually moves across Ms — maximising the
    /// chance that any pin acquired across a real migration window is observed.
    /// The companion `goroutine_exit_releases_m_locks` test covers the dual
    /// failure (a torn acquire leaking/mis-applying a count).
    #[test]
    fn m_lock_pins_current_m_under_load() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use crate::runtime::m::m_lock;

        const N: usize = 128;

        let violations = Arc::new(AtomicUsize::new(0));
        let done       = Arc::new(AtomicUsize::new(0));
        let violations_w = Arc::clone(&violations);
        let done_w       = Arc::clone(&done);

        run_impl(move || {
            for _ in 0..N {
                let v = Arc::clone(&violations_w);
                let d = Arc::clone(&done_w);
                spawn_goroutine(move || {
                    for _ in 0..2_000 {
                        let guard  = m_lock();
                        let pinned = current_m();
                        // Pinned: locks > 0 ⇒ Guard 0 suppresses preemption, so
                        // current_m() must not move while we hold `guard`.
                        for _ in 0..64 {
                            if current_m() != pinned {
                                v.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            std::hint::spin_loop();
                        }
                        drop(guard);
                        // Unpinned: yield so we genuinely migrate across Ms,
                        // making the next acquire race a real migration window.
                        unsafe { gosched() };
                    }
                    d.fetch_add(1, Ordering::Relaxed);
                });
            }
            while done_w.load(Ordering::Acquire) < N {
                unsafe { gosched() };
            }
        });

        assert_eq!(
            violations.load(Ordering::Acquire),
            0,
            "current_m() changed while an MLockGuard was held — the optimistic \
             pin failed to suppress preemption (Guard 0 did not observe the \
             atomic m.locks bump)"
        );
    }

}
