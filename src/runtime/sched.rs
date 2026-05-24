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

use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, Ordering::*};
use std::sync::{Mutex, OnceLock};

use super::g::{current_g, set_current_g, G, GDEAD, GRUNNABLE, GRUNNING};
use super::m::{current_m, M};
use super::p::{GlobalRunQueue, P, PIDLE, PRUNNING};
use super::stack::stack_alloc;
use super::sysmon::start_sysmon;
use super::time::start_timer_thread;

#[cfg(target_arch = "x86_64")]
use super::asm_amd64::{gogo, mcall};
#[cfg(target_arch = "aarch64")]
use super::asm_arm64::{gogo, mcall};

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
    let m = current_m();

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
    let gp = current_g();
    debug_assert!(!gp.is_null(), "gosched: called from g0 or uninitialised thread");
    unsafe { mcall(gp, gosched_m) };
}

/// Mcall target for `gosched`.  Runs on g0's stack.
unsafe extern "C" fn gosched_m(gp: *mut G) {
    let m = current_m();

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
// Goroutine creation — goroutine_entry, goexit_trampoline, new_goroutine
// ---------------------------------------------------------------------------

/// Opaque wrapper that owns a `Box<dyn FnOnce()>` behind a thin (single-word)
/// pointer.  Stored in `G.sched.ctxt` so `goroutine_entry` can retrieve it.
struct GoFn(Box<dyn FnOnce() + Send + 'static>);

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
    (go_fn.0)();
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
    let goid  = NEXT_GOID.fetch_add(1, Relaxed) as u64;
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
pub(crate) unsafe fn spawn_goroutine(f: impl FnOnce() + Send + 'static) {
    let g_ptr = Box::into_raw(new_goroutine(f));
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

    // Spawn one M per P.  Each M thread calls schedule() and loops forever.
    for p_ptr in ps {
        let id = NEXT_MID.fetch_add(1, Relaxed);
        unsafe { spawn_m(id, p_ptr) };
    }

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
/// Blocks the calling thread until `f` returns.
///
/// Ported from the Go runtime bootstrap (`runtime·rt0_go` → `main.main`).
pub(crate) fn run_impl<F: FnOnce() + Send + 'static>(f: F) {
    let nprocs = std::thread::available_parallelism()
        .map(|n| n.get() as i32)
        .unwrap_or(1);

    schedinit(nprocs);

    // Wrap `f` so it unparks the calling thread when it finishes.
    let caller = std::thread::current();
    let wrapper = move || {
        f();
        caller.unpark();
    };

    unsafe { spawn_goroutine(wrapper) };

    // Block until the goroutine calls caller.unpark().
    std::thread::park();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
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

        let s = sched();
        // Drain any leftover entries from other tests.
        while !unsafe { s.global_run_q.pop() }.is_null() {}

        let lo    = 0x200000usize;
        let g1    = G::new(Stack { lo, hi: lo + 65536 }, 99);
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
            unsafe {
                spawn_goroutine(move || {
                    flag_setter.store(true, Ordering::Release);
                });
            }
            while !flag.load(Ordering::Acquire) {
                unsafe { gosched() };
            }
        });
    }
}
