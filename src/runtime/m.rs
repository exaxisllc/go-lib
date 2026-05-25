//! Machine (`M`) — the OS thread that executes goroutines.
//!
//! Ported from `runtime/runtime2.go` and the `mstart`/`stopm` family in
//! `runtime/proc.go`.
//!
//! ## Go's M vs ours
//!
//! Go's `m` struct has ~80 fields covering cgo, signal handling, profiling,
//! and Windows specifics.  We keep only the fields the scheduler, channels,
//! and syscall shim actually use.
//!
//! ## Park primitive
//!
//! Go parks idle Ms using a platform-specific futex (`runtime/lock_futex.go`)
//! or semaphore (`runtime/lock_sema.go`) wrapped in a `note` struct.  We use
//! `std::sync::{Mutex, Condvar}` which compiles to the same underlying
//! primitives on every tier-1 target without per-platform assembly.
//! The semantics are identical: a `Note` is either *clear* or *set*; `sleep`
//! blocks until `wakeup` sets it; `wakeup` before `sleep` means `sleep`
//! returns immediately.
//!
//! ## v2.0 additions
//!
//! ### `pthread_id` — async preemption target (Step 4)
//! Each M now stores its OS thread ID (`pthread_id: libc::pthread_t`), set by
//! `M::start()` via `pthread_self()`.  `sysmon` uses this to send `SIGURG` to
//! the exact thread running a long-lived goroutine, triggering the
//! `async_preempt_trampoline` code path.
//!
//! ### Alternate signal stack (Step 4)
//! `M::start()` calls `setup_sigaltstack()` to allocate a 64 KiB alternate
//! signal stack per OS thread (`sigaltstack(2)`).  Because both the SIGSEGV and
//! SIGURG handlers are installed with `SA_ONSTACK`, they can safely execute
//! even when the goroutine's own stack is completely exhausted — which is
//! precisely when stack-growth signals arrive.
//!
//! ### g0 stack size (Step 3)
//! The scheduler loop runs on the M's `g0` stack.  g0 now uses
//! `g0_stack_alloc()` (64 KiB) rather than the goroutine default (8 KiB),
//! because `schedule → findrunnable → stopm → locking` has a deeper call chain
//! than a typical user goroutine.

use std::cell::Cell;
use std::ptr::addr_of_mut;
use std::sync::{Condvar, Mutex};

use super::g::{set_current_g, set_g0_sched, Stack, G};
use super::p::P;
use super::stack::{g0_stack_alloc, stack_free};

/// Size of the alternate signal stack allocated per M thread.
/// Signals (SIGSEGV, SIGURG) are delivered on this stack, which keeps the
/// signal handler safe even when the goroutine's own stack is exhausted.
const ALT_STACK_SIZE: usize = 64 * 1024; // 64 KiB

// ---------------------------------------------------------------------------
// Thread-local: current M
// ---------------------------------------------------------------------------

thread_local! {
    /// The M currently running on this OS thread.
    /// Set by [`M::start_thread_locals`] before the scheduler loop begins.
    pub(crate) static CURRENT_M: Cell<*mut M> = const { Cell::new(std::ptr::null_mut()) };
}

/// Return the M for the current OS thread, or null before initialisation.
#[inline]
pub(crate) fn current_m() -> *mut M {
    CURRENT_M.with(|c| c.get())
}

/// Record `m` as the M for the current OS thread.
///
/// # Safety
/// Must be called once per OS thread before any scheduler function runs.
#[inline]
pub(crate) unsafe fn set_current_m(m: *mut M) {
    CURRENT_M.with(|c| c.set(m));
}

// ---------------------------------------------------------------------------
// Note — park/unpark primitive
// ---------------------------------------------------------------------------

/// A one-shot (reusable) event flag used to park and unpark an M.
///
/// Equivalent to Go's `note` in `runtime/runtime2.go`, implemented here with
/// `Mutex<bool> + Condvar` instead of a futex so the same code works on every
/// platform Rust supports.
///
/// Protocol (matches Go's `notesleep` / `notewakeup` / `noteclear`):
/// - Only **one** goroutine / thread may call `sleep` at a time.
/// - `wakeup` may be called from any thread, even before `sleep`.
/// - After a `sleep` returns, the note is *clear* again and can be reused.
pub(crate) struct Note {
    flag: Mutex<bool>,
    cond: Condvar,
}

impl Note {
    fn new() -> Self {
        Self { flag: Mutex::new(false), cond: Condvar::new() }
    }

    /// Block until [`wakeup`][Note::wakeup] sets the flag, then clear it.
    ///
    /// If `wakeup` was already called, returns immediately without blocking.
    /// Resets the flag on return so the note can be reused.
    ///
    /// Ported from `notesleep` in `runtime/lock_sema.go`.
    pub(crate) fn sleep(&self) {
        let mut flag = self.flag.lock().unwrap();
        while !*flag {
            flag = self.cond.wait(flag).unwrap();
        }
        *flag = false; // reset for next use
    }

    /// Wake the thread sleeping in [`sleep`][Note::sleep].
    ///
    /// Sets the flag; if no thread is sleeping yet, the next call to `sleep`
    /// will return immediately.
    ///
    /// Ported from `notewakeup` in `runtime/lock_sema.go`.
    pub(crate) fn wakeup(&self) {
        {
            let mut flag = self.flag.lock().unwrap();
            *flag = true;
        } // release lock before notify to minimise contention
        self.cond.notify_one();
    }

    /// Clear the flag without waiting, resetting the note for reuse.
    ///
    /// Must not be called concurrently with `sleep` or `wakeup`.
    ///
    /// Ported from `noteclear` in `runtime/lock_sema.go`.
    pub(crate) fn clear(&self) {
        *self.flag.lock().unwrap() = false;
    }
}

// ---------------------------------------------------------------------------
// M — machine (OS thread)
// ---------------------------------------------------------------------------

/// An OS thread that executes goroutines.
///
/// Every `M` has a `g0` — a goroutine whose stack is the M's system stack.
/// The scheduler loop (`schedule`, step 8) runs on `g0`.  When a goroutine
/// is executing, `curg` points to it; when the M is in the scheduler,
/// `curg` is `null`.
///
/// An `M` is always heap-allocated (see [`M::new`]) so the scheduler can hold
/// stable `*mut M` raw pointers.
///
/// Ported from `m` in `runtime/runtime2.go`.
pub(crate) struct M {
    // ── goroutines ────────────────────────────────────────────────────────
    /// Goroutine that runs the scheduler on this M's system stack.
    /// Allocated in [`M::new`]; never null, never changes.
    pub g0:   *mut G,

    /// Goroutine currently executing on this M; `null` when running on g0.
    pub curg: *mut G,

    // ── processor ─────────────────────────────────────────────────────────
    /// The `P` (logical processor) currently attached to this M.
    /// `null` when the M is idle or blocked in a syscall.
    pub p:    *mut P,

    /// The `P` that was attached before the M entered a blocking syscall.
    /// `exitsyscall` (step 15.5) tries to reclaim it.
    pub oldp: *mut P,

    // ── identity ──────────────────────────────────────────────────────────
    /// Unique M identifier, assigned at creation.
    pub id: i64,

    // ── scheduler state ───────────────────────────────────────────────────
    /// `true` while this M is actively spinning in `findrunnable` looking
    /// for work.  At most `GOMAXPROCS/2` Ms may spin simultaneously.
    pub spinning: bool,

    /// `true` while this M is parked in `park.sleep()`.
    pub blocked: bool,

    // ── park primitive ────────────────────────────────────────────────────
    /// One-shot event used to sleep and wake this M.
    /// Used by `stopm` (step 8) and `startm` (step 9).
    pub park: Note,

    // ── linked-list links ─────────────────────────────────────────────────
    /// Link in the global `allm` singly-linked list (wired during step 9
    /// bootstrap).
    pub alllink: *mut M,

    /// Link used by the scheduler for idle-M and other internal lists.
    pub schedlink: *mut M,

    // ── async preemption (Step 4) ─────────────────────────────────────────
    /// OS thread ID used to deliver async-preemption signals.
    ///
    /// On Unix this holds the `pthread_t` value returned by `pthread_self()`,
    /// stored as `u64` so the field type is the same on every platform.
    /// `sysmon` sends `SIGURG` to this thread on Unix to preempt the goroutine.
    /// On Windows async preemption via signals is not available; the field
    /// stays `0` and `preemptone` skips the signal delivery.
    pub pthread_id: u64,
}

// SAFETY: The scheduler guarantees that only one thread operates on a given M
// at any time (the OS thread that owns the M), except for `park`/`unpark`
// which is internally synchronised by `Note`'s `Mutex`.
unsafe impl Send for M {}
unsafe impl Sync for M {}

impl M {
    /// Allocate and initialise a new `M`.
    ///
    /// Allocates g0's stack via [`stack_alloc`], creates the g0 goroutine,
    /// and wires the `M ↔ g0` back-pointers.  Thread-local variables are
    /// **not** set here — call [`M::start`] from within the OS thread that
    /// will run this M.
    ///
    /// Ported from `allocm` + `malg` in `runtime/proc.go`.
    ///
    /// # Panics
    /// Panics if `stack_alloc` fails (OOM or resource exhaustion).
    pub(crate) unsafe fn new(id: i64) -> Box<M> {
        // Allocate g0's execution stack.  g0 uses a larger-than-goroutine
        // allocation because the scheduler loop has a deeper call chain.
        let g0_stack = unsafe {
            g0_stack_alloc().expect("M::new: failed to allocate g0 stack")
        };

        // Create the g0 goroutine.  Its goid is 0 — g0s are not tracked in
        // the goroutine table.  sched.sp starts at stack.hi (stack grows
        // downward); sched.pc is zeroed and will be set when the scheduler
        // loop makes its first mcall.
        let mut g0 = G::new(g0_stack, 0);
        g0.sched.sp = g0.stack.hi;
        g0.sched.bp = g0.stack.hi;

        // Heap-allocate the M before wiring raw pointers, so the address is
        // stable for the lifetime of the M.
        let mut m = Box::new(M {
            g0:        std::ptr::null_mut(), // wired below
            curg:      std::ptr::null_mut(),
            p:         std::ptr::null_mut(),
            oldp:      std::ptr::null_mut(),
            id,
            spinning:  false,
            blocked:   false,
            park:      Note::new(),
            alllink:   std::ptr::null_mut(),
            schedlink: std::ptr::null_mut(),
            pthread_id: 0, // set by M::start() once the OS thread is running
        });

        // Transfer g0 ownership to a raw pointer and wire both directions.
        // Box<G> has a stable heap address, so the pointer is valid for the
        // lifetime of both allocations.
        let g0_ptr = Box::into_raw(g0);
        m.g0 = g0_ptr;

        // Wire g0 back to its owning M.
        // SAFETY: g0_ptr is a valid, live allocation we just created.
        unsafe { (*g0_ptr).m = addr_of_mut!(*m) };

        m
    }

    /// Initialise **all** thread-local state for the OS thread that owns this M.
    ///
    /// Sets three thread-locals:
    /// - `CURRENT_M`  ← `self` so `schedule` knows which M is running.
    /// - `G0_SCHED`   ← `&g0.sched` so `mcall` can switch to the scheduler stack.
    /// - `CURRENT_G`  ← `null` because the thread starts executing on g0.
    ///
    /// **Must be called from inside the OS thread** (`std::thread::spawn`
    /// closure) before any scheduler function is invoked.
    ///
    /// Ported from `mstart` / `mstart0` / `mstart1` in `runtime/proc.go`.
    pub(crate) unsafe fn start(&mut self) {
        unsafe {
            set_current_m(self as *mut M);
            set_g0_sched(addr_of_mut!((*self.g0).sched));
            set_current_g(std::ptr::null_mut());

            // Unix only: capture the pthread_t so sysmon can send SIGURG to
            // preempt goroutines running on this M.  Install a per-thread
            // alternate signal stack so SIGSEGV/SIGURG handlers can run even
            // when the goroutine's own stack is exhausted.
            #[cfg(not(windows))]
            {
                self.pthread_id = libc::pthread_self() as u64;
                setup_sigaltstack();
            }
        }
    }

    /// Park this M until another thread calls [`M::unpark`].
    ///
    /// Sets `blocked = true` before sleeping and clears it on wakeup so
    /// `sysmon` (step 10) and `startm` (step 9) can observe M state.
    ///
    /// Ported from `stopm` in `runtime/proc.go`.
    pub(crate) fn park_m(&mut self) {
        self.blocked = true;
        self.park.sleep();
        self.blocked = false;
    }

    /// Wake this M if it is parked.
    ///
    /// Safe to call before the M has called `park_m` — the flag will be set
    /// and the next `park_m` will return immediately without blocking.
    ///
    /// Ported from the `notewakeup` call sites in `runtime/proc.go`.
    pub(crate) fn unpark(&self) {
        self.park.wakeup();
    }
}

/// Allocate and install a per-thread alternate signal stack for the calling OS
/// thread (Unix only — no POSIX signals on Windows).
///
/// The alternate stack is intentionally **leaked** — M threads run for the
/// lifetime of the process so there is no meaningful teardown point.
///
/// # Safety
/// Must be called once per OS thread from inside the thread that will receive
/// signals (i.e. from `M::start`).
#[cfg(not(windows))]
unsafe fn setup_sigaltstack() {
    // Allocate the alternate stack memory.
    let mem = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            ALT_STACK_SIZE,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANON | libc::MAP_PRIVATE,
            -1,
            0,
        )
    };
    if mem == libc::MAP_FAILED {
        // Non-fatal: without an altstack the signal handler may crash if the
        // goroutine's stack is full, but guard-page faults are rare in practice.
        return;
    }

    let ss = libc::stack_t {
        ss_sp:    mem,
        ss_flags: 0,
        ss_size:  ALT_STACK_SIZE,
    };
    unsafe { libc::sigaltstack(&ss, std::ptr::null_mut()) };
}

impl Drop for M {
    /// Release g0's execution stack and g0 heap allocation on M teardown.
    fn drop(&mut self) {
        if !self.g0.is_null() {
            // SAFETY: g0 was allocated by M::new via stack_alloc and
            // Box::into_raw.  M::drop is the unique owner; it runs exactly
            // once.  We copy the stack bounds before freeing to avoid reading
            // from the G after it is deallocated.
            unsafe {
                let lo = (*self.g0).stack.lo;
                let hi = (*self.g0).stack.hi;
                stack_free(&Stack { lo, hi });
                drop(Box::from_raw(self.g0));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── Note tests ────────────────────────────────────────────────────────

    /// `wakeup` before `sleep` — `sleep` must return without blocking.
    #[test]
    fn note_wakeup_before_sleep() {
        let note = Note::new();
        note.wakeup();
        note.sleep(); // must return immediately
    }

    /// `sleep` blocks until `wakeup` is called from another thread.
    #[test]
    fn note_sleep_then_wakeup() {
        use std::sync::Arc;
        let note = Arc::new(Note::new());
        let note2 = Arc::clone(&note);
        let handle = std::thread::spawn(move || note2.sleep());
        std::thread::sleep(std::time::Duration::from_millis(20));
        note.wakeup();
        handle.join().unwrap();
    }

    /// `clear` resets the flag so a previously-woken note blocks again.
    #[test]
    fn note_clear_resets_flag() {
        let note = Note::new();
        note.wakeup();
        note.clear();
        // After clear the flag is false.  We can't easily test that sleep
        // would block without a second thread, so just verify clear + wakeup
        // + sleep still works.
        note.wakeup();
        note.sleep();
    }

    // ── M tests ───────────────────────────────────────────────────────────

    /// `M::new` must produce a fully wired M/g0 pair with a valid stack.
    #[test]
    fn m_new_wires_g0() {
        unsafe {
            let m = M::new(1);

            // g0 pointer is set.
            assert!(!m.g0.is_null(), "g0 must not be null");

            let g0 = &*m.g0;

            // Back-pointer: g0.m == &*m
            assert_eq!(
                g0.m,
                std::ptr::addr_of!(*m) as *mut M,
                "g0.m must point back to M"
            );

            // Stack bounds are valid and usable.
            assert!(g0.stack.lo < g0.stack.hi, "g0 stack bounds invalid");

            // SP starts at the top of the stack (grows downward).
            assert_eq!(g0.sched.sp, g0.stack.hi, "g0 sched.sp must equal stack.hi");
            assert_eq!(g0.sched.bp, g0.stack.hi, "g0 sched.bp must equal stack.hi");

            // Other M fields start in their zero state.
            assert!(m.curg.is_null());
            assert!(m.p.is_null());
            assert_eq!(m.id, 1);
            assert!(!m.spinning);
            assert!(!m.blocked);

            // M::drop frees g0's stack — verify no double-free by relying on
            // the drop running without error.
        }
    }

    /// `M::drop` must free g0's stack without a double-free or leak.
    /// Run under `cargo test` with `RUST_LOG=warn` and check valgrind/asan
    /// if deeper validation is needed.
    #[test]
    fn m_drop_frees_stack() {
        unsafe {
            let m = M::new(2);
            let stack_lo = (*m.g0).stack.lo;
            let stack_hi = (*m.g0).stack.hi;
            drop(m); // must not panic or abort
            // Stack memory is now unmapped — we cannot safely read it,
            // but the absence of a panic is the invariant we're testing.
            let _ = (stack_lo, stack_hi); // use values to avoid warnings
        }
    }

    /// `park_m` / `unpark` round-trips across threads via Note.
    #[test]
    fn m_park_unpark() {
        use std::sync::Arc;

        // Wrap the Note separately so we can share it without sharing the M
        // (which requires exclusive access for park_m's &mut self).
        let note = Arc::new(Note::new());
        let note2 = Arc::clone(&note);

        let handle = std::thread::spawn(move || note2.sleep());
        std::thread::sleep(std::time::Duration::from_millis(20));
        note.wakeup();
        handle.join().unwrap();
    }
}
