// SPDX-License-Identifier: Apache-2.0
//! `WaitGroup` — ported from `src/sync/waitgroup.go`.
//!
//! ## Semantics (match Go)
//!
//! - [`WaitGroup::add`]`(delta)` — increment the counter by `delta`.  `delta`
//!   may be negative (used internally by [`done`][WaitGroup::done]).
//!   Panics if the counter goes negative.
//! - [`WaitGroup::done`]`()` — shorthand for `add(-1)`.
//! - [`WaitGroup::wait`]`()` — block until the counter reaches zero.
//!   Multiple goroutines may call `wait` concurrently; all are unblocked when
//!   the last worker calls `done`.
//!
//! ## Implementation
//!
//! ### Goroutine path (the common case)
//!
//! `wait` uses [`gopark`][crate::runtime::park::gopark] to suspend the calling
//! goroutine back into the scheduler **without blocking the OS thread**.  The
//! M and its P remain free to run other goroutines.  When `add` decrements the
//! counter to zero it drains the waiters list and calls
//! [`goready`][crate::runtime::park::goready] on each, re-enqueuing them for
//! scheduling.
//!
//! ### Non-goroutine / loom path
//!
//! If `wait` is called from a bare OS thread (outside the go-lib scheduler, or
//! from a loom model thread) it falls back to blocking on a `Condvar`.  This
//! path is also used by the `negative_counter_panics` unit test which never
//! enters the scheduler.
//!
//! Ported from `sync/waitgroup.go`.

use std::cell::UnsafeCell;

use crate::loom_shim::{Condvar, Mutex};
use crate::runtime::g::{current_g, WaitReason, G};
use crate::runtime::park::{gopark_commit, goready};
use crate::runtime::rawmutex::RawMutex;

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

struct WgState {
    /// Number of outstanding workers (`Add` increments, `Done` decrements).
    count:   i64,
    /// Goroutines suspended in [`WaitGroup::wait`].  Drained by
    /// [`WaitGroup::add`] when the counter reaches zero; each entry is woken
    /// via [`goready`].
    waiters: Vec<*mut G>,
}

// SAFETY: `WgState` is always accessed under the `WaitGroup`'s `Mutex`.
// The `*mut G` pointers are goroutines owned by the scheduler; we never
// dereference them without holding the lock (except to pass to `goready`,
// which is safe once the goroutine has reached `GWAITING`).
unsafe impl Send for WgState {}

// ---------------------------------------------------------------------------
// WaitGroup
// ---------------------------------------------------------------------------

/// A synchronisation barrier: wait for a set of goroutines to complete.
///
/// The typical usage pattern is:
///
/// ```no_run
/// use std::sync::Arc;
/// use go_lib::sync::WaitGroup;
///
/// let wg = Arc::new(WaitGroup::new());
/// for i in 0..5 {
///     let wg = Arc::clone(&wg);
///     go_lib::run(move || {
///         wg.add(1);
///         // ... spawn goroutine that calls wg.done() when finished ...
///     });
/// }
/// // wg.wait();  // blocks until all Done() calls have been made
/// ```
pub struct WaitGroup {
    /// Spinlock protecting `state`.  A `RawMutex` (not `std::sync::Mutex`)
    /// so that the goroutine `wait()` path can hold the lock ACROSS the park
    /// via `gopark_commit` — the lock is released on g0 only after the
    /// waiter is `GWAITING`.  With a guard-based Mutex released before
    /// `gopark`, an async preemption in the release-to-park window made the
    /// registered waiter `GRUNNABLE`; `add()`'s `goready` then dropped the
    /// wake on the floor and the waiter parked forever.
    mu:    RawMutex,
    /// Interior state — always accessed under `mu`.
    state: UnsafeCell<WgState>,
    /// Companion lock for `cond` — used only by the non-goroutine fallback
    /// path (bare OS threads / loom model threads).
    cond_lock: Mutex<()>,
    /// Condvar used only on the non-goroutine fallback path.
    cond:  Condvar,
}

// SAFETY: `state` is only accessed while `mu` is held; the `*mut G` waiters
// are scheduler-owned and dereferenced only under RCU (see `add`).
unsafe impl Send for WaitGroup {}
unsafe impl Sync for WaitGroup {}

impl WaitGroup {
    /// Create a new `WaitGroup` with a counter of zero.
    pub fn new() -> Self {
        Self {
            mu:        RawMutex::new(),
            state:     UnsafeCell::new(WgState { count: 0, waiters: Vec::new() }),
            cond_lock: Mutex::new(()),
            cond:      Condvar::new(),
        }
    }

    /// Add `delta` to the counter.
    ///
    /// `delta` is typically positive when called before spawning goroutines and
    /// negative when they finish (see [`done`][Self::done]).
    ///
    /// # Panics
    ///
    /// Panics if the counter drops below zero.
    pub fn add(&self, delta: i64) {
        // Hold an `m.locks` guard across the std::sync::Mutex critical section.
        // Without it, SIGURG-based async preemption can fire after
        // `pthread_mutex_lock` returns but before we drop the MutexGuard: the
        // preempted goroutine still holds the OS-level pthread mutex, and the
        // next goroutine scheduled on the same M will self-deadlock trying to
        // re-acquire it (the default pthread mutex is non-recursive, so a
        // same-thread re-lock blocks forever in `__psynch_mutexwait`).
        // Captured live via lldb on a hung `many_goroutines` run.
        let _lk = crate::runtime::m::m_lock();
        // No drain synchronisation: WaitGroup waiters are woken only via
        // `goready`, which dereferences only the immortal `G` descriptor and
        // never the waiter's stack.  A concurrent Phase 2b drain that CAS'd a
        // waiter to GDEAD is handled by `goready`'s GDEAD arm.
        // Collect goroutine waiters to wake (if counter reaches zero).
        let goroutine_waiters: Vec<*mut G> = {
            self.mu.lock();
            // SAFETY: `mu` is held.
            let state = unsafe { &mut *self.state.get() };
            state.count += delta;
            if state.count < 0 {
                unsafe { self.mu.unlock() };
                panic!("sync: negative WaitGroup counter");
            }
            let zero = state.count == 0;
            let w = if zero { std::mem::take(&mut state.waiters) } else { Vec::new() };
            unsafe { self.mu.unlock() };
            if zero {
                // Wake condvar waiters (non-goroutine / loom path).  Taking
                // `cond_lock` between the count update and the notify pairs
                // with the re-check the bare-thread `wait` does under
                // `cond_lock`, so the notify cannot be missed.
                let _g = self.cond_lock.lock().unwrap();
                self.cond.notify_all();
            }
            w
        };

        // Wake goroutine waiters outside the lock so we don't hold it during
        // the goready spin (which waits for GRUNNING → GWAITING).
        for gp in goroutine_waiters {
            // Phase 2b: clear the gp.waiting_wg tag — once the goready fires,
            // gp is no longer registered in our waiters list.  The Phase 2b
            // drain will skip gps whose tag has been cleared.
            unsafe { (*gp).waiting_wg = std::ptr::null_mut() };
            unsafe { goready(gp) };
        }
    }

    /// Decrement the counter by one.
    ///
    /// Shorthand for `self.add(-1)`.
    pub fn done(&self) {
        self.add(-1);
    }

    /// Block until the counter is zero.
    ///
    /// When called from a goroutine: suspends the goroutine back into the
    /// scheduler via `gopark` so the M and P remain free to run other
    /// goroutines.  Resumed by `add` calling `goready` when the counter
    /// reaches zero.
    ///
    /// When called from a bare OS thread (outside the go-lib scheduler):
    /// blocks the thread on an internal `Condvar`.
    pub fn wait(&self) {
        // ── Goroutine path ──────────────────────────────────────────────────
        // gopark suspends this goroutine without blocking the OS thread.
        // The M+P are returned to the scheduler to run other goroutines
        // (including whoever will call done() to reach count == 0).
        let gp = current_g();
        if !gp.is_null() {
            // m_lock suppresses async preemption while we hold `mu` AND
            // across the `gopark_commit` below — the increment is
            // transferred to `park_fn`, which balances it on the same M
            // (see gopark_commit's contract).
            let _lk = crate::runtime::m::m_lock();
            self.mu.lock();
            // SAFETY: `mu` is held.
            let state = unsafe { &mut *self.state.get() };
            if state.count == 0 {
                unsafe { self.mu.unlock() };
                return; // fast path: already done (`_lk` drops normally)
            }
            // Register as a waiter; the lock stays held until park_fn has
            // committed this goroutine to GWAITING (commit-park protocol),
            // so add() can only observe the registration once goready is
            // guaranteed to find us parked.  Releasing before the park left
            // a window where preemption made us GRUNNABLE and add()'s wake
            // was silently dropped — the waiter then parked forever.
            state.waiters.push(gp);
            // Phase 2b: tag gp with the WaitGroup it is parked on so the
            // run_impl drain can remove the stale `*mut G` from
            // `state.waiters` if gp is reclaimed while parked.  Cleared by
            // `add()` when it wakes us, and below as a defensive measure.
            unsafe { (*gp).waiting_wg = self as *const WaitGroup as *mut u8 };
            // Transfer the m.locks increment to park_fn.
            std::mem::forget(_lk);
            unsafe {
                gopark_commit(
                    WaitReason::Semacquire,
                    unlock_wg_mutex,
                    &self.mu as *const RawMutex as *mut u8,
                );
            }
            // Phase 2b: clear the tag now that we have been woken.  (The
            // waker — `add()` above — clears it too; doing it here as well is
            // defensive against a future code path that wakes without going
            // through add()).
            unsafe { (*gp).waiting_wg = std::ptr::null_mut() };
            return;
        }

        // ── Non-goroutine / loom path ───────────────────────────────────────
        // Block the calling OS thread on the condvar.  Used by tests that call
        // wait() from a bare thread and by loom model threads.  The count is
        // re-checked under `cond_lock`, which `add()` acquires between
        // setting count == 0 and notifying — so the notify cannot fall
        // between our check and the `cond.wait`.
        let mut guard = self.cond_lock.lock().unwrap();
        loop {
            self.mu.lock();
            // SAFETY: `mu` is held.
            let count = unsafe { (*self.state.get()).count };
            unsafe { self.mu.unlock() };
            if count == 0 {
                return;
            }
            guard = self.cond.wait(guard).unwrap();
        }
    }
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self::new()
    }
}

/// `gopark_commit` unlock shim: release the WaitGroup's `RawMutex` from g0
/// after the parking goroutine has reached `GWAITING`.
///
/// # Safety
/// `arg` must be the `&RawMutex` of a `WaitGroup` whose lock is held by the
/// parking goroutine.
unsafe fn unlock_wg_mutex(arg: *mut u8) {
    unsafe { (*(arg as *const RawMutex)).unlock() }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::runtime::sched::run_impl;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    /// A freshly created WaitGroup has counter zero; wait() returns immediately.
    #[test]
    fn new_wait_returns_immediately() {
        let wg = WaitGroup::new();
        wg.wait(); // must not block
    }

    /// add + done in a single goroutine; wait unblocks after done.
    #[test]
    fn single_worker() {
        run_impl(|| {
            let wg = Arc::new(WaitGroup::new());
            let done = Arc::new(AtomicI32::new(0));

            wg.add(1);
            let wg2   = Arc::clone(&wg);
            let done2 = Arc::clone(&done);
            crate::runtime::sched::spawn_goroutine(move || {
                done2.fetch_add(1, Ordering::Relaxed);
                wg2.done();
            });

            wg.wait();
            assert_eq!(done.load(Ordering::Acquire), 1);
        });
    }

    /// Five workers; wait unblocks only after all five call done.
    #[test]
    fn multiple_workers() {
        const N: i32 = 5;
        let count = Arc::new(AtomicI32::new(0));
        let count2 = Arc::clone(&count);

        run_impl(move || {
            let wg = Arc::new(WaitGroup::new());

            for _ in 0..N {
                wg.add(1);
                let wg2    = Arc::clone(&wg);
                let count3 = Arc::clone(&count2);
                crate::runtime::sched::spawn_goroutine(move || {
                    count3.fetch_add(1, Ordering::Relaxed);
                    wg2.done();
                });
            }

            wg.wait();
            assert_eq!(count2.load(Ordering::Acquire), N);
        });

        assert_eq!(count.load(Ordering::Acquire), N);
    }

    /// Two goroutines both wait; both unblock when the counter reaches zero.
    #[test]
    fn multiple_waiters() {
        let woke = Arc::new(AtomicI32::new(0));
        let woke2 = Arc::clone(&woke);

        run_impl(move || {
            let wg = Arc::new(WaitGroup::new());
            wg.add(1);

            // Spawn two waiters.
            for _ in 0..2 {
                let wg3   = Arc::clone(&wg);
                let woke3 = Arc::clone(&woke2);
                crate::runtime::sched::spawn_goroutine(move || {
                    wg3.wait();
                    woke3.fetch_add(1, Ordering::Relaxed);
                });
            }

            // Yield so the waiters have a chance to call wg.wait() and park.
            for _ in 0..20 { crate::gosched(); }
            wg.done();

            // Poll until both waiter goroutines have run past wg.wait() and
            // incremented woke.  A fixed gosched-loop is not deterministic
            // under parallel test load; polling on the atomic is race-free.
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_millis(500);
            loop {
                if woke2.load(Ordering::Acquire) >= 2 { break; }
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out: only {} of 2 waiters woke",
                    woke2.load(Ordering::Relaxed),
                );
                crate::gosched();
            }
        });

        assert_eq!(woke.load(Ordering::Acquire), 2, "both waiters must wake");
    }

    /// WaitGroup is reusable: after wait() the counter can be incremented again.
    #[test]
    fn reuse_after_wait() {
        run_impl(|| {
            let wg = Arc::new(WaitGroup::new());

            // Round 1.
            wg.add(1);
            let wg2 = Arc::clone(&wg);
            crate::runtime::sched::spawn_goroutine(move || { wg2.done(); });
            wg.wait();

            // Round 2.
            let done = Arc::new(AtomicI32::new(0));
            wg.add(1);
            let wg3   = Arc::clone(&wg);
            let done2 = Arc::clone(&done);
            crate::runtime::sched::spawn_goroutine(move || {
                done2.store(1, Ordering::Relaxed);
                wg3.done();
            });
            wg.wait();
            assert_eq!(done.load(Ordering::Acquire), 1);
        });
    }

    /// add(-1) below zero panics.
    #[test]
    #[should_panic(expected = "sync: negative WaitGroup counter")]
    fn negative_counter_panics() {
        let wg = WaitGroup::new();
        wg.add(-1); // counter is 0 → -1 → panic
    }
}

// ---------------------------------------------------------------------------
// Loom model tests
// ---------------------------------------------------------------------------

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use loom::sync::Arc;

    /// One worker calls done(); the waiter must unblock without deadlocking.
    /// Loom explores all interleavings of done() vs wait().
    #[test]
    fn done_unblocks_wait() {
        loom::model(|| {
            let wg  = Arc::new(WaitGroup::new());
            let wg2 = Arc::clone(&wg);

            wg.add(1);

            let worker = loom::thread::spawn(move || {
                wg2.done();
            });

            wg.wait(); // must not deadlock in any interleaving

            worker.join().unwrap();
        });
    }

    /// Two concurrent done() calls both reach zero; a concurrent wait()
    /// must see the final count of zero in every interleaving.
    #[test]
    fn two_workers_unblock_wait() {
        loom::model(|| {
            let wg  = Arc::new(WaitGroup::new());
            let wg2 = Arc::clone(&wg);
            let wg3 = Arc::clone(&wg);
            let wg4 = Arc::clone(&wg);

            wg.add(2);

            let t1 = loom::thread::spawn(move || wg2.done());
            let t2 = loom::thread::spawn(move || wg3.done());
            let waiter = loom::thread::spawn(move || wg4.wait());

            t1.join().unwrap();
            t2.join().unwrap();
            waiter.join().unwrap();
        });
    }

    /// add() and done() may interleave; wait() must always see the true zero.
    #[test]
    fn add_and_done_interleave() {
        loom::model(|| {
            let wg  = Arc::new(WaitGroup::new());
            let wg2 = Arc::clone(&wg);
            let wg3 = Arc::clone(&wg);

            // One add(1) followed concurrently by done() and wait().
            wg.add(1);

            let adder = loom::thread::spawn(move || wg2.done());
            wg3.wait();

            adder.join().unwrap();
        });
    }
}
