// SPDX-License-Identifier: Apache-2.0
//! `Cond` — goroutine-aware condition variable.
//!
//! Mirrors Go's `sync.Cond`: goroutines waiting on a `Cond` are parked via
//! the scheduler (`gopark`) rather than blocking an OS thread, so other
//! goroutines sharing that M can continue to run.
//!
//! ## Usage
//!
//! ```no_run
//! use std::sync::{Arc, Mutex};
//! use go_lib::sync::Cond;
//!
//! let mu  = Arc::new(Mutex::new(false));
//! let cnd = Arc::new(Cond::new());
//!
//! // Producer goroutine
//! let mu2  = Arc::clone(&mu);
//! let cnd2 = Arc::clone(&cnd);
//! go_lib::run(move || {
//!     let mut ready = mu2.lock().unwrap();
//!     *ready = true;
//!     drop(ready);
//!     cnd2.notify_one();
//! });
//!
//! // Consumer
//! go_lib::run(move || {
//!     let mut ready = mu.lock().unwrap();
//!     while !*ready {
//!         ready = cnd.wait(&mu, ready);
//!     }
//! });
//! ```
//!
//! ## Implementation
//!
//! An internal wait queue (`waitq`) stores raw `*mut G` pointers of parked
//! goroutines.  `notify_one` / `notify_all` call `goready` on them.
//!
//! ### Commit-park protocol
//!
//! `waitq` is protected by a [`RawMutex`] (not `std::sync::Mutex`) so that
//! `wait` can hold the queue lock ACROSS the park via
//! [`gopark_commit`][crate::runtime::park::gopark_commit] — the lock is
//! released on g0 only after the waiter has reached `GWAITING`.  Releasing
//! the queue lock *before* `gopark` (the original design) left a lost-wakeup
//! window: an async preemption (SIGURG) landing between the unlock and the
//! park made the registered waiter `GRUNNABLE`; a concurrent
//! `notify_one`/`notify_all` then popped the waiter and called `goready`,
//! which saw a non-`GWAITING` status and dropped the wake on the floor — the
//! waiter parked forever.  Holding `waitq`'s lock until `park_fn` has
//! committed the goroutine to `GWAITING` closes the window: no notifier can
//! pop the waiter (the pop needs the queue lock) until the park is committed.

use std::collections::VecDeque;
use std::cell::UnsafeCell;
use std::sync::Mutex; // the user's external lock passed to `wait`

use crate::runtime::g::{current_g, WaitReason};
use crate::runtime::park::{gopark_commit, goready};
use crate::runtime::g::G;
use crate::runtime::rawmutex::RawMutex;

// ---------------------------------------------------------------------------
// Cond
// ---------------------------------------------------------------------------

/// A goroutine-aware condition variable.
///
/// Like `std::sync::Condvar` but parks goroutines instead of OS threads,
/// so other goroutines sharing the same M continue to be scheduled while
/// waiters sleep.
///
/// Must be used from within a [`go_lib::run`] context.
pub struct Cond {
    /// Spinlock protecting `waitq`.  A `RawMutex` (not `std::sync::Mutex`) so
    /// the goroutine `wait()` path can hold the queue lock ACROSS the park via
    /// `gopark_commit` — released on g0 only after the waiter is `GWAITING`
    /// (see the module-level "Commit-park protocol" note).
    mu:    RawMutex,
    /// Queue of goroutines waiting on this condition — always accessed under
    /// `mu`.
    waitq: UnsafeCell<VecDeque<*mut G>>,
}

// SAFETY: *mut G pointers are only read under `mu` and are valid for the
// lifetime of the process (goroutines are never freed, only recycled).
unsafe impl Send for Cond {}
unsafe impl Sync for Cond {}

impl Cond {
    /// Create a new `Cond`.
    pub fn new() -> Self {
        Self { mu: RawMutex::new(), waitq: UnsafeCell::new(VecDeque::new()) }
    }

    /// Release `guard`, park the current goroutine until notified, then
    /// re-acquire the mutex and return the new guard.
    ///
    /// Spurious wakeups are possible; always re-check the predicate in a loop:
    ///
    /// ```no_run
    /// # use std::sync::{Arc, Mutex};
    /// # use go_lib::sync::Cond;
    /// # let mu = Arc::new(Mutex::new(false));
    /// # let cnd = Arc::new(Cond::new());
    /// let mut guard = mu.lock().unwrap();
    /// while !*guard {
    ///     guard = cnd.wait(&mu, guard);
    /// }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if called from outside a goroutine context (i.e. before `run()`).
    pub fn wait<'a, T>(
        &self,
        mu:   &'a Mutex<T>,
        guard: std::sync::MutexGuard<'a, T>,
    ) -> std::sync::MutexGuard<'a, T> {
        let gp = current_g();
        assert!(!gp.is_null(), "Cond::wait called outside a goroutine context");

        // m_lock suppresses async preemption while we hold `mu` AND across the
        // `gopark_commit` below — the increment is transferred to `park_fn`,
        // which balances it on the same M (see gopark_commit's contract).
        let _lk = crate::runtime::m::m_lock();
        self.mu.lock();

        // Enqueue ourselves under `mu` so that any concurrent
        // notify_one / notify_all sees us in the queue.  The queue lock stays
        // held until park_fn has committed this goroutine to GWAITING
        // (commit-park protocol), so a notifier cannot pop us and call
        // goready until the park is guaranteed to find us parked.
        // SAFETY: `mu` is held.
        unsafe { (*self.waitq.get()).push_back(gp) };

        // Release the user's mutex *before* parking (Go semantics).  The
        // rendezvous with notifiers is protected by `mu`, which is held across
        // the park, so releasing the user lock here cannot lose a wakeup.
        drop(guard);

        // Transfer the m.locks increment to park_fn, then park.  The queue
        // lock (`mu`) is released on g0 by `unlock_cond_mutex` only after this
        // goroutine is GWAITING.
        std::mem::forget(_lk);
        unsafe {
            gopark_commit(
                WaitReason::CondVar,
                unlock_cond_mutex,
                &self.mu as *const RawMutex as *mut u8,
            );
        }

        // Woken — re-acquire the user's mutex.
        mu.lock().unwrap()
    }

    /// Wake one waiting goroutine.  No-op if there are no waiters.
    pub fn notify_one(&self) {
        // m_lock suppresses async preemption across the `mu` critical section
        // (same rationale as WaitGroup::add); without it a SIGURG between
        // acquiring and releasing the spinlock could deschedule us while the
        // lock is held, deadlocking the next goroutine on this M.
        let _lk = crate::runtime::m::m_lock();
        // A Cond waiter is woken purely via `goready`, which touches only the
        // `G` descriptor — never the waiter's stack.
        self.mu.lock();
        // SAFETY: `mu` is held.
        let gp = unsafe { (*self.waitq.get()).pop_front() };
        unsafe { self.mu.unlock() };
        // Wake outside the lock so we don't hold the spinlock across the
        // goready spin (which waits for GRUNNING → GWAITING).
        if let Some(gp) = gp {
            // SAFETY: gp is a valid goroutine pointer (see module safety comment).
            unsafe { goready(gp) };
        }
    }

    /// Wake all waiting goroutines.
    pub fn notify_all(&self) {
        let _lk = crate::runtime::m::m_lock();
        // See `notify_one`: waiters are woken only via `goready`, which never
        // touches a waiter's stack.
        self.mu.lock();
        // SAFETY: `mu` is held.
        let waiters: Vec<*mut G> = unsafe { (*self.waitq.get()).drain(..).collect() };
        unsafe { self.mu.unlock() };
        // Wake outside the lock (see notify_one).
        for gp in waiters {
            // SAFETY: same as notify_one.
            unsafe { goready(gp) };
        }
    }

}

impl Default for Cond {
    fn default() -> Self { Self::new() }
}

/// `gopark_commit` unlock shim: release the `Cond`'s `RawMutex` from g0 after
/// the parking goroutine has reached `GWAITING`.
///
/// # Safety
/// `arg` must be the `&RawMutex` of a `Cond` whose queue lock is held by the
/// parking goroutine.
unsafe fn unlock_cond_mutex(arg: *mut u8) {
    unsafe { (*(arg as *const RawMutex)).unlock() }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::runtime::sched::run_impl;
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicI32, Ordering};

    /// A single waiter is woken by notify_one.
    #[test]
    fn single_waiter_notify_one() {
        let mu   = Arc::new(Mutex::new(false));
        let cnd  = Arc::new(Cond::new());
        let woke = Arc::new(AtomicI32::new(0));

        let mu2   = Arc::clone(&mu);
        let cnd2  = Arc::clone(&cnd);
        let woke2 = Arc::clone(&woke);
        let woke3 = Arc::clone(&woke);

        run_impl(move || {
            // Spawn a waiter goroutine.
            crate::runtime::sched::spawn_goroutine(move || {
                let mut guard = mu2.lock().unwrap();
                while !*guard {
                    guard = cnd2.wait(&mu2, guard);
                }
                woke2.fetch_add(1, Ordering::Relaxed);
            });

            // Yield so the waiter parks, then signal.
            for _ in 0..20 { crate::gosched(); }
            {
                let mut g = mu.lock().unwrap();
                *g = true;
            }
            cnd.notify_one();

            // Spin-wait for the waiter to record the wakeup.
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_millis(500);
            loop {
                if woke3.load(Ordering::Acquire) == 1 { break; }
                assert!(std::time::Instant::now() < deadline, "waiter did not wake");
                crate::gosched();
            }
        });

        assert_eq!(woke.load(Ordering::Acquire), 1);
    }

    /// All waiters are woken by notify_all.
    #[test]
    fn multiple_waiters_notify_all() {
        const N: i32 = 4;
        let mu   = Arc::new(Mutex::new(false));
        let cnd  = Arc::new(Cond::new());
        let woke = Arc::new(AtomicI32::new(0));

        run_impl({
            let mu   = Arc::clone(&mu);
            let cnd  = Arc::clone(&cnd);
            let woke = Arc::clone(&woke);
            move || {
                for _ in 0..N {
                    let mu2   = Arc::clone(&mu);
                    let cnd2  = Arc::clone(&cnd);
                    let woke2 = Arc::clone(&woke);
                    crate::runtime::sched::spawn_goroutine(move || {
                        let mut guard = mu2.lock().unwrap();
                        while !*guard {
                            guard = cnd2.wait(&mu2, guard);
                        }
                        woke2.fetch_add(1, Ordering::Relaxed);
                    });
                }

                for _ in 0..40 { crate::gosched(); }
                *mu.lock().unwrap() = true;
                cnd.notify_all();

                let deadline = std::time::Instant::now()
                    + std::time::Duration::from_millis(500);
                loop {
                    if woke.load(Ordering::Acquire) == N { break; }
                    assert!(std::time::Instant::now() < deadline,
                        "not all waiters woke: {}/{N}", woke.load(Ordering::Relaxed));
                    crate::gosched();
                }
            }
        });

        assert_eq!(woke.load(Ordering::Acquire), N);
    }

    /// notify_one with no waiters is a no-op (must not panic or block).
    #[test]
    fn notify_one_no_waiters() {
        let cnd = Cond::new();
        cnd.notify_one(); // must return immediately
    }

    /// notify_all with no waiters is a no-op.
    #[test]
    fn notify_all_no_waiters() {
        let cnd = Cond::new();
        cnd.notify_all();
    }
}
