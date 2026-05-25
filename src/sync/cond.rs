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
//! The GRUNNING → GWAITING race is handled by `goready`'s built-in spin loop:
//! even if a notifier calls `goready` between the moment a goroutine pushes
//! itself onto `waitq` and the moment `gopark` actually sets `GWAITING`, the
//! spin safely serialises the two.

use std::collections::VecDeque;
use std::sync::Mutex;

use crate::runtime::g::{current_g, WaitReason};
use crate::runtime::park::{gopark, goready};
use crate::runtime::g::G;

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
    /// Queue of goroutines waiting on this condition.
    waitq: Mutex<VecDeque<*mut G>>,
}

// SAFETY: *mut G pointers are only read under the waitq Mutex and are valid
// for the lifetime of the process (goroutines are never freed, only recycled).
unsafe impl Send for Cond {}
unsafe impl Sync for Cond {}

impl Cond {
    /// Create a new `Cond`.
    pub fn new() -> Self {
        Self { waitq: Mutex::new(VecDeque::new()) }
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

        // Enqueue ourselves before releasing the mutex so that any concurrent
        // notify_one / notify_all sees us in the queue.
        self.waitq.lock().unwrap().push_back(gp);

        // Release the user's mutex.  After this point a notifier may call
        // goready(gp); goready's spin loop handles the GRUNNING→GWAITING race.
        drop(guard);

        // Park until goready transitions us back to GRUNNABLE.
        // SAFETY: we are on a goroutine stack (asserted above).
        unsafe { gopark(WaitReason::CondVar) };

        // Woken — re-acquire the user's mutex.
        mu.lock().unwrap()
    }

    /// Wake one waiting goroutine.  No-op if there are no waiters.
    pub fn notify_one(&self) {
        let gp = self.waitq.lock().unwrap().pop_front();
        if let Some(gp) = gp {
            // SAFETY: gp is a valid goroutine pointer (see module safety comment).
            unsafe { goready(gp) };
        }
    }

    /// Wake all waiting goroutines.
    pub fn notify_all(&self) {
        let waiters: Vec<*mut G> = self.waitq.lock().unwrap().drain(..).collect();
        for gp in waiters {
            // SAFETY: same as notify_one.
            unsafe { goready(gp) };
        }
    }
}

impl Default for Cond {
    fn default() -> Self { Self::new() }
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
