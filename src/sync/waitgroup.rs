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
//! Uses `Mutex<WgState> + Condvar` rather than Go's semaphore / `sync/atomic`.
//! The blocking path is wrapped in [`crate::with_syscall`] so the goroutine's
//! P is handed off to another M while the OS thread sleeps in the kernel.
//!
//! Ported from `sync/waitgroup.go`.

use crate::loom_shim::{Condvar, Mutex};

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

struct WgState {
    /// Number of outstanding workers (`Add` increments, `Done` decrements).
    count: i64,
}

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
    state: Mutex<WgState>,
    cond:  Condvar,
}

impl WaitGroup {
    /// Create a new `WaitGroup` with a counter of zero.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(WgState { count: 0 }),
            cond:  Condvar::new(),
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
        let mut state = self.state.lock().unwrap();
        state.count += delta;
        if state.count < 0 {
            drop(state);
            panic!("sync: negative WaitGroup counter");
        }
        if state.count == 0 {
            // Notify all waiters — counter has reached zero.
            drop(state);
            self.cond.notify_all();
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
    /// The calling goroutine's P is handed off while waiting so other
    /// goroutines can run on this M's processor.
    pub fn wait(&self) {
        // Fast path: already zero.
        {
            let state = self.state.lock().unwrap();
            if state.count == 0 {
                return;
            }
        }

        // Slow path: block until notified.  Wrap in with_syscall so the P is
        // released to the scheduler while this OS thread sleeps.
        crate::with_syscall(|| {
            let mut state = self.state.lock().unwrap();
            while state.count > 0 {
                state = self.cond.wait(state).unwrap();
            }
        });
    }
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self::new()
    }
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
            unsafe {
                crate::runtime::sched::spawn_goroutine(move || {
                    done2.fetch_add(1, Ordering::Relaxed);
                    wg2.done();
                });
            }

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
                unsafe {
                    crate::runtime::sched::spawn_goroutine(move || {
                        count3.fetch_add(1, Ordering::Relaxed);
                        wg2.done();
                    });
                }
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
                unsafe {
                    crate::runtime::sched::spawn_goroutine(move || {
                        wg3.wait();
                        woke3.fetch_add(1, Ordering::Relaxed);
                    });
                }
            }

            // Yield so the waiters block, then release.
            for _ in 0..20 { crate::gosched(); }
            wg.done();
            for _ in 0..50 { crate::gosched(); }
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
            unsafe {
                crate::runtime::sched::spawn_goroutine(move || { wg2.done(); });
            }
            wg.wait();

            // Round 2.
            let done = Arc::new(AtomicI32::new(0));
            wg.add(1);
            let wg3   = Arc::clone(&wg);
            let done2 = Arc::clone(&done);
            unsafe {
                crate::runtime::sched::spawn_goroutine(move || {
                    done2.store(1, Ordering::Relaxed);
                    wg3.done();
                });
            }
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
