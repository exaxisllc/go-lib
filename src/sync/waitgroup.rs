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

use crate::loom_shim::{Condvar, Mutex};
use crate::runtime::g::{current_g, WaitReason, G};
use crate::runtime::park::{gopark, goready};

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
    state: Mutex<WgState>,
    /// Condvar used only on the non-goroutine fallback path.
    cond:  Condvar,
}

impl WaitGroup {
    /// Create a new `WaitGroup` with a counter of zero.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(WgState { count: 0, waiters: Vec::new() }),
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
        // Collect goroutine waiters to wake (if counter reaches zero).
        let goroutine_waiters: Vec<*mut G> = {
            let mut state = self.state.lock().unwrap();
            state.count += delta;
            if state.count < 0 {
                drop(state);
                panic!("sync: negative WaitGroup counter");
            }
            if state.count == 0 {
                // Wake condvar waiters (non-goroutine / loom path).
                // Drain goroutine waiters to wake via goready below.
                let w = std::mem::take(&mut state.waiters);
                drop(state);
                self.cond.notify_all();
                w
            } else {
                Vec::new()
            }
        };

        // Wake goroutine waiters outside the lock so we don't hold it during
        // the goready spin (which waits for GRUNNING → GWAITING).
        for gp in goroutine_waiters {
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
            let mut state = self.state.lock().unwrap();
            if state.count == 0 {
                return; // fast path: already done
            }
            // Register as a waiter *before* releasing the lock so that any
            // concurrent add() that drives count to zero will see us and call
            // goready.  goready itself spins until our status reaches GWAITING,
            // which closes the window between drop(state) and gopark().
            state.waiters.push(gp);
            drop(state);
            // Suspend this goroutine.  Execution resumes here after add()
            // calls goready(gp) once the counter reaches zero.
            gopark(WaitReason::Semacquire);
            return;
        }

        // ── Non-goroutine / loom path ───────────────────────────────────────
        // Block the calling OS thread on the condvar.  Used by tests that call
        // wait() from a bare thread and by loom model threads.
        let mut state = self.state.lock().unwrap();
        while state.count > 0 {
            state = self.cond.wait(state).unwrap();
        }
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
