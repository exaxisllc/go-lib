// SPDX-License-Identifier: Apache-2.0
//! RCU-style epoch reclamation for safely freeing goroutine descriptors.
//!
//! ## Why this exists
//!
//! Phase 2b of the goroutine-leak fix (drain GWAITING goroutines at
//! `run_impl` exit) needs to free the `Box<G>` of a parked goroutine after
//! transitioning it `GWAITING → GDEAD`.  But several runtime call sites hold
//! a `*mut G` to a possibly-parked goroutine and then dereference it via
//! `goready`:
//!
//! - Channel send / recv / close (sudog.g pulled from sendq/recvq)
//! - WaitGroup.add when the counter reaches zero (waiters drain)
//! - Cond notify_one / notify_all (waiters drain)
//! - Timer fire_expired (gp from the timer heap)
//! - Netpoll wake (gp from the REG table)
//! - Select cleanup (sudog dequeue)
//!
//! Even after `goready`'s GDEAD early-return guard (PR #29), each of those
//! call sites still loads the pointer first and then enters `goready` —
//! which dereferences the pointer to read `(*gp).atomicstatus`.  If we
//! free the `Box<G>` between the load and the deref, the deref is a
//! use-after-free.
//!
//! ## The design
//!
//! We protect every "load a `*mut G` from shared state and then call
//! `goready` on it" sequence with an [`RcuGuard`].  When the run_impl
//! drainer wants to free a batch of GDEAD goroutines it holds a
//! [`DrainSync`], which:
//!
//! 1. Blocks new `RcuGuard` acquisitions (they spin until the drainer is
//!    done) — so no new callers can pick up a doomed pointer.
//! 2. Waits for any already-in-flight `RcuGuard` holders to drop —
//!    so no thread can still be mid-deref.
//!
//! Only when both are true does the drainer free the Boxes; new
//! `RcuGuard` acquisitions resume when `DrainSync` is dropped.
//!
//! ## Why not full epoch-based reclamation?
//!
//! The standard `crossbeam-epoch`-style implementation tracks a global
//! epoch and per-thread "observed-epoch" fields, advancing the epoch
//! when a free is deferred and freeing only when every thread has
//! observed the advance (or is quiescent).  That is more efficient under
//! steady-state churn — readers never block.
//!
//! Our drainer runs **once per `run_impl` invocation** and is not on any
//! hot path.  The simpler counter+flag scheme below adds a single
//! cacheline of contention to `goready` (and the other wrapped sites)
//! but spares us a per-thread registry and the associated lifetime
//! tracking through Ms.  We can revisit the design if profiling shows
//! the read side becomes a bottleneck.
//!
//! ## Memory ordering
//!
//! Both `RCU_IN_CS` and `DRAIN_BLOCK` use `SeqCst` ordering on the slow
//! paths and `Acquire`/`Release` on the hot path so the "increment
//! counter THEN observe DRAIN_BLOCK" and "set DRAIN_BLOCK THEN observe
//! counter" pairs are correctly synchronised.  Because the drainer
//! sequence is `store(DRAIN_BLOCK) → load(RCU_IN_CS)` and the reader
//! sequence is `fetch_add(RCU_IN_CS) → load(DRAIN_BLOCK)`, we need a
//! total order between the two stores and the two loads — exactly what
//! `SeqCst` provides.  See `rcu_synchronize` and `RcuGuard::new` for the
//! concrete fence placement.
//!
//! ## Nested DrainSyncs
//!
//! In production there is exactly one drainer at a time (the `run_impl`
//! exit path is serialised by the calling thread's `park()`).  However,
//! the unit tests below — and any future parallel use — may construct
//! multiple `DrainSync` instances concurrently, so we track the number
//! of active drainers with a counter rather than a bool.  Each
//! `DrainSync::new()` increments the counter, blocks new readers via
//! `DRAIN_BLOCK > 0`, and waits for `RCU_IN_CS` to drain.  The flag
//! clears only when the last `DrainSync` drops.

use std::sync::atomic::{AtomicI64, Ordering};

// ---------------------------------------------------------------------------
// Global state
// ---------------------------------------------------------------------------

/// Number of currently-active [`RcuGuard`] holders.  Drainer waits for this
/// to reach 0 after raising [`DRAIN_BLOCK`].
static RCU_IN_CS: AtomicI64 = AtomicI64::new(0);

/// Number of currently-active [`DrainSync`] holders — non-zero while any
/// drainer is in its exclusive phase.  New [`RcuGuard`] acquisitions back
/// off and spin until this returns to zero.
static DRAIN_BLOCK: AtomicI64 = AtomicI64::new(0);

// ---------------------------------------------------------------------------
// RcuGuard — read-side critical section
// ---------------------------------------------------------------------------

/// RAII guard marking a read-side RCU critical section.
///
/// Wrap every "load a `*mut G` from shared state and then call `goready` on
/// it" sequence in `let _cs = RcuGuard::new();`.  While at least one
/// `RcuGuard` is alive, the drainer's `DrainSync` blocks before freeing
/// `Box<G>` allocations.
///
/// The guard does **not** block other goroutines or impose ordering between
/// concurrent `goready` calls — only between readers and the (rare)
/// drainer.
pub(crate) struct RcuGuard;

impl RcuGuard {
    /// Enter a read-side critical section.  Spins briefly while a drainer
    /// is active.
    #[inline]
    pub(crate) fn new() -> Self {
        // Fast path: increment the in-CS counter unconditionally, then
        // verify the drainer isn't holding us out.  This pattern (commit
        // first, validate, back out) avoids a race where the drainer raises
        // DRAIN_BLOCK between our check and our increment.
        loop {
            RCU_IN_CS.fetch_add(1, Ordering::SeqCst);
            if DRAIN_BLOCK.load(Ordering::SeqCst) == 0 {
                return RcuGuard;
            }
            // Drainer active — back out and wait for it to finish.
            RCU_IN_CS.fetch_sub(1, Ordering::SeqCst);
            while DRAIN_BLOCK.load(Ordering::Acquire) != 0 {
                std::hint::spin_loop();
            }
        }
    }
}

impl Drop for RcuGuard {
    #[inline]
    fn drop(&mut self) {
        RCU_IN_CS.fetch_sub(1, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// DrainSync — writer / synchronize
// ---------------------------------------------------------------------------

/// RAII guard for the drainer's exclusive phase.
///
/// `DrainSync::new()` returns only after every existing [`RcuGuard`] has
/// been dropped, *and* prevents new ones from being acquired (they spin)
/// until the `DrainSync` itself is dropped.  Between the two events, the
/// drainer has exclusive access — no other thread can be mid-`goready`
/// holding a `*mut G` we are about to free.
///
/// # Usage
///
/// ```text
/// // Inside run_impl's exit path, after the timer/netpoll drains:
/// {
///     let _sync = DrainSync::new();        // wait for in-flight goready calls
///     for gp in to_free {
///         drop(Box::from_raw(gp));         // safe — no readers
///     }
/// } // _sync drops — readers may proceed again
/// ```
pub(crate) struct DrainSync;

impl DrainSync {
    /// Enter the exclusive phase: block new readers, wait for current
    /// readers to drain.
    pub(crate) fn new() -> Self {
        // Raise the drain counter.  SeqCst pairs with the SeqCst increment
        // in RcuGuard::new(): if a reader's RCU_IN_CS fetch_add succeeds
        // before our DRAIN_BLOCK fetch_add, our subsequent load of
        // RCU_IN_CS sees the increment; if our fetch_add completes first,
        // the reader's load of DRAIN_BLOCK observes a non-zero value and
        // backs out.
        DRAIN_BLOCK.fetch_add(1, Ordering::SeqCst);
        // Wait for any in-flight reader to finish.
        while RCU_IN_CS.load(Ordering::SeqCst) > 0 {
            std::hint::spin_loop();
        }
        DrainSync
    }
}

impl Drop for DrainSync {
    fn drop(&mut self) {
        DRAIN_BLOCK.fetch_sub(1, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::Relaxed;
    use std::thread;
    use std::time::{Duration, Instant};

    /// `RcuGuard::new()` + drop is a no-op observable from outside.
    ///
    /// We deliberately do not assert exact `RCU_IN_CS` values because other
    /// tests run in parallel and hold their own `RcuGuard`s, so the absolute
    /// counter value is unpredictable.  Just check that constructing and
    /// dropping a guard doesn't panic and that we can do it many times.
    #[test]
    fn rcu_guard_roundtrip() {
        for _ in 0..100 {
            let _cs = RcuGuard::new();
        }
    }

    /// `DrainSync` waits for an outstanding `RcuGuard` to drop.
    #[test]
    fn drain_sync_waits_for_in_flight_reader() {
        let started = Arc::new(AtomicBool::new(false));
        let started2 = Arc::clone(&started);
        let release  = Arc::new(AtomicBool::new(false));
        let release2 = Arc::clone(&release);

        let reader = thread::spawn(move || {
            let _cs = RcuGuard::new();
            started2.store(true, Relaxed);
            // Hold the CS until told to release.
            while !release2.load(Relaxed) {
                std::hint::spin_loop();
            }
        });

        // Wait for the reader to enter its CS.
        while !started.load(Relaxed) {
            std::hint::spin_loop();
        }

        let drain_thread = thread::spawn(|| {
            let _sync = DrainSync::new();
            // If we reach here, the reader has released.
        });

        // Drainer must NOT have completed yet (reader still holding CS).
        thread::sleep(Duration::from_millis(20));
        assert!(!drain_thread.is_finished(),
            "DrainSync returned before reader released its RcuGuard");

        // Release the reader; DrainSync should now complete.
        release.store(true, Relaxed);

        let t0 = Instant::now();
        while !drain_thread.is_finished() {
            assert!(t0.elapsed() < Duration::from_secs(1),
                "DrainSync did not complete after reader released");
            std::hint::spin_loop();
        }

        reader.join().unwrap();
        drain_thread.join().unwrap();
    }

    /// A `RcuGuard::new()` while `DrainSync` is held spins until the drainer
    /// releases.  The test acquires `DrainSync` *before* spawning the reader
    /// so there is no window in which the reader can sneak through.
    #[test]
    fn rcu_guard_waits_for_drain() {
        let reader_acquired = Arc::new(AtomicBool::new(false));
        let reader_acquired2 = Arc::clone(&reader_acquired);

        // Acquire DrainSync first.  At this point, no other test in the same
        // process can be running an RcuGuard either (DrainSync waits for them
        // to drain) — so the global RCU_IN_CS is 0 when we return.
        let sync = DrainSync::new();

        let reader = thread::spawn(move || {
            // This must spin until the main thread drops `sync`.
            let _cs = RcuGuard::new();
            reader_acquired2.store(true, Relaxed);
        });

        // Reader should NOT have acquired its CS yet.
        thread::sleep(Duration::from_millis(20));
        assert!(!reader_acquired.load(Relaxed),
            "RcuGuard acquired while DrainSync was held");

        // Release DRAIN_BLOCK.
        drop(sync);

        // Reader must now proceed within a reasonable time.
        let t0 = Instant::now();
        while !reader_acquired.load(Relaxed) {
            assert!(t0.elapsed() < Duration::from_secs(1),
                "RcuGuard did not acquire after DrainSync released");
            std::hint::spin_loop();
        }

        reader.join().unwrap();
    }
}
