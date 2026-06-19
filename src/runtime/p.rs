// SPDX-License-Identifier: Apache-2.0
//! Processor (`P`) and its 256-slot lock-free local run queue.
//!
//! Ported from `runtime/runtime2.go` (P struct) and `runtime/proc.go`
//! (`runqput` / `runqget` / `runqsteal` / `runqgrab`).
//!
//! ## Run queue design
//!
//! Each P has a fixed 256-slot ring buffer (`runq`) protected by two
//! monotonically-increasing counters (`runqhead`, `runqtail`).  The owner P
//! is the only writer of `runqtail`; other Ps (stealers) may advance
//! `runqhead` via CAS.  All accesses use the same Acquire/Release memory
//! ordering as Go's `LoadAcq`/`StoreRel`/`CasRel` macros.
//!
//! In addition each P has a `runnext` slot: a single G that the owning P will
//! run next, bypassing the ring.  Stealers may also claim `runnext`.
//!
//! ## GlobalRunQueue
//!
//! When the local ring overflows, `runqputslow` drains half the ring and
//! pushes the batch onto the `GlobalRunQueue`.  The scheduler (step 8) also
//! consults the global queue periodically for fairness.

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering::*};
use crate::loom_shim::Mutex;

use super::g::G;
use super::m::M;

// ---------------------------------------------------------------------------
// P status constants — from runtime/runtime2.go
// ---------------------------------------------------------------------------

/// P is idle (not running user code, not in scheduler loop).
pub(crate) const PIDLE: u32 = 0;
/// P is running user code.
pub(crate) const PRUNNING: u32 = 1;
/// P is in a syscall.
pub(crate) const PSYSCALL: u32 = 2;
/// P is stopped for GC (stop-the-world).  Unused until GC is implemented.
#[allow(dead_code)]
pub(crate) const PGCSTOP: u32 = 3;
/// P has been destroyed.  Unused until GOMAXPROCS-decrease tears down Ps.
#[allow(dead_code)]
pub(crate) const PDEAD: u32 = 4;

/// Capacity of the local run queue ring buffer. Must be a power of two.
const RUNQ_CAP: usize = 256;

// ---------------------------------------------------------------------------
// GlobalRunQueue
// ---------------------------------------------------------------------------

/// The scheduler's global run queue — a singly-linked list of `G`s.
///
/// All access is serialised by an internal `Mutex`.  When a P's local ring
/// overflows, `runqputslow` pushes a batch here.  The scheduler (step 8)
/// drains it periodically and when all Ps are idle.
///
/// Ported from `sched.runq` / `globrunqputbatch` / `globrunqget` in
/// `runtime/proc.go`.  Lives in `p.rs` for now; will move to `sched.rs`
/// in step 8.
pub(crate) struct GlobalRunQueue {
    inner: Mutex<GlobalRunQueueInner>,
}

struct GlobalRunQueueInner {
    head:  *mut G,
    tail:  *mut G,
    count: u32,
}

// SAFETY: All *mut G access inside GlobalRunQueueInner is guarded by the Mutex.
unsafe impl Send for GlobalRunQueueInner {}

impl GlobalRunQueue {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(GlobalRunQueueInner {
                head:  std::ptr::null_mut(),
                tail:  std::ptr::null_mut(),
                count: 0,
            }),
        }
    }

    /// Append a batch of goroutines (linked via `schedlink`, terminated by null)
    /// to the global queue.
    ///
    /// `tail.schedlink` is set to null before appending, matching Go's
    /// `globrunqputbatch` which calls `gQueue.pushBackAll`.
    ///
    /// # Safety
    /// `head`..=`tail` must be a valid `count`-element chain, linked through
    /// `G.schedlink`.  The goroutines must not be reachable from any other
    /// queue after this call.
    pub(crate) unsafe fn push_batch(&self, head: *mut G, tail: *mut G, count: u32) {
        // Terminate the chain.
        unsafe { (*tail).schedlink = std::ptr::null_mut() };

        let mut inner = self.inner.lock().unwrap();
        if inner.tail.is_null() {
            inner.head = head;
        } else {
            // SAFETY: inner.tail is a valid G (we put it there earlier).
            unsafe { (*inner.tail).schedlink = head };
        }
        inner.tail = tail;
        inner.count += count;
    }

    /// Remove and return the head goroutine, or `null` if the queue is empty.
    ///
    /// Ported from `globrunqget` in `runtime/proc.go` (single-pop path).
    ///
    /// # Safety
    /// The returned pointer is the exclusive owner of that G once dequeued.
    pub(crate) unsafe fn pop(&self) -> *mut G {
        let mut inner = self.inner.lock().unwrap();
        let gp = inner.head;
        if gp.is_null() {
            return std::ptr::null_mut();
        }
        inner.head = unsafe { (*gp).schedlink };
        if inner.head.is_null() {
            inner.tail = std::ptr::null_mut();
        }
        unsafe { (*gp).schedlink = std::ptr::null_mut() };
        inner.count -= 1;
        gp
    }

    /// Current length of the global queue (approximate, as it may race with other threads).
    pub(crate) fn len(&self) -> u32 {
        self.inner.lock().unwrap().count
    }
}

// ---------------------------------------------------------------------------
// P — logical processor
// ---------------------------------------------------------------------------

/// A logical processor (P) that owns a local run queue of goroutines.
///
/// The scheduler (step 8) attaches exactly one P to each M that is running
/// user code.  The run queue machinery is lock-free on the owner thread and
/// uses only a single CAS for stealers, matching Go's design exactly.
///
/// Ported from `p` in `runtime/runtime2.go`.
pub(crate) struct P {
    // ── identity ──────────────────────────────────────────────────────────
    /// Used to shard timers across Ps (see `time::timer_shard`) and in traces.
    pub id:     i32,

    // ── status ────────────────────────────────────────────────────────────
    /// Current status — one of the `P*` constants.
    pub status: AtomicU32,

    // ── M binding ─────────────────────────────────────────────────────────
    /// The M currently running this P; null when the P is idle or dead.
    pub m:      *mut M,

    // ── local run queue (lock-free ring) ──────────────────────────────────
    /// Monotonically-increasing head index.  Consumer (stealer) advances
    /// this via CAS-Release.  Read with Acquire to synchronise with stores.
    runqhead: AtomicU32,

    /// Monotonically-increasing tail index.  Only the owner P advances this,
    /// via Store-Release.  Read with Acquire by stealers.
    runqtail: AtomicU32,

    /// 256-slot ring buffer.  Slot index = `head % RUNQ_CAP`.
    /// Each element stores a `*mut G` cast to `usize`; `0` encodes null.
    runq:     [AtomicUsize; RUNQ_CAP],

    // ── runnext ───────────────────────────────────────────────────────────
    /// If non-zero, the next G to run on this P before consulting `runq`.
    /// `0` means empty.  Swapped atomically so stealers can grab it.
    runnext: AtomicUsize,

    // ── scheduler tick ────────────────────────────────────────────────────
    /// Monotonically-increasing count of goroutines scheduled on this P.
    /// Used by `schedule` to check the global run queue every 61 ticks.
    pub schedtick: AtomicU32,

    /// Monotonically-increasing count of syscalls entered on this P.
    /// Bumped by `entersyscall` (step 15.5) and by `retake` when it steals
    /// the P, so `exitsyscall` can detect that its P was taken away.
    ///
    /// Ported from `p.syscalltick` in `runtime/runtime2.go`.
    pub syscalltick: AtomicU32,

    // ── scheduler links ───────────────────────────────────────────────────
    /// Intrusive link for the idle-P list maintained by the scheduler (step 8).
    pub link: *mut P,

    // ── per-P sudog free list (Go's `pp.sudogcache`) ──────────────────────
    /// Local cache of free [`Sudog`](super::sudog::Sudog) records.  Refilled
    /// from / flushed to the global central list in `sudog.rs`.
    ///
    /// Protected by its own `Mutex` rather than accessed lock-free as in Go:
    /// a goroutine can be async-preempted (SIGURG) and migrated to another M
    /// between picking "the current P" and touching its cache, so two threads
    /// can momentarily race on one P's list.  The lock makes that safe; it is
    /// virtually always uncontended (the owning M is the only regular user),
    /// so it costs one futex CAS — far cheaper than the `pthread_sigmask`
    /// pin that lock-free access would otherwise require, and unlike the old
    /// single global list it does not serialise every channel park
    /// process-wide.  A plain `std::sync::Mutex` (not the loom shim) keeps the
    /// cache out of loom modelling — it participates in no model test.
    pub sudog_cache: std::sync::Mutex<super::sudog::SudogCache>,
}

// SAFETY: The scheduler ensures only one M operates on a P at any time, except
// for the lock-free steals described in the run-queue comments above.
unsafe impl Send for P {}
unsafe impl Sync for P {}

impl P {
    /// Allocate and initialise a new `P` in the idle state.
    ///
    /// All run-queue slots are zeroed (empty).  The caller (step 9 bootstrap)
    /// is responsible for adding the P to the scheduler's `allp` array and
    /// transitioning it to `PRUNNING` before attaching it to an M.
    pub(crate) fn new(id: i32) -> Box<P> {
        Box::new(P {
            id,
            status:   AtomicU32::new(PIDLE),
            m:        std::ptr::null_mut(),
            runqhead: AtomicU32::new(0),
            runqtail: AtomicU32::new(0),
            runq:     std::array::from_fn(|_| AtomicUsize::new(0)),
            runnext:     AtomicUsize::new(0),
            schedtick:   AtomicU32::new(0),
            syscalltick: AtomicU32::new(0),
            link:        std::ptr::null_mut(),
            sudog_cache: std::sync::Mutex::new(super::sudog::SudogCache::new()),
        })
    }

    // ── Local run queue ────────────────────────────────────────────────────

    /// Add `gp` to the run queue.
    ///
    /// If `next` is `true`, place `gp` in the `runnext` slot so it runs
    /// before any goroutine already in the ring — then push the displaced G
    /// (if any) into the ring.
    ///
    /// If the ring is full, transfers half its contents plus `gp` to the
    /// global queue via `runqputslow`.
    ///
    /// Ported from `runqput` in `runtime/proc.go`.
    ///
    /// # Safety
    /// Must be called from the owner M/P.  `gp` must be in `GRUNNABLE` state
    /// and not enqueued anywhere else.
    pub(crate) unsafe fn runqput(
        &self,
        mut gp: *mut G,
        next: bool,
        global_q: &GlobalRunQueue,
    ) {
        if next {
            // Try to install gp as runnext, atomically swapping the old value out.
            let mut old = self.runnext.load(Relaxed);
            loop {
                match self.runnext.compare_exchange_weak(old, gp as usize, AcqRel, Relaxed) {
                    Ok(_) => {
                        if old == 0 {
                            // runnext was empty; we're done.
                            return;
                        }
                        // The old runnext must go into the ring.
                        gp = old as *mut G;
                        break;
                    }
                    Err(cur) => old = cur,
                }
            }
        }

        // Try to enqueue gp into the local ring.
        'retry: loop {
            let h = self.runqhead.load(Acquire); // sync with consumers
            let t = self.runqtail.load(Relaxed);
            if t.wrapping_sub(h) < RUNQ_CAP as u32 {
                self.runq[(t as usize) % RUNQ_CAP].store(gp as usize, Relaxed);
                self.runqtail.store(t.wrapping_add(1), Release); // make visible
                return;
            }
            // Ring is full. Transfer half + gp to global queue.
            if unsafe { self.runqputslow(gp, h, t, global_q) } {
                return;
            }
            // CAS in runqputslow lost a race; retry the whole thing.
            continue 'retry;
        }
    }

    /// Slow path of `runqput`: drain half the local ring into the global queue.
    ///
    /// Returns `true` if the batch was successfully committed, `false` if a
    /// concurrent stealer moved the head and we must retry.
    ///
    /// Ported from `runqputslow` in `runtime/proc.go`.
    ///
    /// # Safety
    /// Same as `runqput`.  `h` and `t` must have been freshly loaded.
    unsafe fn runqputslow(
        &self,
        gp: *mut G,
        h: u32,
        t: u32,
        global_q: &GlobalRunQueue,
    ) -> bool {
        // n = half the elements currently in the ring.
        let n = t.wrapping_sub(h) / 2;
        debug_assert_eq!(
            n,
            (RUNQ_CAP / 2) as u32,
            "runqputslow: queue not full (n={n})"
        );

        // Atomically commit the drain: advance runqhead by n (CAS-Release).
        if self
            .runqhead
            .compare_exchange(h, h.wrapping_add(n), Release, Relaxed)
            .is_err()
        {
            return false; // someone else moved the head; retry
        }

        // Collect the n drained Gs plus the new gp into a fixed array.
        let n_usize = n as usize;
        let mut batch = [std::ptr::null_mut::<G>(); RUNQ_CAP / 2 + 1];
        for (i, b) in batch.iter_mut().enumerate().take(n_usize) {
            let slot = self.runq[(h.wrapping_add(i as u32) as usize) % RUNQ_CAP].load(Relaxed);
            *b = slot as *mut G;
        }
        batch[n_usize] = gp;

        // Link them into a singly-linked list via schedlink.
        for i in 0..n_usize {
            unsafe { (*batch[i]).schedlink = batch[i + 1] };
        }

        let head_g = batch[0];
        let tail_g = batch[n_usize];

        // Push the whole batch to the global queue.
        unsafe { global_q.push_batch(head_g, tail_g, (n_usize + 1) as u32) };
        true
    }

    /// Remove and return the next runnable goroutine from this P's queue.
    ///
    /// Checks `runnext` first (AcqRel CAS), then drains from the head of the
    /// local ring (Acquire load + CAS-Release).  Returns `(null, false)` when
    /// the local queue is empty.
    ///
    /// The second element is `inherit_time` — `true` when the returned G was
    /// in `runnext`, meaning it should inherit the current scheduling quantum
    /// rather than starting a fresh one.
    ///
    /// Ported from `runqget` in `runtime/proc.go`.
    pub(crate) fn runqget(&self) -> (*mut G, bool) {
        // Check runnext.
        let next = self.runnext.load(Relaxed);
        if next != 0 {
            // A stealer on another P may race with us here; that's allowed.
            if self
                .runnext
                .compare_exchange(next, 0, AcqRel, Relaxed)
                .is_ok()
            {
                return (next as *mut G, true);
            }
        }

        // Drain the ring.
        loop {
            let h = self.runqhead.load(Acquire); // sync with producers/stealers
            let t = self.runqtail.load(Relaxed);
            if t == h {
                return (std::ptr::null_mut(), false);
            }
            let gp = self.runq[(h as usize) % RUNQ_CAP].load(Relaxed) as *mut G;
            if self
                .runqhead
                .compare_exchange(h, h.wrapping_add(1), Release, Relaxed)
                .is_ok()
            {
                return (gp, false);
            }
        }
    }

    /// Steal goroutines from `victim` into this P's run queue.
    ///
    /// Returns the stolen G that should run next, or null if nothing was
    /// stolen.  The remaining stolen Gs (up to `ceil(n/2) - 1`) are deposited
    /// into `self.runq` starting at `self.runqtail`.
    ///
    /// Ported from `runqsteal` in `runtime/proc.go`.
    pub(crate) fn runqsteal(&self, victim: &P, steal_run_next: bool) -> *mut G {
        let t = self.runqtail.load(Relaxed);
        let n = victim.runqgrab(&self.runq, t, steal_run_next);
        if n == 0 {
            return std::ptr::null_mut();
        }
        let n = n - 1;
        // The last stolen G is the one we run immediately; don't enqueue it.
        let gp = self.runq[(t.wrapping_add(n) as usize) % RUNQ_CAP].load(Relaxed) as *mut G;
        if n == 0 {
            return gp;
        }
        // Make the remainder visible.
        let h = self.runqhead.load(Acquire);
        debug_assert!(
            t.wrapping_sub(h).wrapping_add(n) < RUNQ_CAP as u32,
            "runqsteal: runq overflow"
        );
        self.runqtail.store(t.wrapping_add(n), Release);
        gp
    }

    /// Grab up to `ceil(n/2)` goroutines from `self` into `batch`.
    ///
    /// `batch_head` is the index in `batch` to start writing at.  Returns the
    /// number of goroutines written.
    ///
    /// Ported from `runqgrab` in `runtime/proc.go`.
    fn runqgrab(
        &self,
        batch: &[AtomicUsize; RUNQ_CAP],
        batch_head: u32,
        steal_run_next: bool,
    ) -> u32 {
        loop {
            let h = self.runqhead.load(Acquire);
            let t = self.runqtail.load(Acquire);
            let n_full = t.wrapping_sub(h);
            // Steal ceil(n/2): n - n/2.
            let n = n_full - n_full / 2;

            if n == 0 {
                if steal_run_next {
                    // Try to steal runnext.
                    let next = self.runnext.load(Relaxed);
                    if next != 0 {
                        // If the victim is PRUNNING, wait a tiny bit to avoid
                        // stealing a G that the victim is just about to schedule,
                        // which would cause an expensive round-trip.
                        if self.status.load(Acquire) == PRUNNING {
                            std::thread::sleep(std::time::Duration::from_micros(3));
                        }
                        if self
                            .runnext
                            .compare_exchange(next, 0, AcqRel, Relaxed)
                            .is_ok()
                        {
                            batch[(batch_head as usize) % RUNQ_CAP].store(next, Relaxed);
                            return 1;
                        }
                        // Lost the race; retry from the top.
                        continue;
                    }
                }
                return 0;
            }

            // Guard against reading an inconsistent head+tail pair.
            if n > (RUNQ_CAP / 2) as u32 {
                continue; // retry with fresh loads
            }

            for i in 0..n {
                let slot = self.runq[(h.wrapping_add(i) as usize) % RUNQ_CAP].load(Relaxed);
                batch[(batch_head.wrapping_add(i) as usize) % RUNQ_CAP].store(slot, Relaxed);
            }

            // Commit the steal by advancing runqhead.
            if self
                .runqhead
                .compare_exchange(h, h.wrapping_add(n), Release, Relaxed)
                .is_ok()
            {
                return n;
            }
            // Lost the race; retry.
        }
    }

    /// Number of goroutines currently in the local run queue (ring + runnext).
    ///
    /// Approximate: may race with concurrent modifications.  Used only for
    /// diagnostics and the scheduler's work-steal heuristic.
    pub(crate) fn runq_size(&self) -> u32 {
        let h = self.runqhead.load(Acquire);
        let t = self.runqtail.load(Acquire);
        let ring = t.wrapping_sub(h);
        let rn = if self.runnext.load(Relaxed) != 0 { 1 } else { 0 };
        ring + rn
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::runtime::g::{Stack, G};
    use crate::runtime::stack::GOROUTINE_STACK_BYTES;

    fn make_g(id: u64) -> Box<G> {
        // Use fake but valid stack bounds — not executed, just used as pointers.
        let lo = (id as usize + 1) << 20; // 1 MiB spacing
        G::new(Stack { lo, hi: lo + GOROUTINE_STACK_BYTES }, id)
    }

    #[test]
    fn p_new_initial_state() {
        let p = P::new(1);
        assert_eq!(p.id, 1);
        assert_eq!(p.status.load(Relaxed), PIDLE);
        assert!(p.m.is_null());
        assert_eq!(p.runqhead.load(Relaxed), 0);
        assert_eq!(p.runqtail.load(Relaxed), 0);
        assert_eq!(p.runnext.load(Relaxed), 0);
        assert!(p.link.is_null());
    }

    #[test]
    fn global_queue_push_pop() {
        let gq = GlobalRunQueue::new();
        let g1 = make_g(1);
        let g2 = make_g(2);

        let g1_ptr = Box::into_raw(g1);
        let g2_ptr = Box::into_raw(g2);

        unsafe {
            (*g1_ptr).schedlink = g2_ptr;
            gq.push_batch(g1_ptr, g2_ptr, 2);
            assert_eq!(gq.len(), 2);

            let got1 = gq.pop();
            assert_eq!(got1, g1_ptr);
            assert_eq!(gq.len(), 1);

            let got2 = gq.pop();
            assert_eq!(got2, g2_ptr);
            assert_eq!(gq.len(), 0);
        }

        // Cleanup (outside unsafe block)
        let _ = unsafe { Box::from_raw(g1_ptr) };
        let _ = unsafe { Box::from_raw(g2_ptr) };
    }

    #[test]
    fn global_queue_pop_empty() {
        let gq = GlobalRunQueue::new();
        let got = unsafe { gq.pop() };
        assert!(got.is_null());
        assert_eq!(gq.len(), 0);
    }

    #[test]
    fn runqput_runqget_fifo() {
        let p = P::new(0);
        let gq = GlobalRunQueue::new();

        let mut goroutines: Vec<Box<G>> = (0..10).map(|i| make_g(i as u64)).collect();
        let ptrs: Vec<*mut G> = goroutines.iter_mut().map(|g| &mut **g as *mut G).collect();

        // Enqueue all 10.
        for ptr in &ptrs {
            unsafe { p.runqput(*ptr, false, &gq) };
        }

        // Dequeue and verify FIFO order.
        for (i, expected_ptr) in ptrs.iter().enumerate() {
            let (got, inherit) = p.runqget();
            assert_eq!(got, *expected_ptr, "mismatch at position {i}");
            assert!(!inherit, "should not inherit time for normal enqueue");
        }

        // Queue should be empty now.
        let (got, _) = p.runqget();
        assert!(got.is_null());

        // Cleanup.
        for g in goroutines {
            let _ = unsafe { Box::from_raw(Box::into_raw(g)) };
        }
    }

    #[test]
    fn runqput_next_installs_runnext() {
        let p = P::new(0);
        let gq = GlobalRunQueue::new();
        let g1 = make_g(1);
        let g1_ptr = Box::into_raw(g1);

        // Put g1 with next=true.
        unsafe { p.runqput(g1_ptr, true, &gq) };

        // Get should return it with inherit_time=true.
        let (got, inherit) = p.runqget();
        assert_eq!(got, g1_ptr);
        assert!(inherit);

        let _ = unsafe { Box::from_raw(g1_ptr) };
    }

    #[test]
    fn runqput_next_displaces_old_runnext() {
        let p = P::new(0);
        let gq = GlobalRunQueue::new();
        let g1 = make_g(1);
        let g2 = make_g(2);

        let g1_ptr = Box::into_raw(g1);
        let g2_ptr = Box::into_raw(g2);

        // Put g1 with next=true.
        unsafe { p.runqput(g1_ptr, true, &gq) };

        // Put g2 with next=true — should displace g1 to the ring.
        unsafe { p.runqput(g2_ptr, true, &gq) };

        // g2 should be in runnext.
        let (got, inherit) = p.runqget();
        assert_eq!(got, g2_ptr);
        assert!(inherit);

        // g1 should now be in the ring.
        let (got, inherit) = p.runqget();
        assert_eq!(got, g1_ptr);
        assert!(!inherit);

        unsafe {
            let _ = Box::from_raw(g1_ptr);
            let _ = Box::from_raw(g2_ptr);
        }
    }

    #[test]
    fn runqput_overflow_to_global() {
        let p = P::new(0);
        let gq = GlobalRunQueue::new();

        // Fill the ring with 256 goroutines.
        let mut goroutines: Vec<Box<G>> = (0..256).map(|i| make_g(i as u64)).collect();
        let ptrs: Vec<*mut G> = goroutines.iter_mut().map(|g| &mut **g as *mut G).collect();

        for ptr in &ptrs {
            unsafe { p.runqput(*ptr, false, &gq) };
        }

        // Next put should trigger runqputslow and overflow to global queue.
        let g257 = make_g(257);
        let g257_ptr = Box::into_raw(g257);
        unsafe { p.runqput(g257_ptr, false, &gq) };

        // Global queue should have ~128 goroutines (half of 256, plus the overflow).
        let gq_len = gq.len();
        assert!(gq_len > 0, "expected overflow to global queue, got {gq_len}");

        let _ = unsafe { Box::from_raw(g257_ptr) };
        for g in goroutines {
            let _ = unsafe { Box::from_raw(Box::into_raw(g)) };
        }
    }

    #[test]
    fn runq_size_counts_runnext() {
        let p = P::new(0);
        let gq = GlobalRunQueue::new();

        let g1 = make_g(1);
        let g1_ptr = Box::into_raw(g1);

        // Empty queue.
        assert_eq!(p.runq_size(), 0);

        // Add via runnext.
        unsafe { p.runqput(g1_ptr, true, &gq) };
        assert_eq!(p.runq_size(), 1);

        let _ = unsafe { Box::from_raw(g1_ptr) };
    }

    #[test]
    fn runqsteal_basic() {
        let victim = P::new(0);
        let stealer = P::new(1);
        let gq = GlobalRunQueue::new();

        // Fill victim's queue.
        let mut goroutines: Vec<Box<G>> = (0..10).map(|i| make_g(i as u64)).collect();
        let ptrs: Vec<*mut G> = goroutines.iter_mut().map(|g| &mut **g as *mut G).collect();

        for ptr in &ptrs {
            unsafe { victim.runqput(*ptr, false, &gq) };
        }

        // Stealer should grab roughly half.
        let stolen = stealer.runqsteal(&victim, false);
        assert!(!stolen.is_null(), "should have stolen something");

        // Stealer's queue should have the stolen G.
        let (got, _) = stealer.runqget();
        assert!(!got.is_null(), "stealer should have something to run");

        for g in goroutines {
            let _ = unsafe { Box::from_raw(Box::into_raw(g)) };
        }
    }
}

// ---------------------------------------------------------------------------
// Loom model tests
// ---------------------------------------------------------------------------

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use crate::runtime::g::{Stack, G};
    use crate::runtime::stack::GOROUTINE_STACK_BYTES;
    use loom::sync::Arc;

    // Wrapper so the raw G pointer can cross a loom::thread boundary.
    // SAFETY: The test manages the lifetime of each G explicitly; the pointer
    // is valid for the entire model invocation.
    struct GPtr(*mut G);
    unsafe impl Send for GPtr {}

    fn make_g(id: u64) -> *mut G {
        let lo = (id as usize + 1) << 20;
        Box::into_raw(G::new(Stack { lo, hi: lo + GOROUTINE_STACK_BYTES }, id))
    }

    /// One thread pushes a single G; the main thread pops.  Loom explores
    /// every ordering and verifies the Mutex is always correctly released and
    /// exactly one G is recovered in total.
    #[test]
    fn concurrent_push_pop() {
        loom::model(|| {
            let gq  = Arc::new(GlobalRunQueue::new());
            let gq2 = Arc::clone(&gq);

            let g1 = GPtr(make_g(1));

            let pusher = loom::thread::spawn(move || {
                let gp = g1.0;
                // push_batch terminates the chain internally before locking.
                unsafe { gq2.push_batch(gp, gp, 1) };
            });

            // Main thread races with the pusher.
            let got = unsafe { gq.pop() };

            pusher.join().unwrap();

            // Drain whatever the pusher left behind.
            let got2 = unsafe { gq.pop() };

            // Exactly one G was inserted; it must be dequeued exactly once.
            let retrieved = [got, got2].iter().filter(|p| !p.is_null()).count();
            assert_eq!(retrieved, 1, "expected exactly one G across both pops");

            for p in [got, got2] {
                if !p.is_null() {
                    let _ = unsafe { Box::from_raw(p) };
                }
            }
        });
    }

    /// Two threads pop concurrently after a batch has been pushed.  Each G
    /// must be returned to at most one thread.
    #[test]
    fn concurrent_two_pops() {
        loom::model(|| {
            let gq  = Arc::new(GlobalRunQueue::new());
            let gq2 = Arc::clone(&gq);
            let gq3 = Arc::clone(&gq);

            // Pre-populate two Gs (single-threaded setup before model races).
            let g1 = make_g(1);
            let g2 = make_g(2);
            unsafe {
                (*g1).schedlink = g2;
                gq.push_batch(g1, g2, 2);
            }

            let t1 = loom::thread::spawn(move || unsafe { gq2.pop() });
            let t2 = loom::thread::spawn(move || unsafe { gq3.pop() });

            let p1 = t1.join().unwrap();
            let p2 = t2.join().unwrap();

            // Both threads together must have obtained exactly 2 distinct Gs.
            let mut ptrs: Vec<*mut G> = [p1, p2, unsafe { gq.pop() }]
                .into_iter()
                .filter(|p| !p.is_null())
                .collect();
            assert_eq!(ptrs.len(), 2, "expected exactly 2 Gs from 2 pops");
            ptrs.sort();
            ptrs.dedup();
            assert_eq!(ptrs.len(), 2, "each G must be returned to at most one thread");

            for p in ptrs {
                let _ = unsafe { Box::from_raw(p) };
            }
        });
    }
}
