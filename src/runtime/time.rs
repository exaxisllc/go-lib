// SPDX-License-Identifier: Apache-2.0
//! Timer heap — ported from `runtime/time.go`.
//!
//! ## Design — sharded min-heaps
//!
//! Go's runtime uses per-P 4-ary min-heaps for timers.  We approximate that
//! with a fixed array of [`TIMER_SHARDS`] independent min-heaps, each behind
//! its own `Mutex` and fronted by an `AtomicU64` caching that shard's
//! earliest deadline.  A goroutine's `sleep` inserts into the shard selected
//! by its current `P`'s id, so sleepers on different Ps do not contend on a
//! single lock; the old single global heap serialised every `sleep` and the
//! timer thread on one mutex.
//!
//! A fixed shard array (rather than a literally per-P heap) sidesteps the
//! `allp`-can-grow / `GOMAXPROCS`-resize problem: the timer thread iterates a
//! compile-time-constant set of shards with no dependency on the scheduler's
//! P list, reading each shard's `earliest` atomic locklessly to skip empty or
//! not-yet-due shards without taking its lock.  Shard choice never affects
//! correctness — every entry is found by a full all-shard scan — so a
//! goroutine migrating between selecting a shard and locking it is harmless,
//! requiring no preemption pin.
//!
//! ## Execution model
//!
//! A dedicated OS timer thread (`timer_thread`) sleeps with
//! `thread::park_timeout` until the earliest deadline across all shards.  It
//! then, for each shard whose `earliest ≤ now`:
//! 1. Pops all timers whose `when ≤ now` from that shard's heap.
//! 2. Calls `goready` on each parked G.
//!
//! When `sleep` adds a timer earlier than its shard's previous earliest, it
//! calls `thread::unpark` on the timer thread to shorten its sleep.
//!
//! ## Relationship with sysmon
//!
//! The timer thread is completely independent from `sysmon`; `sysmon` handles
//! CPU preemption and P-retake, while the timer thread handles only sleeping
//! goroutines.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use super::g::{current_g, WaitReason};
use super::park::{gopark, goready};
use super::g::G;
use super::sched::set_current_rt;

// ---------------------------------------------------------------------------
// Monotonic clock helper
// ---------------------------------------------------------------------------

/// Return monotonic nanoseconds since an arbitrary epoch.
///
/// `Instant` is guaranteed monotonic; we use a process-start epoch for a
/// compact `u64` representation that won't overflow for ~580 years.
fn mono_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = *START.get_or_init(Instant::now);
    Instant::now().duration_since(start).as_nanos() as u64
}

// ---------------------------------------------------------------------------
// TimerEntry
// ---------------------------------------------------------------------------

/// One pending timer in the heap.
#[derive(Eq, PartialEq)]
struct TimerEntry {
    /// Expiry time in monotonic nanoseconds.
    when:   u64,
    /// Goroutine to wake when the timer fires.  Raw pointer valid because
    /// the goroutine is parked (GWAITING) for the entire duration.
    gp:     *mut G,
}

// SAFETY: a TimerEntry is only touched under its shard's Mutex.
unsafe impl Send for TimerEntry {}

/// Order by `when` ascending (smallest `when` fires first).
impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is a max-heap; wrap in Reverse to get a min-heap.
        Reverse(self.when).cmp(&Reverse(other.when))
    }
}

// ---------------------------------------------------------------------------
// Sharded timer heaps + thread handle
// ---------------------------------------------------------------------------

/// Number of independent timer heaps.  Power of two so shard selection is a
/// mask.  Sized to comfortably cover typical `GOMAXPROCS`, so each P usually
/// maps to its own shard (`p.id & (TIMER_SHARDS-1)`); above this Ps share.
const TIMER_SHARDS: usize = 64;

/// Sentinel `earliest` for an empty shard.
const NO_TIMER: u64 = u64::MAX;

/// One timer heap shard: a min-heap plus a lockless cache of its earliest
/// deadline so the timer thread can skip it without taking the lock.
struct TimerShard {
    heap:     Mutex<BinaryHeap<TimerEntry>>,
    /// Earliest `when` currently in `heap`, or [`NO_TIMER`] when empty.
    /// Written only while holding `heap`'s lock (so a non-`NO_TIMER` value
    /// always means the heap is non-empty); read locklessly as a hint.
    earliest: AtomicU64,
}

impl TimerShard {
    const fn new() -> Self {
        TimerShard {
            heap:     Mutex::new(BinaryHeap::new()),
            earliest: AtomicU64::new(NO_TIMER),
        }
    }
}

/// Refresh `shard.earliest` from the heap top.  Caller holds `heap`'s lock.
#[inline]
fn refresh_earliest(shard: &TimerShard, heap: &BinaryHeap<TimerEntry>) {
    let e = heap.peek().map(|e| e.when).unwrap_or(NO_TIMER);
    shard.earliest.store(e, Relaxed);
}

static TIMER_SHARDS_ARR: [TimerShard; TIMER_SHARDS] =
    [const { TimerShard::new() }; TIMER_SHARDS];

static TIMER_THREAD: OnceLock<std::thread::Thread> = OnceLock::new();

/// Pick the timer shard for the current goroutine: its `P`'s id masked into
/// the shard range, or shard 0 when there is no current P (the run_impl
/// caller and other bare threads — which in practice never call `sleep`,
/// since `sleep` requires a running goroutine).
#[inline]
fn current_shard() -> &'static TimerShard {
    let p = super::sched::current_p();
    let idx = if p.is_null() {
        0
    } else {
        (unsafe { (*p).id } as usize) & (TIMER_SHARDS - 1)
    };
    &TIMER_SHARDS_ARR[idx]
}

// ---------------------------------------------------------------------------
// Timer thread
// ---------------------------------------------------------------------------

/// Start the background timer thread.
///
/// Idempotent — the thread is started at most once per process.  Called from
/// `schedinit` (step 9).
pub(crate) fn start_timer_thread() {
    // `TIMER_THREAD` is set inside `timer_thread_body`, so checking it here
    // races with concurrent `schedinit` calls (concurrent run_impls) — both
    // see `None` and both spawn, leaving TWO timer threads firing
    // concurrently for the lifetime of the process.  `Once` serialises the
    // spawn itself.
    static STARTED: std::sync::Once = std::sync::Once::new();
    STARTED.call_once(|| {
        std::thread::Builder::new()
            .name("go-timer".into())
            .spawn(timer_thread_body)
            .expect("time: failed to spawn timer thread");
    });
}

/// Main body of the background timer thread.
fn timer_thread_body() {
    // Register the thread handle so `sleep` can unpark us early.
    let handle = std::thread::current();
    // It's OK if another thread already initialised TIMER_THREAD.
    let _ = TIMER_THREAD.set(handle);

    loop {
        // Compute how long to sleep until the earliest deadline across all
        // shards, reading the per-shard `earliest` caches locklessly.
        let next = TIMER_SHARDS_ARR
            .iter()
            .map(|s| s.earliest.load(Relaxed))
            .min()
            .unwrap_or(NO_TIMER);
        let sleep_dur = if next == NO_TIMER {
            Duration::from_secs(1) // idle; wake to re-check
        } else {
            let now = mono_ns();
            if next <= now {
                Duration::ZERO
            } else {
                Duration::from_nanos(next - now)
            }
        };

        if sleep_dur > Duration::ZERO {
            std::thread::park_timeout(sleep_dur);
        }

        // Fire all expired timers.
        fire_expired();
    }
}

/// Pop every expired entry from the heap and call `goready` on its G.
///
/// # Race: GRUNNABLE goroutine
///
/// A goroutine can be asynchronously preempted (via SIGURG on Unix) between
/// the moment it inserts its timer entry and the moment it calls `gopark`.
/// In that window the goroutine transitions `GRUNNING → GRUNNABLE` while the
/// timer entry is already in the heap.  When the timer fires the goroutine
/// is in `GRUNNABLE` rather than `GWAITING`.
///
/// Calling `goready` on a `GRUNNABLE` goroutine would assert-fail and kill the
/// timer thread (all subsequent `sleep` calls would hang).  Instead we re-insert
/// a near-immediate timer so the goroutine is woken once it does reach `GWAITING`
/// via its in-flight `gopark` call.  The retry delay (5 ms) is intentionally
/// larger than the typical preemption-to-resume latency so the goroutine is
/// almost certainly in `GWAITING` by the time the re-inserted timer fires.
fn fire_expired() {
    use super::g::{readgstatus, GDEAD, GWAITING};

    let now = mono_ns();
    struct WakeEntry { gp: *mut G }

    // Bind the timer thread to the singleton Rt so goready -> sched()/startm
    // work.  Null only before the first schedinit, when no timer entry can
    // exist yet (sleep requires a running goroutine).
    let rt = super::sched::global_rt_ptr();
    if rt.is_null() {
        return;
    }
    set_current_rt(rt);

    let retry_when = now.saturating_add(5_000_000); // +5 ms

    // Process each shard independently.  Skip shards not yet due via the
    // lockless `earliest` cache; for due shards, hold THAT shard's lock
    // across the ENTIRE pop → status-check → goready → retry-re-push
    // sequence, so popped entries are never observed in an inconsistent
    // intermediate state by another caller touching the same shard.
    //
    // Holding the lock across `goready` is safe: goready never touches a timer
    // shard, and its GRUNNING→GWAITING spin completes independently (the
    // parking goroutine does not need this lock to finish gopark).  `sleep`
    // callers targeting this shard simply block for the duration of its batch.
    for shard in &TIMER_SHARDS_ARR {
        if shard.earliest.load(Relaxed) > now {
            continue; // not due (covers NO_TIMER too)
        }

        let mut heap = shard.heap.lock().unwrap();

        let mut to_wake: Vec<WakeEntry> = Vec::new();
        while let Some(entry) = heap.peek() {
            if entry.when <= now {
                let entry = heap.pop().unwrap();
                to_wake.push(WakeEntry { gp: entry.gp });
            } else {
                break;
            }
        }

        for entry in to_wake {
            // Check the goroutine's status before calling goready.
            let s = unsafe { readgstatus(entry.gp) };
            if s == GWAITING {
                unsafe { goready(entry.gp) };
            } else if s != GDEAD {
                // GRUNNABLE/GRUNNING: preempted between timer insertion and its
                // gopark call.  Re-insert a near-immediate timer rather than
                // spinning or asserting; the retry delay (5 ms) is larger than
                // the typical preemption-to-resume latency so the goroutine is
                // almost certainly GWAITING by the time it fires.
                heap.push(TimerEntry { when: retry_when, gp: entry.gp });
            }
            // GDEAD: the goroutine has already exited — drop the entry, never
            // wake or retry.
        }

        refresh_earliest(shard, &heap);
    }
}

// ---------------------------------------------------------------------------
// sleep — public entry point
// ---------------------------------------------------------------------------

/// Park the current goroutine for at least `d`.
///
/// Adds a timer to the global heap and parks the goroutine via `gopark`.  The
/// background timer thread calls `goready` when the timer expires.
///
/// # Safety
/// Must be called from a goroutine stack (not g0 or a bare OS thread).
pub(crate) unsafe fn sleep(d: Duration) {
    if d.is_zero() {
        // Zero sleep: just yield to let other goroutines run.
        unsafe { super::sched::gosched() };
        return;
    }

    let when = mono_ns().saturating_add(d.as_nanos() as u64);
    let gp   = current_g();
    debug_assert!(!gp.is_null(), "sleep: called from g0");

    // Insert into this goroutine's shard.  Unpark the timer thread when this
    // entry lowers the shard's earliest deadline: the global minimum can only
    // have decreased if some shard's minimum did, and that shard is this one.
    // (When `when` is not below the shard's old earliest the global minimum is
    // unchanged, so no wake is needed; a spurious wake would be harmless
    // anyway — the thread just re-evaluates.)
    let shard = current_shard();
    let need_unpark = {
        let mut heap = shard.heap.lock().unwrap();
        let lowered = shard.earliest.load(Relaxed) > when;
        heap.push(TimerEntry { when, gp });
        if lowered {
            shard.earliest.store(when, Relaxed);
        }
        lowered
    };

    if need_unpark && let Some(t) = TIMER_THREAD.get() {
        t.unpark();
    }

    gopark(WaitReason::Sleep);
    // Returns here after the timer fires and goready transitions us to GRUNNABLE.
}

// ---------------------------------------------------------------------------
// Crate-internal entry point — called by lib.rs::sleep()
// ---------------------------------------------------------------------------

/// Thin wrapper around [`sleep`] used by `lib.rs::sleep`.
///
/// # Safety
/// Must be called from a goroutine stack (not g0 or a bare OS thread).
pub(crate) unsafe fn goroutine_sleep(d: Duration) {
    unsafe { sleep(d) };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    /// mono_ns is strictly monotonically non-decreasing.
    #[test]
    fn mono_ns_monotonic() {
        let a = mono_ns();
        let b = mono_ns();
        assert!(b >= a, "mono_ns must be non-decreasing");
    }

    /// fire_expired (via the timer thread) wakes a sleeping goroutine.
    ///
    /// The original test injected a fake G (non-mmap'd stack) directly into
    /// the timer heap and called fire_expired().  After goready() pushed the
    /// fake G into the live global run queue, background M-threads would
    /// execute it and SIGSEGV on the bogus stack address.
    ///
    /// This version tests the full path with a real goroutine: sleep() inserts
    /// a real G into the heap, the timer thread fires it, and the goroutine
    /// resumes after the expected delay.
    #[test]
    #[go_lib::main]
    fn fire_expired_wakes_goroutine() {
        use std::time::Instant;
        let t0 = Instant::now();
        unsafe { sleep(Duration::from_millis(10)) };
        let elapsed = t0.elapsed();
        assert!(
            elapsed >= Duration::from_millis(8),
            "timer did not fire: elapsed only {:?}", elapsed
        );
    }

    /// sleep(0) yields without blocking.
    #[test]
    #[go_lib::main]
    fn sleep_zero_yields() {
        unsafe { sleep(Duration::ZERO) };
    }

    /// sleep(short) completes within a reasonable wall-clock window.
    #[test]
    #[go_lib::main]
    fn sleep_short_duration() {
        use crate::runtime::sched::spawn_goroutine;
        use crate::sync::WaitGroup;

        // WaitGroup parks the main goroutine via gopark, releasing M+P so
        // the sleeper goroutine can be scheduled.  Using std::thread::sleep
        // in the polling loop would hold the M without releasing its P,
        // potentially starving the sleeper under concurrent test load.
        let wg  = Arc::new(WaitGroup::new());
        let wg2 = Arc::clone(&wg);
        wg.add(1);

        spawn_goroutine(move || {
            unsafe { sleep(Duration::from_millis(5)) };
            wg2.done();
        });

        wg.wait();
    }

    /// Many goroutines sleeping concurrently all wake — exercises timer
    /// entries spread across multiple shards (more sleepers than there are
    /// Ps, so shard reuse and cross-shard firing are both covered).
    #[test]
    #[go_lib::main]
    fn many_sleepers_across_shards() {
        use crate::runtime::sched::spawn_goroutine;
        use crate::sync::WaitGroup;

        const N: i32 = 200;
        let awoke = Arc::new(AtomicI32::new(0));
        let awoke2 = Arc::clone(&awoke);

        let wg = Arc::new(WaitGroup::new());
        for k in 0..N {
            let awoke3 = Arc::clone(&awoke2);
            let wg2 = Arc::clone(&wg);
            wg.add(1);
            spawn_goroutine(move || {
                // Vary the delay so entries interleave within and across
                // shards rather than all expiring at once.
                unsafe { sleep(Duration::from_millis(5 + (k % 7) as u64)) };
                awoke3.fetch_add(1, Ordering::Relaxed);
                wg2.done();
            });
        }
        wg.wait();

        assert_eq!(awoke.load(Ordering::Acquire), N);
    }

    /// Multiple goroutines sleeping concurrently all wake.
    #[test]
    #[go_lib::main]
    fn concurrent_sleepers() {
        use crate::runtime::sched::spawn_goroutine;
        use crate::sync::WaitGroup;

        const N: i32 = 4;
        let awoke = Arc::new(AtomicI32::new(0));
        let awoke2 = Arc::clone(&awoke);

        // WaitGroup parks the main goroutine (releases M+P) while the N
        // sleeper goroutines run.  Polling with std::thread::sleep would
        // hold the M's P, leaving no idle P for the timer-woken goroutines
        // under concurrent test pressure, causing an indefinite hang on
        // macOS where scheduler-tick preemption is SIGURG-based.
        let wg = Arc::new(WaitGroup::new());

        for _ in 0..N {
            let awoke3 = Arc::clone(&awoke2);
            let wg2   = Arc::clone(&wg);
            wg.add(1);
            spawn_goroutine(move || {
                unsafe { sleep(Duration::from_millis(10)) };
                awoke3.fetch_add(1, Ordering::Relaxed);
                wg2.done();
            });
        }

        wg.wait();

        assert_eq!(awoke.load(Ordering::Acquire), N);
    }
}
