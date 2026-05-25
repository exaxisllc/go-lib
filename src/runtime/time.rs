// SPDX-License-Identifier: Apache-2.0
//! Timer heap — ported from `runtime/time.go`.
//!
//! ## Design (v1 simplified)
//!
//! Go's runtime uses per-P 4-ary min-heaps for timers; that requires changes
//! to the P struct and deep scheduler integration.  v1 uses a single global
//! min-heap behind a `Mutex`.  The per-P tier can be layered on later without
//! changing the public `sleep` API.
//!
//! ## Execution model
//!
//! A dedicated OS timer thread (`timer_thread`) sleeps with
//! `thread::park_timeout` until the earliest timer fires.  It then:
//! 1. Pops all timers whose `when ≤ now` from the heap.
//! 2. Calls `goready` on each parked G.
//!
//! When `sleep` adds a timer that is earlier than the current next wakeup, it
//! calls `thread::unpark` on the timer thread to shorten its sleep.
//!
//! ## Relationship with sysmon
//!
//! The timer thread is completely independent from `sysmon`; `sysmon` handles
//! CPU preemption and P-retake, while the timer thread handles only sleeping
//! goroutines.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use super::g::{current_g, WaitReason};
use super::park::{gopark, goready};
use super::g::G;

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
    when: u64,
    /// Goroutine to wake when the timer fires.  Raw pointer valid because
    /// the goroutine is parked (GWAITING) for the entire duration.
    gp:   *mut G,
}

// SAFETY: TimerEntry is only touched under TIMER_HEAP's Mutex.
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
// Global timer heap + thread handle
// ---------------------------------------------------------------------------

static TIMER_HEAP:   Mutex<BinaryHeap<TimerEntry>> = Mutex::new(BinaryHeap::new());
static TIMER_THREAD: OnceLock<std::thread::Thread>  = OnceLock::new();

// ---------------------------------------------------------------------------
// Timer thread
// ---------------------------------------------------------------------------

/// Start the background timer thread.
///
/// Idempotent — the thread is started at most once per process.  Called from
/// `schedinit` (step 9).
pub(crate) fn start_timer_thread() {
    // If already started, do nothing.
    if TIMER_THREAD.get().is_some() { return; }

    std::thread::Builder::new()
        .name("go-timer".into())
        .spawn(timer_thread_body)
        .expect("time: failed to spawn timer thread");
}

/// Main body of the background timer thread.
fn timer_thread_body() {
    // Register the thread handle so `sleep` can unpark us early.
    let handle = std::thread::current();
    // It's OK if another thread already initialised TIMER_THREAD.
    let _ = TIMER_THREAD.set(handle);

    loop {
        // Compute how long to sleep until the next expiry.
        let sleep_dur = {
            let heap = TIMER_HEAP.lock().unwrap();
            if let Some(entry) = heap.peek() {
                let now = mono_ns();
                if entry.when <= now {
                    Duration::ZERO
                } else {
                    Duration::from_nanos(entry.when - now)
                }
            } else {
                Duration::from_secs(1) // idle; wake to re-check
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
fn fire_expired() {
    let now = mono_ns();
    let mut to_wake: Vec<*mut G> = Vec::new();

    {
        let mut heap = TIMER_HEAP.lock().unwrap();
        while let Some(entry) = heap.peek() {
            if entry.when <= now {
                let entry = heap.pop().unwrap();
                to_wake.push(entry.gp);
            } else {
                break;
            }
        }
    }

    for gp in to_wake {
        unsafe { goready(gp) };
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

    // Insert the timer.  If it is the earliest, wake the timer thread so it
    // re-evaluates its sleep deadline.
    let need_unpark = {
        let mut heap = TIMER_HEAP.lock().unwrap();
        let was_earliest = heap.peek().map(|e| e.when > when).unwrap_or(true);
        heap.push(TimerEntry { when, gp });
        was_earliest
    };

    if need_unpark && let Some(t) = TIMER_THREAD.get() {
        t.unpark();
    }

    unsafe { gopark(WaitReason::Sleep) };
    // Returns here after the timer fires and goready transitions us to GRUNNABLE.
}

// ---------------------------------------------------------------------------
// Public API re-exported from lib.rs
// ---------------------------------------------------------------------------

/// Sleep for `d` inside a goroutine.
///
/// Exposed as a standalone function so `select!` can handle timeouts.
///
/// # Safety
/// Must be called from a goroutine stack.
pub unsafe fn goroutine_sleep(d: Duration) {
    unsafe { sleep(d) };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
#[allow(unused_unsafe)] // closures calling unsafe fn inside an outer unsafe{} block
mod tests {
    use super::*;
    use crate::runtime::sched::run_impl;
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
    fn fire_expired_wakes_goroutine() {
        use std::time::Instant;
        run_impl(|| {
            let t0 = Instant::now();
            unsafe { sleep(Duration::from_millis(10)) };
            let elapsed = t0.elapsed();
            assert!(
                elapsed >= Duration::from_millis(8),
                "timer did not fire: elapsed only {:?}", elapsed
            );
        });
    }

    /// sleep(0) yields without blocking.
    #[test]
    fn sleep_zero_yields() {
        run_impl(|| {
            unsafe { sleep(Duration::ZERO) };
        });
    }

    /// sleep(short) completes within a reasonable wall-clock window.
    #[test]
    fn sleep_short_duration() {
        use crate::runtime::sched::spawn_goroutine;
        let done = Arc::new(AtomicI32::new(0));
        let done2 = Arc::clone(&done);

        run_impl(move || {
            unsafe {
                spawn_goroutine(move || {
                    unsafe { sleep(Duration::from_millis(5)) };
                    done2.store(1, Ordering::Relaxed);
                });
            }

            // Spin-yield for up to 200 ms waiting for the sleeper to finish.
            let deadline = Instant::now() + Duration::from_millis(200);
            loop {
                if done.load(Ordering::Acquire) == 1 { break; }
                if Instant::now() >= deadline {
                    panic!("sleep did not complete within 200 ms");
                }
                crate::gosched();
                std::thread::sleep(Duration::from_millis(1));
            }
        });
    }

    /// Multiple goroutines sleeping concurrently all wake.
    #[test]
    fn concurrent_sleepers() {
        use crate::runtime::sched::spawn_goroutine;
        const N: i32 = 4;
        let awoke = Arc::new(AtomicI32::new(0));
        let awoke2 = Arc::clone(&awoke);

        run_impl(move || {
            for _ in 0..N {
                let awoke3 = Arc::clone(&awoke2);
                unsafe {
                    spawn_goroutine(move || {
                        unsafe { sleep(Duration::from_millis(10)) };
                        awoke3.fetch_add(1, Ordering::Relaxed);
                    });
                }
            }

            let deadline = Instant::now() + Duration::from_millis(500);
            loop {
                if awoke2.load(Ordering::Acquire) == N { break; }
                if Instant::now() >= deadline {
                    panic!("not all sleepers woke within 500 ms");
                }
                crate::gosched();
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        assert_eq!(awoke.load(Ordering::Acquire), N);
    }
}
