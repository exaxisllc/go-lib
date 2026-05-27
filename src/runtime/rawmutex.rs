// SPDX-License-Identifier: Apache-2.0
//! Adaptive spinlock used inside `Hchan<T>`.
//!
//! Unlike `std::sync::Mutex`, `RawMutex` has no guard — you call `lock()` and
//! `unlock()` directly.  This lets `selectgo` hold multiple locks of different
//! channel types simultaneously without needing typed `MutexGuard<T>` storage.
//!
//! ## Backoff strategy
//!
//! Spins up to 100 iterations (with `spin_loop` hints), then calls
//! `thread::yield_now()` to give the OS scheduler a chance to run the lock
//! holder.  Channel critical sections are very short so the spin almost always
//! wins without yielding.

use std::sync::atomic::{AtomicBool, Ordering};

// ---------------------------------------------------------------------------
// RawMutex
// ---------------------------------------------------------------------------

/// A non-recursive adaptive spinlock.
pub(crate) struct RawMutex {
    locked: AtomicBool,
}

impl RawMutex {
    /// Create an unlocked mutex (usable in `const` context).
    pub(crate) const fn new() -> Self {
        Self { locked: AtomicBool::new(false) }
    }

    /// Acquire the lock, spinning until available.
    ///
    /// Not re-entrant — calling `lock` while already holding it will spin
    /// forever (deadlock in the current thread).
    pub(crate) fn lock(&self) {
        let mut spins = 0u32;
        loop {
            if self
                .locked
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
            spins += 1;
            if spins < 100 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
                spins = 0;
            }
        }
    }

    /// Try to acquire the lock without spinning.
    ///
    /// Returns `true` if the lock was acquired, `false` if it was already
    /// held by someone else.
    #[allow(dead_code)] // used by sysmon fast-path lock; wired when sysmon gains trylock
    pub(crate) fn try_lock(&self) -> bool {
        self.locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// Release the lock.
    ///
    /// # Safety
    /// The caller must hold the lock.
    pub(crate) unsafe fn unlock(&self) {
        self.locked.store(false, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// LockGuard — RAII wrapper for panic safety
// ---------------------------------------------------------------------------

/// Releases a `RawMutex` when dropped.  Use inside functions that acquire a
/// channel lock so that a panic never leaves the lock permanently held.
pub(crate) struct LockGuard<'a>(pub &'a RawMutex);

impl Drop for LockGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: LockGuard is only constructed after successfully calling lock().
        unsafe { self.0.unlock() };
    }
}

impl<'a> LockGuard<'a> {
    /// Acquire `m` and return a guard that releases it on drop.
    pub(crate) fn new(m: &'a RawMutex) -> Self {
        m.lock();
        LockGuard(m)
    }
}
