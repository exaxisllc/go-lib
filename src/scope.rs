// SPDX-License-Identifier: Apache-2.0
//! Scoped goroutines — safe short-lived borrows.
//!
//! [`scope`] is the goroutine equivalent of [`std::thread::scope`]: every
//! goroutine spawned through the [`Scope`] handle is guaranteed to finish
//! before `scope` returns, so the closures may safely borrow data from the
//! calling goroutine's stack frame without a `'static` bound.
//!
//! # How the lifetime safety works
//!
//! The two lifetime parameters on [`Scope<'scope, 'env>`] encode the
//! invariant:
//!
//! - **`'env`** — the lifetime of data that goroutines are allowed to borrow
//!   (e.g. a `&Vec<i32>` from the surrounding function).
//! - **`'scope`** — the lifetime of the [`Scope`] reference itself; it lasts
//!   exactly as long as the closure passed to [`scope`] runs.
//! - The bound `'env: 'scope` ensures that borrowed data outlives the scope.
//!
//! Inside [`Scope::spawn`] the closure's `'scope` lifetime is erased to
//! `'static` (via `transmute`) so it can be handed to the scheduler.  This is
//! sound because [`scope`] blocks (via `WaitGroup::wait`) until every spawned
//! goroutine has called `wg.done()`, which happens only after the closure
//! returns or panics — so no goroutine can outlive `'scope`.
//!
//! # Panic behaviour
//!
//! go-lib goroutines are scheduled M:N across OS threads.  Rust's panic
//! unwinding relies on C++ exception-handling (EH) machinery whose landing
//! pads are registered per-OS-thread.  Calling `std::panic::resume_unwind`
//! after a goroutine has been parked and resumed (potentially on a different
//! OS thread) would silently bypass the inner landing pad and reach the outer
//! `goroutine_entry` catch — crashing.
//!
//! Therefore:
//! - [`ScopedJoinHandle::join`] returns `std::thread::Result<R>` rather than
//!   `R`, so the caller decides how to handle a scoped goroutine's panic
//!   without crossing any scheduling boundary.
//! - If the **outer** scope closure itself panics, `scope` catches it, waits
//!   for all goroutines, and then re-raises it with
//!   [`std::panic::panic_any`] — which starts a fresh panic on the current
//!   OS thread, always finding the correct landing pad.
//!
//! # Example
//!
//! ```no_run
//! go_lib::run(|| {
//!     let data = vec![1_i64, 2, 3, 4, 5];
//!
//!     let sum = go_lib::scope(|s| {
//!         let h1 = s.spawn(|| data[..3].iter().sum::<i64>());
//!         let h2 = s.spawn(|| data[3..].iter().sum::<i64>());
//!         h1.join().unwrap() + h2.join().unwrap()
//!     });
//!
//!     assert_eq!(sum, 15);
//! });
//! ```

use std::marker::PhantomData;
use std::sync::Arc;

use crate::sync::WaitGroup;

// ---------------------------------------------------------------------------
// Internal shared state
// ---------------------------------------------------------------------------

struct ScopeData {
    /// Starts at 0; each `spawn` call increments by 1.  Each goroutine
    /// decrements by 1 when it exits.  [`scope`] waits for this to reach 0.
    wg: WaitGroup,
}

// ---------------------------------------------------------------------------
// Scope — the handle given to the user's closure
// ---------------------------------------------------------------------------

/// A scope for spawning goroutines with bounded lifetimes.
///
/// Obtained by calling [`scope`].  Every goroutine spawned via
/// [`Scope::spawn`] is guaranteed to finish before [`scope`] returns, which
/// allows closures to borrow data with lifetime `'env` without requiring
/// `'static`.
///
/// The type is **invariant** over both `'scope` and `'env` to prevent the
/// compiler from shrinking either lifetime in ways that could unsafely extend
/// a goroutine's ability to access stack data.
pub struct Scope<'scope, 'env: 'scope> {
    data: Arc<ScopeData>,
    /// Invariance over both lifetimes.
    _marker: PhantomData<&'scope mut &'env ()>,
}

// ---------------------------------------------------------------------------
// ScopedJoinHandle — optional per-goroutine result retrieval
// ---------------------------------------------------------------------------

/// A handle to a goroutine spawned inside a [`Scope`].
///
/// Call [`join`][Self::join] to park the current goroutine until the scoped
/// goroutine finishes and retrieve its result.
///
/// Dropping the handle without joining is safe — the goroutine still runs to
/// completion (the enclosing [`scope`] guarantees this).  Any return value
/// or panic from an un-joined goroutine is silently discarded.
///
/// # Why `join` returns `Result` instead of the value directly
///
/// go-lib goroutines are M:N scheduled; they can migrate between OS threads.
/// Rust's `resume_unwind` depends on C++ EH machinery that is bound to the
/// OS thread on which `catch_unwind` was called.  Calling `resume_unwind`
/// after a goroutine has parked and been rescheduled on a different thread
/// bypasses the inner landing pad and produces undefined behaviour.  Returning
/// `std::thread::Result<R>` lets the *caller* choose what to do — typically
/// `.unwrap()` or matching on the payload — without crossing any scheduling
/// boundary.
pub struct ScopedJoinHandle<'scope, R> {
    /// One-shot channel: the goroutine sends exactly one `Result`.
    rx: crate::chan::Receiver<std::thread::Result<R>>,
    /// Ties the handle to the scope so it cannot be sent outside it.
    _marker: PhantomData<&'scope ()>,
}

impl<'scope, R: Send + 'static> ScopedJoinHandle<'scope, R> {
    /// Park the current goroutine until the scoped goroutine finishes.
    ///
    /// Returns `Ok(value)` if the goroutine returned normally, or
    /// `Err(panic_payload)` if it panicked.
    ///
    /// To propagate the panic as-is call
    /// [`std::panic::resume_unwind`]`(err)` **from outside any goroutine
    /// scheduling boundary** — i.e., directly in the scope closure without
    /// any intervening channel/wait operations.  For the common case, `.unwrap()`
    /// is usually simpler.
    pub fn join(self) -> std::thread::Result<R> {
        self.rx.recv().expect("scoped goroutine result channel closed unexpectedly")
    }
}

// ---------------------------------------------------------------------------
// Scope::spawn
// ---------------------------------------------------------------------------

impl<'scope, 'env: 'scope> Scope<'scope, 'env> {
    /// Spawn a goroutine in this scope.
    ///
    /// The closure may borrow any data with lifetime `'env` or longer — i.e.
    /// anything that was alive when [`scope`] was called.
    ///
    /// Returns a [`ScopedJoinHandle`] for optional early joining.  If you do
    /// not need the return value, simply drop the handle; the goroutine still
    /// runs to completion before the surrounding [`scope`] returns.
    pub fn spawn<F, R>(&'scope self, f: F) -> ScopedJoinHandle<'scope, R>
    where
        F: FnOnce() -> R + Send + 'scope,
        R: Send + 'static,
    {
        self.data.wg.add(1);
        let data = Arc::clone(&self.data);

        // One-shot buffered channel (capacity 1) — the goroutine sends its
        // result here; `join()` (or the implicit scope wait) receives it.
        let (tx, rx) = crate::chan::chan::<std::thread::Result<R>>(1);

        // Erase `'scope` → `'static` so `spawn_goroutine` accepts the closure.
        //
        // SAFETY: `scope` calls `data.wg.wait()` before it returns, which
        // parks until every goroutine has called `data.wg.done()`.  That
        // call happens only after the closure `f` has returned (or panicked),
        // so no goroutine can observe data past `'scope`.  The transmute is
        // therefore sound: we extend the *apparent* lifetime, but the runtime
        // invariant ensures the actual data remains valid for the goroutine's
        // entire lifetime.
        let f: Box<dyn FnOnce() -> R + Send + 'scope> = Box::new(f);
        let f: Box<dyn FnOnce() -> R + Send + 'static> =
            unsafe { std::mem::transmute(f) };

        crate::runtime::sched::spawn_goroutine(move || {
            // Catch panics so we can (a) forward them through the channel to
            // `join()` and (b) always call `wg.done()` even on panic.
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));

            // Send result *before* done() so that a concurrent `join()` that
            // wakes up immediately after done() always sees the value in the
            // channel buffer.
            tx.send(result);
            drop(tx);
            data.wg.done();
        });

        ScopedJoinHandle { rx, _marker: PhantomData }
    }
}

// ---------------------------------------------------------------------------
// scope — public entry point
// ---------------------------------------------------------------------------

/// Run a closure that can spawn short-lived goroutines borrowing local data.
///
/// `scope` is the goroutine equivalent of [`std::thread::scope`].  The
/// closure receives a [`&Scope`][Scope] handle; goroutines spawned via
/// [`Scope::spawn`] may borrow any data that is alive in the caller's
/// environment (`'env`).  All spawned goroutines are guaranteed to finish
/// before `scope` returns.
///
/// The return value of the outer closure is propagated to the caller.
///
/// # Scheduling
///
/// The `wg.wait()` at the end of `scope` uses goroutine-level parking: the
/// calling goroutine yields to the scheduler (the M and P remain free to run
/// other goroutines, including the scoped ones).  No OS thread is blocked.
///
/// # Panics in the outer closure
///
/// If the closure passed to `scope` panics, `scope` still waits for every
/// already-spawned goroutine to finish.  The panic is then re-raised via
/// [`std::panic::panic_any`] on the current OS thread — ensuring the correct
/// landing pad is found even if the goroutine was rescheduled during
/// `wg.wait()`.  Note: this causes the panic hook to fire a second time;
/// the message will appear in stderr, but the panic itself behaves correctly.
///
/// # Panics in a scoped goroutine
///
/// A goroutine panic is delivered to [`ScopedJoinHandle::join`] as `Err(payload)`.
/// If the handle is dropped without joining, the panic payload is silently
/// discarded.
///
/// # Example — parallel slice reduction
///
/// ```no_run
/// go_lib::run(|| {
///     let data = vec![1_i64, 2, 3, 4, 5];
///
///     let sum = go_lib::scope(|s| {
///         let h1 = s.spawn(|| data[..3].iter().sum::<i64>());
///         let h2 = s.spawn(|| data[3..].iter().sum::<i64>());
///         h1.join().unwrap() + h2.join().unwrap()
///     });
///
///     assert_eq!(sum, 15);
/// });
/// ```
///
/// # Example — fire-and-forget goroutines with shared stack state
///
/// ```no_run
/// use std::sync::atomic::{AtomicI32, Ordering};
///
/// go_lib::run(|| {
///     let counter = std::sync::atomic::AtomicI32::new(0);
///
///     go_lib::scope(|s| {
///         for _ in 0..8 {
///             s.spawn(|| { counter.fetch_add(1, Ordering::Relaxed); });
///         }
///         // scope blocks here until all 8 goroutines have finished
///     });
///
///     assert_eq!(counter.load(Ordering::SeqCst), 8);
/// });
/// ```
pub fn scope<'env, F, R>(f: F) -> R
where
    F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> R,
{
    let data = Arc::new(ScopeData { wg: WaitGroup::new() });

    let scope_obj = Scope {
        data: Arc::clone(&data),
        _marker: PhantomData,
    };

    // Run the user closure, catching any panic so we can still wait for
    // goroutines before propagating it.
    let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&scope_obj)));

    // Block (goroutine-park) until every spawned goroutine has exited.
    // This is the key invariant that makes the lifetime transmute in
    // Scope::spawn sound.
    data.wg.wait();

    match result {
        Ok(v) => v,
        // Re-panic using `panic_any` rather than `resume_unwind`.
        //
        // `resume_unwind` continues an existing C++ unwind which may have been
        // initiated on a different OS thread (before a gopark/gogo cycle in
        // `wg.wait`).  Continuing such an unwind on a different thread bypasses
        // the inner catch_unwind landing pad and causes a SIGSEGV.
        //
        // `panic_any` starts a *new* panic on the *current* OS thread, which
        // always finds the correct nearest landing pad.  The trade-off is that
        // the panic hook fires again (double stderr output), but correctness is
        // more important.
        Err(payload) => std::panic::panic_any(payload),
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

    /// Basic borrow: goroutines read a local Vec without cloning it.
    #[test]
    fn borrow_local_data() {
        run_impl(|| {
            let data = [1_i64, 2, 3, 4, 5];

            let sum = scope(|s| {
                let h1 = s.spawn(|| data[..3].iter().sum::<i64>());
                let h2 = s.spawn(|| data[3..].iter().sum::<i64>());
                h1.join().unwrap() + h2.join().unwrap()
            });

            assert_eq!(sum, 15);
            // `data` is still accessible here — scope guarantees it lives long enough.
            assert_eq!(data.len(), 5);
        });
    }

    /// Fire-and-forget: drop all handles; scope still waits for goroutines.
    #[test]
    fn fire_and_forget() {
        run_impl(|| {
            let counter = AtomicI32::new(0);

            scope(|s| {
                for _ in 0..8 {
                    s.spawn(|| { counter.fetch_add(1, Ordering::Relaxed); });
                }
                // All handles dropped here; scope will wait for all goroutines.
            });

            assert_eq!(counter.load(Ordering::SeqCst), 8);
        });
    }

    /// Panic in a scoped goroutine is retrievable via join() as Err.
    /// Panic in a scoped goroutine is retrievable via join() as Err.
    #[test]
    fn goroutine_panic_via_join() {
        run_impl(|| {
            let result = scope(|s| {
                let h = s.spawn(|| -> i32 { panic!("scoped panic") });
                // join() returns Result — no resume_unwind across scheduling boundaries
                h.join()
            });
            assert!(result.is_err(), "expected Err from a panicking goroutine");
            let payload = result.unwrap_err();
            let msg = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str));
            assert_eq!(msg, Some("scoped panic"), "panic payload should be 'scoped panic'");
        });
    }

    /// Panic in a scoped goroutine whose handle is dropped is silently discarded.
    #[test]
    fn goroutine_panic_dropped_handle() {
        run_impl(|| {
            // Should not propagate — the handle is dropped without join().
            scope(|s| {
                let _h = s.spawn(|| -> i32 { panic!("silent panic") });
                // _h dropped here
            });
            // scope returns normally
        });
    }

    /// Nested scopes work correctly.
    #[test]
    fn nested_scopes() {
        run_impl(|| {
            let outer = [10_i64, 20, 30];

            let total = scope(|s_outer| {
                let h = s_outer.spawn(|| {
                    // Inner scope borrows both `outer` and its own locals.
                    let inner = [1_i64, 2, 3];
                    scope(|s_inner| {
                        let h1 = s_inner.spawn(|| inner.iter().sum::<i64>());
                        let h2 = s_inner.spawn(|| outer.iter().sum::<i64>());
                        h1.join().unwrap() + h2.join().unwrap()
                    })
                });
                h.join().unwrap()
            });

            // inner.sum = 6, outer.sum = 60
            assert_eq!(total, 66);
        });
    }

    /// scope propagates a panic from the outer closure after waiting for goroutines.
    #[test]
    fn outer_closure_panic_waits_for_goroutines() {
        run_impl(|| {
            let finished = AtomicI32::new(0);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                scope(|s| {
                    s.spawn(|| { finished.fetch_add(1, Ordering::Relaxed); });
                    panic!("outer panic");
                    #[allow(unreachable_code)]
                    42_i32
                })
            }));
            assert!(result.is_err());
            // The goroutine must have finished despite the outer panic.
            assert_eq!(finished.load(Ordering::SeqCst), 1);
        });
    }
}
