// SPDX-License-Identifier: Apache-2.0
//! Cancellation and deadline propagation — equivalent to Go's `context` package.
//!
//! ## Quick start
//!
//! ```no_run
//! use go_lib::context;
//! use std::time::Duration;
//!
//! go_lib::run(|| {
//!     // Root context — never cancels on its own.
//!     let bg = context::background();
//!
//!     // Child with explicit cancel.
//!     let (ctx, cancel) = context::with_cancel(&bg);
//!
//!     go_lib::go!(move || {
//!         // Worker loops until the context is done.
//!         loop {
//!             go_lib::select! {
//!                 recv(ctx.done()) -> _v => { break }
//!                 default => { /* do work */ go_lib::gosched(); }
//!             }
//!         }
//!     });
//!
//!     go_lib::sleep(Duration::from_millis(10));
//!     cancel.cancel(); // signal the worker to stop
//! });
//! ```
//!
//! ## Design
//!
//! Each `Context` is a thin `Arc` wrapper around a `ContextInner` that holds:
//!
//! - An optional `deadline: Instant`.
//! - A `done` channel (`Receiver<()>`) that fires (returns `None`) when the
//!   context is cancelled or its deadline elapses.
//! - A `children` list so cancellation propagates from parent to child.
//!
//! Cancellation closes the done channel by dropping its internal `Sender<()>`.
//! Closed channels return `None` from `recv()`, which fires any `select!` arm
//! that waits on them — the standard Go done-channel idiom.
//!
//! ## Requirements
//!
//! `with_deadline` / `with_timeout` spawn a timer goroutine and therefore
//! require the go-lib scheduler to be running (i.e. called from inside
//! [`go_lib::run`]).  `background()` and `with_cancel()` are safe to call
//! from anywhere.

use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use crate::chan::{chan, Receiver, Sender};

// ---------------------------------------------------------------------------
// ContextError
// ---------------------------------------------------------------------------

/// Why a context was cancelled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextError {
    /// The context was cancelled explicitly via [`CancelFn::cancel`].
    Cancelled,
    /// The context's deadline elapsed.
    DeadlineExceeded,
}

impl std::fmt::Display for ContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled        => f.write_str("context cancelled"),
            Self::DeadlineExceeded => f.write_str("context deadline exceeded"),
        }
    }
}

// ---------------------------------------------------------------------------
// ContextInner — shared state
// ---------------------------------------------------------------------------

struct ContextInner {
    deadline: Option<Instant>,
    /// Sender kept alive to hold the done channel open.  Dropped (closing the
    /// channel) when the context is cancelled.
    done_tx:  Mutex<Option<Sender<()>>>,
    done_rx:  Receiver<()>,
    err:      Mutex<Option<ContextError>>,
    children: Mutex<Vec<Weak<ContextInner>>>,
}

impl ContextInner {
    /// Cancel this context with `err` and propagate to all children.
    /// Idempotent — subsequent calls are no-ops.
    fn cancel(&self, err: ContextError) {
        // Fast path: already cancelled.
        {
            let mut e = self.err.lock().unwrap();
            if e.is_some() { return; }
            *e = Some(err.clone());
        }

        // Close the done channel by dropping the sender.
        if let Some(tx) = self.done_tx.lock().unwrap().take() {
            tx.close();
        }

        // Propagate to children.
        let children: Vec<Weak<ContextInner>> =
            self.children.lock().unwrap().drain(..).collect();
        for weak in children {
            if let Some(child) = weak.upgrade() {
                child.cancel(err.clone());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Context — public handle
// ---------------------------------------------------------------------------

/// A context value carrying a cancellation signal and optional deadline.
///
/// Cheap to clone — backed by `Arc`.
#[derive(Clone)]
pub struct Context(Arc<ContextInner>);

impl Context {
    /// A receiver that fires (returns `None`) when this context is cancelled or
    /// its deadline elapses.  Use it in `select!`:
    ///
    /// ```no_run
    /// # use go_lib::context;
    /// # let (ctx, _cancel) = context::with_cancel(&context::background());
    /// go_lib::select! {
    ///     recv(ctx.done()) -> _v => { /* cancelled */ }
    ///     default              => { /* still running */ }
    /// }
    /// ```
    pub fn done(&self) -> &Receiver<()> {
        &self.0.done_rx
    }

    /// The deadline of this context, or `None` for contexts without one.
    pub fn deadline(&self) -> Option<Instant> {
        self.0.deadline
    }

    /// The cancellation error, or `None` if the context is still active.
    pub fn err(&self) -> Option<ContextError> {
        self.0.err.lock().unwrap().clone()
    }

    /// `true` if this context has been cancelled or its deadline has elapsed.
    pub fn is_done(&self) -> bool {
        self.err().is_some()
    }
}

// ---------------------------------------------------------------------------
// CancelFn — the function returned by with_cancel / with_deadline
// ---------------------------------------------------------------------------

/// Cancels the associated [`Context`] when called.
///
/// Cloneable; multiple holders can all call `cancel()` — only the first call
/// takes effect.
#[derive(Clone)]
pub struct CancelFn(Arc<ContextInner>);

impl CancelFn {
    /// Cancel the context.  Idempotent; safe to call multiple times.
    pub fn cancel(&self) {
        self.0.cancel(ContextError::Cancelled);
    }
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

/// Return a background context: it is never cancelled and has no deadline.
///
/// Use this as the root from which to derive child contexts.
pub fn background() -> Context {
    // The sender is kept inside the ContextInner; the channel stays open until
    // the Context is dropped, at which point nobody should be waiting on it.
    let (done_tx, done_rx) = chan::<()>(0);
    Context(Arc::new(ContextInner {
        deadline: None,
        done_tx:  Mutex::new(Some(done_tx)),
        done_rx,
        err:      Mutex::new(None),
        children: Mutex::new(Vec::new()),
    }))
}

/// Return a child context and a cancel function.
///
/// Calling `cancel.cancel()` (or dropping the last clone of it) cancels the
/// returned `Context` and all of its descendants.  Cancellation also fires if
/// the parent is cancelled first.
pub fn with_cancel(parent: &Context) -> (Context, CancelFn) {
    let (ctx, cancel) = make_child(parent, None);
    (ctx, cancel)
}

/// Return a child context that is automatically cancelled at `deadline`.
///
/// Also returns a `CancelFn` for early cancellation.
///
/// # Requirements
///
/// Must be called from within a goroutine (inside `go_lib::run`) because it
/// spawns a timer goroutine.
pub fn with_deadline(parent: &Context, deadline: Instant) -> (Context, CancelFn) {
    let (ctx, cancel) = make_child(parent, Some(deadline));

    // Spawn a goroutine that sleeps until the deadline then cancels.
    let cancel_dl = cancel.clone();
    let now = Instant::now();
    if deadline <= now {
        // Already past the deadline — cancel immediately.
        cancel_dl.0.cancel(ContextError::DeadlineExceeded);
    } else {
        let d = deadline.duration_since(now);
        let inner_weak = Arc::downgrade(&cancel_dl.0);
        // SAFETY: spawn_goroutine only requires the scheduler to be running.
        unsafe {
            crate::runtime::sched::spawn_goroutine(move || {
                crate::sleep(d);
                if let Some(inner) = inner_weak.upgrade() {
                    inner.cancel(ContextError::DeadlineExceeded);
                }
            });
        }
    }

    (ctx, cancel)
}

/// Return a child context that is automatically cancelled after `timeout`.
///
/// Sugar over [`with_deadline`].
///
/// # Requirements
///
/// Same as `with_deadline` — must be called from within `go_lib::run`.
pub fn with_timeout(parent: &Context, timeout: Duration) -> (Context, CancelFn) {
    with_deadline(parent, Instant::now() + timeout)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Allocate a new child context and register it with `parent`.
fn make_child(parent: &Context, deadline: Option<Instant>) -> (Context, CancelFn) {
    let (done_tx, done_rx) = chan::<()>(0);
    let inner = Arc::new(ContextInner {
        deadline,
        done_tx:  Mutex::new(Some(done_tx)),
        done_rx,
        err:      Mutex::new(None),
        children: Mutex::new(Vec::new()),
    });

    let parent_inner = &parent.0;

    // Check parent cancellation under both locks to avoid a TOCTOU window.
    let parent_err = parent_inner.err.lock().unwrap().clone();
    if let Some(err) = parent_err {
        // Parent already cancelled — cancel child immediately.
        inner.cancel(err);
    } else {
        // Register child so parent cancellation propagates.
        parent_inner
            .children
            .lock()
            .unwrap()
            .push(Arc::downgrade(&inner));
    }

    let cancel_fn = CancelFn(Arc::clone(&inner));
    (Context(inner), cancel_fn)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::runtime::sched::run_impl;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// background() context is never done.
    #[test]
    fn background_not_done() {
        let bg = background();
        assert!(bg.err().is_none());
        assert!(!bg.is_done());
        assert!(bg.deadline().is_none());
    }

    /// with_cancel: cancelling sets err and closes done channel.
    #[test]
    fn with_cancel_cancels() {
        let bg = background();
        let (ctx, cancel) = with_cancel(&bg);
        assert!(!ctx.is_done());
        cancel.cancel();
        assert_eq!(ctx.err(), Some(ContextError::Cancelled));
    }

    /// with_cancel: idempotent — double cancel is safe.
    #[test]
    fn with_cancel_idempotent() {
        let bg = background();
        let (ctx, cancel) = with_cancel(&bg);
        cancel.cancel();
        cancel.cancel(); // must not panic
        assert_eq!(ctx.err(), Some(ContextError::Cancelled));
    }

    /// Parent cancellation propagates to child.
    #[test]
    fn cancel_propagates_to_child() {
        let bg = background();
        let (parent, parent_cancel) = with_cancel(&bg);
        let (child, _child_cancel)  = with_cancel(&parent);

        parent_cancel.cancel();
        assert_eq!(child.err(), Some(ContextError::Cancelled));
    }

    /// Child cancellation does not affect parent.
    #[test]
    fn child_cancel_does_not_affect_parent() {
        let bg = background();
        let (parent, _parent_cancel) = with_cancel(&bg);
        let (_child, child_cancel)   = with_cancel(&parent);

        child_cancel.cancel();
        assert!(parent.err().is_none(), "parent must not be cancelled by child");
    }

    /// Child of an already-cancelled parent is immediately cancelled.
    #[test]
    fn child_of_cancelled_parent_is_immediate() {
        let bg = background();
        let (parent, parent_cancel) = with_cancel(&bg);
        parent_cancel.cancel();

        // Create child after parent is already cancelled.
        let (child, _) = with_cancel(&parent);
        assert!(child.is_done(), "child must inherit parent's cancellation");
    }

    /// done() channel fires after cancel inside a goroutine.
    #[test]
    fn done_channel_fires_in_goroutine() {
        let fired = std::sync::Arc::new(AtomicBool::new(false));
        let fired2 = std::sync::Arc::clone(&fired);

        run_impl(move || {
            let bg = background();
            let (ctx, cancel) = with_cancel(&bg);

            unsafe {
                crate::runtime::sched::spawn_goroutine(move || {
                    ctx.done().recv(); // blocks until cancelled
                    fired2.store(true, Ordering::Release);
                });
            }

            // Let the goroutine park on the done channel.
            for _ in 0..20 { crate::gosched(); }
            cancel.cancel();

            // Wait for the goroutine to record the wakeup.
            let deadline = Instant::now() + Duration::from_millis(500);
            loop {
                if fired.load(Ordering::Acquire) { break; }
                assert!(Instant::now() < deadline, "done channel did not fire");
                crate::gosched();
            }
        });
    }

    /// with_timeout cancels after the given duration.
    #[test]
    fn with_timeout_cancels_after_duration() {
        run_impl(|| {
            let bg = background();
            let (ctx, _cancel) = with_timeout(&bg, Duration::from_millis(20));

            // Wait for the timeout to fire.
            ctx.done().recv(); // blocks until deadline exceeded
            assert_eq!(ctx.err(), Some(ContextError::DeadlineExceeded));
        });
    }

    /// with_deadline in the past cancels immediately.
    #[test]
    fn with_deadline_in_past_cancels_immediately() {
        run_impl(|| {
            let bg = background();
            let past = Instant::now() - Duration::from_secs(1);
            let (ctx, _cancel) = with_deadline(&bg, past);
            assert!(ctx.is_done(), "past deadline must cancel immediately");
        });
    }

    /// CancelFn is Clone and either clone can cancel.
    #[test]
    fn cancel_fn_clone_works() {
        let bg = background();
        let (ctx, cancel1) = with_cancel(&bg);
        let cancel2 = cancel1.clone();
        cancel2.cancel(); // cancel via the clone
        assert!(ctx.is_done());
    }
}
