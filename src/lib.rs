//! # go-lib
//!
//! Go-style concurrency for Rust: goroutines, channels, `select`, `WaitGroup` —
//! built on a port of the M:N scheduler from <https://github.com/golang/go>.
//!
//! No async runtime is used: the scheduler, channels, and parking primitives
//! are ported from `src/runtime/` in the Go repo. Mutexes and read-write locks
//! are taken straight from [`std::sync`] because their uncontended path is
//! just an atomic CAS — porting Go's versions would be code without benefit.
//! See [`runtime::syscall`] for the shim that keeps `std` blocking calls
//! scheduler-safe.
//!
//! ## Public surface
//! - `go!` / `select!` macros — spawn goroutines, multiplex channel ops
//! - [`chan`] — buffered and unbuffered channels
//! - [`sync::WaitGroup`] — wait for a collection of goroutines
//! - [`sync::Cond`] — goroutine-aware condition variable
//! - [`sync::Mutex`] / [`sync::RwLock`] — re-exports of `std::sync`
//! - [`context`] — cancellation and deadline propagation
//! - [`set_panic_handler`] — customise goroutine-panic behaviour
//! - [`set_gomaxprocs`] / [`gomaxprocs`] — runtime parallelism control
//!
//! ## Internals
//! See [`runtime`] for the scheduler (G/M/P, parking, work stealing, sysmon).
//!
//! ## Known limitations (v1 — deferred to v2+)
//!
//! These items are intentionally out of scope for v1.  Each is a real
//! limitation; work-arounds are noted where applicable.
//!
//! ### Stack growth
//! Every goroutine is allocated a **fixed 64 KiB stack** backed by an
//! `mmap`'d region with a single guard page.  Go's `morestack` / `copystack`
//! stack-growth mechanism is not ported.  Deep recursion or large stack frames
//! will overflow into the guard page and segfault.
//!
//! *Work-around*: keep goroutine call stacks shallow; move large buffers to the
//! heap (use `Box` / `Vec`).
//!
//! ### Async (signal-based) preemption
//! v1 is **cooperative only**.  The sysmon thread sets a preemption hint after
//! 10 ms of wall time, but because there are no stack-check traps a goroutine
//! will not be preempted until it calls [`gosched`] (or blocks on a channel /
//! sleep).
//!
//! *Work-around*: sprinkle `gosched()` inside CPU-bound loops.
//!
//! ### `defer` / `recover` / cross-goroutine `panic`
//! Goroutine panics are caught and routed to [`set_panic_handler`]; the
//! process does not abort.  Go's `recover()` (stopping panic propagation at a
//! call-stack boundary) has no direct Rust equivalent — use `catch_unwind`
//! inside the goroutine body when fine-grained recovery is needed.
//!
//! ### Netpoll / I/O integration
//! There is no integration with the OS event loop (`epoll`/`kqueue`).
//! Goroutines that call blocking I/O should wrap the call with
//! [`with_syscall`] so the scheduler can hand off the P during the wait.
//!
//! ### Race detector
//! The Go race detector is a compiler/runtime feature with no Rust equivalent
//! in this crate.  Use `cargo test --release` with `loom` (separate crate) for
//! concurrency model checking.
//!
//! ## Unsafe conventions
//! The runtime modules (`src/runtime/`) are a direct port of Go's C-adjacent
//! runtime code.  Almost every function is `unsafe fn` because it operates on
//! raw goroutine pointers and `mmap`'d memory.  Inner `unsafe {}` blocks are
//! omitted for brevity (suppressed via `unsafe_op_in_unsafe_fn`) — the caller's
//! obligation is documented in each function's `# Safety` section instead.
#![deny(missing_docs)]
// The runtime is a deliberate port of Go's low-level C-adjacent scheduler code.
// Virtually every function is `unsafe fn`; requiring inner `unsafe {}` blocks
// on every raw-pointer dereference would add noise without safety information.
// Each `unsafe fn`'s contract is documented in its `# Safety` section instead.
#![allow(unsafe_op_in_unsafe_fn)]

pub mod chan;
pub mod context;
pub mod runtime;
pub mod select;
pub mod sync;

mod go_macro;

/// Initialise the go-lib scheduler and run `f` as the first goroutine.
///
/// Blocks the calling thread until `f` returns.  The scheduler threads
/// (one per logical CPU) continue running in the background after `run`
/// returns; they park themselves when there is no more work.
///
/// # Example
///
/// ```no_run
/// go_lib::run(|| {
///     println!("hello from a goroutine");
/// });
/// ```
pub fn run<F: FnOnce() + Send + 'static>(f: F) {
    runtime::sched::run_impl(f);
}

/// Yield the CPU, giving other goroutines a chance to run.
///
/// Moves the current goroutine to the back of the global run queue and
/// re-enters the scheduler.  Execution resumes at the next `gosched()` call
/// site once the goroutine is rescheduled.
///
/// CPU-bound loops should call `gosched()` periodically.  The background
/// sysmon thread also sets a preemption hint after 10 ms, but because v1 has
/// no stack-check traps the goroutine must call `gosched()` voluntarily for
/// the hint to take effect.
///
/// # Panics
///
/// Panics if called from outside a goroutine (e.g. from `main` before
/// calling [`run`]).
///
/// # Example
///
/// ```no_run
/// go_lib::run(|| {
///     for i in 0..1_000_000 {
///         if i % 10_000 == 0 {
///             go_lib::gosched(); // let other goroutines run
///         }
///     }
/// });
/// ```
pub fn gosched() {
    // SAFETY: we are on a goroutine stack (enforced by the debug_assert inside
    // the internal gosched that current_g() is non-null).
    unsafe { runtime::sched::gosched() }
}

/// Wrap a potentially-blocking operation so the go-lib scheduler can
/// hand off this goroutine's P to another M while the OS thread is in the
/// kernel.
///
/// Calls [`entersyscall`][runtime::syscall::entersyscall] before `f` and
/// [`exitsyscall`][runtime::syscall::exitsyscall] after `f` returns.  This is
/// a no-op when called outside the scheduler (before [`run`]).
///
/// # Example
///
/// ```no_run
/// go_lib::run(|| {
///     let data = go_lib::with_syscall(|| std::fs::read("file.txt"));
/// });
/// ```
pub fn with_syscall<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    runtime::syscall::with_syscall(f)
}

/// Sleep the current goroutine for at least `d`.
///
/// Parks the goroutine and lets other goroutines run; the background timer
/// thread calls [`goready`][runtime::park] when the duration elapses.
///
/// Passing `Duration::ZERO` yields to the scheduler without sleeping.
///
/// # Panics
///
/// Debug-panics if called from outside a goroutine.
///
/// # Example
///
/// ```no_run
/// go_lib::run(|| {
///     go_lib::sleep(std::time::Duration::from_millis(10));
/// });
/// ```
pub fn sleep(d: std::time::Duration) {
    // SAFETY: called from a goroutine context (checked by debug_assert in sleep).
    unsafe { runtime::time::goroutine_sleep(d) }
}

/// Spawn a goroutine.  Called by the [`go!`] macro; not for direct use.
///
/// Must be called from within a running goroutine (i.e. inside [`run`]).
///
/// # Panics
///
/// Debug-panics if called from outside a goroutine context.
#[doc(hidden)]
pub fn __spawn<F: FnOnce() + Send + 'static>(f: F) {
    // SAFETY: callers are expected to be inside a goroutine context.
    unsafe { runtime::sched::spawn_goroutine(f) }
}

// ---------------------------------------------------------------------------
// GOMAXPROCS
// ---------------------------------------------------------------------------

/// Return the current number of logical processors (GOMAXPROCS).
///
/// This equals the value set by the `GOMAXPROCS` environment variable at
/// startup, or [`set_gomaxprocs`], or `available_parallelism` if neither was
/// provided.
pub fn gomaxprocs() -> usize {
    runtime::sched::gomaxprocs()
}

/// Set the number of logical processors and return the previous value.
///
/// See [`runtime::sched::set_gomaxprocs`] for full semantics.
///
/// # Example
///
/// ```no_run
/// let old = go_lib::set_gomaxprocs(2);
/// println!("was {old}, now {}", go_lib::gomaxprocs());
/// ```
pub fn set_gomaxprocs(n: usize) -> usize {
    runtime::sched::set_gomaxprocs(n)
}

// ---------------------------------------------------------------------------
// Goroutine panic handler
// ---------------------------------------------------------------------------

/// Register a custom handler for goroutine panics.
///
/// By default, a panicking goroutine prints its payload to stderr and the
/// scheduler continues running other goroutines — the process does **not**
/// abort.
///
/// Calling `set_panic_handler` replaces the previous handler.  The handler
/// receives the `Box<dyn Any + Send>` payload from `std::panic::catch_unwind`.
///
/// # Example
///
/// ```no_run
/// go_lib::set_panic_handler(|payload| {
///     if let Some(s) = payload.downcast_ref::<String>() {
///         eprintln!("goroutine panicked: {s}");
///     }
/// });
/// ```
pub fn set_panic_handler<F>(f: F)
where
    F: Fn(Box<dyn std::any::Any + Send + 'static>) + Send + Sync + 'static,
{
    runtime::sched::set_panic_handler(f);
}
