// SPDX-License-Identifier: Apache-2.0
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
//! - [`net`] — goroutine-aware `TcpListener` / `TcpStream` *(v0.2.0)*
//! - [`scope`] / [`scope::Scope`] — scoped goroutines with safe short-lived borrows
//! - [`sync::WaitGroup`] — wait for a collection of goroutines
//! - [`sync::Cond`] — goroutine-aware condition variable
//! - [`sync::Mutex`] / [`sync::RwLock`] — re-exports of `std::sync`
//! - [`context`] — cancellation and deadline propagation
//! - [`set_panic_handler`] — customise goroutine-panic behaviour
//! - [`set_gomaxprocs`] / [`gomaxprocs`] — runtime parallelism control
//!
//! ## Internals
//! See [`runtime`] for the scheduler (G/M/P, parking, work stealing, sysmon,
//! stack growth, async preemption, netpoll).
//!
//! ## v0.4.0 — new in this release
//!
//! - **`scope` / `ScopedJoinHandle`**: scoped goroutines with safe short-lived
//!   borrows, mirroring `std::thread::scope`.  Goroutines spawned inside a
//!   `scope` closure can borrow data from the enclosing stack frame; the
//!   scheduler guarantees every spawned goroutine finishes before `scope`
//!   returns.  `ScopedJoinHandle::join()` returns `std::thread::Result<R>` so
//!   goroutine panics surface as `Err` rather than aborting the process.
//! - **Channel double-free fix**: the blocking-receive resume path in
//!   `chanrecv` used `ptr::read` followed by `Box::from_raw` on the same
//!   allocation, which double-dropped the inner value and caused use-after-free
//!   when the moved-out value was later inspected (e.g. a panic payload passed
//!   through a scoped join handle).  Fixed by casting the `Box` to
//!   `ManuallyDrop<Option<T>>` before dropping, so only the heap allocation is
//!   freed without re-running the destructor.
//!
//! ## v0.3.1 — new in this release
//!
//! - **G state machine**: `casgstatus` centralises all goroutine status
//!   transitions.  `GSYSCALL`, `GCOPYSTACK`, `GPREEMPTED`, and `GSCAN` are
//!   now wired into `entersyscall`/`exitsyscall`, `copystack`, and `preemptm`
//!   respectively, matching Go 1.14+ semantics.
//! - **`systemstack`**: runs a closure on the M's g0 (system) stack via a
//!   naked-assembly RSP/SP switch.  Implemented for both AMD64 (SysV + Windows
//!   x64) and AArch64 (AAPCS64).
//!
//! ## v0.2.0 — new in this release
//!
//! - **Dynamic stack growth** (Step 3): goroutines start with a 64 KiB stack
//!   and grow automatically up to 1 GiB via SIGSEGV guard-page detection and
//!   `copystack` (conservative pointer adjustment).
//! - **Async preemption** (Step 4): sysmon sends `SIGURG` to the M thread whose
//!   goroutine has run > 10 ms.  The signal handler redirects execution to an
//!   assembly trampoline that saves all registers, calls `async_preempt2`, and
//!   restores state on resume — a transparent, non-cooperative yield.
//! - **Netpoll / async I/O** (Step 5): `epoll` on Linux, `kqueue` on macOS,
//!   IOCP on Windows.  Goroutines park on blocking I/O and are re-enqueued
//!   when the operation is ready (Unix) or completes (Windows IOCP).
//!   See the [`net`] module for `TcpListener` / `TcpStream`.
//!
//! ## Known limitations
//!
//! ### `defer` / `recover` / cross-goroutine `panic`
//! Goroutine panics are caught and routed to [`set_panic_handler`]; the
//! process does not abort.  Go's `recover()` (stopping panic propagation at a
//! call-stack boundary) has no direct Rust equivalent — use `catch_unwind`
//! inside the goroutine body when fine-grained recovery is needed.
//!
//! ### Race detector
//! The Go race detector is a compiler/runtime feature with no Rust equivalent
//! in this crate.  Use `cargo test --cfg loom` with the [loom model checker]
//! for systematic concurrency testing.
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

/// Attribute macro that wraps a function body in [`run`].
///
/// ```rust,ignore
/// #[go_lib::run]
/// fn main() {
///     let (tx, rx) = go_lib::chan::chan::<i32>(0);
///     go_lib::go!(move || tx.send(42));
///     println!("{}", rx.recv().unwrap());
/// }
/// ```
///
/// See the [`go_lib_macros::run`][`main`] documentation for the full
/// expansion rules and return-type support.
pub use go_lib_macros::run;

pub mod chan;
pub mod context;
/// Goroutine-aware TCP networking (Step 5: netpoll integration).
///
/// See [`net::TcpListener`] and [`net::TcpStream`].
///
/// On Linux and macOS the backend is `epoll` / `kqueue` (readiness-based).
/// On Windows the backend is I/O Completion Ports (IOCP): overlapped
/// `WSARecv`/`WSASend` operations are issued and the goroutine parks until
/// `GetQueuedCompletionStatusEx` signals completion.
#[cfg(not(windows))]
pub mod net;
#[cfg(windows)]
#[path = "net_windows.rs"]
pub mod net;
pub mod runtime;
pub mod scope;
pub mod select;
pub mod sync;

mod go_macro;
pub(crate) mod loom_shim;

/// Initialise the go-lib scheduler, run `f` as the first goroutine, and
/// return whatever `f` returns.
///
/// Blocks the calling thread until `f` returns.  The scheduler threads
/// (one per logical CPU) continue running in the background after `run`
/// returns; they park themselves when there is no more work.
///
/// # Parameters
///
/// `f` can capture any values it needs from the surrounding scope via a
/// `move` closure — there is no need to pass parameters directly to `run`:
///
/// ```no_run
/// let base = 10_i32;
/// let result = go_lib::run(move || base * 2);
/// assert_eq!(result, 20);
/// ```
///
/// # Return values
///
/// The closure's return value is propagated back to the caller:
///
/// ```no_run
/// let sum = go_lib::run(|| {
///     let (tx, rx) = go_lib::chan::chan::<i32>(4);
///     for i in 1..=4 { let t = tx.clone(); go_lib::__spawn(move || t.send(i)); }
///     (0..4).filter_map(|_| rx.recv()).sum::<i32>()
/// });
/// assert_eq!(sum, 10);
/// ```
///
/// When the closure returns `()` (the default), `run` behaves exactly as
/// before — the return value can simply be ignored.
///
/// # Panics
///
/// Panics if `f` panics before producing a return value.  A panicking
/// goroutine is caught by the scheduler's `catch_unwind`; the panic payload
/// is forwarded to [`set_panic_handler`] and the calling thread is woken
/// with an `expect` failure.
pub fn run<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    runtime::sched::run_impl(f)
}

/// Spawn short-lived goroutines that can borrow data from the calling scope.
///
/// A thin re-export of [`scope::scope`] — see that module for full
/// documentation, examples, and the lifetime-safety argument.
///
/// # Quick example
///
/// ```no_run
/// go_lib::run(|| {
///     let data = vec![1_i64, 2, 3, 4, 5];
///
///     let sum = go_lib::scope(|s| {
///         let h1 = s.go(|| data[..3].iter().sum::<i64>());
///         let h2 = s.go(|| data[3..].iter().sum::<i64>());
///         h1.join().unwrap() + h2.join().unwrap()
///     });
///
///     assert_eq!(sum, 15);
/// });
/// ```
pub fn scope<'env, F, R>(f: F) -> R
where
    F: for<'scope> FnOnce(&'scope scope::Scope<'scope, 'env>) -> R,
{
    scope::scope(f)
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
    runtime::sched::spawn_goroutine(f)
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
