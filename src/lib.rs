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
//! - [`sync::Mutex`] / [`sync::RwLock`] — re-exports of `std::sync`
//!
//! ## Internals
//! See [`runtime`] for the scheduler (G/M/P, parking, work stealing, sysmon).
#![deny(missing_docs)]

pub mod chan;
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
