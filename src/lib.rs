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
