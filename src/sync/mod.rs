//! Synchronization primitives.
//!
//! [`Mutex`] and [`RwLock`] are re-exports of `std::sync`'s implementations:
//! the uncontended path is just an atomic CAS, and the contended path is
//! made scheduler-safe by wrapping `.lock()` with the M's syscall-handoff
//! shim (see [`crate::runtime::syscall`]). Porting Go's `sync.Mutex` plus
//! `runtime.sema` would add hundreds of lines for no measurable win.
//!
//! [`WaitGroup`] *is* ported, because the one-waiter / many-workers pattern
//! benefits from awareness of the scheduler.

pub use std::sync::{Mutex, RwLock};

mod waitgroup;
pub use waitgroup::WaitGroup;
