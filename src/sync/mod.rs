// SPDX-License-Identifier: Apache-2.0
//! Synchronization primitives.
//!
//! [`Mutex`] and [`RwLock`] are re-exports of `std::sync`'s implementations:
//! the uncontended path is just an atomic CAS, and the contended path is
//! made scheduler-safe by wrapping `.lock()` with the M's syscall-handoff
//! shim (see [`crate::runtime::syscall`]). Porting Go's `sync.Mutex` plus
//! `runtime.sema` would add hundreds of lines for no measurable win.
//!
//! [`WaitGroup`] and [`Cond`] *are* ported because they benefit from
//! goroutine-level parking: waiters yield to the scheduler rather than
//! blocking an OS thread.

pub use std::sync::{Mutex, RwLock};

mod cond;
pub use cond::Cond;

mod waitgroup;
pub use waitgroup::WaitGroup;
