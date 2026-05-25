// SPDX-License-Identifier: Apache-2.0
//! Conditional synchronisation primitives — `std` in production, `loom` under
//! `cargo test --cfg loom`.
//!
//! Data structures that participate in loom model tests import their Mutex /
//! Condvar from this module.  When `--cfg loom` is **not** set, the re-exports
//! resolve to the standard-library types and have zero runtime overhead.
//!
//! ## Scope
//!
//! Only types that are both:
//! - Used in data structures with meaningful concurrent invariants, AND
//! - Compatible with loom's model (i.e. not entangled with raw goroutine stacks)
//!
//! …are included here.  Assembly-level primitives (`gopark`, `goready`, `gogo`,
//! `mcall`) are **outside** the loom boundary; loom cannot model stack switches.
//!
//! ## Why `all(loom, test)` and not just `loom`
//!
//! `loom` is a `[dev-dependencies]` entry, so it is only linked into test
//! binaries.  Library code compiled outside of `cargo test` (e.g. `cargo build
//! --cfg loom`) would fail to resolve the crate name.  By requiring `test` too
//! we ensure the loom re-exports are only activated in the test binary, where
//! dev-deps are available.  Normal tests run without `--cfg loom` and always
//! see the `std` path.
//!
//! ## Usage
//!
//! Replace `use std::sync::Mutex;` with `use crate::loom_shim::Mutex;` in any
//! module you want to make loom-testable.

// ---------------------------------------------------------------------------
// Mutex
// ---------------------------------------------------------------------------

/// A mutual-exclusion lock.  Maps to `loom::sync::Mutex` under
/// `cargo test --cfg loom`, `std::sync::Mutex` otherwise.
#[cfg(all(loom, test))]
pub(crate) use loom::sync::Mutex;
#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::Mutex;

// ---------------------------------------------------------------------------
// Condvar
// ---------------------------------------------------------------------------

/// A condition variable.  Maps to `loom::sync::Condvar` under
/// `cargo test --cfg loom`, `std::sync::Condvar` otherwise.
#[cfg(all(loom, test))]
pub(crate) use loom::sync::Condvar;
#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::Condvar;
