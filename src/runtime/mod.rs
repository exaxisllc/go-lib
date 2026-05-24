//! Goroutine scheduler internals.
//!
//! Ported from `src/runtime/` in <https://github.com/golang/go>. The mapping
//! is one-to-one where possible:
//!
//! Many constants, struct fields, and helper functions in this module are
//! faithful ports of Go runtime symbols that are not yet wired up in v1
//! (stack growth, async preemption, GC integration, …).  They are retained
//! intentionally so that future work can land incrementally without having to
//! re-discover or re-port them.  The `#[allow(dead_code)]` annotation below
//! suppresses the resulting unused-item warnings.
//!
//! | This module   | Go source                                            |
//! |---------------|------------------------------------------------------|
//! | `g`           | `runtime/runtime2.go`                                |
//! | `m`           | `runtime/runtime2.go`                                |
//! | `p`           | `runtime/runtime2.go`, `runtime/proc.go`             |
//! | `sched`       | `runtime/proc.go`                                    |
//! | `stack`       | `runtime/stack.go`                                   |
//! | `park`        | `runtime/proc.go` (gopark / goready)                 |
//! | `sudog`       | `runtime/runtime2.go`, `runtime/proc.go`             |
//! | `syscall`     | `runtime/proc.go` (entersyscall / exitsyscall)       |
//! | `sysmon`      | `runtime/proc.go` (sysmon)                           |
//! | `time`        | `runtime/time.go`                                    |
//! | `asm_amd64`   | `runtime/asm_amd64.s`                                |
//! | `asm_arm64`   | `runtime/asm_arm64.s`                                |

// Faithful Go-runtime ports contain symbols that will be used when deferred
// features land.  Suppress dead-code warnings across the whole runtime module.
#![allow(dead_code)]

pub(crate) mod g;
pub(crate) mod m;
pub(crate) mod rawmutex;
pub(crate) mod p;
pub(crate) mod park;
pub(crate) mod sched;
pub(crate) mod stack;
pub(crate) mod sudog;
pub(crate) mod syscall;
pub(crate) mod sysmon;
pub(crate) mod time;

#[cfg(target_arch = "x86_64")]
pub(crate) mod asm_amd64;
#[cfg(target_arch = "aarch64")]
pub(crate) mod asm_arm64;

// Re-export the three context-switch primitives from the correct asm module
// so the rest of the runtime uses `crate::runtime::{gogo, mcall, systemstack}`
// without caring about the target architecture.
#[cfg(target_arch = "aarch64")]
pub(crate) use asm_arm64::{gogo, mcall, systemstack};
