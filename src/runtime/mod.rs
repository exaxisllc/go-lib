//! Goroutine scheduler internals.
//!
//! Ported from `src/runtime/` in <https://github.com/golang/go>. The mapping
//! is one-to-one where possible:
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

pub(crate) mod g;
pub(crate) mod m;
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
#[cfg(target_arch = "x86_64")]
pub(crate) use asm_amd64::{gogo, mcall, systemstack};
