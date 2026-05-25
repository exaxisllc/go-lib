//! Goroutine scheduler internals.
//!
//! Ported from `src/runtime/` in <https://github.com/golang/go>.  Each submodule
//! maps to the Go source file(s) shown below.
//!
//! ## v2.0 additions
//!
//! | New module / symbol | Purpose |
//! |----|-----|
//! | `stack` — `newstack`, `copystack`, `sigsegv_handler` | Dynamic goroutine stack growth (Step 3) |
//! | `m` — `pthread_id`, `setup_sigaltstack` | Per-M thread ID + 64 KiB alternate signal stack (Step 4) |
//! | `sched` — `async_preempt2`, `sigurg_handler` | Non-cooperative goroutine preemption via SIGURG (Step 4) |
//! | `asm_amd64`/`asm_arm64` — `async_preempt_trampoline` | Save/restore all registers around the preemption yield (Step 4) |
//! | `sysmon` — `pthread_kill(SIGURG)` in `preemptone` | Signal delivery for async preemption (Step 4) |
//! | `netpoll` | epoll (Linux) / kqueue (macOS) fd-readiness backend (Step 5) |
//!
//! | This module   | Go source                                                   |
//! |---------------|-------------------------------------------------------------|
//! | `g`           | `runtime/runtime2.go`                                       |
//! | `m`           | `runtime/runtime2.go`, `runtime/proc.go`                    |
//! | `p`           | `runtime/runtime2.go`, `runtime/proc.go`                    |
//! | `sched`       | `runtime/proc.go`, `runtime/preempt.go`                     |
//! | `stack`       | `runtime/stack.go`, `runtime/signal_unix.go`                |
//! | `netpoll`     | `runtime/netpoll_epoll.go`, `runtime/netpoll_kqueue.go`     |
//! | `park`        | `runtime/proc.go` (gopark / goready)                        |
//! | `sudog`       | `runtime/runtime2.go`, `runtime/proc.go`                    |
//! | `syscall`     | `runtime/proc.go` (entersyscall / exitsyscall)              |
//! | `sysmon`      | `runtime/proc.go` (sysmon / retake)                         |
//! | `time`        | `runtime/time.go`                                           |
//! | `asm_amd64`   | `runtime/asm_amd64.s`, `runtime/preempt_amd64.s`           |
//! | `asm_arm64`   | `runtime/asm_arm64.s`, `runtime/preempt_arm64.s`           |

// Faithful Go-runtime ports contain symbols that will be used when deferred
// features land.  Suppress dead-code warnings across the whole runtime module.
#![allow(dead_code)]

pub(crate) mod g;
pub(crate) mod m;
pub(crate) mod rawmutex;
pub(crate) mod netpoll;
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
