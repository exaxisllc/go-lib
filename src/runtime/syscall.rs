//! Syscall handoff shim — ported from `runtime/proc.go`
//! (`entersyscall` / `exitsyscall`).
//!
//! Wraps any std primitive that may park the OS thread (`std::sync::Mutex`,
//! `std::sync::Condvar`, real syscalls). On enter, the M marks itself as in
//! a syscall and hands its P off to another M if work is queued; on exit,
//! it reacquires a P (or parks itself if none are available).
//!
//! TODO(step 15.5): implement `entersyscall`, `exitsyscall`, `handoffp`,
//! `retake` (called from sysmon).
