//! Background monitor thread — ported from `runtime/proc.go` (`sysmon`).
//!
//! TODO(step 10): trimmed `sysmon` loop — fire ready timers, retake Ps
//! stuck in syscalls. No async preemption (signal-based) in v1.
