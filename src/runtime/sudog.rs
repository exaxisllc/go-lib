//! Waiter records (`sudog`) — ported from `runtime/runtime2.go` and the
//! `acquireSudog` / `releaseSudog` helpers in `runtime/proc.go`.
//!
//! TODO(step 12): port the per-P sudog cache; channels allocate these
//! constantly so pooling matters.
