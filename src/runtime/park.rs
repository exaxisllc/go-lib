//! `gopark` / `goready` — the parking primitives every blocking op uses.
//! Ported from `runtime/proc.go`.
//!
//! TODO(step 8): park a G by setting status to `Gwaiting` and calling
//! `mcall(schedule)`; `goready` re-enqueues a G on a P's run queue.
