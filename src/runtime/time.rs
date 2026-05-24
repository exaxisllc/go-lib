//! Timer heap — ported from `runtime/time.go`.
//!
//! TODO(step 17): single global 4-ary min-heap behind a `Mutex`; `sysmon`
//! fires expired timers by `goready`-ing their parked Gs. Per-P heaps are
//! a later optimization.
