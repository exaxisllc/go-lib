//! `WaitGroup` — ported from `src/sync/waitgroup.go`.

/// Waits for a collection of goroutines to finish.
///
/// TODO(step 16): implement on `Mutex<u64> + Condvar`, with `wait()`
/// wrapped in the syscall-handoff shim so a blocked M releases its P.
pub struct WaitGroup(());
