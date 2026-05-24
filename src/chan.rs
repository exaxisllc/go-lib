//! Channels — ported from `src/runtime/chan.go`.
//!
//! Buffered and unbuffered. Sends and receives integrate with the scheduler
//! (see `crate::runtime::park`) so a blocked goroutine never parks its OS
//! thread.
//!
//! TODO(step 13): port `hchan`, `chansend`, `chanrecv`, `closechan`.
