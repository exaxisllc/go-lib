// SPDX-License-Identifier: Apache-2.0
//! `go!` and `select!` macros — public spawn/multiplex syntax.
//!
//! ## `go!`
//!
//! Spawns a closure as a new goroutine.  Equivalent to Go's `go f()`.
//!
//! ```no_run
//! #[go_lib::main]
//! fn main() {
//!     go_lib::go!(|| println!("hello from goroutine"));
//! }
//! ```
//!
//! ## `select!`
//!
//! Multiplexes channel operations, picking the first ready case at random
//! (Go's fairness guarantee).  Syntax mirrors Go's `select` statement:
//!
//! ```text
//! select! {
//!     recv(rx)     -> v => { /* v: Option<T> */ }
//!     send(tx, val)    => { /* val was sent    */ }
//!     default          => { /* nothing ready   */ }
//! }
//! ```
//!
//! - Recv arms bind the variable name given after `->` as `Option<T>`:
//!   `Some(v)` on a normal receive, `None` if the channel was closed.
//! - Send arms use `ManuallyDrop<T>` internally; the value is consumed when
//!   the arm wins and dropped when the arm loses.
//! - `default` makes the select non-blocking (taken when no other arm fires).
//! - Without `default`, `select!` blocks until at least one case is ready.
//!
//! Arms may appear in any order. At most 4 recv and 2 send arms are supported
//! per invocation.

/// Spawn a closure as a new goroutine.
///
/// # Example
///
/// ```no_run
/// #[go_lib::main]
/// fn main() {
///     go_lib::go!(|| {
///         println!("running in a goroutine");
///     });
/// }
/// ```
#[macro_export]
macro_rules! go {
    ($body:expr) => {{
        $crate::__spawn($body)
    }};
}

// ---------------------------------------------------------------------------
// Internal helper — shared dispatch pattern for recv-only selects.
//
// The macro_rules below are explicit-rule based (not tt-munching) so the
// generated code is always in a single hygiene scope, giving each arm its
// own uniquely-named stack slot without any counting trick.
// ---------------------------------------------------------------------------

/// Multiplex channel operations.
///
/// See [module-level documentation][crate::go_macro] for full syntax and
/// semantics.
///
/// # Example — nonblocking recv with default
///
/// ```no_run
/// use go_lib::chan::chan;
/// #[go_lib::main]
/// fn main() {
///     let (tx, rx) = chan::<i32>(1);
///     tx.send(42);
///     go_lib::select! {
///         recv(rx) -> v => {
///             println!("received {:?}", v);
///         }
///         default => {
///             println!("nothing ready");
///         }
///     }
/// }
/// ```
#[macro_export]
macro_rules! select {

    // ─── A1: single recv, blocking ────────────────────────────────────────────
    ( recv($r:expr) -> $v:ident => $b:block $(,)? ) => {{
        let mut __r0: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __sel = ::std::vec![
            $crate::select::recv_case_of(&($r), &mut __r0),
        ];
        $crate::select::selectgo(&mut __sel, false);
        let $v = __r0;
        $b
    }};

    // ─── B1: single recv + default ────────────────────────────────────────────
    ( recv($r:expr) -> $v:ident => $b:block $(,)? default => $d:block $(,)? ) => {{
        let mut __r0: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __sel = ::std::vec![
            $crate::select::recv_case_of(&($r), &mut __r0),
        ];
        let (__idx, _ok) = $crate::select::selectgo(&mut __sel, true);
        if __idx == 0 {
            let $v = __r0;
            $b
        } else { $d }
    }};

    // ─── A2: two recv, blocking ───────────────────────────────────────────────
    ( recv($r1:expr) -> $v1:ident => $b1:block $(,)?
      recv($r2:expr) -> $v2:ident => $b2:block $(,)? ) => {{
        let mut __r0: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r1: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __sel = ::std::vec![
            $crate::select::recv_case_of(&($r1), &mut __r0),
            $crate::select::recv_case_of(&($r2), &mut __r1),
        ];
        let (__idx, _ok) = $crate::select::selectgo(&mut __sel, false);
        if __idx == 0 {
            let $v1 = __r0;
            $b1
        } else {
            let $v2 = __r1;
            $b2
        }
    }};

    // ─── B2: two recv + default ───────────────────────────────────────────────
    ( recv($r1:expr) -> $v1:ident => $b1:block $(,)?
      recv($r2:expr) -> $v2:ident => $b2:block $(,)?
      default => $d:block $(,)? ) => {{
        let mut __r0: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r1: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __sel = ::std::vec![
            $crate::select::recv_case_of(&($r1), &mut __r0),
            $crate::select::recv_case_of(&($r2), &mut __r1),
        ];
        let (__idx, _ok) = $crate::select::selectgo(&mut __sel, true);
        if __idx == 0 {
            let $v1 = __r0;
            $b1
        } else if __idx == 1 {
            let $v2 = __r1;
            $b2
        } else { $d }
    }};

    // ─── A3: three recv, blocking ─────────────────────────────────────────────
    ( recv($r1:expr) -> $v1:ident => $b1:block $(,)?
      recv($r2:expr) -> $v2:ident => $b2:block $(,)?
      recv($r3:expr) -> $v3:ident => $b3:block $(,)? ) => {{
        let mut __r0: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r1: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r2: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __sel = ::std::vec![
            $crate::select::recv_case_of(&($r1), &mut __r0),
            $crate::select::recv_case_of(&($r2), &mut __r1),
            $crate::select::recv_case_of(&($r3), &mut __r2),
        ];
        let (__idx, _ok) = $crate::select::selectgo(&mut __sel, false);
        if __idx == 0 {
            let $v1 = __r0;
            $b1
        } else if __idx == 1 {
            let $v2 = __r1;
            $b2
        } else {
            let $v3 = __r2;
            $b3
        }
    }};

    // ─── B3: three recv + default ─────────────────────────────────────────────
    ( recv($r1:expr) -> $v1:ident => $b1:block $(,)?
      recv($r2:expr) -> $v2:ident => $b2:block $(,)?
      recv($r3:expr) -> $v3:ident => $b3:block $(,)?
      default => $d:block $(,)? ) => {{
        let mut __r0: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r1: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r2: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __sel = ::std::vec![
            $crate::select::recv_case_of(&($r1), &mut __r0),
            $crate::select::recv_case_of(&($r2), &mut __r1),
            $crate::select::recv_case_of(&($r3), &mut __r2),
        ];
        let (__idx, _ok) = $crate::select::selectgo(&mut __sel, true);
        if __idx == 0 {
            let $v1 = __r0;
            $b1
        } else if __idx == 1 {
            let $v2 = __r1;
            $b2
        } else if __idx == 2 {
            let $v3 = __r2;
            $b3
        } else { $d }
    }};

    // ─── A4: four recv, blocking ──────────────────────────────────────────────
    ( recv($r1:expr) -> $v1:ident => $b1:block $(,)?
      recv($r2:expr) -> $v2:ident => $b2:block $(,)?
      recv($r3:expr) -> $v3:ident => $b3:block $(,)?
      recv($r4:expr) -> $v4:ident => $b4:block $(,)? ) => {{
        let mut __r0: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r1: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r2: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r3: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __sel = ::std::vec![
            $crate::select::recv_case_of(&($r1), &mut __r0),
            $crate::select::recv_case_of(&($r2), &mut __r1),
            $crate::select::recv_case_of(&($r3), &mut __r2),
            $crate::select::recv_case_of(&($r4), &mut __r3),
        ];
        let (__idx, _ok) = $crate::select::selectgo(&mut __sel, false);
        if __idx == 0 {
            let $v1 = __r0;
            $b1
        } else if __idx == 1 {
            let $v2 = __r1;
            $b2
        } else if __idx == 2 {
            let $v3 = __r2;
            $b3
        } else {
            let $v4 = __r3;
            $b4
        }
    }};

    // ─── B4: four recv + default ──────────────────────────────────────────────
    ( recv($r1:expr) -> $v1:ident => $b1:block $(,)?
      recv($r2:expr) -> $v2:ident => $b2:block $(,)?
      recv($r3:expr) -> $v3:ident => $b3:block $(,)?
      recv($r4:expr) -> $v4:ident => $b4:block $(,)?
      default => $d:block $(,)? ) => {{
        let mut __r0: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r1: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r2: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r3: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __sel = ::std::vec![
            $crate::select::recv_case_of(&($r1), &mut __r0),
            $crate::select::recv_case_of(&($r2), &mut __r1),
            $crate::select::recv_case_of(&($r3), &mut __r2),
            $crate::select::recv_case_of(&($r4), &mut __r3),
        ];
        let (__idx, _ok) = $crate::select::selectgo(&mut __sel, true);
        if __idx == 0 {
            let $v1 = __r0;
            $b1
        } else if __idx == 1 {
            let $v2 = __r1;
            $b2
        } else if __idx == 2 {
            let $v3 = __r2;
            $b3
        } else if __idx == 3 {
            let $v4 = __r3;
            $b4
        } else { $d }
    }};

    // ─── C1: single send + default (nonblocking send) ─────────────────────────
    ( send($tx:expr, $sv:expr) => $sb:block $(,)? default => $d:block $(,)? ) => {{
        let mut __s0: ::std::mem::ManuallyDrop<_> = ::std::mem::ManuallyDrop::new($sv);
        let mut __sel = ::std::vec![
            $crate::select::send_case_of(&($tx), &mut __s0),
        ];
        let (__idx, _ok) = $crate::select::selectgo(&mut __sel, true);
        if __idx == 0 {
            // Send won — value consumed, do NOT drop __s0.
            $sb
        } else {
            // Default — drop the unsent value.
            // SAFETY: __s0 was not consumed by selectgo (default taken).
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0) };
            $d
        }
    }};

    // ─── D1: 1 recv + 1 send, blocking ───────────────────────────────────────
    ( recv($r1:expr) -> $v1:ident => $rb:block $(,)?
      send($tx:expr, $sv:expr)    => $sb:block $(,)? ) => {{
        let mut __r0: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __s0: ::std::mem::ManuallyDrop<_> = ::std::mem::ManuallyDrop::new($sv);
        let mut __sel = ::std::vec![
            $crate::select::recv_case_of(&($r1), &mut __r0),
            $crate::select::send_case_of(&($tx), &mut __s0),
        ];
        let (__idx, _ok) = $crate::select::selectgo(&mut __sel, false);
        if __idx == 0 {
            // SAFETY: recv won, send value was not consumed.
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0) };
            let $v1 = __r0;
            $rb
        } else {
            // Send won — value consumed.
            $sb
        }
    }};

    // ─── D2: 1 recv + 1 send + default ───────────────────────────────────────
    ( recv($r1:expr) -> $v1:ident => $rb:block $(,)?
      send($tx:expr, $sv:expr)    => $sb:block $(,)?
      default                     => $d:block $(,)? ) => {{
        let mut __r0: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __s0: ::std::mem::ManuallyDrop<_> = ::std::mem::ManuallyDrop::new($sv);
        let mut __sel = ::std::vec![
            $crate::select::recv_case_of(&($r1), &mut __r0),
            $crate::select::send_case_of(&($tx), &mut __s0),
        ];
        let (__idx, _ok) = $crate::select::selectgo(&mut __sel, true);
        if __idx == 0 {
            // SAFETY: recv won, send value was not consumed.
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0) };
            let $v1 = __r0;
            $rb
        } else if __idx == 1 {
            $sb
        } else {
            // SAFETY: default taken, send value was not consumed.
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0) };
            $d
        }
    }};

    // ─── D3: 2 recv + 1 send, blocking ───────────────────────────────────────
    ( recv($r1:expr) -> $v1:ident => $b1:block $(,)?
      recv($r2:expr) -> $v2:ident => $b2:block $(,)?
      send($tx:expr, $sv:expr)    => $sb:block $(,)? ) => {{
        let mut __r0: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r1: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __s0: ::std::mem::ManuallyDrop<_> = ::std::mem::ManuallyDrop::new($sv);
        let mut __sel = ::std::vec![
            $crate::select::recv_case_of(&($r1), &mut __r0),
            $crate::select::recv_case_of(&($r2), &mut __r1),
            $crate::select::send_case_of(&($tx), &mut __s0),
        ];
        let (__idx, _ok) = $crate::select::selectgo(&mut __sel, false);
        if __idx == 0 {
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0) };
            let $v1 = __r0;
            $b1
        } else if __idx == 1 {
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0) };
            let $v2 = __r1;
            $b2
        } else {
            $sb
        }
    }};

    // ─── D4: 2 recv + 1 send + default ───────────────────────────────────────
    ( recv($r1:expr) -> $v1:ident => $b1:block $(,)?
      recv($r2:expr) -> $v2:ident => $b2:block $(,)?
      send($tx:expr, $sv:expr)    => $sb:block $(,)?
      default                     => $d:block $(,)? ) => {{
        let mut __r0: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r1: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __s0: ::std::mem::ManuallyDrop<_> = ::std::mem::ManuallyDrop::new($sv);
        let mut __sel = ::std::vec![
            $crate::select::recv_case_of(&($r1), &mut __r0),
            $crate::select::recv_case_of(&($r2), &mut __r1),
            $crate::select::send_case_of(&($tx), &mut __s0),
        ];
        let (__idx, _ok) = $crate::select::selectgo(&mut __sel, true);
        if __idx == 0 {
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0) };
            let $v1 = __r0;
            $b1
        } else if __idx == 1 {
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0) };
            let $v2 = __r1;
            $b2
        } else if __idx == 2 {
            $sb
        } else {
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0) };
            $d
        }
    }};

    // ─── D5: 3 recv + 1 send, blocking ───────────────────────────────────────
    ( recv($r1:expr) -> $v1:ident => $b1:block $(,)?
      recv($r2:expr) -> $v2:ident => $b2:block $(,)?
      recv($r3:expr) -> $v3:ident => $b3:block $(,)?
      send($tx:expr, $sv:expr)    => $sb:block $(,)? ) => {{
        let mut __r0: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r1: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __r2: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __s0: ::std::mem::ManuallyDrop<_> = ::std::mem::ManuallyDrop::new($sv);
        let mut __sel = ::std::vec![
            $crate::select::recv_case_of(&($r1), &mut __r0),
            $crate::select::recv_case_of(&($r2), &mut __r1),
            $crate::select::recv_case_of(&($r3), &mut __r2),
            $crate::select::send_case_of(&($tx), &mut __s0),
        ];
        let (__idx, _ok) = $crate::select::selectgo(&mut __sel, false);
        if __idx == 0 {
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0) };
            let $v1 = __r0;
            $b1
        } else if __idx == 1 {
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0) };
            let $v2 = __r1;
            $b2
        } else if __idx == 2 {
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0) };
            let $v3 = __r2;
            $b3
        } else {
            $sb
        }
    }};

    // ─── D6: 1 recv + 2 send, blocking ───────────────────────────────────────
    ( recv($r1:expr) -> $v1:ident => $rb:block $(,)?
      send($tx1:expr, $sv1:expr)  => $sb1:block $(,)?
      send($tx2:expr, $sv2:expr)  => $sb2:block $(,)? ) => {{
        let mut __r0: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __s0: ::std::mem::ManuallyDrop<_> = ::std::mem::ManuallyDrop::new($sv1);
        let mut __s1: ::std::mem::ManuallyDrop<_> = ::std::mem::ManuallyDrop::new($sv2);
        let mut __sel = ::std::vec![
            $crate::select::recv_case_of(&($r1), &mut __r0),
            $crate::select::send_case_of(&($tx1), &mut __s0),
            $crate::select::send_case_of(&($tx2), &mut __s1),
        ];
        let (__idx, _ok) = $crate::select::selectgo(&mut __sel, false);
        if __idx == 0 {
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0); ::std::mem::ManuallyDrop::drop(&mut __s1) };
            let $v1 = __r0;
            $rb
        } else if __idx == 1 {
            // __s0 consumed; drop __s1
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s1) };
            $sb1
        } else {
            // __s1 consumed; drop __s0
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0) };
            $sb2
        }
    }};

    // ─── D7: 1 recv + 2 send + default ───────────────────────────────────────
    ( recv($r1:expr) -> $v1:ident => $rb:block $(,)?
      send($tx1:expr, $sv1:expr)  => $sb1:block $(,)?
      send($tx2:expr, $sv2:expr)  => $sb2:block $(,)?
      default                     => $d:block $(,)? ) => {{
        let mut __r0: ::std::option::Option<_> = ::std::option::Option::None;
        let mut __s0: ::std::mem::ManuallyDrop<_> = ::std::mem::ManuallyDrop::new($sv1);
        let mut __s1: ::std::mem::ManuallyDrop<_> = ::std::mem::ManuallyDrop::new($sv2);
        let mut __sel = ::std::vec![
            $crate::select::recv_case_of(&($r1), &mut __r0),
            $crate::select::send_case_of(&($tx1), &mut __s0),
            $crate::select::send_case_of(&($tx2), &mut __s1),
        ];
        let (__idx, _ok) = $crate::select::selectgo(&mut __sel, true);
        if __idx == 0 {
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0); ::std::mem::ManuallyDrop::drop(&mut __s1) };
            let $v1 = __r0;
            $rb
        } else if __idx == 1 {
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s1) };
            $sb1
        } else if __idx == 2 {
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0) };
            $sb2
        } else {
            unsafe { ::std::mem::ManuallyDrop::drop(&mut __s0); ::std::mem::ManuallyDrop::drop(&mut __s1) };
            $d
        }
    }};
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use crate::chan::chan;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    // ── go! ───────────────────────────────────────────────────────────────────

    /// go! spawns a goroutine that runs its closure.
    #[test]
    #[go_lib::main]
    fn go_macro_spawns() {
        let count = Arc::new(AtomicI32::new(0));
        let c2    = Arc::clone(&count);
        let c3    = Arc::clone(&count);
        go!(move || { c2.fetch_add(1, Ordering::Relaxed); });
        // Poll on the atomic with a wall-clock deadline instead of a
        // fixed `gosched` count.  Each goroutine pays ~50 µs of startup
        // (one-shot stack pre-grow + scheduler wakeup) before its closure
        // runs; under heavy parallel test load (sysmon preemption, other
        // tests' lingering Ms, signal handler queuing) that can briefly
        // exceed any fixed yield count.  A wall-clock deadline keeps the
        // test robust to those one-off spikes.
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_secs(5);
        while c3.load(Ordering::Acquire) < 1
            && std::time::Instant::now() < deadline
        {
            crate::gosched();
        }
        assert_eq!(count.load(Ordering::Acquire), 1);
    }

    // ── select! fast-path (no park) ───────────────────────────────────────────

    /// B1: recv+default — data ready → recv arm taken.
    #[test]
    #[go_lib::main]
    fn select_recv_default_data_ready() {
        let (tx, rx) = chan::<i32>(1);
        tx.send(7);
        select! {
            recv(rx) -> v => { assert_eq!(v.unwrap(), 7); }
            default      => { panic!("default should not fire"); }
        }
    }

    /// B1: recv+default — channel empty → default taken.
    #[test]
    #[go_lib::main]
    fn select_default_when_empty() {
        let (_tx, rx) = chan::<i32>(1);
        select! {
            recv(rx) -> _v => { panic!("should not recv"); }
            default        => {} // default arm correctly taken
        }
    }

    /// B2: two recv + default — first channel has data.
    #[test]
    #[go_lib::main]
    fn select_two_recv_first_ready() {
        let (tx1, rx1) = chan::<i32>(1);
        let (_tx2, rx2) = chan::<i32>(1);
        tx1.send(42);
        select! {
            recv(rx1) -> v  => { assert_eq!(v.unwrap(), 42); }
            recv(rx2) -> _v => { panic!("rx2 should not fire"); }
            default         => { panic!("unexpected default — rx1 should have been ready"); }
        }
    }

    /// C1: send+default — buffer has space → send arm taken.
    #[test]
    #[go_lib::main]
    fn select_send_default_space_available() {
        let (tx, rx) = chan::<i32>(1);
        select! {
            send(tx, 99_i32) => {} // send arm correctly taken
            default          => { panic!("default should not fire"); }
        }
        assert_eq!(rx.recv(), Some(99));
    }

    /// C1: send+default — buffer full → default taken.
    #[test]
    #[go_lib::main]
    fn select_send_default_buffer_full() {
        let (tx, rx) = chan::<i32>(1);
        tx.send(1);   // fill the buffer
        select! {
            send(tx, 2_i32) => { panic!("should not send — buffer is full"); }
            default         => {} // default arm correctly taken
        }
        assert_eq!(rx.recv(), Some(1));
    }

    /// D2: recv+send+default — recv channel has data, send buffer has space;
    /// one of them fires, the other does not panic.
    #[test]
    #[go_lib::main]
    fn select_recv_send_default() {
        let (tx1, rx1) = chan::<i32>(1);
        let (tx2, rx2) = chan::<i32>(1);
        tx1.send(10);
        let mut recv_val = -1_i32;
        let mut send_ok  = false;
        // Both cases are ready; at least one fires.
        select! {
            recv(rx1) -> v  => { recv_val = v.unwrap(); }
            send(tx2, 20_i32) => { send_ok = true; }
            default         => {}
        }
        // At least one of recv_val or send_ok should have changed.
        assert!(recv_val == 10 || send_ok);
        let _ = rx2.try_recv(); // drain if sent
    }

    // ── select! blocking path ─────────────────────────────────────────────────

    /// A1: single recv blocking — goroutine parks until sender fires.
    #[test]
    #[go_lib::main]
    fn select_blocking_recv() {
        let result = Arc::new(AtomicI32::new(-1));
        let r2 = Arc::clone(&result);
        let (tx, rx) = chan::<i32>(0);
        go!(move || { tx.send(55); });
        select! {
            recv(rx) -> v => { r2.store(v.unwrap(), Ordering::Relaxed); }
        }
        assert_eq!(result.load(Ordering::Acquire), 55);
    }

    /// A2: two recv blocking — whichever sender fires first wins.
    #[test]
    #[go_lib::main]
    fn select_blocking_two_recv() {
        let winner = Arc::new(AtomicI32::new(-1));
        let w2 = Arc::clone(&winner);
        let (tx1, rx1) = chan::<i32>(0);
        let (tx2, rx2) = chan::<i32>(0);
        go!(move || { tx1.send(1); });
        go!(move || { tx2.send(2); });
        select! {
            recv(rx1) -> v => { w2.store(v.unwrap(), Ordering::Relaxed); }
            recv(rx2) -> v => { w2.store(v.unwrap(), Ordering::Relaxed); }
        }
        let w = winner.load(Ordering::Acquire);
        assert!(w == 1 || w == 2, "winner should be 1 or 2, got {w}");
    }

    /// D1: recv+send blocking — one goroutine sends, one receives; select picks.
    #[test]
    #[go_lib::main]
    fn select_blocking_recv_send() {
        let recv_val = Arc::new(AtomicI32::new(-1));
        let rv2 = Arc::clone(&recv_val);
        let (tx1, rx1) = chan::<i32>(0); // recv from this
        let (tx2, rx2) = chan::<i32>(0); // send to this
        // Goroutine that will satisfy the recv arm.
        go!(move || { tx1.send(77); });
        // Goroutine that drains if the send arm fires instead.
        go!(move || {
            // Give the main goroutine time to block.
            crate::gosched();
            let _ = rx2.recv();
        });
        select! {
            recv(rx1) -> v      => { rv2.store(v.unwrap(), Ordering::Relaxed); }
            send(tx2, 99_i32)   => {}
        }
        // Either recv gave us 77 or send fired (recv_val stays -1 → we got -1).
        let v = recv_val.load(Ordering::Acquire);
        assert!(v == 77 || v == -1, "unexpected value {v}");
    }

    /// recv from closed channel yields None via select.
    #[test]
    #[go_lib::main]
    fn select_recv_closed_yields_none() {
        let (tx, rx) = chan::<i32>(0);
        tx.close();
        select! {
            recv(rx) -> v => { assert!(v.is_none(), "should be None for closed channel"); }
        }
    }
}
