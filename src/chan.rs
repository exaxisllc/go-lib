// SPDX-License-Identifier: Apache-2.0
//! Channels — ported from `src/runtime/chan.go`.
//!
//! Buffered and unbuffered channels backed by the same G/M/P scheduler used by
//! goroutines.  A goroutine that blocks on a channel send or receive is parked
//! via `gopark` and resumed via `goready`; no OS thread is ever blocked.
//!
//! ## Public surface
//!
//! ```no_run
//! let (tx, rx) = go_lib::chan::chan::<i32>(0);   // unbuffered
//! let (tx, rx) = go_lib::chan::chan::<i32>(16);  // buffered, capacity 16
//!
//! tx.send(42_i32);
//! let v = rx.recv();  // Some(42); None means closed + empty
//! tx.close();
//! ```
//!
//! ## Internals
//!
//! Each channel is an `Arc<Hchan<T>>`.  The lock-protected interior holds a
//! `VecDeque<T>` ring buffer plus two wait queues (`sendq` / `recvq`) of
//! `Sudog` records (one per blocked goroutine).
//!
//! Locking uses `RawMutex` (not `std::sync::Mutex`) so that `selectgo` can
//! hold multiple heterogeneous channel locks simultaneously without needing a
//! typed `MutexGuard<HchanState<T>>` for each one.
//!
//! ### Blocking protocol
//!
//! When a goroutine must block it:
//! 1. Allocates a `Sudog` from the pool.
//! 2. Heap-allocates a `ManuallyDrop<T>` (send) or `Option<T>` (recv) as the
//!    value staging area (`sudog.elem`).
//! 3. Enqueues the sudog in `sendq` / `recvq` (under the channel lock).
//! 4. Releases the lock.
//! 5. Calls `gopark` — `park_fn` sets `GWAITING` on g0's stack.
//!
//! The goroutine that completes the operation:
//! - Reads or writes through `sudog.elem`.
//! - Sets `sudog.success` and `(*gp).param = sudog as *mut u8`.
//! - Calls `goready`, which spins until the target is `GWAITING` before
//!   marking it `GRUNNABLE`.
//!
//! ### Close semantics (matches Go)
//!
//! - Sending on a closed channel **panics**.
//! - Receiving from a closed empty channel returns `None`.
//! - Closing an already-closed channel **panics**.
//!
//! Ported from `hchan`, `chansend`, `chanrecv`, `closechan` in
//! `runtime/chan.go`.

use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::Arc;

use crate::runtime::g::{current_g, WaitReason};
use crate::runtime::park::{gopark, goready};
use crate::runtime::rawmutex::{LockGuard, RawMutex};
use crate::runtime::sudog::{acquire_sudog, release_sudog, Sudog, WaitQ};

// ---------------------------------------------------------------------------
// Hchan — the heap channel object
// ---------------------------------------------------------------------------

/// Lock-protected interior of a channel.
pub(crate) struct HchanState<T> {
    /// Buffered elements waiting to be received (FIFO).
    pub(crate) buf:    VecDeque<T>,
    /// Buffer capacity (0 = unbuffered / synchronous).
    pub(crate) cap:    usize,
    /// True after `close()`.
    pub(crate) closed: bool,
    /// Goroutines blocked in `send` (buffer full or unbuffered with no receiver).
    pub(crate) sendq:  WaitQ,
    /// Goroutines blocked in `recv` (buffer empty or unbuffered with no sender).
    pub(crate) recvq:  WaitQ,
}

impl<T> HchanState<T> {
    fn new(cap: usize) -> Self {
        Self {
            buf:    VecDeque::with_capacity(cap),
            cap,
            closed: false,
            sendq:  WaitQ::new(),
            recvq:  WaitQ::new(),
        }
    }
}

/// The channel heap object, shared via `Arc` between all `Sender`/`Receiver`
/// clones.
///
/// `pub(crate)` so that `selectgo` (step 14) can access the state directly.
///
/// The `mutex` field is first so that `Arc::as_ptr(h) as *const RawMutex` gives
/// a stable address suitable for address-ordered lock acquisition in `selectgo`.
pub(crate) struct Hchan<T> {
    /// Raw adaptive spinlock protecting `state`.
    ///
    /// Exposed `pub(crate)` so `selectgo` can lock/unlock multiple heterogeneous
    /// channels without needing typed `MutexGuard` storage.
    pub(crate) mutex: RawMutex,
    /// Interior state — always accessed under `mutex`.
    pub(crate) state: UnsafeCell<HchanState<T>>,
}

unsafe impl<T: Send> Send for Hchan<T> {}
unsafe impl<T: Send> Sync for Hchan<T> {}

impl<T> Hchan<T> {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            mutex: RawMutex::new(),
            state: UnsafeCell::new(HchanState::new(cap)),
        }
    }

    /// Acquire the lock and return a guard + mutable state reference.
    ///
    /// The guard releases the lock when dropped.  Drop it *before* calling
    /// `gopark` so the scheduler can't see the lock still held.
    ///
    /// # Safety
    /// The returned `&mut HchanState<T>` must not be used after the guard is
    /// dropped (the lock no longer protects access).
    #[allow(clippy::mut_from_ref)] // intentional: state is behind UnsafeCell
    pub(crate) unsafe fn lock_state(&self) -> (LockGuard<'_>, &mut HchanState<T>) {
        let g = LockGuard::new(&self.mutex);
        // SAFETY: We just acquired the lock; no other thread holds a reference.
        let s = unsafe { &mut *self.state.get() };
        (g, s)
    }
}

// ---------------------------------------------------------------------------
// Public channel halves
// ---------------------------------------------------------------------------

/// The sending half of a channel.  Cheap to `clone`.
pub struct Sender<T>(Arc<Hchan<T>>);

/// The receiving half of a channel.  Cheap to `clone`.
pub struct Receiver<T>(Arc<Hchan<T>>);

impl<T> Clone for Sender<T>   { fn clone(&self) -> Self { Sender(Arc::clone(&self.0))   } }
impl<T> Clone for Receiver<T> { fn clone(&self) -> Self { Receiver(Arc::clone(&self.0)) } }

unsafe impl<T: Send> Send for Sender<T>   {}
unsafe impl<T: Send> Sync for Sender<T>   {}
unsafe impl<T: Send> Send for Receiver<T> {}
unsafe impl<T: Send> Sync for Receiver<T> {}

/// Create a new channel with the given buffer capacity.
///
/// `cap == 0` gives an unbuffered (synchronous rendezvous) channel; `cap > 0`
/// gives a buffered channel that holds up to `cap` values without blocking the
/// sender.
///
/// Returns `(Sender<T>, Receiver<T>)`.
pub fn chan<T: Send + 'static>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let h = Arc::new(Hchan::new(cap));
    (Sender(Arc::clone(&h)), Receiver(h))
}

impl<T: Send + 'static> Sender<T> {
    /// Send `val`, blocking until a receiver is ready or buffer space opens.
    ///
    /// # Panics
    /// Panics if the channel has been closed.
    pub fn send(&self, val: T) {
        unsafe { chansend(&self.0, val, true) };
    }

    /// Non-blocking send.  Returns `false` if the buffer is full or there is
    /// no waiting receiver.  Panics if the channel is closed.
    pub fn try_send(&self, val: T) -> bool {
        unsafe { chansend(&self.0, val, false) }
    }

    /// Close the channel.  Panics if already closed.
    pub fn close(&self) {
        unsafe { closechan(&self.0) };
    }

    /// Raw access to the underlying `Hchan` for use by `selectgo`.
    pub(crate) fn hchan(&self) -> &Arc<Hchan<T>> { &self.0 }
}

impl<T: Send + 'static> Receiver<T> {
    /// Receive a value, blocking until one is available or the channel closes.
    ///
    /// Returns `Some(val)` on success, `None` if the channel is closed and
    /// the buffer is fully drained.
    pub fn recv(&self) -> Option<T> {
        unsafe { chanrecv(&self.0, true) }
    }

    /// Non-blocking receive.
    ///
    /// - `Some(Some(val))` — received.
    /// - `Some(None)`      — channel closed and empty.
    /// - `None`            — would block (nothing ready yet).
    pub fn try_recv(&self) -> Option<Option<T>> {
        unsafe { chanrecv_nb(&self.0) }
    }

    /// Raw access to the underlying `Hchan` for use by `selectgo`.
    pub(crate) fn hchan(&self) -> &Arc<Hchan<T>> { &self.0 }
}

// ---------------------------------------------------------------------------
// chansend
// ---------------------------------------------------------------------------

/// Send `val` to `c`.
///
/// `block = true`  → park the goroutine if the channel has no space.
/// `block = false` → return `false` immediately if the channel has no space.
///
/// # Safety
/// Must be called from a goroutine (not g0 or an OS-thread main function).
///
/// Ported from `chansend` in `runtime/chan.go`.
pub(crate) unsafe fn chansend<T: Send + 'static>(
    c:     &Arc<Hchan<T>>,
    val:   T,
    block: bool,
) -> bool {
    // SAFETY: we hold the lock for the duration of the guard's scope.
    let (_g, state) = unsafe { c.lock_state() };

    if state.closed {
        drop(_g);
        panic!("send on closed channel");
    }

    // ── Case 1: direct handoff to a waiting receiver ─────────────────────────
    let recv_sg = unsafe { state.recvq.dequeue() };
    if !recv_sg.is_null() {
        let gp       = unsafe { (*recv_sg).g };
        // recv_sg.elem points to Option<T> (allocated by chanrecv or selectgo).
        let elem_ptr = unsafe { (*recv_sg).elem as *mut Option<T> };
        if !elem_ptr.is_null() {
            unsafe { *elem_ptr = Some(val) };
        }
        unsafe {
            (*recv_sg).success = true;
            (*gp).param        = recv_sg as *mut u8;
        }
        drop(_g);
        unsafe { goready(gp) };
        return true;
    }

    // ── Case 2: buffer has space ──────────────────────────────────────────────
    if state.buf.len() < state.cap {
        state.buf.push_back(val);
        return true;
    }

    // ── Case 3: non-blocking — cannot proceed ────────────────────────────────
    if !block {
        return false;
    }

    // ── Case 4: block — enqueue this goroutine as a waiting sender ───────────
    let gp = current_g();
    debug_assert!(!gp.is_null(), "chansend: called from g0");

    // Box<ManuallyDrop<T>> — the receiver moves the value out and frees the box.
    let elem_ptr = Box::into_raw(Box::new(ManuallyDrop::new(val))) as *mut u8;

    let s = acquire_sudog();
    unsafe {
        (*s).g          = gp;
        (*s).elem       = elem_ptr;
        (*s).boxed_elem = true; // Box<ManuallyDrop<T>> — must be freed by receiver
        (*s).success    = false;
        (*s).c          = Arc::as_ptr(c) as *mut u8;
        (*gp).param     = ptr::null_mut();
        state.sendq.enqueue(s);
    }

    drop(_g); // release lock BEFORE parking
    gopark(WaitReason::ChanSend);

    // ── Resumed: inspect outcome ─────────────────────────────────────────────
    let ok = unsafe {
        let s2 = (*gp).param as *mut Sudog;
        (*gp).param = ptr::null_mut();
        let ok = (*s2).success;

        if !ok && !(*s2).elem.is_null() {
            // Send was rejected (channel closed after we parked).
            // elem points to ManuallyDrop<T> — drop the value, then free the box.
            let ep = (*s2).elem as *mut ManuallyDrop<T>;
            (*s2).elem = ptr::null_mut();
            ManuallyDrop::drop(&mut *ep); // run T's destructor
            if (*s2).boxed_elem { let _ = Box::from_raw(ep); }
        }
        (*s2).g = ptr::null_mut();
        (*s2).c = ptr::null_mut();
        release_sudog(s2);
        ok
    };

    if !ok {
        panic!("send on closed channel");
    }
    true
}

// ---------------------------------------------------------------------------
// chanrecv
// ---------------------------------------------------------------------------

/// Receive from `c`.
///
/// `block = true`  → park until a value or close.
/// `block = false` → return `None` immediately if nothing is ready.
///
/// Returns `Some(val)` on success or `None` for closed-and-empty / would-block.
///
/// # Safety
/// Must be called from a goroutine (not g0 or an OS-thread main function).
///
/// Ported from `chanrecv` in `runtime/chan.go`.
pub(crate) unsafe fn chanrecv<T: Send + 'static>(
    c:     &Arc<Hchan<T>>,
    block: bool,
) -> Option<T> {
    let (_g, state) = unsafe { c.lock_state() };

    // ── Case 1: direct handoff from a waiting sender ─────────────────────────
    let send_sg = unsafe { state.sendq.dequeue() };
    if !send_sg.is_null() {
        let val = recv_from_sender(state, send_sg);
        drop(_g);
        return Some(val);
    }

    // ── Case 2: buffer has data ───────────────────────────────────────────────
    if !state.buf.is_empty() {
        return Some(state.buf.pop_front().unwrap());
    }

    // ── Case 3: closed and empty ──────────────────────────────────────────────
    if state.closed {
        return None;
    }

    // ── Case 4: non-blocking — nothing ready ─────────────────────────────────
    if !block {
        return None;
    }

    // ── Case 5: block — enqueue as a waiting receiver ────────────────────────
    let gp = current_g();
    debug_assert!(!gp.is_null(), "chanrecv: called from g0");

    // Box<Option<T>> — the sender writes Some(val) into this slot, or it
    // stays None if the channel closes.  Freed on wakeup.
    let elem_ptr = Box::into_raw(Box::new(None::<T>)) as *mut u8;

    let s = acquire_sudog();
    unsafe {
        (*s).g          = gp;
        (*s).elem       = elem_ptr;
        (*s).boxed_elem = true; // Box<Option<T>> — must be freed on wakeup
        (*s).success    = false;
        (*s).c          = Arc::as_ptr(c) as *mut u8;
        (*gp).param     = ptr::null_mut();
        state.recvq.enqueue(s);
    }

    drop(_g);
    gopark(WaitReason::ChanReceive);

    // ── Resumed: read outcome ─────────────────────────────────────────────────
    unsafe {
        let s2 = (*gp).param as *mut Sudog;
        (*gp).param = ptr::null_mut();
        let ok = (*s2).success;

        let boxed = (*s2).boxed_elem;
        // elem points to Option<T>: Some(val) if sender delivered, None if closed.
        let result = if ok {
            debug_assert!(!(*s2).elem.is_null(), "chanrecv: success but elem is null");
            let ep = (*s2).elem as *mut Option<T>;
            (*s2).elem = ptr::null_mut();
            // ptr::read bitwise-copies the Option<T> out of *ep.  The allocation
            // at `ep` still holds the original bytes, but ownership of T has been
            // transferred to `val` — running Option<T>'s destructor on *ep would
            // double-drop T.  Cast to ManuallyDrop<Option<T>> to free the Box
            // allocation without triggering Option<T>::drop().
            let val = ptr::read(ep).expect("chanrecv: elem was None on success");
            if boxed {
                let _ = Box::from_raw(ep as *mut std::mem::ManuallyDrop<Option<T>>);
            }
            Some(val)
        } else {
            if !(*s2).elem.is_null() {
                // Channel was closed — the slot contains None (T was never written).
                // Dropping None does not touch any T; no ManuallyDrop needed.
                let ep = (*s2).elem as *mut Option<T>;
                (*s2).elem = ptr::null_mut();
                if boxed { let _ = Box::from_raw(ep); }
            }
            None
        };

        (*s2).g = ptr::null_mut();
        (*s2).c = ptr::null_mut();
        release_sudog(s2);
        result
    }
}

/// Non-blocking receive.
///
/// Returns:
/// - `Some(Some(v))` — value received.
/// - `Some(None)`    — channel closed and empty.
/// - `None`          — would block (channel has nothing ready right now).
///
/// # Safety
/// May be called outside the scheduler as long as the blocking path is never
/// triggered.
pub(crate) unsafe fn chanrecv_nb<T: Send + 'static>(
    c: &Arc<Hchan<T>>,
) -> Option<Option<T>> {
    let (_g, state) = unsafe { c.lock_state() };

    let send_sg = unsafe { state.sendq.dequeue() };
    if !send_sg.is_null() {
        let val = recv_from_sender(state, send_sg);
        drop(_g);
        return Some(Some(val));
    }

    if !state.buf.is_empty() {
        return Some(Some(state.buf.pop_front().unwrap()));
    }

    if state.closed {
        return Some(None);
    }

    None
}

/// Receive from a **dequeued** sender sudog and wake the sender.
///
/// For unbuffered channels (`cap == 0`): value is moved directly from the
/// sender's staging box.
/// For buffered channels (always full when a sender is queued): take the head
/// of the buffer, rotate the sender's value into the tail.
///
/// **Caller must release the channel lock after this returns**, before the
/// woken goroutine can be scheduled.
///
/// Ported from `recv` in `runtime/chan.go`.
fn recv_from_sender<T: Send + 'static>(
    state:   &mut HchanState<T>,
    send_sg: *mut Sudog,
) -> T {
    let gp = unsafe { (*send_sg).g };

    let boxed = unsafe { (*send_sg).boxed_elem };

    // send_sg.elem points to ManuallyDrop<T> (both chansend and selectgo use
    // ManuallyDrop semantics — same memory layout as T, no destructor).
    let val = if state.cap == 0 {
        let ep = unsafe { (*send_sg).elem as *mut ManuallyDrop<T> };
        let v  = unsafe { ManuallyDrop::into_inner(ptr::read(ep)) };
        unsafe {
            if boxed { let _ = Box::from_raw(ep); }
            (*send_sg).elem = ptr::null_mut();
        }
        v
    } else {
        let head = state.buf.pop_front().unwrap();
        let ep   = unsafe { (*send_sg).elem as *mut ManuallyDrop<T> };
        let sv   = unsafe { ManuallyDrop::into_inner(ptr::read(ep)) };
        unsafe {
            if boxed { let _ = Box::from_raw(ep); }
            (*send_sg).elem = ptr::null_mut();
        }
        state.buf.push_back(sv);
        head
    };

    unsafe {
        (*send_sg).success = true;
        (*gp).param        = send_sg as *mut u8;
    }
    unsafe { goready(gp) };
    val
}

// ---------------------------------------------------------------------------
// closechan
// ---------------------------------------------------------------------------

/// Close `c`.
///
/// Marks the channel closed, drains all waiting receivers (they get `None`)
/// and senders (they panic), and wakes all of them.
///
/// # Panics
/// Panics if the channel is already closed.
///
/// # Safety
/// Must be called from a goroutine (not g0 / OS-thread main).
///
/// Ported from `closechan` in `runtime/chan.go`.
pub(crate) unsafe fn closechan<T: Send + 'static>(c: &Arc<Hchan<T>>) {
    let (_g, state) = unsafe { c.lock_state() };

    if state.closed {
        drop(_g);
        panic!("close of closed channel");
    }
    state.closed = true;

    let mut wakeup: Vec<*mut crate::runtime::g::G> = Vec::new();

    loop {
        let sg = unsafe { state.recvq.dequeue() };
        if sg.is_null() { break; }
        let gp = unsafe { (*sg).g };
        unsafe {
            (*sg).success = false;
            (*gp).param   = sg as *mut u8;
        }
        wakeup.push(gp);
    }

    loop {
        let sg = unsafe { state.sendq.dequeue() };
        if sg.is_null() { break; }
        let gp = unsafe { (*sg).g };
        unsafe {
            (*sg).success = false;
            (*gp).param   = sg as *mut u8;
        }
        wakeup.push(gp);
    }

    drop(_g);

    for gp in wakeup {
        unsafe { goready(gp) };
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::runtime::sched::run_impl;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    // ── Buffered: fast paths (no goroutine park) ──────────────────────────────

    /// Single send + recv completes without blocking.
    #[test]
    fn buffered_send_recv() {
        run_impl(|| {
            let (tx, rx) = chan::<i32>(1);
            tx.send(42);
            assert_eq!(rx.recv(), Some(42));
        });
    }

    /// Values arrive in FIFO order.
    #[test]
    fn buffered_fifo_order() {
        run_impl(|| {
            let (tx, rx) = chan::<i32>(4);
            for i in 0..4_i32 { tx.send(i); }
            for i in 0..4_i32 { assert_eq!(rx.recv(), Some(i)); }
        });
    }

    /// Close drains buffered values, then recv returns None.
    #[test]
    fn buffered_close_drains_then_none() {
        run_impl(|| {
            let (tx, rx) = chan::<i32>(2);
            tx.send(1);
            tx.send(2);
            tx.close();
            assert_eq!(rx.recv(), Some(1));
            assert_eq!(rx.recv(), Some(2));
            assert_eq!(rx.recv(), None);
            assert_eq!(rx.recv(), None); // idempotent
        });
    }

    // ── Non-blocking ops ──────────────────────────────────────────────────────

    /// try_recv on an empty open channel returns None (would block).
    #[test]
    fn try_recv_empty() {
        run_impl(|| {
            let (_tx, rx) = chan::<i32>(4);
            assert_eq!(rx.try_recv(), None);
        });
    }

    /// try_recv on a closed empty channel returns Some(None).
    #[test]
    fn try_recv_closed_empty() {
        run_impl(|| {
            let (tx, rx) = chan::<i32>(4);
            tx.close();
            assert_eq!(rx.try_recv(), Some(None));
        });
    }

    /// try_send to a full channel returns false.
    #[test]
    fn try_send_full() {
        run_impl(|| {
            let (tx, _rx) = chan::<i32>(2);
            assert!(tx.try_send(1));
            assert!(tx.try_send(2));
            assert!(!tx.try_send(3));
        });
    }

    // ── Panic paths ───────────────────────────────────────────────────────────
    //
    // These don't exercise goroutine parking — the panic must unwind back to
    // the test thread's #[should_panic] handler, so we must NOT wrap in run_impl.

    /// Closing an already-closed channel panics.
    #[test]
    #[should_panic(expected = "close of closed channel")]
    fn close_twice_panics() {
        let (tx, _rx) = chan::<i32>(1);
        tx.close();
        tx.close();
    }

    /// Sending on a closed channel panics.
    #[test]
    #[should_panic(expected = "send on closed channel")]
    fn send_on_closed_panics() {
        let (tx, _rx) = chan::<i32>(1);
        tx.close();
        tx.send(1);
    }

    // ── Goroutine rendezvous (exercises park/unpark) ──────────────────────────

    /// Unbuffered send and recv across two goroutines.
    #[test]
    fn unbuffered_rendezvous() {
        use crate::runtime::sched::spawn_goroutine;

        run_impl(|| {
            let (tx, rx) = chan::<i32>(0);
            spawn_goroutine(move || { tx.send(99); });
            assert_eq!(rx.recv(), Some(99));
        });
    }

    /// Ping-pong ten rounds across two goroutines.
    #[test]
    fn unbuffered_ping_pong() {
        use crate::runtime::sched::spawn_goroutine;

        run_impl(|| {
            let (ping_tx, ping_rx) = chan::<i32>(0);
            let (pong_tx, pong_rx) = chan::<i32>(0);

            spawn_goroutine(move || {
                for _ in 0..10 {
                    let v = ping_rx.recv().unwrap();
                    pong_tx.send(v + 1);
                }
            });

            let mut n = 0_i32;
            for _ in 0..10 {
                ping_tx.send(n);
                n = pong_rx.recv().unwrap();
            }
            assert_eq!(n, 10);
        });
    }

    /// Buffered producer/consumer: 20 values summed by a goroutine.
    #[test]
    fn producer_consumer() {
        use crate::runtime::sched::spawn_goroutine;

        const N: i32 = 20;
        let sum = Arc::new(AtomicI32::new(0));
        let sum2 = Arc::clone(&sum);

        run_impl(move || {
            let (tx, rx) = chan::<i32>(4);
            let sum3 = Arc::clone(&sum2);

            spawn_goroutine(move || {
                for i in 0..N { tx.send(i); }
                tx.close();
            });

            spawn_goroutine(move || {
                while let Some(v) = rx.recv() {
                    sum3.fetch_add(v, Ordering::Relaxed);
                }
            });

            // A wall-clock deadline is robust across CI runner speeds and
            // build profiles (debug, coverage/nightly) where frame sizes and
            // instrumentation overhead can make goroutines run much slower.
            let expected = N * (N - 1) / 2;
            let deadline =
                std::time::Instant::now() + std::time::Duration::from_secs(5);
            while sum2.load(Ordering::Acquire) != expected
                && std::time::Instant::now() < deadline
            {
                crate::gosched();
            }
        });

        assert_eq!(sum.load(Ordering::Acquire), N * (N - 1) / 2);
    }
    
    /// multi-producer, single consumer: 10 producers summing 10 values
    #[test]
    fn multi_producer_single_consumer() {
        use crate::runtime::sched::spawn_goroutine;
        use crate::sync::WaitGroup;
        
        const N: i32 = 10;
        let total = N*N;
        let sum = Arc::new(AtomicI32::new(0));
        let sum2 = Arc::clone(&sum);

        run_impl(move || {
            let (tx, rx) = chan::<i32>(4);
            let sum3 = Arc::clone(&sum2);
            let wg  = Arc::new(WaitGroup::new());

            for g in 0 .. N {
                let start = N*g;
                let end = start+N;
                let tx_clone = tx.clone();
                let wg_clone = Arc::clone(&wg);
                // add(1) must happen before spawn_goroutine so that wg.wait()
                // cannot return before all producers have registered themselves.
                wg_clone.add(1);
                spawn_goroutine(move || {
                    for i in start..end { tx_clone.send(i); }
                    wg_clone.done()
                });
            }
            
            spawn_goroutine(move || {
                while let Some(v) = rx.recv() {
                    sum3.fetch_add(v, Ordering::Relaxed);
                }
            });

            wg.wait();
            tx.close();
            
            // A wall-clock deadline is robust across CI runner speeds and
            // build profiles (debug, coverage/nightly) where frame sizes and
            // instrumentation overhead can make goroutines run much slower.
            let expected = total * (total - 1) / 2;
            let deadline =
                std::time::Instant::now() + std::time::Duration::from_secs(5);
            while sum2.load(Ordering::Acquire) != expected
                && std::time::Instant::now() < deadline
            {
                crate::gosched();
            }
        });

        assert_eq!(sum.load(Ordering::Acquire), total * (total - 1) / 2);

    }

    /// Close wakes a goroutine that is blocked on recv.
    #[test]
    fn close_wakes_blocked_receiver() {
        use crate::runtime::sched::spawn_goroutine;

        let got_none = Arc::new(AtomicI32::new(0));
        let got2 = Arc::clone(&got_none);
        let got3 = Arc::clone(&got_none); // checked inside run_impl to bound the post-close loop

        run_impl(move || {
            let (tx, rx) = chan::<i32>(0);

            spawn_goroutine(move || {
                // Block on recv until the channel is closed.
                if rx.recv().is_none() {
                    got2.fetch_add(1, Ordering::Relaxed);
                }
            });

            // Give the spawned goroutine time to start and block on recv.
            // More iterations than before because parallel test load on CI can
            // starve goroutines for many scheduler rounds.
            for _ in 0..500 { crate::gosched(); }
            tx.close();
            // Wait for the goroutine to observe the close and record its result.
            // A wall-clock deadline is robust across CI runner speeds.
            let deadline =
                std::time::Instant::now() + std::time::Duration::from_secs(5);
            while got3.load(Ordering::Acquire) == 0
                && std::time::Instant::now() < deadline
            {
                crate::gosched();
            }
        });

        assert_eq!(got_none.load(Ordering::Acquire), 1);
    }
}
