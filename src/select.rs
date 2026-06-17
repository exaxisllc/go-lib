// SPDX-License-Identifier: Apache-2.0
//! `selectgo` — the runtime heart of `select { }`.
//!
//! Ported from `runtime/select.go`.
//!
//! ## How it works
//!
//! `selectgo` receives a slice of [`SCase`]s and an optional `has_default`
//! flag and picks the first case that can proceed without blocking.
//!
//! ```text
//! 1. Build pollorder  — a random permutation of case indices (fairness).
//! 2. Build lockorder  — case indices sorted by channel address (deadlock prevention).
//! 3. Acquire all channel locks in lockorder.
//! 4. First pass (pollorder): check each case for immediate readiness.
//!    – buffer op:    perform it, release all locks, return winner.
//!    – direct handoff (partner waiting): dequeue partner's sudog, perform op,
//!      release all locks, call goready(partner), return winner.
//!    – send on closed: release all locks, panic.
//! 5. If has_default: release all locks, return (CASE_DEFAULT, false).
//! 6. Blocking path:
//!    a. For every case, allocate a sudog (is_select=true) and enqueue it.
//!    b. Reset G.selectdone to 0 and G.param to null.
//!    c. gopark_commit(Select) — park while still holding ALL channel locks;
//!       they are released on g0 only after the goroutine reaches GWAITING
//!       (commit-park protocol, closes the SIGURG lost-wakeup window).
//! 7. On wakeup (winner wrote G.param = winning sudog):
//!    a. Acquire all locks in lockorder.
//!    b. Dequeue all *losing* sudogs (dequeue_sudog is a no-op if a racing
//!       channel op already removed them).
//!    c. Release all locks.
//!    d. Release all sudogs back to the free list.
//!    e. Return (winner_index, ok).
//! ```
//!
//! ## Type erasure
//!
//! Channels are generic (`Hchan<T>`) but `selectgo` must operate over a
//! heterogeneous set of them.  Each [`SCase`] carries four function pointers
//! that are monomorphised at the call site (by the `select!` macro):
//!
//! | pointer       | purpose                                        |
//! |---------------|------------------------------------------------|
//! | `lock_fn`     | acquire the channel's `RawMutex`               |
//! | `unlock_fn`   | release the channel's `RawMutex`               |
//! | `try_fn`      | attempt the channel op while all locks held    |
//! | `enqueue_fn`  | enqueue a sudog on the channel's wait queue    |
//! | `dequeue_fn`  | remove a specific sudog (O(1) cleanup)         |
//!
//! `chan_ptr` is the type-erased `*const Hchan<T>` used as the channel
//! identity for deduplication and address-ordered locking.
//!
//! ## Sentinel index
//!
//! `selectgo` returns `CASE_DEFAULT` (`usize::MAX`) when the default case is
//! taken.  Channel cases use their 0-based index within the slice.

use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::chan::{Hchan, Receiver, Sender};
use crate::runtime::g::{current_g, G, WaitReason};
use crate::runtime::park::{gopark_commit, goready};
use crate::runtime::sudog::{acquire_sudog, release_sudog, Sudog};

// ---------------------------------------------------------------------------
// Public constants
// ---------------------------------------------------------------------------

/// Return value from [`selectgo`] when the default case is taken.
pub const CASE_DEFAULT: usize = usize::MAX;

// ---------------------------------------------------------------------------
// TryResult — outcome of a single case's fast-path attempt
// ---------------------------------------------------------------------------

/// The result of attempting a channel case while all locks are held.
#[derive(Debug)]
pub(crate) enum TryResult {
    /// Case is not immediately satisfiable.
    NotReady,

    /// Case completed via a buffer read/write.
    /// `ok`: true for a normal value, false for a closed-channel receive.
    Done { ok: bool },

    /// Case completed via a direct goroutine-to-goroutine handoff.
    /// The partner goroutine has been set up (`param` set, `success` set) but
    /// not yet made runnable.  Caller must call `goready(gp)` after releasing
    /// all locks.
    Handoff { gp: *mut G, ok: bool },

    /// Send attempted on a closed channel.  Caller must release all locks and
    /// then `panic!("send on closed channel")`.
    ClosedSend,
}

// SAFETY: TryResult is only ever used in a single goroutine between lock
// acquire and lock release; the raw *mut G is not shared across threads.
unsafe impl Send for TryResult {}

// ---------------------------------------------------------------------------
// SCase — one arm of a select statement
// ---------------------------------------------------------------------------

/// One arm of a `select` statement (send, receive, or default).
///
/// Constructed by [`recv_case_of`] / [`send_case_of`]; do not build directly.
#[doc(hidden)]
pub struct SCase {
    /// Type-erased `*const Hchan<T>`.  Used as the channel identity for
    /// deduplication and address-ordered locking.  `null` for a default arm.
    pub(crate) chan_ptr: *const (),

    /// The sudog enrolled on this channel while the goroutine is parked.
    /// Set by `selectgo` in the blocking path; `null` for default and
    /// fast-path returns.
    pub(crate) sg: *mut Sudog,

    /// Type-erased value pointer.
    ///
    /// **Send**: `*mut ManuallyDrop<T>` — the value to send (read by the fn pointers).
    /// **Recv**: `*mut Option<T>` — output slot; written as `Some(val)` on a
    ///           successful receive, left as `None` when the channel is closed.
    /// **Default**: `null`.
    pub(crate) elem: *mut u8,

    // ─── vtable — filled in by select! macro ──────────────────────────────

    /// Acquire the channel's lock.
    pub(crate) lock_fn: unsafe fn(*const ()),

    /// Release the channel's lock.
    pub(crate) unlock_fn: unsafe fn(*const ()),

    /// Try the channel operation while all locks are held.
    ///
    /// Signature: `(chan_ptr, elem) -> TryResult`
    ///
    /// For a send case, `elem` is `*mut ManuallyDrop<T>` (the value to send).
    /// For a recv case, `elem` is `*mut Option<T>` (the output slot).
    pub(crate) try_fn: unsafe fn(*const (), *mut u8) -> TryResult,

    /// Enqueue `sg` on the channel's sendq or recvq (under the lock).
    pub(crate) enqueue_fn: unsafe fn(*const (), *mut Sudog),

    /// Remove `sg` from the channel's sendq or recvq (under the lock).
    /// No-op if `sg` was already removed by a racing channel operation.
    pub(crate) dequeue_fn: unsafe fn(*const (), *mut Sudog),

    /// Phase 2b unlink function — stored into `Sudog.unlink_for_drain` so
    /// the drain can remove this sudog from the channel without knowing `T`.
    pub(crate) unlink_for_drain_fn:
        Option<unsafe extern "C" fn(*mut u8, *mut Sudog) -> bool>,
}

// SAFETY: SCase is always used within a single goroutine context; the raw
// pointers are only shared via the scheduler under goroutine-exclusion.
unsafe impl Send for SCase {}

// ---------------------------------------------------------------------------
// Lehmer RNG — tiny PRNG for poll-order shuffling
// ---------------------------------------------------------------------------

/// A Lehmer (Park–Miller) multiplicative congruential PRNG.
///
/// Used only to produce the random poll order; cryptographic quality is
/// not required.  Seeded from the current goroutine's `goid`.
struct Lehmer(u64);

impl Lehmer {
    fn from_goid() -> Self {
        let gp = current_g();
        // SAFETY: gp is only dereferenced after the null check.
        let goid = if gp.is_null() { 1 } else { (unsafe { (*gp).goid }) | 1 };
        Lehmer(goid | 1) // must be odd and non-zero
    }

    /// Return a pseudo-random value in `[0, n)`.
    fn next_usize(&mut self, n: usize) -> usize {
        // 64-bit Lehmer with multiplier from Knuth TAOCP Vol 2 §3.3.4.
        self.0 = self.0.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        ((self.0 >> 33) as usize) % n
    }
}

// ---------------------------------------------------------------------------
// Commit-park unlock shim
// ---------------------------------------------------------------------------

/// Everything `unlock_select_chans` needs to release every channel lock held
/// by a parked `selectgo`, from g0, after the goroutine is `GWAITING`.
///
/// Lives on the selecting goroutine's stack across the park.  That stack stays
/// mapped while the goroutine is `GWAITING` (and, in the dead-invocation reap
/// path, until `park_fn` finishes and releases the locks here), so the raw
/// pointers remain valid for the single shim call.
struct SelectParkCtx {
    /// `cases.as_ptr()` — the select's case slice base.
    cases:     *const SCase,
    /// `cases.len()`.
    n:         usize,
    /// `lockorder.as_ptr()` — address-sorted, deduped case indices.
    lockorder: *const usize,
    /// `lockorder.len()`.
    nlock:     usize,
}

/// `gopark_commit` unlock shim for `selectgo`: release ALL of the select's
/// channel locks (in lockorder) from g0 after the parking goroutine has
/// reached `GWAITING`.  Each `unlock_fn` is `unlock_chan`, which also drops
/// the `m.locks` increment that the matching `lock_chan` left held.
///
/// # Safety
/// `arg` must point to a live [`SelectParkCtx`] whose channel locks are all
/// held by the parking goroutine.
unsafe fn unlock_select_chans(arg: *mut u8) {
    let ctx       = unsafe { &*(arg as *const SelectParkCtx) };
    let cases     = unsafe { std::slice::from_raw_parts(ctx.cases, ctx.n) };
    let lockorder = unsafe { std::slice::from_raw_parts(ctx.lockorder, ctx.nlock) };
    for &i in lockorder {
        unsafe { (cases[i].unlock_fn)(cases[i].chan_ptr) };
    }
}

// ---------------------------------------------------------------------------
// selectgo
// ---------------------------------------------------------------------------

/// Run a `select` over the given cases.
///
/// `cases` must contain only **channel** cases (send or receive); pass
/// `has_default = true` if the select has a `default` arm.
///
/// Returns `(chosen_index, received_ok)` where:
/// - `chosen_index` is the 0-based index into `cases`, or [`CASE_DEFAULT`] if
///   the default arm was taken.
/// - `received_ok` is `true` for a normal channel recv, `false` if the
///   channel was closed (and the receive wrote `None` into the slot).  Always
///   `false` for send/default arms.
///
/// # Preconditions
///
/// - All `SCase` values must be created by [`recv_case_of`] or [`send_case_of`].
/// - Must be called from a goroutine stack (not g0 or a bare OS thread).
///   A `debug_assert` fires in debug builds if this is violated.
///
/// This function is intended only for use by the `select!` macro.
#[doc(hidden)]
pub fn selectgo(cases: &mut [SCase], has_default: bool) -> (usize, bool) {
    let n = cases.len();

    // ── 1. Build pollorder (random permutation) ───────────────────────────────
    let mut pollorder: Vec<usize> = (0..n).collect();
    let mut rng = Lehmer::from_goid();
    // Fisher-Yates shuffle.
    for i in (1..n).rev() {
        let j = rng.next_usize(i + 1);
        pollorder.swap(i, j);
    }

    // ── 2. Build lockorder (sorted by channel address; dedup same channel) ────
    let mut lockorder: Vec<usize> = (0..n).collect();
    lockorder.sort_by_key(|&i| cases[i].chan_ptr as usize);
    // Deduplicate consecutive equal channels so we don't double-lock.
    lockorder.dedup_by_key(|&mut i| cases[i].chan_ptr as usize);

    // ── 3. Acquire all locks ──────────────────────────────────────────────────
    for &i in &lockorder {
        unsafe { (cases[i].lock_fn)(cases[i].chan_ptr) };
    }

    // ── 4. First pass: check each case in poll order ──────────────────────────
    //
    // `try_fn` dequeues a peer's sudog and reads/writes its `elem` (which, for
    // a select peer, points into that peer's `selectgo` stack frame) entirely
    // under the channel locks acquired in step 3.  Those locks are not
    // released until the `unlock_fn` loop below, *after* the elem access — so
    // a concurrent Phase 2b drain's `unregister_drained_g`, which unlinks the
    // peer's sudog under the same channel lock before any `stack_free`, cannot
    // munmap the peer's stack while we hold the lock (see the happens-before
    // proof on `unregister_drained_g`).  The G descriptor is immortal, so the
    // `gp` we hand to `goready` is always valid.
    for &i in &pollorder {
        let result = unsafe { (cases[i].try_fn)(cases[i].chan_ptr, cases[i].elem) };
        match result {
            TryResult::NotReady => continue,

            TryResult::Done { ok } => {
                // Buffer op completed under the locks; release all and return.
                for &j in &lockorder {
                    unsafe { (cases[j].unlock_fn)(cases[j].chan_ptr) };
                }
                return (i, ok);
            }

            TryResult::Handoff { gp, ok } => {
                // Partner dequeued and set up; release locks, wake partner.
                for &j in &lockorder {
                    unsafe { (cases[j].unlock_fn)(cases[j].chan_ptr) };
                }
                unsafe { goready(gp) };
                return (i, ok);
            }

            TryResult::ClosedSend => {
                for &j in &lockorder {
                    unsafe { (cases[j].unlock_fn)(cases[j].chan_ptr) };
                }
                panic!("send on closed channel");
            }
        }
    }

    // ── 5. Default case ───────────────────────────────────────────────────────
    if has_default {
        for &i in &lockorder {
            unsafe { (cases[i].unlock_fn)(cases[i].chan_ptr) };
        }
        return (CASE_DEFAULT, false);
    }

    // ── 6. Blocking path: enqueue sudogs on all channels ─────────────────────
    let gp = current_g();
    debug_assert!(!gp.is_null(), "selectgo: called from g0");

    for case in cases.iter_mut() {
        let sg = acquire_sudog();
        unsafe {
            (*sg).g         = gp;
            (*sg).elem      = case.elem;
            (*sg).is_select = true;
            (*sg).success   = false;
            (*sg).c         = case.chan_ptr as *mut u8;
            (*sg).unlink_for_drain = case.unlink_for_drain_fn;
        }
        case.sg = sg;
        unsafe { (case.enqueue_fn)(case.chan_ptr, sg) };
        // Phase 2b: link the sudog into gp's waiting list so the drain can
        // unregister it from this case's channel if the goroutine is
        // reclaimed while parked in `gopark(Select)` below.
        unsafe { crate::runtime::g::push_waiting_sudog(gp, sg) };
    }

    // Reset selectdone so this goroutine can be claimed by exactly one case.
    unsafe { (*gp).selectdone.store(0, Ordering::Release) };
    unsafe { (*gp).param = ptr::null_mut() };

    // ── 6c. Commit-park: hold all channel locks across the park ───────────────
    //
    // Releasing the locks *before* gopark (the old design) opened a
    // lost-wakeup window: between the unlock loop and the park an async
    // preemption (SIGURG) could move this selecting goroutine to
    // GRUNNABLE+queued; a peer completing one of our cases would then call
    // goready, see the non-GWAITING status and return early, so when we
    // finally parked (GWAITING) nobody would wake us → hang.  Instead we keep
    // every channel lock held across the `mcall` and release them all on g0,
    // via `unlock_select_chans`, only after `park_fn` has committed us to
    // GWAITING.  A peer cannot dequeue our sudog (that needs the channel
    // lock) until the park is committed.
    //
    // m.locks accounting: each `lock_chan` left one `m.locks` increment held
    // (it `mem::forget`s its MLockGuard), so `m.locks` is currently elevated
    // by `lockorder.len()`; the shim's matching `unlock_chan` calls remove
    // exactly those.  `gopark_commit`/`park_fn` additionally do one
    // unconditional `m.locks -= 1` for the handoff, so we add one extra
    // increment here for park_fn to balance.
    let ctx = SelectParkCtx {
        cases:     cases.as_ptr(),
        n:         cases.len(),
        lockorder: lockorder.as_ptr(),
        nlock:     lockorder.len(),
    };
    std::mem::forget(crate::runtime::m::m_lock());
    unsafe {
        gopark_commit(
            WaitReason::Select,
            unlock_select_chans,
            &ctx as *const SelectParkCtx as *mut u8,
        );
    }

    // ── 7. Woken: find winner, clean up losers ────────────────────────────────
    //
    // The winning channel operation stored the winning sudog in G.param.
    let sg_winner = unsafe { (*gp).param as *mut Sudog };
    unsafe { (*gp).param = ptr::null_mut() };
    let ok = unsafe { (*sg_winner).success };

    // Identify which case won.
    let winner = cases
        .iter()
        .position(|c| c.sg == sg_winner)
        .expect("selectgo: winning sudog not found in cases");

    // 7a. Re-acquire all locks.
    for &i in &lockorder {
        unsafe { (cases[i].lock_fn)(cases[i].chan_ptr) };
    }

    // 7b. Dequeue all losing sudogs from their channels.
    for (i, case) in cases.iter_mut().enumerate() {
        if i == winner { continue; }
        let sg = case.sg;
        unsafe { (case.dequeue_fn)(case.chan_ptr, sg) };
    }

    // 7c. Release all locks.
    for &i in &lockorder {
        unsafe { (cases[i].unlock_fn)(cases[i].chan_ptr) };
    }

    // 7d. Release all sudogs back to the pool.
    for case in cases.iter_mut() {
        let sg = case.sg;
        case.sg = ptr::null_mut();
        unsafe {
            // Phase 2b: unlink the sudog from gp's waiting list now that
            // the select has resolved.
            crate::runtime::g::remove_waiting_sudog(gp, sg);
            (*sg).g    = ptr::null_mut();
            (*sg).elem = ptr::null_mut();
            (*sg).c    = ptr::null_mut();
            release_sudog(sg);
        }
    }

    (winner, ok)
}

// ---------------------------------------------------------------------------
// Generic vtable functions — monomorphised for each T at the call site
// ---------------------------------------------------------------------------

pub(crate) unsafe fn lock_chan<T>(p: *const ()) {
    // Suppress SIGURG-driven async preemption while the channel spinlock is
    // held — same rationale as `LockGuard::new`.  `selectgo` may hold several
    // chan locks at once; each `lock_chan`/`unlock_chan` pair bumps and then
    // decrements `m.locks`, so the counter is back to zero once all locks
    // are released.
    std::mem::forget(crate::runtime::m::m_lock());
    (*(p as *const Hchan<T>)).mutex.lock();
}

pub(crate) unsafe fn unlock_chan<T>(p: *const ()) {
    (*(p as *const Hchan<T>)).mutex.unlock();
    // Manual `m.locks -= 1` to match the `mem::forget`ed guard in
    // `lock_chan`.  We avoid constructing/dropping an MLockGuard here
    // because the lock/unlock are split across two separate functions.
    let mp = crate::runtime::m::current_m();
    if !mp.is_null() {
        (*mp).locks.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

pub(crate) unsafe fn try_send_chan<T: Send + 'static>(
    p: *const (),
    elem: *mut u8,
) -> TryResult {
    let hchan = &*(p as *const Hchan<T>);
    let state = &mut *hchan.state.get();

    if state.closed {
        return TryResult::ClosedSend;
    }

    // Waiting receiver?
    let recv_sg = state.recvq.dequeue();
    if !recv_sg.is_null() {
        let gp = (*recv_sg).g;
        // elem is *mut ManuallyDrop<T> (send slot); recv_sg.elem is *mut Option<T>.
        let ep = (*recv_sg).elem as *mut Option<T>;
        if !ep.is_null() {
            *ep = Some(ptr::read(elem as *const T));
        }
        (*recv_sg).success = true;
        (*gp).param        = recv_sg as *mut u8;
        return TryResult::Handoff { gp, ok: true };
    }

    // Buffer space?
    if state.buf.len() < state.cap {
        state.buf.push_back(ptr::read(elem as *const T));
        return TryResult::Done { ok: true };
    }

    TryResult::NotReady
}

pub(crate) unsafe fn try_recv_chan<T: Send + 'static>(
    p: *const (),
    elem: *mut u8, // *mut Option<T>
) -> TryResult {
    let hchan = &*(p as *const Hchan<T>);
    let state = &mut *hchan.state.get();

    // Waiting sender?
    let send_sg = state.sendq.dequeue();
    if !send_sg.is_null() {
        let gp    = (*send_sg).g;
        // send_sg.elem is *mut ManuallyDrop<T>; use ManuallyDrop explicitly so
        // Box::from_raw for the boxed path does not run T's destructor.
        let ep    = (*send_sg).elem as *mut ManuallyDrop<T>;
        let boxed = (*send_sg).boxed_elem;
        let val = if state.cap == 0 {
            let v = ManuallyDrop::into_inner(ptr::read(ep));
            if boxed { let _ = Box::from_raw(ep); }
            (*send_sg).elem = ptr::null_mut();
            v
        } else {
            let head = state.buf.pop_front().unwrap();
            let sv   = ManuallyDrop::into_inner(ptr::read(ep));
            if boxed { let _ = Box::from_raw(ep); }
            (*send_sg).elem = ptr::null_mut();
            state.buf.push_back(sv);
            head
        };
        *(elem as *mut Option<T>) = Some(val);
        (*send_sg).success = true;
        (*gp).param        = send_sg as *mut u8;
        return TryResult::Handoff { gp, ok: true };
    }

    // Buffer data?
    if !state.buf.is_empty() {
        let val = state.buf.pop_front().unwrap();
        *(elem as *mut Option<T>) = Some(val);
        return TryResult::Done { ok: true };
    }

    // Closed and empty → elem stays None; caller checks ok=false.
    if state.closed {
        return TryResult::Done { ok: false };
    }

    TryResult::NotReady
}

pub(crate) unsafe fn enqueue_send_chan<T: Send + 'static>(p: *const (), sg: *mut Sudog) {
    let hchan = &*(p as *const Hchan<T>);
    (*hchan.state.get()).sendq.enqueue(sg);
}

pub(crate) unsafe fn enqueue_recv_chan<T: Send + 'static>(p: *const (), sg: *mut Sudog) {
    let hchan = &*(p as *const Hchan<T>);
    (*hchan.state.get()).recvq.enqueue(sg);
}

pub(crate) unsafe fn dequeue_send_chan<T: Send + 'static>(p: *const (), sg: *mut Sudog) {
    let hchan = &*(p as *const Hchan<T>);
    (*hchan.state.get()).sendq.dequeue_sudog(sg);
}

pub(crate) unsafe fn dequeue_recv_chan<T: Send + 'static>(p: *const (), sg: *mut Sudog) {
    let hchan = &*(p as *const Hchan<T>);
    (*hchan.state.get()).recvq.dequeue_sudog(sg);
}

// ---------------------------------------------------------------------------
// Public factory functions — used by the select! macro
// ---------------------------------------------------------------------------

/// Build a receive [`SCase`] for use in [`selectgo`].
///
/// `slot` must point to an `Option<T>` initialised to `None` that outlives the
/// `selectgo` call.  On a successful receive (`ok = true`) the slot is
/// `Some(value)`; on `ok = false` (channel closed) the slot remains `None`.
///
/// Called by the `select!` macro; not intended for direct use.
#[doc(hidden)]
pub fn recv_case_of<T: Send + 'static>(rx: &Receiver<T>, slot: *mut Option<T>) -> SCase {
    SCase {
        chan_ptr:    Arc::as_ptr(rx.hchan()) as *const (),
        sg:          ptr::null_mut(),
        elem:        slot as *mut u8,
        lock_fn:     lock_chan::<T>,
        unlock_fn:   unlock_chan::<T>,
        try_fn:      try_recv_chan::<T>,
        enqueue_fn:  enqueue_recv_chan::<T>,
        dequeue_fn:  dequeue_recv_chan::<T>,
        unlink_for_drain_fn: Some(crate::chan::unlink_sudog_for_drain::<T>),
    }
}

/// Build a send [`SCase`] for use in [`selectgo`].
///
/// `val` must point to a `ManuallyDrop<T>` that outlives the `selectgo` call.
/// If the case wins, the value is moved into the channel and the caller must
/// **not** drop `*val`.  If the case loses, the caller must call
/// `ManuallyDrop::drop(val)` to avoid a leak.
///
/// Called by the `select!` macro; not intended for direct use.
#[doc(hidden)]
pub fn send_case_of<T: Send + 'static>(tx: &Sender<T>, val: *mut ManuallyDrop<T>) -> SCase {
    SCase {
        chan_ptr:    Arc::as_ptr(tx.hchan()) as *const (),
        sg:          ptr::null_mut(),
        elem:        val as *mut u8,
        lock_fn:     lock_chan::<T>,
        unlock_fn:   unlock_chan::<T>,
        try_fn:      try_send_chan::<T>,
        enqueue_fn:  enqueue_send_chan::<T>,
        dequeue_fn:  dequeue_send_chan::<T>,
        unlink_for_drain_fn: Some(crate::chan::unlink_sudog_for_drain::<T>),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::chan::{chan, Hchan};
    use crate::runtime::sudog::Sudog;
    use crate::runtime::sched::run_impl;
    use std::ptr;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    // ─── vtable helpers for Hchan<i32> ────────────────────────────────────────

    unsafe fn lock_i32(p: *const ()) {
        (*(p as *const Hchan<i32>)).mutex.lock();
    }
    unsafe fn unlock_i32(p: *const ()) {
        unsafe { (*(p as *const Hchan<i32>)).mutex.unlock() };
    }

    /// try_fn for a **send** case on `Hchan<i32>`.
    ///
    /// `elem` points to a `i32` to send.  Checks recvq and buffer.
    unsafe fn try_send_i32(p: *const (), elem: *mut u8) -> TryResult {
        let hchan = &*(p as *const Hchan<i32>);
        // SAFETY: caller holds the channel lock.
        let state = &mut *hchan.state.get();

        if state.closed {
            return TryResult::ClosedSend;
        }

        // Waiting receiver?
        let recv_sg = state.recvq.dequeue();
        if !recv_sg.is_null() {
            let gp  = (*recv_sg).g;
            let ep  = (*recv_sg).elem as *mut Option<i32>;
            if !ep.is_null() {
                *ep = Some(ptr::read(elem as *const i32));
            }
            (*recv_sg).success = true;
            (*gp).param        = recv_sg as *mut u8;
            return TryResult::Handoff { gp, ok: true };
        }

        // Buffer space?
        if state.buf.len() < state.cap {
            state.buf.push_back(ptr::read(elem as *const i32));
            return TryResult::Done { ok: true };
        }

        TryResult::NotReady
    }

    /// try_fn for a **recv** case on `Hchan<i32>`.
    ///
    /// `elem` points to an `Option<i32>` output slot (initialised to `None`).
    unsafe fn try_recv_i32(p: *const (), elem: *mut u8) -> TryResult {
        let hchan = &*(p as *const Hchan<i32>);
        let state = &mut *hchan.state.get();

        // Waiting sender?
        let send_sg = state.sendq.dequeue();
        if !send_sg.is_null() {
            let gp    = (*send_sg).g;
            let ep    = (*send_sg).elem as *mut ManuallyDrop<i32>;
            let boxed = (*send_sg).boxed_elem;
            let val = if state.cap == 0 {
                let v = ManuallyDrop::into_inner(ptr::read(ep));
                if boxed { let _ = Box::from_raw(ep); }
                (*send_sg).elem = ptr::null_mut();
                v
            } else {
                let head = state.buf.pop_front().unwrap();
                let sv   = ManuallyDrop::into_inner(ptr::read(ep));
                if boxed { let _ = Box::from_raw(ep); }
                (*send_sg).elem = ptr::null_mut();
                state.buf.push_back(sv);
                head
            };
            *(elem as *mut Option<i32>) = Some(val);
            (*send_sg).success = true;
            (*gp).param        = send_sg as *mut u8;
            return TryResult::Handoff { gp, ok: true };
        }

        // Buffer has data?
        if !state.buf.is_empty() {
            let val = state.buf.pop_front().unwrap();
            *(elem as *mut Option<i32>) = Some(val);
            return TryResult::Done { ok: true };
        }

        // Closed and empty → elem stays None.
        if state.closed {
            return TryResult::Done { ok: false };
        }

        TryResult::NotReady
    }

    unsafe fn enqueue_send_i32(p: *const (), sg: *mut Sudog) {
        let hchan = &*(p as *const Hchan<i32>);
        (*hchan.state.get()).sendq.enqueue(sg);
    }
    unsafe fn enqueue_recv_i32(p: *const (), sg: *mut Sudog) {
        let hchan = &*(p as *const Hchan<i32>);
        (*hchan.state.get()).recvq.enqueue(sg);
    }
    unsafe fn dequeue_send_sg_i32(p: *const (), sg: *mut Sudog) {
        let hchan = &*(p as *const Hchan<i32>);
        (*hchan.state.get()).sendq.dequeue_sudog(sg);
    }
    unsafe fn dequeue_recv_sg_i32(p: *const (), sg: *mut Sudog) {
        let hchan = &*(p as *const Hchan<i32>);
        (*hchan.state.get()).recvq.dequeue_sudog(sg);
    }

    /// Build an `SCase` for a buffered-send of `val` on channel `h`.
    fn send_case(h: &Arc<Hchan<i32>>, val: &mut i32) -> SCase {
        SCase {
            chan_ptr:   Arc::as_ptr(h) as *const (),
            sg:        ptr::null_mut(),
            elem:      val as *mut i32 as *mut u8,
            lock_fn:   lock_i32,
            unlock_fn: unlock_i32,
            try_fn:    try_send_i32,
            enqueue_fn: enqueue_send_i32,
            dequeue_fn: dequeue_send_sg_i32,
            unlink_for_drain_fn: Some(crate::chan::unlink_sudog_for_drain::<i32>),
        }
    }

    /// Build an `SCase` for a recv on channel `h`, output into `slot`.
    fn recv_case(h: &Arc<Hchan<i32>>, slot: &mut Option<i32>) -> SCase {
        SCase {
            chan_ptr:   Arc::as_ptr(h) as *const (),
            sg:        ptr::null_mut(),
            elem:      slot as *mut Option<i32> as *mut u8,
            lock_fn:   lock_i32,
            unlock_fn: unlock_i32,
            try_fn:    try_recv_i32,
            enqueue_fn: enqueue_recv_i32,
            dequeue_fn: dequeue_recv_sg_i32,
            unlink_for_drain_fn: Some(crate::chan::unlink_sudog_for_drain::<i32>),
        }
    }

    // ── Fast-path tests (no goroutine park) ───────────────────────────────────

    /// select { rx.recv() => ... ; default } on a buffered channel with data.
    #[test]
    fn fast_recv_buffered() {
        run_impl(|| {
            let (tx, rx) = chan::<i32>(4);
            tx.send(42);

            let mut slot: Option<i32> = None;
            let mut cases = [recv_case(rx.hchan(), &mut slot)];
            let (idx, ok) = selectgo(&mut cases, true);

            assert_eq!(idx, 0, "should pick recv case");
            assert!(ok,        "should be ok (not closed)");
            assert_eq!(slot.unwrap(), 42);
        });
    }

    /// select { tx.send(v) => ... ; default } on a channel with buffer space.
    #[test]
    fn fast_send_buffered() {
        run_impl(|| {
            let (tx, rx) = chan::<i32>(4);

            let mut val = 99_i32;
            let mut cases = [send_case(tx.hchan(), &mut val)];
            let (idx, ok) = selectgo(&mut cases, true);

            assert_eq!(idx, 0);
            assert!(ok, "buffered send completes with ok=true");
            assert_eq!(rx.recv(), Some(99));
        });
    }

    /// select { ... ; default } when no case is ready → default taken.
    #[test]
    fn default_taken_when_not_ready() {
        run_impl(|| {
            let (_tx, rx) = chan::<i32>(0);

            let mut slot: Option<i32> = None;
            let mut cases = [recv_case(rx.hchan(), &mut slot)];
            let (idx, ok) = selectgo(&mut cases, true);

            assert_eq!(idx, CASE_DEFAULT);
            assert!(!ok);
        });
    }

    /// select recv on closed+empty channel returns ok=false.
    #[test]
    fn recv_closed_empty() {
        run_impl(|| {
            let (tx, rx) = chan::<i32>(0);
            tx.close();

            let mut slot: Option<i32> = None;
            let mut cases = [recv_case(rx.hchan(), &mut slot)];
            let (idx, ok) = selectgo(&mut cases, false);

            assert_eq!(idx, 0);
            assert!(!ok, "recv from closed returns ok=false");
            assert!(slot.is_none(), "closed recv slot must stay None");
        });
    }

    // ── Multi-case selection ──────────────────────────────────────────────────

    /// Two recv cases; only one channel has data — that case wins.
    #[test]
    fn multi_case_first_ready_wins() {
        run_impl(|| {
            let (tx1, rx1) = chan::<i32>(1);
            let (_tx2, rx2) = chan::<i32>(1);

            tx1.send(7);

            let mut s1: Option<i32> = None;
            let mut s2: Option<i32> = None;
            let mut cases = [
                recv_case(rx1.hchan(), &mut s1),
                recv_case(rx2.hchan(), &mut s2),
            ];
            let (idx, ok) = selectgo(&mut cases, false);

            assert_eq!(idx, 0);
            assert!(ok);
            assert_eq!(s1.unwrap(), 7);
        });
    }

    // ── Blocking path tests (goroutine park/unpark) ───────────────────────────

    /// Goroutine blocks on select recv, then a sender unblocks it.
    #[test]
    fn blocking_recv_unblocked_by_send() {
        use crate::runtime::sched::spawn_goroutine;

        let result = Arc::new(AtomicI32::new(-1));
        let result2 = Arc::clone(&result);

        run_impl(move || {
            let (tx, rx) = chan::<i32>(0);

            spawn_goroutine(move || {
                // Sender: wait a bit, then send.
                crate::gosched();
                tx.send(55);
            });

            let mut slot: Option<i32> = None;
            let mut cases = [recv_case(rx.hchan(), &mut slot)];
            // No default → will block.
            let (idx, ok) = selectgo(&mut cases, false);

            assert_eq!(idx, 0);
            assert!(ok);
            result2.store(slot.unwrap(), Ordering::Relaxed);
        });

        assert_eq!(result.load(Ordering::Acquire), 55);
    }

    /// Goroutine blocks on select send, then a receiver unblocks it.
    #[test]
    fn blocking_send_unblocked_by_recv() {
        use crate::runtime::sched::spawn_goroutine;

        run_impl(|| {
            let (tx, rx) = chan::<i32>(0);

            spawn_goroutine(move || {
                crate::gosched();
                // Consume the value the select sends.
                let _ = rx.recv();
            });

            let mut val = 77_i32;
            let mut cases = [send_case(tx.hchan(), &mut val)];
            let (idx, _ok) = selectgo(&mut cases, false);

            assert_eq!(idx, 0);
        });
    }

    /// Two goroutines racing on the same channel; exactly one wins via select.
    #[test]
    fn select_race_one_winner() {
        use crate::runtime::sched::spawn_goroutine;

        let wins = Arc::new(AtomicI32::new(0));
        let wins2 = Arc::clone(&wins);
        let wins3 = Arc::clone(&wins);
        let wins4 = Arc::clone(&wins);

        run_impl(move || {
            let (tx, rx) = chan::<i32>(1);
            tx.send(1); // one value in the buffer

            spawn_goroutine({
                let wins = Arc::clone(&wins2);
                let rx = rx.clone();
                move || {
                    let mut slot: Option<i32> = None;
                    let mut cases = [recv_case(rx.hchan(), &mut slot)];
                    let (idx, ok) = selectgo(&mut cases, true);
                    if idx == 0 && ok { wins.fetch_add(1, Ordering::Relaxed); }
                }
            });

            spawn_goroutine({
                let wins = Arc::clone(&wins3);
                let rx = rx.clone();
                move || {
                    let mut slot: Option<i32> = None;
                    let mut cases = [recv_case(rx.hchan(), &mut slot)];
                    let (idx, ok) = selectgo(&mut cases, true);
                    if idx == 0 && ok { wins.fetch_add(1, Ordering::Relaxed); }
                }
            });

            // Poll on the atomic with a wall-clock deadline so the test is
            // robust to per-goroutine startup latency (a one-shot stack
            // pre-grow + scheduler wakeup is ~50 µs, and the loser goroutine
            // blocks forever in selectgo — we just need the winner to record
            // its win).  Five seconds is comfortable headroom even on slow
            // CI runners.
            let deadline =
                std::time::Instant::now() + std::time::Duration::from_secs(5);
            while wins4.load(Ordering::Acquire) < 1
                && std::time::Instant::now() < deadline
            {
                crate::gosched();
            }
        });

        // Exactly one goroutine should have received the value.
        assert_eq!(wins.load(Ordering::Acquire), 1);
    }
}
