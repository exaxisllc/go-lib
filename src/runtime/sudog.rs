// SPDX-License-Identifier: Apache-2.0
//! Waiter records (`sudog`) and wait queues (`waitq`).
//!
//! Ported from `runtime/runtime2.go` (`sudog`, `waitq`) and the
//! `acquireSudog` / `releaseSudog` helpers in `runtime/proc.go`.
//!
//! ## What a `Sudog` is
//!
//! Every time a goroutine blocks on a channel send or receive, a `Sudog` is
//! allocated to represent that waiter in the channel's `sendq` or `recvq`.
//! The fields carry:
//!
//! - which goroutine is blocked (`g`)
//! - where the data lives (`elem` — a pointer into the blocked goroutine's stack)
//! - whether the operation succeeded (`success`, false when the channel was closed)
//! - whether this is part of a `select` (`is_select`)
//!
//! A `sudog` is returned to the free list by [`release_sudog`] after the
//! channel operation completes.
//!
//! ## Allocation / caching
//!
//! Go caches sudogs per-P (a `[]*sudog` slice, up to 128 elements) plus a
//! global overflow list protected by a dedicated lock.  v1 uses a single
//! global free list (`SUDOG_CACHE`) for simplicity.  The per-P tier can be
//! layered on top in step 15.5 without changing the `Sudog` struct or call
//! sites.
//!
//! ## Wait queues
//!
//! [`WaitQ`] is the doubly-linked FIFO of sudogs that each channel maintains
//! for its blocked senders (`sendq`) and blocked receivers (`recvq`).
//! Ported from `waitq` / `waitq.enqueue` / `waitq.dequeue` in
//! `runtime/runtime2.go` and `runtime/chan.go`.

use std::sync::atomic::Ordering;
use std::sync::Mutex;

use super::g::G;

// ---------------------------------------------------------------------------
// Sudog
// ---------------------------------------------------------------------------

/// One goroutine waiting in one channel or select operation.
///
/// Instances are obtained from the global free list via [`acquire_sudog`] and
/// returned via [`release_sudog`].  The layout intentionally mirrors Go's
/// `sudog` so field names in the channel and select code map directly.
///
/// Ported from `sudog` in `runtime/runtime2.go`.
pub(crate) struct Sudog {
    /// The goroutine that is waiting.
    ///
    /// Cleared to `null` by the caller *before* calling [`release_sudog`].
    pub g: *mut G,

    /// Next sudog in the doubly-linked wait queue (`sendq` / `recvq`).
    ///
    /// Also reused as the intrusive free-list link while the sudog is cached.
    /// The channel always clears this field after dequeueing.
    pub next: *mut Sudog,

    /// The wait queue this sudog is currently enqueued on, or `null` when it is
    /// not in any queue.
    ///
    /// A sudog lives in **exactly one** queue at a time (a channel's `sendq` or
    /// `recvq`).  `selectgo`'s cleanup pass calls [`WaitQ::dequeue_sudog`] on
    /// every losing case to cancel its sudog — but a peer's `dequeue` (or a
    /// lost `selectdone` race) may have already unlinked that sudog from the
    /// queue concurrently.  Without an owner record, `dequeue_sudog` cannot
    /// tell "this sudog is the tail of the *other* queue" from "the tail of
    /// *this* queue" using only `prev`/`next`, and would splice the node out
    /// using the wrong queue's head/tail — leaving a queue's `last` dangling at
    /// a removed (and about-to-be-nulled) sudog (observed as `chansend` /
    /// `try_recv_chan` dequeuing a sudog with a null `g`/`elem`, plus
    /// lost-wakeup hangs).  This field makes queue membership authoritative:
    /// `enqueue` sets it, `dequeue` / `dequeue_sudog` clear it, and
    /// `dequeue_sudog` is a no-op unless the sudog actually belongs to the
    /// queue it was called on.
    ///
    /// Only ever mutated under the owning channel's lock, so it is consistent
    /// for every reader.
    pub q: *mut WaitQ,

    /// Previous sudog in the doubly-linked wait queue.
    ///
    /// `null` while the sudog is in the free list.
    pub prev: *mut Sudog,

    /// Pointer to the data element involved in this operation.
    ///
    /// **Send**: address of the value being sent — lives in the sender's
    /// goroutine stack while that G is parked.
    /// **Receive**: address of the variable that will receive the value —
    /// lives in the receiver's goroutine stack while parked — or `null` if
    /// the caller discards the received value (`<-ch` without an lvalue).
    ///
    /// Typed as `*mut u8` (erased type) because the runtime has no compile-time
    /// knowledge of the channel element type; the channel code casts to the
    /// concrete `*mut T`.
    ///
    /// Cleared to `null` by the caller *before* calling [`release_sudog`].
    pub elem: *mut u8,

    /// `true` while this sudog is part of a `select` operation.
    ///
    /// When `true`, `selectgo` will race via `g.selectdone` to claim the win
    /// before actually performing the channel operation.
    pub is_select: bool,

    /// Whether the channel operation completed successfully.
    ///
    /// Set to `false` by the channel-close path to signal that the goroutine
    /// was unblocked because the channel was closed, not because a peer
    /// completed the complementary operation.  Mirrors Go's `sudog.success`.
    pub success: bool,

    /// Whether `elem` is a heap-allocated `Box` that must be freed after the
    /// value is consumed.
    ///
    /// `true`  — allocated by `chansend` (send path: `Box<ManuallyDrop<T>>`)
    ///           or `chanrecv` (recv path: `Box<Option<T>>`); the consuming
    ///           side must call `Box::from_raw(elem)` after reading.
    /// `false` — `elem` is a direct stack/select-slot pointer;
    ///           the consuming side must NOT call `Box::from_raw`.
    pub boxed_elem: bool,

    /// The channel this sudog is queued on.
    ///
    /// Typed `*mut u8` until `chan::Hchan` is defined (step 13).
    pub c: *mut u8,
}

// SAFETY: Sudog instances are exchanged between goroutines only through the
// channel machinery, which ensures at most one goroutine accesses a given
// Sudog at any point in time.
unsafe impl Send for Sudog {}
unsafe impl Sync for Sudog {}

impl Sudog {
    /// Return a fully-zeroed `Sudog`.  Used both for fresh allocations and for
    /// sanitising a recycled instance before handing it to the caller.
    fn zeroed() -> Self {
        Sudog {
            g:          std::ptr::null_mut(),
            next:       std::ptr::null_mut(),
            q:          std::ptr::null_mut(),
            prev:       std::ptr::null_mut(),
            elem:       std::ptr::null_mut(),
            is_select:  false,
            success:    false,
            boxed_elem: false,
            c:          std::ptr::null_mut(),
        }
    }
}

// ---------------------------------------------------------------------------
// WaitQ — doubly-linked FIFO of sudogs
// ---------------------------------------------------------------------------

/// A doubly-linked FIFO of [`Sudog`] waiters.
///
/// Used as a channel's `sendq` (goroutines blocked sending) and `recvq`
/// (goroutines blocked receiving).  The queue is *not* protected by its own
/// lock; the enclosing channel struct's `Mutex` serialises all access.
///
/// Ported from `waitq` in `runtime/runtime2.go` and its methods in
/// `runtime/chan.go`.
pub(crate) struct WaitQ {
    /// First (oldest) waiter, or `null` when the queue is empty.
    pub first: *mut Sudog,
    /// Last (newest) waiter, or `null` when the queue is empty.
    pub last:  *mut Sudog,
}

// SAFETY: WaitQ is always accessed under a channel lock.
unsafe impl Send for WaitQ {}

impl WaitQ {
    /// Create an empty queue.
    pub(crate) const fn new() -> Self {
        WaitQ { first: std::ptr::null_mut(), last: std::ptr::null_mut() }
    }

    /// Return `true` if the queue has no waiters.
    #[inline]
    #[allow(dead_code)] // exercised by unit tests; no production caller yet
    pub(crate) fn is_empty(&self) -> bool {
        self.first.is_null()
    }

    /// Append `sgp` to the back of the queue.
    ///
    /// `sgp.next` and `sgp.prev` are set by this function; the caller must
    /// not rely on their previous values.
    ///
    /// Ported from `waitq.enqueue` in `runtime/chan.go`.
    pub(crate) unsafe fn enqueue(&mut self, sgp: *mut Sudog) {
        unsafe {
            (*sgp).next = std::ptr::null_mut();
            // Record the owning queue so `dequeue_sudog` can authoritatively
            // tell which of a channel's two queues this sudog lives in (see the
            // `Sudog.q` doc-comment).
            (*sgp).q = self as *mut WaitQ;

            let last = self.last;
            if last.is_null() {
                // Queue was empty.
                (*sgp).prev = std::ptr::null_mut();
                self.first   = sgp;
                self.last    = sgp;
            } else {
                (*sgp).prev  = last;
                (*last).next = sgp;
                self.last    = sgp;
            }
        }
    }

    /// Remove and return the oldest waiter, or `null` if the queue is empty.
    ///
    /// For `is_select` sudogs (enrolled in a `select` statement) the function
    /// races via `g.selectdone` (a CAS 0 → 1) to claim the win.  Only one
    /// case across all channels may claim the same goroutine; losers are
    /// **unlinked from this queue but not released** — `selectgo`'s cleanup
    /// phase releases them after re-acquiring all locks.
    ///
    /// Non-select sudogs are always returned without further filtering; by
    /// invariant they are in `GWAITING` when `dequeue` is called.
    ///
    /// The returned sudog has its `next` and `prev` fields cleared.
    ///
    /// Ported from `waitq.dequeue` in `runtime/chan.go`.
    pub(crate) unsafe fn dequeue(&mut self) -> *mut Sudog {
        loop {
            let sgp = self.first;
            if sgp.is_null() {
                return std::ptr::null_mut();
            }

            // Unlink from the front.
            let next = unsafe { (*sgp).next };
            if next.is_null() {
                self.first = std::ptr::null_mut();
                self.last  = std::ptr::null_mut();
            } else {
                unsafe { (*next).prev = std::ptr::null_mut() };
                self.first = next;
                unsafe { (*sgp).next = std::ptr::null_mut() };
            }
            unsafe { (*sgp).prev = std::ptr::null_mut() };
            // Unlinked from this queue (whether it wins below or is a skipped
            // select loser) — clear the owner record.
            unsafe { (*sgp).q = std::ptr::null_mut() };

            // For select sudogs, race to claim the win.
            if unsafe { (*sgp).is_select } {
                let gp = unsafe { (*sgp).g };
                let won = !gp.is_null()
                    && unsafe {
                        (*gp).selectdone
                            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed)
                    }
                    .is_ok();
                if !won {
                    // Loser: already unlinked from the queue.  Do NOT release —
                    // selectgo cleanup will release it.
                    continue;
                }
            }

            return sgp;
        }
    }

    /// Remove a specific sudog from anywhere in the queue (O(1) via `prev`/`next`).
    ///
    /// Used by `selectgo` cleanup to cancel losing sudogs on non-winning
    /// channels.  If `sgp` was already unlinked (e.g. a racing `dequeue` on
    /// that channel already removed it), the function returns immediately
    /// without modifying the queue.
    ///
    /// Caller must hold the channel lock.
    ///
    /// Ported from `dequeueSudoG` in `runtime/chan.go`.
    pub(crate) unsafe fn dequeue_sudog(&mut self, sgp: *mut Sudog) -> bool {
        // Authoritative membership check.  A sudog lives in exactly one queue;
        // `sgp.q` records which.  If it is not *this* queue, do nothing — the
        // sudog was already dequeued (q == null, e.g. a racing peer's `dequeue`
        // or a lost `selectdone` race claimed it).  Relying on `prev`/`next`
        // alone is unsound: those links belong to whatever queue the sudog is
        // really in, so splicing through them while operating on the wrong queue
        // corrupts that queue's head/tail and leaves a dangling `last` (see the
        // `Sudog.q` doc-comment).
        if !std::ptr::eq(unsafe { (*sgp).q }, self) {
            return false;
        }

        let prev = unsafe { (*sgp).prev };
        let next = unsafe { (*sgp).next };

        if !prev.is_null() {
            unsafe { (*prev).next = next };
        } else {
            self.first = next;
        }

        if !next.is_null() {
            unsafe { (*next).prev = prev };
        } else {
            self.last = prev;
        }

        unsafe {
            (*sgp).next = std::ptr::null_mut();
            (*sgp).prev = std::ptr::null_mut();
            (*sgp).q    = std::ptr::null_mut();
        }
        true
    }
}

// ---------------------------------------------------------------------------
// Sudog free lists — per-P caches + a global central overflow list
// ---------------------------------------------------------------------------

/// An intrusive LIFO free list of `Sudog`s, chained through `Sudog.next`.
///
/// Two tiers use this type:
/// * one per `P` (`P.sudog_cache`) — the fast path, almost always
///   uncontended; refilled from / flushed to the central list in batches;
/// * one process-wide central list ([`SUDOG_CENTRAL`]) — the overflow tier
///   and the path taken when there is no current `P` (the `run_impl` caller
///   and unit tests).
///
/// Mirrors Go's `pp.sudogcache` + `sched.sudogcache` split (`acquireSudog` /
/// `releaseSudog` in `runtime/proc.go`).
pub(crate) struct SudogCache {
    head:  *mut Sudog,
    count: usize,
}

// SAFETY: a SudogCache is only ever accessed while holding the Mutex that
// wraps it (the per-P one on `P`, or `SUDOG_CENTRAL`).
unsafe impl Send for SudogCache {}

impl SudogCache {
    pub(crate) const fn new() -> Self {
        SudogCache { head: std::ptr::null_mut(), count: 0 }
    }

    /// Pop the head, or null if empty.
    #[inline]
    fn pop(&mut self) -> *mut Sudog {
        let s = self.head;
        if !s.is_null() {
            self.head = unsafe { (*s).next };
            self.count -= 1;
        }
        s
    }

    /// Push `s` onto the head.
    #[inline]
    fn push(&mut self, s: *mut Sudog) {
        unsafe { (*s).next = self.head };
        self.head = s;
        self.count += 1;
    }
}

/// Per-P local cache capacity (Go's `len(pp.sudogbuf)` = 128).  On overflow,
/// half is flushed to the central list; on underflow, a batch is pulled from
/// it.
const LOCAL_CAP: usize = 128;

/// Number of sudogs moved in a single refill/flush between a P cache and the
/// central list (Go flushes/​refills half the local buffer).
const BATCH: usize = LOCAL_CAP / 2;

/// Cap on the central overflow list, bounding total retained free sudogs to
/// roughly `CENTRAL_CAP + LOCAL_CAP * GOMAXPROCS`.  Beyond it, released
/// sudogs are freed back to the allocator.
const CENTRAL_CAP: usize = 4_096;

/// Process-wide central sudog free list — overflow tier and the no-P path.
static SUDOG_CENTRAL: Mutex<SudogCache> = Mutex::new(SudogCache::new());

// ---------------------------------------------------------------------------
// acquire_sudog / release_sudog
// ---------------------------------------------------------------------------

/// Acquire a zeroed `Sudog`, allocating one if every tier is empty.
///
/// Fast path: pop the current P's local cache (refilling it from the central
/// list in one batch if empty).  With no current P, pop the central list
/// directly.  The returned pointer is exclusively owned by the caller until
/// passed to [`release_sudog`].
///
/// Ported from `acquireSudog` in `runtime/proc.go`.
pub(crate) fn acquire_sudog() -> *mut Sudog {
    // Pin to this M (m.locks > 0) across the cache critical section.  The per-P
    // `sudog_cache` and `SUDOG_CENTRAL` are plain `Mutex`es; if SIGURG async-
    // preempts a goroutine holding one and the scheduler then runs another
    // goroutine that touches the same cache, it deadlocks on the held lock.
    // `m_lock` makes `sigurg_handler`'s Guard 0 skip preemption here (same
    // contract the channel `RawMutex` path relies on).  Dropped last, after the
    // cache lock.
    let _mlk = super::m::m_lock();
    let p = super::sched::current_p();
    let s = if !p.is_null() {
        // Per-P fast path.  Lock ordering is always local-then-central.
        let pref = unsafe { &*p };
        let mut local = pref.sudog_cache.lock().unwrap();
        if local.head.is_null() {
            let mut central = SUDOG_CENTRAL.lock().unwrap();
            for _ in 0..BATCH {
                let t = central.pop();
                if t.is_null() {
                    break;
                }
                local.push(t);
            }
        }
        local.pop() // null if local + central were both empty
    } else {
        SUDOG_CENTRAL.lock().unwrap().pop()
    };

    if s.is_null() {
        Box::into_raw(Box::new(Sudog::zeroed()))
    } else {
        // Zero-out any stale fields from the previous use.
        unsafe { std::ptr::write(s, Sudog::zeroed()) };
        s
    }
}

/// Return a `Sudog` to the free list after its channel operation completes.
///
/// The caller **must** clear `s.g`, `s.elem`, and `s.c` before calling.
/// This matches Go's `releaseSudog` precondition (panics in debug mode if
/// the pointer fields are non-null on arrival).
///
/// Fast path: push onto the current P's local cache (flushing half to the
/// central list first if it is full).  With no current P, push onto the
/// central list, or free the sudog if the central list is at capacity.
///
/// # Safety
/// `s` must have been obtained from [`acquire_sudog`] and must not be used
/// after this call.
///
/// Ported from `releaseSudog` in `runtime/proc.go`.
pub(crate) unsafe fn release_sudog(s: *mut Sudog) {
    debug_assert!(
        unsafe { (*s).elem.is_null() },
        "release_sudog: elem not cleared"
    );
    debug_assert!(
        unsafe { (*s).g.is_null() },
        "release_sudog: g not cleared"
    );
    debug_assert!(
        unsafe { (*s).c.is_null() },
        "release_sudog: c not cleared"
    );

    // Pin to this M across the cache critical section — see `acquire_sudog`.
    let _mlk = super::m::m_lock();
    let p = super::sched::current_p();
    if !p.is_null() {
        // Per-P fast path.  Lock ordering is always local-then-central.
        let pref = unsafe { &*p };
        let mut local = pref.sudog_cache.lock().unwrap();
        if local.count >= LOCAL_CAP {
            // Flush half the local cache into the central list so other Ps can
            // reuse it (and so this list stays bounded).
            let mut central = SUDOG_CENTRAL.lock().unwrap();
            for _ in 0..BATCH {
                let t = local.pop();
                if t.is_null() {
                    break;
                }
                if central.count < CENTRAL_CAP {
                    central.push(t);
                } else {
                    let _ = unsafe { Box::from_raw(t) };
                }
            }
        }
        local.push(s);
    } else {
        let mut central = SUDOG_CENTRAL.lock().unwrap();
        if central.count < CENTRAL_CAP {
            central.push(s);
        } else {
            drop(central);
            let _ = unsafe { Box::from_raw(s) };
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::runtime::g::GWAITING;
    use std::sync::atomic::Ordering::Release;

    // Helper: allocate a minimal G for tests (just needs a stable address).
    fn make_g() -> *mut G {
        use crate::runtime::g::{Stack, G};
        Box::into_raw(G::new(Stack { lo: 0x100000, hi: 0x110000 }, 1))
    }

    // ---------------------------------------------------------------------------
    // Sudog allocation / caching
    // ---------------------------------------------------------------------------

    /// A freshly acquired sudog has all pointer fields null and bool flags false.
    #[test]
    fn acquire_returns_zeroed_sudog() {
        let s = acquire_sudog();
        unsafe {
            assert!((*s).g.is_null(),         "g must be null");
            assert!((*s).next.is_null(),      "next must be null");
            assert!((*s).prev.is_null(),      "prev must be null");
            assert!((*s).elem.is_null(),      "elem must be null");
            assert!(!(*s).is_select,          "is_select must be false");
            assert!(!(*s).success,            "success must be false");
            assert!((*s).c.is_null(),         "c must be null");
        }
        // Clean up: clear required fields then release.
        unsafe { release_sudog(s) };
    }

    /// A released sudog is recycled by the next acquire call.
    #[test]
    fn released_sudog_is_reused() {
        // Drain any cached sudogs so we get a fresh allocation.
        let s1 = acquire_sudog();
        let addr1 = s1 as usize;
        unsafe { release_sudog(s1) };

        let s2 = acquire_sudog();
        let addr2 = s2 as usize;
        // The next acquire should return the just-released sudog.
        assert_eq!(addr1, addr2, "acquire should reuse the just-released sudog");
        // And it must be zeroed again.
        unsafe {
            assert!((*s2).g.is_null(),    "recycled sudog.g must be null");
            assert!((*s2).elem.is_null(), "recycled sudog.elem must be null");
            assert!((*s2).c.is_null(),    "recycled sudog.c must be null");
        }
        unsafe { release_sudog(s2) };
    }

    // ---------------------------------------------------------------------------
    // WaitQ
    // ---------------------------------------------------------------------------

    /// Enqueueing into an empty WaitQ sets both first and last.
    #[test]
    fn waitq_enqueue_into_empty() {
        let gp = make_g();
        let s  = acquire_sudog();
        unsafe { (*s).g = gp };

        let mut q = WaitQ::new();
        assert!(q.is_empty());

        unsafe { q.enqueue(s) };
        assert!(!q.is_empty());
        assert_eq!(q.first, s);
        assert_eq!(q.last,  s);
        unsafe {
            assert!((*s).prev.is_null(), "first element has no prev");
            assert!((*s).next.is_null(), "only element has no next");
        }

        // Clean up
        unsafe {
            (*s).g = std::ptr::null_mut();
            (*s).c = std::ptr::null_mut();
            release_sudog(s);
            let _ = Box::from_raw(gp);
        }
    }

    /// Enqueue two sudogs; dequeue returns them in FIFO order.
    #[test]
    fn waitq_fifo_order() {
        // Build two minimal G/sudog pairs.
        let g1 = make_g();
        let g2 = make_g();
        let s1 = acquire_sudog();
        let s2 = acquire_sudog();

        unsafe {
            (*s1).g = g1;
            (*s2).g = g2;
            // Mark both as GWAITING so dequeue doesn't skip them.
            (*g1).atomicstatus.store(GWAITING, Release);
            (*g2).atomicstatus.store(GWAITING, Release);
        }

        let mut q = WaitQ::new();
        unsafe {
            q.enqueue(s1);
            q.enqueue(s2);
        }

        // Dequeue should return s1 first (FIFO).
        let got1 = unsafe { q.dequeue() };
        assert_eq!(got1, s1, "first dequeue must return s1");
        let got2 = unsafe { q.dequeue() };
        assert_eq!(got2, s2, "second dequeue must return s2");
        assert!(q.is_empty(), "queue must be empty after both dequeues");
        assert_eq!(unsafe { q.dequeue() }, std::ptr::null_mut());

        // Verify prev/next were cleared.
        unsafe {
            assert!((*s1).next.is_null());
            assert!((*s1).prev.is_null());
            assert!((*s2).next.is_null());
            assert!((*s2).prev.is_null());
        }

        // Clean up
        unsafe {
            (*s1).g = std::ptr::null_mut(); (*s1).c = std::ptr::null_mut();
            (*s2).g = std::ptr::null_mut(); (*s2).c = std::ptr::null_mut();
            release_sudog(s1); release_sudog(s2);
            let _ = Box::from_raw(g1); let _ = Box::from_raw(g2);
        }
    }

    /// `dequeue` skips an `is_select` sudog whose goroutine's `selectdone` CAS
    /// already lost (another case claimed the win).
    #[test]
    fn waitq_dequeue_skips_non_waiting() {
        use crate::runtime::g::GWAITING;

        // g_skip: is_select sudog that already lost the selectdone race.
        // g_take: is_select sudog whose selectdone is still 0 (uncontested).
        let g_skip = make_g();
        let g_take = make_g();
        let s_skip = acquire_sudog();
        let s_take = acquire_sudog();

        unsafe {
            (*s_skip).g         = g_skip;
            (*s_skip).is_select = true;
            (*s_take).g         = g_take;
            (*s_take).is_select = true;

            // Mark g_skip as already won by another case.
            (*g_skip).atomicstatus.store(GWAITING, Release);
            (*g_skip).selectdone.store(1, Release); // already claimed
            (*g_take).atomicstatus.store(GWAITING, Release);
            // g_take.selectdone stays 0 — dequeue should claim it.
        }

        let mut q = WaitQ::new();
        unsafe { q.enqueue(s_skip); q.enqueue(s_take); }

        // dequeue should skip s_skip (selectdone CAS fails) and return s_take.
        let got = unsafe { q.dequeue() };
        assert_eq!(got, s_take, "dequeue must skip the select-lost sudog");
        assert!(q.is_empty());

        // Verify dequeue set selectdone = 1 on g_take.
        assert_eq!(
            unsafe { (*g_take).selectdone.load(Ordering::Relaxed) },
            1,
            "dequeue must CAS selectdone to 1 for the winning sudog"
        );

        // Clean up
        unsafe {
            (*s_take).g = std::ptr::null_mut(); (*s_take).c = std::ptr::null_mut();
            release_sudog(s_take);
            // s_skip was unlinked (not released) by dequeue — free it manually.
            let _ = Box::from_raw(s_skip);
            let _ = Box::from_raw(g_skip); let _ = Box::from_raw(g_take);
        }
    }

    /// `dequeue_sudog` removes a specific element from the middle of the queue.
    #[test]
    fn waitq_dequeue_sudog_middle() {
        let g1 = make_g(); let g2 = make_g(); let g3 = make_g();
        let s1 = acquire_sudog();
        let s2 = acquire_sudog();
        let s3 = acquire_sudog();

        unsafe {
            (*s1).g = g1; (*s2).g = g2; (*s3).g = g3;
            (*g1).atomicstatus.store(GWAITING, Release);
            (*g2).atomicstatus.store(GWAITING, Release);
            (*g3).atomicstatus.store(GWAITING, Release);
        }

        let mut q = WaitQ::new();
        unsafe { q.enqueue(s1); q.enqueue(s2); q.enqueue(s3); }

        // Remove s2 from the middle.
        unsafe { q.dequeue_sudog(s2) };

        // Queue should now be s1 ↔ s3.
        assert_eq!(q.first, s1);
        assert_eq!(q.last,  s3);
        unsafe {
            assert_eq!((*s1).next, s3);
            assert_eq!((*s3).prev, s1);
            assert!((*s2).next.is_null());
            assert!((*s2).prev.is_null());
        }

        // Dequeue the remaining two.
        let got1 = unsafe { q.dequeue() };
        let got3 = unsafe { q.dequeue() };
        assert_eq!(got1, s1);
        assert_eq!(got3, s3);
        assert!(q.is_empty());

        // Clean up
        unsafe {
            for (s, g) in [(s1,g1),(s2,g2),(s3,g3)] {
                (*s).g = std::ptr::null_mut(); (*s).c = std::ptr::null_mut();
                release_sudog(s);
                let _ = Box::from_raw(g);
            }
        }
    }
}
