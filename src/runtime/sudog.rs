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

    /// Whether `elem` points to a heap-allocated `Box<MaybeUninit<T>>` that
    /// must be freed after the value is consumed.
    ///
    /// `true`  — allocated by `chansend` / `chanrecv` blocking paths; the
    ///           receiving side must call `Box::from_raw(elem)` after reading.
    /// `false` — `elem` is a direct pointer (goroutine stack or select slot);
    ///           the receiving side must NOT call `Box::from_raw`.
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
    pub(crate) unsafe fn dequeue_sudog(&mut self, sgp: *mut Sudog) {
        let prev = unsafe { (*sgp).prev };
        let next = unsafe { (*sgp).next };

        // Guard: if sgp has no prev link and is not the head, it has already
        // been unlinked by a racing `dequeue()` call.  Do nothing.
        if prev.is_null() && self.first != sgp {
            return;
        }

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
        }
    }
}

// ---------------------------------------------------------------------------
// Global sudog free list
// ---------------------------------------------------------------------------

/// Internal free-list node.  `next` in `Sudog` is reused as the chain link.
struct SudogCache {
    head:  *mut Sudog,
    count: usize,
}

// SAFETY: The cache is only ever accessed while holding SUDOG_CACHE's Mutex.
unsafe impl Send for SudogCache {}

/// Maximum number of sudogs kept in the global free list.
///
/// Mirrors Go's per-P cap of 128; our single global list is capped at 1 024
/// so that channels under high concurrency don't exhaust the heap.
const CACHE_CAP: usize = 1_024;

/// Process-wide sudog free list.
///
/// v1 uses a single global list protected by a `Mutex`.  The per-P tier
/// (step 15.5) will be layered on top without changing `acquire_sudog` /
/// `release_sudog`'s signatures.
static SUDOG_CACHE: Mutex<SudogCache> = Mutex::new(SudogCache {
    head:  std::ptr::null_mut(),
    count: 0,
});

// ---------------------------------------------------------------------------
// acquire_sudog / release_sudog
// ---------------------------------------------------------------------------

/// Acquire a zeroed `Sudog` from the free list, allocating one if necessary.
///
/// The returned pointer is exclusively owned by the caller until passed to
/// [`release_sudog`].
///
/// Ported from `acquireSudog` in `runtime/proc.go`.
pub(crate) fn acquire_sudog() -> *mut Sudog {
    let mut cache = SUDOG_CACHE.lock().unwrap();
    if !cache.head.is_null() {
        let s = cache.head;
        // Advance the free-list head.
        unsafe { cache.head = (*s).next };
        cache.count -= 1;
        drop(cache); // release the lock before zeroing

        // Zero-out any stale fields from the previous use.
        unsafe { std::ptr::write(s, Sudog::zeroed()) };
        s
    } else {
        drop(cache); // release lock before heap allocation
        Box::into_raw(Box::new(Sudog::zeroed()))
    }
}

/// Return a `Sudog` to the free list after its channel operation completes.
///
/// The caller **must** clear `s.g`, `s.elem`, and `s.c` before calling.
/// This matches Go's `releaseSudog` precondition (panics in debug mode if
/// the pointer fields are non-null on arrival).
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

    let mut cache = SUDOG_CACHE.lock().unwrap();
    if cache.count < CACHE_CAP {
        // Link into the free list via the `next` field.
        unsafe { (*s).next = cache.head };
        cache.head  = s;
        cache.count += 1;
    } else {
        drop(cache);
        // Cache is full: free the allocation directly.
        let _ = unsafe { Box::from_raw(s) };
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
