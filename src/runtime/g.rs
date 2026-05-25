// SPDX-License-Identifier: Apache-2.0
//! Goroutine (`G`) and register save area (`Gobuf`) — ported from
//! `runtime/runtime2.go`.
//!
//! Steps 2 and 5 of the porting plan are implemented here together because
//! `G` embeds `Gobuf` directly and they cannot be compiled in isolation.

use std::cell::Cell;
use std::sync::atomic::AtomicU32;

use super::m::M;

// ---------------------------------------------------------------------------
// Goroutine status — atomicstatus values from runtime/runtime2.go
// ---------------------------------------------------------------------------

/// G was just allocated and has not yet been initialized.
pub(crate) const GIDLE: u32 = 0;
/// G is on a run queue, waiting to be scheduled.
pub(crate) const GRUNNABLE: u32 = 1;
/// G is currently executing on an M.
pub(crate) const GRUNNING: u32 = 2;
/// G is blocked in a system call.
pub(crate) const GSYSCALL: u32 = 3;
/// G is parked — blocked on a channel op, mutex, or timer.
pub(crate) const GWAITING: u32 = 4;
/// G exited; its stack may be reused.
pub(crate) const GDEAD: u32 = 6;
/// G is mid stack-copy (v1: unused — fixed stacks only).
pub(crate) const GCOPYSTACK: u32 = 8;
/// G was preempted at an async safe point (v1: unused — cooperative only).
pub(crate) const GPREEMPTED: u32 = 9;
/// OR'd with a base status while the GC is scanning the stack (v1: no GC).
pub(crate) const GSCAN: u32 = 0x1000;

// ---------------------------------------------------------------------------
// Stack constants — from runtime/stack.go
// ---------------------------------------------------------------------------

/// Sentinel value for `G.stackguard0` that triggers cooperative preemption.
/// Matches Go's `stackPreempt = uintptr(-1300)` in spirit; using `usize::MAX`
/// as a conservative sentinel that is never a valid stack address.
pub(crate) const STACK_PREEMPT: usize = usize::MAX;

/// Guard offset from `Stack.lo` placed into `stackguard0` at goroutine start.
/// Equals Go's `stackGuard` for non-Windows 64-bit:
///   `stackNosplit (800) + stackSystem (0) + StackGuardExtraSize (128) = 928`.
/// Revisit when stack growth is ported (step 4).
pub(crate) const STACK_GUARD: usize = 928;

// ---------------------------------------------------------------------------
// Stack — goroutine stack bounds
// ---------------------------------------------------------------------------

/// A goroutine's stack bounds.  The live region is `[lo, hi)`.
///
/// `#[repr(C)]` because this struct sits at offset 0 of `G` and the assembly
/// (step 3) may need a stable layout if `G` itself becomes `#[repr(C)]`.
#[repr(C)]
pub(crate) struct Stack {
    /// Low address — one page above the guard page after `mmap` (step 4).
    pub lo: usize,
    /// High address — the initial stack pointer is set to `hi` on first use.
    pub hi: usize,
}

// ---------------------------------------------------------------------------
// Gobuf — register save area
// ---------------------------------------------------------------------------

/// Saved register state for a goroutine that is not currently on-CPU.
///
/// `#[repr(C)]` is **mandatory**: `asm_arm64.rs` and `asm_amd64.rs` (step 3)
/// address each field by its byte offset using the `GOBUF_*_OFFSET` constants
/// below.  Any change to field order or type **must** update those constants;
/// the compile-time assertions immediately following this struct will catch
/// any mismatch.
///
/// Ported from `gobuf` in `runtime/runtime2.go`.
#[repr(C)]
pub(crate) struct Gobuf {
    /// Saved stack pointer.
    pub sp:   usize,
    /// Saved program counter — the instruction the G will resume at.
    pub pc:   usize,
    /// Back-pointer to the owning `G`.  Wired by `G::new`; never reassigned.
    pub g:    *mut G,
    /// Closure context pointer.  Kept for field-offset ABI compatibility with
    /// Go's gobuf; unused in v1 (no GC write barriers).
    pub ctxt: *mut u8,
    /// Return value threaded from an `mcall` callee back through `gogo`.
    pub ret:  usize,
    /// Link register (`x30` on AArch64).  Unused on x86-64.
    pub lr:   usize,
    /// Frame pointer / base pointer for frame-pointer-enabled builds.
    pub bp:   usize,
}

// Byte offsets into `Gobuf` on a 64-bit target, derived from the
// `#[repr(C)]` layout.  Used as immediate constants in `global_asm!` (step 3)
// where Rust `const` values cannot be referenced directly.
pub(crate) const GOBUF_SP_OFFSET:   usize = 0;
pub(crate) const GOBUF_PC_OFFSET:   usize = 8;
pub(crate) const GOBUF_G_OFFSET:    usize = 16;
pub(crate) const GOBUF_CTXT_OFFSET: usize = 24;
pub(crate) const GOBUF_RET_OFFSET:  usize = 32;
pub(crate) const GOBUF_LR_OFFSET:   usize = 40;
pub(crate) const GOBUF_BP_OFFSET:   usize = 48;

// Compile-time verification that the constants match the actual repr(C) layout.
const _: () = {
    use std::mem::offset_of;
    assert!(offset_of!(Gobuf, sp)   == GOBUF_SP_OFFSET);
    assert!(offset_of!(Gobuf, pc)   == GOBUF_PC_OFFSET);
    assert!(offset_of!(Gobuf, g)    == GOBUF_G_OFFSET);
    assert!(offset_of!(Gobuf, ctxt) == GOBUF_CTXT_OFFSET);
    assert!(offset_of!(Gobuf, ret)  == GOBUF_RET_OFFSET);
    assert!(offset_of!(Gobuf, lr)   == GOBUF_LR_OFFSET);
    assert!(offset_of!(Gobuf, bp)   == GOBUF_BP_OFFSET);
};

// SAFETY: `Gobuf` is passed between threads when the scheduler migrates a G.
// Exactly one M runs a given G at any time, providing the mutual exclusion
// that makes cross-thread pointer passing sound.
unsafe impl Send for Gobuf {}
unsafe impl Sync for Gobuf {}

impl Default for Gobuf {
    fn default() -> Self {
        Self {
            sp:   0,
            pc:   0,
            g:    std::ptr::null_mut(),
            ctxt: std::ptr::null_mut(),
            ret:  0,
            lr:   0,
            bp:   0,
        }
    }
}

// ---------------------------------------------------------------------------
// WaitReason — why a G is in GWAITING state
// ---------------------------------------------------------------------------

/// The reason a goroutine is parked in `GWAITING`.
///
/// Subset of `waitReason` from `runtime/runtime2.go`; only values relevant
/// to channels, select, mutexes, and timers are included.  GC wait reasons
/// are omitted.
#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum WaitReason {
    #[default]
    Zero        = 0,
    Select      = 9,   // "select"
    ChanReceive = 14,  // "chan receive"
    ChanSend    = 15,  // "chan send"
    Semacquire  = 18,  // "semacquire"
    Sleep       = 19,  // "sleep"
    CondVar     = 20,  // "condvar wait"
    IOWait      = 23,  // "IO wait" (netpoll — Step 5)
}

impl WaitReason {
    /// Human-readable description matching Go's `waitReason.String()`.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Zero        => "",
            Self::Select      => "select",
            Self::ChanReceive => "chan receive",
            Self::ChanSend    => "chan send",
            Self::Semacquire  => "semacquire",
            Self::Sleep       => "sleep",
            Self::CondVar     => "condvar wait",
            Self::IOWait      => "IO wait",
        }
    }
}

// ---------------------------------------------------------------------------
// G — goroutine
// ---------------------------------------------------------------------------

/// A goroutine — the fundamental unit of concurrency.
///
/// Ported from `g` in `runtime/runtime2.go`.  This is a strict subset of
/// Go's version; GC, defer, panic, stack-growth, and tracer fields are
/// omitted.
///
/// A `G` is always heap-allocated via `G::new` so the scheduler can hold
/// stable `*mut G` raw pointers across thread migrations.  The goroutine's
/// execution stack is a separate `mmap`'d region tracked by `G.stack`; the
/// `G` struct itself lives on the Rust heap.
pub(crate) struct G {
    // Stack parameters at the top of the struct to match Go's layout in case
    // `#[repr(C)]` is needed for assembly access in step 3.
    /// Bounds of the goroutine's execution stack.  Live region: `[lo, hi)`.
    pub stack:       Stack,
    /// Stack pointer limit used in the cooperative preemption check.
    /// Normally `stack.lo + STACK_GUARD`.  The scheduler sets this to
    /// `STACK_PREEMPT` to request a yield at the G's next `gosched()` call.
    pub stackguard0: usize,

    /// The M this G is currently running on; `null` when not on-CPU.
    pub m:           *mut M,

    /// Saved register state while the G is off-CPU.  Written by `mcall`,
    /// read by `gogo` (see `asm_arm64.rs` / `asm_amd64.rs`, step 3).
    pub sched:       Gobuf,

    /// Current goroutine status — one of the `G*` constants.  Written
    /// atomically so sysmon can observe status without holding a lock.
    pub atomicstatus: AtomicU32,

    /// Unique monotonically-increasing goroutine identifier.
    pub goid:        u64,

    /// Intrusive singly-linked list link used by run queues and wait queues.
    /// `null` when the G is not on any list.
    pub schedlink:   *mut G,

    /// Nanosecond timestamp when the G entered `GWAITING` (from
    /// `std::time::Instant`).  Zero when not waiting.
    pub waitsince:   i64,

    /// Why this G is waiting; meaningful only when
    /// `atomicstatus == GWAITING`.
    pub waitreason:  WaitReason,

    /// Generic pointer passed to a waking G by the operation that unblocks
    /// it.  Channels use this to hand off an element; `selectgo` uses it to
    /// identify the winning case; timers set it to a zero-value sentinel.
    pub param:       *mut u8,

    /// Cooperative preemption flag.  The scheduler sets this to request a
    /// yield; the G should call `gosched()` at its next safe point.
    /// Mirrors setting `stackguard0 = STACK_PREEMPT`.
    pub preempt:     bool,

    /// `selectgo` race flag.  Set atomically to 1 by the first M to claim
    /// the select win; others see the 1 and back off.
    pub selectdone:  AtomicU32,

    /// Cached timer for `time::sleep`.  `null` when no timer is active.
    /// Typed `*mut u8` until `runtime::time::Timer` is defined (step 17).
    pub timer:       *mut u8,
}

// SAFETY: The scheduler guarantees at most one M executes a given G at any
// time.  That mutual exclusion makes it sound to pass `*mut G` across the
// thread boundary when the scheduler migrates a goroutine.
unsafe impl Send for G {}
unsafe impl Sync for G {}

impl G {
    /// Allocate a new goroutine with the given stack and goroutine ID.
    ///
    /// `sched.g` is wired back to the heap allocation immediately so the
    /// assembly can follow the `G → Gobuf → G` pointer cycle.  `sched.sp`
    /// and `sched.pc` are left zeroed; the caller must initialise them (via
    /// `runtime::sched`) before making the G runnable.
    pub(crate) fn new(stack: Stack, goid: u64) -> Box<G> {
        let stackguard0 = stack.lo + STACK_GUARD;
        let mut g = Box::new(G {
            stackguard0,
            stack,
            m:            std::ptr::null_mut(),
            sched:        Gobuf::default(),
            atomicstatus: AtomicU32::new(GIDLE),
            goid,
            schedlink:    std::ptr::null_mut(),
            waitsince:    0,
            waitreason:   WaitReason::Zero,
            param:        std::ptr::null_mut(),
            preempt:      false,
            selectdone:   AtomicU32::new(0),
            timer:        std::ptr::null_mut(),
        });
        // Box<G> has a stable heap address — moving the Box moves only the
        // pointer, not the allocation — so this self-referential pointer is
        // valid for the lifetime of the allocation.
        g.sched.g = std::ptr::addr_of_mut!(*g);
        g
    }
}

// ---------------------------------------------------------------------------
// Per-thread context — current G and g0 Gobuf pointers
// ---------------------------------------------------------------------------

thread_local! {
    /// The goroutine currently running on this OS thread.
    /// `null` when the thread is executing the scheduler loop on g0.
    /// Set by `gogo` (via `set_current_g`) before every context switch.
    pub(crate) static CURRENT_G: Cell<*mut G> =
        const { Cell::new(std::ptr::null_mut()) };

    /// Pointer to g0's `Gobuf` for this OS thread.
    /// Initialised by `M::new` (step 6) when the M is created.
    /// `mcall` reads this to know where to switch the stack when parking.
    pub(crate) static G0_SCHED: Cell<*mut Gobuf> =
        const { Cell::new(std::ptr::null_mut()) };
}

/// Return the goroutine currently running on this OS thread, or `null` on g0.
#[inline]
pub(crate) fn current_g() -> *mut G {
    CURRENT_G.with(|c| c.get())
}

/// Record `g` as the goroutine running on this OS thread.
/// Called by `gogo` immediately before every context switch.
///
/// # Safety
/// `g` must point to a live, heap-allocated `G` whose ownership has been
/// transferred to the current OS thread by the scheduler.
#[inline]
pub(crate) unsafe fn set_current_g(g: *mut G) {
    CURRENT_G.with(|c| c.set(g));
}

/// Record `buf` as g0's `Gobuf` for this OS thread.
/// Called once from `M::new` (step 6) during M initialisation.
///
/// # Safety
/// `buf` must point to the `sched` field of a live g0 `G` that is pinned
/// to this OS thread for its lifetime.
#[inline]
pub(crate) unsafe fn set_g0_sched(buf: *mut Gobuf) {
    G0_SCHED.with(|c| c.set(buf));
}

/// Return g0's `Gobuf` for this OS thread.
/// Returns `null` before `M::new` has been called.
#[inline]
pub(crate) fn g0_sched() -> *mut Gobuf {
    G0_SCHED.with(|c| c.get())
}
