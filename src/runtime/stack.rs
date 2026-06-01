// SPDX-License-Identifier: Apache-2.0
//! Goroutine stack allocator and growth machinery — ported from
//! `runtime/stack.go`, `runtime/signal_unix.go`.
//!
//! ## v0.2.0 — dynamic stack growth
//!
//! Each goroutine starts with a 2 KiB stack in release builds (matching
//! Go's `stackMin = 2048`).  Debug builds use larger initial sizes
//! (Linux 16 KiB, macOS 64 KiB, Windows 64 KiB) to absorb the wider
//! non-optimised frames produced by debug codegen — see
//! [`GOROUTINE_STACK_BYTES`] for the full table.
//! The guard page (`PROT_NONE`) immediately below `stack.lo` turns overflows
//! into a `SIGSEGV` (Linux/Windows) or `SIGBUS` (macOS) that the runtime
//! intercepts and recovers from by growing the stack.
//!
//! When the guard page is touched:
//! 1. `sigsegv_handler` identifies the fault as a goroutine stack overflow.
//! 2. It calls `grow_goroutine_stack_from_signal` which:
//!    a. Allocates a new stack double the current size (capped at 1 GiB).
//!    b. Copies the live portion of the old stack to the new one.
//!    c. Adjusts all pointer-sized words in `[old_lo, old_hi)` (conservative scan).
//!    d. Updates `G.stack`, `G.stackguard0`, and SP in `ucontext_t` (OS retries the instruction).
//! 3. The SIGSEGV handler returns; the OS restores the updated register state;
//!    the faulting instruction is re-executed and now succeeds.
//!
//! ## Layout of each allocation
//!
//! ```text
//! base ──► ┌──────────────────────────────┐
//!          │  guard page  (PROT_NONE)     │  1 × page_size
//!          ├──────────────────────────────┤ ◄── Stack.lo
//!          │                              │
//!          │  execution stack             │  SP starts at hi, grows ↓
//!          │                              │
//!          └──────────────────────────────┘ ◄── Stack.hi
//! ```
//!
//! ## copystack — conservative pointer adjustment
//!
//! Without GC stack maps we scan every pointer-sized word in the live stack
//! region and adjust those that fall within `[old_guard_lo, old_hi)` (the
//! usable old stack plus its guard page).  Return addresses are in the code
//! segment (a completely different address range) and are never mistakenly
//! adjusted.  Integer values that coincidentally equal a stack address are
//! a theoretical false positive but vanishingly rare for the narrow
//! 2–1024 KiB windows used here.

// Mutex is only needed for the SIGSEGV handler's static on Unix.
#[cfg(not(windows))]
use std::sync::Mutex;
use std::sync::OnceLock;

// Unix-only: mmap constants and signal types.
#[cfg(not(windows))]
use libc::{MAP_ANON, MAP_FAILED, MAP_PRIVATE, PROT_NONE, PROT_READ, PROT_WRITE};

// current_g is only needed by the SIGSEGV handler (Unix-only).
#[cfg(not(windows))]
use super::g::current_g;
use super::g::{casgstatus, readgstatus, Stack, GCOPYSTACK, STACK_GUARD, G};

// ---------------------------------------------------------------------------
// Windows: Win32 virtual-memory API (no libc wrappers available)
// ---------------------------------------------------------------------------

#[cfg(windows)]
mod win32 {
    pub const MEM_COMMIT:   u32 = 0x0000_1000;
    pub const MEM_RESERVE:  u32 = 0x0000_2000;
    pub const MEM_RELEASE:  u32 = 0x0000_8000;
    pub const PAGE_READWRITE: u32 = 0x04;
    pub const PAGE_NOACCESS:  u32 = 0x01;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        pub fn VirtualAlloc(
            lpAddress:         *mut u8,
            dwSize:            usize,
            flAllocationType:  u32,
            flProtect:         u32,
        ) -> *mut u8;
        pub fn VirtualFree(
            lpAddress:   *mut u8,
            dwSize:      usize,
            dwFreeType:  u32,
        ) -> i32;
        pub fn VirtualProtect(
            lpAddress:      *mut u8,
            dwSize:         usize,
            flNewProtect:   u32,
            lpflOldProtect: *mut u32,
        ) -> i32;
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Minimum goroutine stack size (bytes).
///
/// Matches Go's `stackMin = 2048`.  The stack grows on demand via the
/// SIGBUS/SIGSEGV guard-page handler; 2 KiB is the initial allocation.
/// Debug builds start larger to avoid frequently triggering the growth path
/// with the deeper frames produced by non-optimised code.
#[allow(dead_code)] // used in GOROUTINE_STACK_BYTES and documentation
pub(crate) const STACK_MIN: usize = 2 * 1024;

/// Maximum goroutine stack size (bytes). 1 GiB matches Go's `maxstacksize`.
pub(crate) const STACK_MAX: usize = 1024 * 1024 * 1024;

/// Initial stack size for every new goroutine.
///
/// ## Platform table
///
/// | Platform      | Profile | Size  | Notes                                         |
/// |---------------|---------|-------|-----------------------------------------------|
/// | Linux release | release | 2 KiB | Matches Go's `stackMin`; grows on demand      |
/// | Linux AArch64 | release | 2 KiB | Same; page_size = 4 KiB on most kernels       |
/// | macOS release | release | 2 KiB | Guard-page faults raise SIGBUS, not SIGSEGV   |
/// | Linux debug   | debug   | 16 KiB| Deeper frames avoid unnecessary growth        |
/// | macOS debug   | debug   | 64 KiB| AArch64 CI: 16 KiB pages + wider debug frames |
/// | Windows debug | debug   | 64 KiB| SEH/VEH overhead + 3–5× wider debug frames    |
/// | Windows release| release| 2 KiB | Proactive growth handles overflow             |
///
/// ## Per-goroutine memory (release builds)
///
/// ```text
/// Platform         Stack   OS guard   G struct   Total
/// ───────────────  ──────  ─────────  ────────── ──────────
/// Linux / Win x86  2 KiB   4 KiB      128 B      ~6.1 KiB
/// macOS x86-64     2 KiB   4 KiB      128 B      ~6.1 KiB
/// macOS AArch64    2 KiB  16 KiB      128 B      ~18 KiB
/// ```
///
/// Go achieves ~2.4 KiB (2 KiB stack + ~392 B descriptor, no OS guard page)
/// by emitting `morestack` stack-check prologues from the compiler.  Without
/// that compiler support, eliminating the OS guard page would turn stack
/// overflows into silent memory corruption rather than a clean crash.
#[cfg(any(all(windows, debug_assertions), all(target_os = "macos", debug_assertions)))]
pub(crate) const GOROUTINE_STACK_BYTES: usize = 128 * 1024;
// Debug builds across all platforms: 128 KiB initial stack.
//
// Previously 16 KiB on Linux and 64 KiB on macOS/Windows, but stress tests
// (`many_goroutines` with 5000 workers) intermittently SIGSEGV'd inside
// scheduler internals shortly after a stack growth.  The crash pattern is
// always a wild dereference (e.g. `atomic_compare_exchange_weak` at a
// pointer-shaped-but-invalid address) — captured live under lldb.
//
// Root cause: `update_sp_in_context`'s two-range GPR adjustment
// deliberately narrows the argument-register range to the guard page only
// (to avoid false positives when many goroutines have stacks at adjacent
// addresses), which leaves a small UAF window when a caller-saved register
// happens to hold a pointer into the *usable* portion of the old stack.
//
// Mitigation: bumping the initial stack so most goroutines never grow at
// all dramatically shrinks the window.  This is a band-aid; the proper fix
// is to free the old stack *before* `update_sp_in_context` so the kernel
// can be authoritative about which address ranges are still mapped.
#[cfg(all(debug_assertions, not(any(windows, target_os = "macos"))))]
pub(crate) const GOROUTINE_STACK_BYTES: usize = 128 * 1024;
#[cfg(not(debug_assertions))]
pub(crate) const GOROUTINE_STACK_BYTES: usize = STACK_MIN;

/// Stack size for each M's g0 (the scheduler stack).
///
/// The scheduler loop (`schedule` → `findrunnable` → `stopm` → locking →
/// `Condvar::wait`) has a deeper call chain than a typical goroutine, and on
/// Windows in debug builds lock operations and system-call trampolines consume
/// 3–5× more stack than on Unix.  The `exitsyscall0_mcall` slow path adds an
/// extra frame level because `schedule()` is called from within
/// `exitsyscall0_mcall`'s frame rather than from the top of g0.  512 KiB
/// provides comfortable headroom across all platforms and build profiles.
/// Go uses the OS thread's native stack (typically 8 MiB) for g0; we use a
/// fixed 512 KiB mmap'd region with the same guard-page layout as a normal
/// goroutine stack.
pub(crate) const G0_STACK_BYTES: usize = 512 * 1024;

// ---------------------------------------------------------------------------
// Page size
// ---------------------------------------------------------------------------

/// Returns the OS page size, queried once and then cached.
///
/// On macOS/AArch64 (Apple Silicon) this is **16 KiB**; on Linux x86-64 and
/// Windows x86-64 it is typically **4 KiB**.
pub(crate) fn page_size() -> usize {
    static PAGE_SIZE: OnceLock<usize> = OnceLock::new();
    *PAGE_SIZE.get_or_init(|| {
        #[cfg(not(windows))]
        {
            let n = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
            assert!(n > 0, "sysconf(_SC_PAGESIZE) returned {n}");
            n as usize
        }
        // Windows x86-64 always uses 4 KiB pages.
        #[cfg(windows)]
        { 4096usize }
    })
}

// ---------------------------------------------------------------------------
// Allocation
// ---------------------------------------------------------------------------

/// Allocate a goroutine stack of exactly `size` usable bytes (+ 1 guard page).
///
/// Returns a [`Stack`] describing the usable region `[lo, hi)`.
///
/// # Errors
/// Returns a static error string on allocation failure.
pub(crate) unsafe fn stack_alloc_size(size: usize) -> Result<Stack, &'static str> {
    debug_assert!(size.is_power_of_two() || size == STACK_MAX,
        "stack_alloc_size: size must be a power of two");
    let ps    = page_size();
    let total = size + ps; // guard page + usable stack

    #[cfg(not(windows))]
    {
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                total,
                PROT_READ | PROT_WRITE,
                MAP_ANON | MAP_PRIVATE,
                -1,
                0,
            )
        };
        if base == MAP_FAILED {
            return Err("stack_alloc_size: mmap failed");
        }
        if unsafe { libc::mprotect(base, ps, PROT_NONE) } != 0 {
            unsafe { libc::munmap(base, total) };
            return Err("stack_alloc_size: mprotect guard page failed");
        }
        let base_addr = base as usize;
        Ok(Stack { lo: base_addr + ps, hi: base_addr + total })
    }

    #[cfg(windows)]
    {
        use win32::*;
        let base = unsafe {
            VirtualAlloc(
                std::ptr::null_mut(),
                total,
                MEM_COMMIT | MEM_RESERVE,
                PAGE_READWRITE,
            )
        };
        if base.is_null() {
            return Err("stack_alloc_size: VirtualAlloc failed");
        }
        let mut old_protect: u32 = 0;
        if unsafe { VirtualProtect(base, ps, PAGE_NOACCESS, &mut old_protect) } == 0 {
            unsafe { VirtualFree(base, 0, MEM_RELEASE) };
            return Err("stack_alloc_size: VirtualProtect guard page failed");
        }
        let base_addr = base as usize;
        Ok(Stack { lo: base_addr + ps, hi: base_addr + total })
    }
}

/// Allocate a new goroutine stack of the default initial size.
///
/// Ported from `stackalloc` in `runtime/stack.go`.
pub(crate) unsafe fn stack_alloc() -> Result<Stack, &'static str> {
    unsafe { stack_alloc_size(GOROUTINE_STACK_BYTES) }
}

/// Allocate a new g0 (scheduler) stack.
///
/// g0 stacks are larger than goroutine stacks because the scheduler call chain
/// (`schedule` → `findrunnable` → locking → park) is deeper.
pub(crate) unsafe fn g0_stack_alloc() -> Result<Stack, &'static str> {
    unsafe { stack_alloc_size(G0_STACK_BYTES) }
}

// ---------------------------------------------------------------------------
// Deallocation
// ---------------------------------------------------------------------------

/// Free a goroutine stack previously returned by `stack_alloc_size`.
///
/// # Safety
/// `stack` must have been returned by `stack_alloc_size` and must not have
/// been freed before.
pub(crate) unsafe fn stack_free(stack: &Stack) {
    let ps   = page_size();
    let base = (stack.lo - ps) as *mut u8;

    #[cfg(not(windows))]
    {
        let total = (stack.hi - stack.lo) + ps;
        unsafe { libc::munmap(base as *mut libc::c_void, total) };
    }

    #[cfg(windows)]
    {
        use win32::{MEM_RELEASE, VirtualFree};
        // VirtualFree with MEM_RELEASE requires dwSize = 0.
        unsafe { VirtualFree(base, 0, MEM_RELEASE) };
    }
}

// ---------------------------------------------------------------------------
// Stack growth — newstack / copystack
// ---------------------------------------------------------------------------

/// Double the goroutine's stack (up to STACK_MAX) and resume it.
///
/// Called from the SIGSEGV handler when a guard page fault is detected.
/// On return the goroutine's stack fields are updated; the caller must also
/// update the interrupted `RSP/SP` in the platform `ucontext_t`.
///
/// Returns the delta applied to all adjusted pointers.
///
/// # Safety
/// Must be called from a signal handler context; `gp` must be the goroutine
/// whose guard page was touched.
///
/// **Not compiled on Windows** — Windows has no POSIX SIGSEGV mechanism.
/// Proactive growth via `grow_stack_if_needed` handles Windows instead.
#[cfg(not(windows))]
pub(crate) unsafe fn newstack(gp: *mut G) -> isize {
    let old_stack = Stack {
        lo: unsafe { (*gp).stack.lo },
        hi: unsafe { (*gp).stack.hi },
    };
    let old_size = old_stack.hi - old_stack.lo;

    if old_size >= STACK_MAX {
        // Stack already at maximum — this is a genuine overflow.
        eprintln!("goroutine stack overflow: stack size {old_size} >= STACK_MAX ({STACK_MAX})");
        unsafe { libc::abort() };
    }

    let new_size = (old_size * 2).min(STACK_MAX);
    let new_stack = unsafe {
        stack_alloc_size(new_size).expect("newstack: failed to allocate new goroutine stack")
    };

    // Copy live portion and adjust pointers.
    let delta = unsafe { copystack(gp, &old_stack, &new_stack) };

    // Update G's stack bookkeeping.
    unsafe {
        (*gp).stack       = Stack { lo: new_stack.lo, hi: new_stack.hi };
        (*gp).stackguard0 = new_stack.lo + STACK_GUARD;
    }

    // Free the old stack.
    unsafe { stack_free(&old_stack) };

    delta
}

/// Copy the live portion of a goroutine's stack to a new allocation and
/// apply a conservative pointer-adjustment scan.
///
/// Returns the delta (`new_stack.lo as isize - old_stack.lo as isize`).
///
/// # Conservative pointer adjustment
///
/// Every pointer-sized word in the copied region that falls within
/// `[old_lo, old_hi)` is incremented by `delta`.  Words outside that range
/// (code pointers / heap pointers / integers) are unchanged.
///
/// # Safety
/// `old_stack` must be the goroutine's current live stack; `new_stack` must
/// be freshly allocated with at least the same usable size.
unsafe fn copystack(gp: *mut G, old_stack: &Stack, new_stack: &Stack) -> isize {
    // Bracket the copy with GCOPYSTACK so a future GC scanner skips this G
    // while its stack is in a half-copied state.
    // GRUNNING → GCOPYSTACK → GRUNNING  (matches Go's casgcopystack protocol).
    let old_status = unsafe { readgstatus(gp) };
    unsafe { casgstatus(gp, old_status, GCOPYSTACK) };

    let old_lo = old_stack.lo;
    let old_hi = old_stack.hi;
    let new_lo = new_stack.lo;
    let new_hi = new_stack.hi;
    let _old_size = old_hi - old_lo;
    let new_size = new_hi - new_lo;

    // The live stack occupies the top portion (stacks grow down).
    // Saved SP tells us how far down the goroutine has grown.
    // If sched.sp is 0 (goroutine not yet started), treat the whole stack as live.
    let saved_sp = unsafe { (*gp).sched.sp };
    let live_start_old = if saved_sp != 0 && saved_sp >= old_lo && saved_sp < old_hi {
        saved_sp
    } else {
        old_lo // treat as fully live for safety
    };

    // Offset of the live portion from old_hi.
    let live_bytes = old_hi - live_start_old;

    // Corresponding start in the new stack (preserving relative position from hi).
    let live_start_new = new_hi - live_bytes;

    // Bounds check: the new stack must be large enough.
    debug_assert!(
        new_size >= live_bytes,
        "copystack: new stack ({new_size} B) too small for live region ({live_bytes} B)"
    );

    // Copy live bytes from old → new.
    unsafe {
        std::ptr::copy_nonoverlapping(
            live_start_old as *const u8,
            live_start_new as *mut u8,
            live_bytes,
        );
    }

    // Delta is the displacement applied to old-stack pointers to produce
    // new-stack pointers.  The live region is copied relative to `hi`
    // (stacks grow downward), so the correct displacement is new_hi − old_hi,
    // NOT new_lo − old_lo.  Using new_lo − old_lo would be wrong when the two
    // stacks differ in size (the new stack is larger), which is always the
    // case during growth.  This matches Go runtime's adjinfo.delta calculation:
    //   delta = new.hi - old.hi
    let delta: isize = new_hi as isize - old_hi as isize;

    // Conservative scan: adjust any pointer-sized word in the new live region
    // that falls within [old_guard_lo, old_hi).
    //
    // We extend the lower bound from `old_lo` to `old_lo − page_size` (the
    // start of the old guard page) so that we also adjust words that hold
    // addresses the goroutine computed via a large negative offset from the
    // frame pointer — e.g. `lea rdi, [rbp − 4168]` where the result lands in
    // the guard page.  Those addresses are passed as arguments to functions
    // (e.g. `memset`) and must be relocated to the equivalent position in the
    // new, larger stack.
    let old_guard_lo = old_lo.saturating_sub(page_size());
    let mut addr = live_start_new;
    let word = std::mem::size_of::<usize>();
    while addr + word <= new_hi {
        let val = unsafe { *(addr as *const usize) };
        if val >= old_guard_lo && val < old_hi {
            unsafe { *(addr as *mut usize) = ((val as isize) + delta) as usize };
        }
        addr += word;
    }

    // Update G's saved registers that point into the old stack.
    unsafe {
        let sp = (*gp).sched.sp;
        if sp >= old_lo && sp < old_hi {
            (*gp).sched.sp = ((sp as isize) + delta) as usize;
        }
        let bp = (*gp).sched.bp;
        if bp >= old_lo && bp < old_hi {
            (*gp).sched.bp = ((bp as isize) + delta) as usize;
        }
    }

    // Restore the original status: GCOPYSTACK → old_status.
    unsafe { casgstatus(gp, GCOPYSTACK, old_status) };

    delta
}

// ---------------------------------------------------------------------------
// SIGSEGV handler — guard page detection and stack growth (Unix only)
// ---------------------------------------------------------------------------
// Windows does not have POSIX signals; guard-page faults are handled instead
// by the proactive `grow_stack_if_needed` checkpoint in the scheduler.

/// Previous SIGSEGV handler (chained if fault is not a stack overflow).
#[cfg(not(windows))]
static PREV_SIGSEGV: Mutex<Option<libc::sigaction>> = Mutex::new(None);

/// Install the runtime's SIGSEGV handler for goroutine stack guard pages.
///
/// If the faulting address falls in the guard page of the current goroutine's
/// stack, the handler grows the stack and updates the interrupt context so the
/// retry succeeds.  All other SIGSEGVs are forwarded to the previous handler.
///
/// # Safety
/// Call once from the main initialisation path (inside `schedinit`).
#[cfg(not(windows))]
pub(crate) unsafe fn install_sigsegv_handler() {
    let mut sa: libc::sigaction = unsafe { std::mem::zeroed() };
    sa.sa_sigaction = sigsegv_handler as *const () as usize;
    // sa_flags is c_ulong on Linux and c_int on macOS; `as _` lets Rust infer the right type.
    sa.sa_flags     = (libc::SA_SIGINFO | libc::SA_ONSTACK | libc::SA_RESTART) as _;
    unsafe { libc::sigemptyset(&mut sa.sa_mask) };

    let mut old: libc::sigaction = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::sigaction(libc::SIGSEGV, &sa, &mut old) };
    assert_eq!(ret, 0, "install_sigsegv_handler: sigaction failed");

    *PREV_SIGSEGV.lock().unwrap() = Some(old);
}

/// Shared guard-page fault handler — called from both SIGSEGV and SIGBUS handlers.
///
/// On Linux, `mprotect(PROT_NONE)` guard-page faults raise `SIGSEGV`.
/// On macOS, the same access raises `SIGBUS` (permission violation on a mapped
/// page) rather than `SIGSEGV` (unmapped address).  Both signals use this
/// identical detection-and-growth path.
///
/// ## Why we zero `gp.sched.sp` before copying
///
/// `gp.sched.sp` holds the SP that was saved at the goroutine's last
/// scheduling point (the most recent `mcall` or `gopark`).  While the goroutine
/// is *running*, any frames pushed since that point are below the saved SP and
/// are not reflected in `gp.sched.sp`.  `copystack` uses `gp.sched.sp` as the
/// start of the live region; if it is stale, only the top few bytes of the
/// stack would be copied and the actual live frames would be lost.
///
/// Setting `gp.sched.sp = 0` before calling `newstack` triggers `copystack`'s
/// "treat entire stack as live" fallback (`saved_sp == 0` → `live_start =
/// stack.lo`), which is always correct: copying a few extra dead bytes is safe,
/// but missing live frames is not.
///
/// ## Why we adjust all general-purpose registers
///
/// A function that overflows the guard page may compute a stack address via a
/// large negative offset from its frame pointer and pass that address to a
/// library call (e.g. `lea rdi, [rbp−4168]; call memset`) before allocating
/// the frame with `sub rsp, 4168`.  When `memset` faults at the destination
/// address, RDI holds a value in the **guard page** (below `old_lo`).
/// Updating only RSP/RBP leaves RDI pointing to the freed guard page; the
/// retry faults again as SIGSEGV on the now-unmapped page.
///
/// `update_sp_in_context` therefore adjusts **all** general-purpose registers
/// whose values fall in `[old_lo − page_size, old_hi)` — the usable old stack
/// plus the guard page — by `delta`, relocating every potentially stale stack
/// reference to the equivalent location in the new, larger stack.
///
/// Returns `true` if the fault was a goroutine stack overflow and has been
/// handled (caller should return from the signal handler); `false` if the fault
/// is unrelated to a guard page and the caller should chain to its normal path.
#[cfg(not(windows))]
pub(crate) unsafe fn try_grow_stack_from_signal(
    fault_addr: usize,
    ctx:        *mut libc::c_void,
) -> bool {
    let gp = current_g();
    if gp.is_null() { return false; }

    let stack_lo = unsafe { (*gp).stack.lo };
    let stack_hi = unsafe { (*gp).stack.hi };
    let guard_lo = stack_lo - page_size();
    let guard_hi = stack_lo;

    if fault_addr < guard_lo || fault_addr >= guard_hi {
        return false; // not a goroutine stack overflow
    }

    // Force copystack to copy the full old stack (see doc comment above).
    unsafe { (*gp).sched.sp = 0 };

    // Save old bounds before newstack() updates gp.stack.
    let old_lo = stack_lo;
    let old_hi = stack_hi;

    // Grow the stack; adjust all GPRs that hold old-stack references so that
    // the OS-retried faulting instruction succeeds on the new stack.
    let delta = unsafe { newstack(gp) };
    unsafe { update_sp_in_context(ctx, old_lo, old_hi, delta) };
    true
}

/// SIGSEGV handler: detect goroutine guard page faults and grow the stack.
#[cfg(not(windows))]
unsafe extern "C" fn sigsegv_handler(
    sig:  libc::c_int,
    info: *mut libc::siginfo_t,
    ctx:  *mut libc::c_void,
) {
    let fault_addr = unsafe { (*info).si_addr() } as usize;
    if unsafe { try_grow_stack_from_signal(fault_addr, ctx) } {
        return; // handler return → OS retries the faulting instruction
    }

    // Not a stack fault — chain to the previous handler.
    let prev = *PREV_SIGSEGV.lock().unwrap();
    match prev {
        Some(old) if old.sa_sigaction != libc::SIG_DFL
                  && old.sa_sigaction != libc::SIG_IGN => {
            // Call the previous handler.
            type SaFn = unsafe extern "C" fn(libc::c_int, *mut libc::siginfo_t, *mut libc::c_void);
            let f: SaFn = unsafe { std::mem::transmute(old.sa_sigaction) };
            unsafe { f(sig, info, ctx) };
        }
        _ => {
            // Default action: terminate with SIGSEGV.
            unsafe { libc::raise(libc::SIGSEGV) };
        }
    }
}

/// Return the number of bytes that SP was **pre-decremented** by the faulting
/// AArch64 instruction at `pc`, or 0 if the instruction does not pre-decrement.
///
/// On AArch64, pre-indexed store instructions (e.g. `stp x29, x30, [sp, #-16]!`)
/// commit the base-register writeback before the data-abort fires on most
/// implementations.  Retrying the instruction would decrement SP a second time,
/// corrupting the caller's frame.  Adding the pre-decrement back to SP in the
/// ucontext before the retry causes the instruction to land at the correct
/// location.
///
/// On x86-64, `push rXX` does NOT commit the RSP decrement when the store
/// page-faults (the push is architecturally atomic).  This function is therefore
/// only compiled for AArch64 targets.
#[cfg(all(not(windows), target_arch = "aarch64"))]
unsafe fn sp_predecrement_at_pc(pc: usize) -> usize {
    if pc == 0 { return 0; }
    // AArch64 instructions are always 4 bytes, little-endian.
    let instr = unsafe { *(pc as *const u32) };

    // STP 64-bit GPR pre-indexed: bits [31:22] = 0x2A6, imm7 scaled ×8.
    if (instr >> 22) & 0x3FF == 0x2A6 {
        let imm7 = (((instr >> 15) & 0x7F) as i32) << 25 >> 25;
        if imm7 < 0 { return (-imm7 * 8) as usize; }
    }
    // STP 32-bit GPR pre-indexed: bits [31:22] = 0x0A6, imm7 scaled ×4.
    if (instr >> 22) & 0x3FF == 0x0A6 {
        let imm7 = (((instr >> 15) & 0x7F) as i32) << 25 >> 25;
        if imm7 < 0 { return (-imm7 * 4) as usize; }
    }
    // STR 64-bit pre-indexed: bits [31:21] = 0x7C0, bits [11:10] = 3.
    if (instr >> 21) & 0x7FF == 0x7C0 && (instr >> 10) & 3 == 3 {
        let imm9 = (((instr >> 12) & 0x1FF) as i32) << 23 >> 23;
        if imm9 < 0 { return (-imm9) as usize; }
    }
    0
}

/// Update the interrupted register state saved in the signal `ucontext_t` after
/// a goroutine stack has been grown.
///
/// ## Two-range register adjustment
///
/// A function that overflows the guard page may compute a stack address via a
/// large negative offset from the frame pointer — e.g.
/// `lea rdi, [rbp − 4168]; call memset` — so RDI holds an address in the
/// guard page (below `old_lo`).  Updating only RSP/RBP leaves RDI pointing to
/// the now-unmapped guard page; the retry faults again immediately.
///
/// We therefore scan the register file in two passes:
///
/// 1. **SP, FP, callee-saved** (RSP, RBP, RBX, R12–R15 / SP, FP, x19–x28):
///    full range `[old_guard_lo, old_hi)`.  These registers hold frame-chain
///    pointers that are almost always in the old usable stack.
///
/// 2. **Caller-saved / argument** (RAX–RDX, RSI, RDI, R8–R11 / x0–x18):
///    narrow range `[old_guard_lo, old_lo)` — the guard page only.  This
///    handles the `lea rdi, [rbp−N]` pattern while avoiding false-positive
///    adjustments of heap pointers that could coincide with the usable-stack
///    address range when many goroutines are active.
///
/// Platform-specific: Linux x86-64, Linux AArch64, macOS x86-64, macOS AArch64.
#[cfg(not(windows))]
unsafe fn update_sp_in_context(
    ctx:    *mut libc::c_void,
    old_lo: usize,
    old_hi: usize,
    delta:  isize,
) {
    // old_guard_lo: start of the old guard page (PROT_NONE region).
    // No valid heap allocation can point into [old_guard_lo, old_lo).
    let old_guard_lo = old_lo.saturating_sub(page_size());

    /// Adjust `val` by `delta` iff it falls within `[lo, hi)`.
    #[inline(always)]
    fn adj(val: u64, lo: usize, hi: usize, delta: isize) -> u64 {
        let v = val as usize;
        if v >= lo && v < hi { (v as isize + delta) as u64 } else { val }
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    unsafe {
        use libc::{REG_RAX,REG_RBX,REG_RCX,REG_RDX,REG_RSI,REG_RDI,
                   REG_RBP,REG_RSP,REG_R8,REG_R9,REG_R10,REG_R11,
                   REG_R12,REG_R13,REG_R14,REG_R15};
        let mc = &mut (*(ctx as *mut libc::ucontext_t)).uc_mcontext;
        // SP, FP, callee-saved: full range.
        for reg in [REG_RSP, REG_RBP, REG_RBX, REG_R12, REG_R13, REG_R14, REG_R15] {
            let v = mc.gregs[reg as usize] as u64;
            mc.gregs[reg as usize] = adj(v, old_guard_lo, old_hi, delta) as libc::greg_t;
        }
        // Argument/caller-saved: guard-page range only.
        for reg in [REG_RAX, REG_RCX, REG_RDX, REG_RSI, REG_RDI,
                    REG_R8, REG_R9, REG_R10, REG_R11] {
            let v = mc.gregs[reg as usize] as u64;
            mc.gregs[reg as usize] = adj(v, old_guard_lo, old_lo, delta) as libc::greg_t;
        }
    }

    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    unsafe {
        let mc = &mut (*(ctx as *mut libc::ucontext_t)).uc_mcontext;
        // SP and FP (x29): full range.
        mc.sp       = adj(mc.sp,       old_guard_lo, old_hi, delta);
        mc.regs[29] = adj(mc.regs[29], old_guard_lo, old_hi, delta);
        // Callee-saved x19–x28: full range.
        for i in 19..=28usize {
            mc.regs[i] = adj(mc.regs[i], old_guard_lo, old_hi, delta);
        }
        // Argument/caller-saved x0–x18: guard-page range only.
        for i in 0..=18usize {
            mc.regs[i] = adj(mc.regs[i], old_guard_lo, old_lo, delta);
        }
        // AArch64 pre-indexed stores (e.g. `stp x29,x30,[sp,#-16]!`) commit
        // the base-register update even on a data-abort on most implementations.
        // Undo the pre-decrement so the retry instruction lands correctly.
        let correction = sp_predecrement_at_pc(mc.pc as usize) as u64;
        if correction != 0 { mc.sp += correction; }
    }

    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    unsafe {
        let ss = &mut (*(*  (ctx as *mut libc::ucontext_t)).uc_mcontext).__ss;
        // SP, FP, callee-saved: full range.  Skip __rip (code pointer).
        ss.__rsp = adj(ss.__rsp, old_guard_lo, old_hi, delta);
        ss.__rbp = adj(ss.__rbp, old_guard_lo, old_hi, delta);
        ss.__rbx = adj(ss.__rbx, old_guard_lo, old_hi, delta);
        ss.__r12 = adj(ss.__r12, old_guard_lo, old_hi, delta);
        ss.__r13 = adj(ss.__r13, old_guard_lo, old_hi, delta);
        ss.__r14 = adj(ss.__r14, old_guard_lo, old_hi, delta);
        ss.__r15 = adj(ss.__r15, old_guard_lo, old_hi, delta);
        // Argument/caller-saved: guard-page range only.
        ss.__rax = adj(ss.__rax, old_guard_lo, old_lo, delta);
        ss.__rcx = adj(ss.__rcx, old_guard_lo, old_lo, delta);
        ss.__rdx = adj(ss.__rdx, old_guard_lo, old_lo, delta);
        ss.__rsi = adj(ss.__rsi, old_guard_lo, old_lo, delta);
        ss.__rdi = adj(ss.__rdi, old_guard_lo, old_lo, delta);
        ss.__r8  = adj(ss.__r8,  old_guard_lo, old_lo, delta);
        ss.__r9  = adj(ss.__r9,  old_guard_lo, old_lo, delta);
        ss.__r10 = adj(ss.__r10, old_guard_lo, old_lo, delta);
        ss.__r11 = adj(ss.__r11, old_guard_lo, old_lo, delta);
        // Note: x86-64 `push rXX` that page-faults does NOT commit the RSP
        // decrement (the push is atomic).  No SP correction is needed.
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    unsafe {
        let ss = &mut (*(*(ctx as *mut libc::ucontext_t)).uc_mcontext).__ss;
        // SP and FP (__fp = x29): full range.
        ss.__sp = adj(ss.__sp, old_guard_lo, old_hi, delta);
        ss.__fp = adj(ss.__fp, old_guard_lo, old_hi, delta);
        // Callee-saved x19–x28: full range.
        for i in 19..=28usize {
            ss.__x[i] = adj(ss.__x[i], old_guard_lo, old_hi, delta);
        }
        // Argument/caller-saved x0–x18: guard-page range only.
        for i in 0..=18usize {
            ss.__x[i] = adj(ss.__x[i], old_guard_lo, old_lo, delta);
        }
        // AArch64 pre-indexed stores commit SP before the data-abort.
        let correction = sp_predecrement_at_pc(ss.__pc as usize) as u64;
        if correction != 0 { ss.__sp += correction; }
    }
}

// ---------------------------------------------------------------------------
// Grow stack at scheduler re-entry (checkpoint growth)
// ---------------------------------------------------------------------------

/// Check whether `gp`'s saved stack pointer is within `STACK_GUARD` bytes of
/// the guard page, and if so grow the stack proactively before resuming.
///
/// Called from `execute` in `sched.rs` (on g0, before every `gogo` call).
/// Handles the case where a goroutine exhausts most of its stack across
/// multiple scheduler quanta and the SIGSEGV handler is about to fire.
///
/// ## Threshold — `STACK_GUARD` (not `2 × STACK_GUARD`)
///
/// We use `STACK_GUARD` (928 bytes) as the low-water mark, matching Go's
/// `stackGuard`.  This leaves exactly one guard zone of headroom — enough
/// for a typical scheduler-call depth — before triggering a doubling.
/// Using `2 × STACK_GUARD` was excessively conservative: on a 2 KiB initial
/// stack it left only 192 bytes of effective space (`2048 − 1856 = 192`)
/// and would force almost every goroutine to grow on its first scheduling
/// point even when the stack is far from exhausted.
///
/// The reactive SIGBUS/SIGSEGV growth handler remains the safety net for the
/// rare case where a goroutine exhausts the remaining guard zone between two
/// scheduling points without ever calling `gosched`.
///
/// # Safety
/// Must be called from g0; `gp` must be about to be resumed via `gogo`.
pub(crate) unsafe fn grow_stack_if_needed(gp: *mut G) {
    let sp   = unsafe { (*gp).sched.sp };
    let lo   = unsafe { (*gp).stack.lo };

    // sp == 0 means the goroutine has never run yet (initial call).
    if sp == 0 || sp < lo + STACK_GUARD {
        // Growing here avoids the SIGSEGV race on the very first quantum.
        if sp != 0 {
            // Stack is nearly full; proactively double it.
            let old_stack = Stack {
                lo: unsafe { (*gp).stack.lo },
                hi: unsafe { (*gp).stack.hi },
            };
            let old_size = old_stack.hi - old_stack.lo;
            if old_size < STACK_MAX {
                let new_size  = (old_size * 2).min(STACK_MAX);
                let new_stack = unsafe {
                    stack_alloc_size(new_size)
                        .expect("grow_stack_if_needed: allocation failed")
                };
                let delta = unsafe { copystack(gp, &old_stack, &new_stack) };
                unsafe {
                    (*gp).stack       = Stack { lo: new_stack.lo, hi: new_stack.hi };
                    (*gp).stackguard0 = new_stack.lo + STACK_GUARD;
                }
                unsafe { stack_free(&old_stack) };
                let _ = delta;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_write_free() {
        unsafe {
            let stack = stack_alloc().expect("stack_alloc failed");
            let ps = page_size();

            assert_eq!(stack.hi - stack.lo, GOROUTINE_STACK_BYTES);
            // stack.lo is the first byte of the usable region, aligned to a
            // page boundary (it sits immediately above the guard page).
            assert_eq!(stack.lo % ps, 0);
            // stack.hi is stack.lo + GOROUTINE_STACK_BYTES.  On platforms with
            // large pages (e.g. macOS AArch64: 16 KiB), hi may not be aligned
            // when GOROUTINE_STACK_BYTES < page_size.  Do not assert alignment.
            assert!(stack.hi > stack.lo);

            let top = (stack.hi - 8) as *mut u64;
            top.write(0xDEAD_BEEF_CAFE_BABE);
            assert_eq!(top.read(), 0xDEAD_BEEF_CAFE_BABE);

            stack_free(&stack);
        }
    }

    #[test]
    fn page_size_sanity() {
        let ps = page_size();
        assert!(ps.is_power_of_two());
        assert!(ps >= 4096);
        println!("page_size = {ps}");
    }

    #[test]
    fn page_size_concurrent() {
        let handles: Vec<_> = (0..8)
            .map(|_| std::thread::spawn(page_size))
            .collect();
        let sizes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(sizes.windows(2).all(|w| w[0] == w[1]));
    }

    /// stack_alloc_size: variable-size allocation round-trips correctly.
    #[test]
    fn variable_size_alloc() {
        unsafe {
            for &size in &[8 * 1024usize, 16 * 1024, 32 * 1024, 64 * 1024] {
                let stack = stack_alloc_size(size).unwrap();
                assert_eq!(stack.hi - stack.lo, size);
                stack_free(&stack);
            }
        }
    }
}
