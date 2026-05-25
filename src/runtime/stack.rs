//! Goroutine stack allocator and growth machinery — ported from
//! `runtime/stack.go`, `runtime/signal_unix.go`.
//!
//! ## v2.0 — dynamic stack growth
//!
//! Each goroutine starts with an 8 KiB stack (matching Go's `stackMin`).
//! The guard page (`PROT_NONE`) immediately below `stack.lo` turns overflows
//! into a `SIGSEGV` that the runtime intercepts.
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
//! region and adjust those that fall within `[old_lo, old_hi)`.  Return
//! addresses are in the code segment (a completely different address range)
//! and are never mistakenly adjusted.  Integer values that coincidentally
//! equal a stack address are a theoretical false positive but vanishingly
//! rare for the narrow 8–1024 KiB windows used here.

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
use super::g::{Stack, STACK_GUARD, G};

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

/// Minimum goroutine stack size (bytes).  Matches Go's `stackMin = 8 KiB`.
pub(crate) const STACK_MIN: usize = 8 * 1024;

/// Maximum goroutine stack size (bytes). 1 GiB matches Go's `maxstacksize`.
pub(crate) const STACK_MAX: usize = 1024 * 1024 * 1024;

/// Initial stack size for every new goroutine.
pub(crate) const GOROUTINE_STACK_BYTES: usize = STACK_MIN;

/// Stack size for each M's g0 (the scheduler stack).
///
/// The scheduler loop (`schedule` → `findrunnable` → `stopm` → locking) has a
/// deeper call chain than a typical goroutine.  64 KiB gives comfortable
/// headroom without per-goroutine waste.  Go uses the OS thread's native stack
/// (typically 8 MiB) for g0; we use a fixed 64 KiB mmap'd region with the
/// same guard-page layout as a normal goroutine stack.
pub(crate) const G0_STACK_BYTES: usize = 64 * 1024;

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
        return Ok(Stack { lo: base_addr + ps, hi: base_addr + total });
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
        return Ok(Stack { lo: base_addr + ps, hi: base_addr + total });
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
    // that falls within [old_lo, old_hi).
    let mut addr = live_start_new;
    let word = std::mem::size_of::<usize>();
    while addr + word <= new_hi {
        let val = unsafe { *(addr as *const usize) };
        if val >= old_lo && val < old_hi {
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

/// SIGSEGV handler: detect goroutine guard page faults and grow the stack.
#[cfg(not(windows))]
unsafe extern "C" fn sigsegv_handler(
    sig:  libc::c_int,
    info: *mut libc::siginfo_t,
    ctx:  *mut libc::c_void,
) {
    let gp = current_g();
    if !gp.is_null() {
        let fault_addr = unsafe { (*info).si_addr() } as usize;
        let guard_lo   = unsafe { (*gp).stack.lo } - page_size();
        let guard_hi   = unsafe { (*gp).stack.lo };

        if fault_addr >= guard_lo && fault_addr < guard_hi {
            // Guard page access = goroutine stack overflow. Grow it.
            let delta = unsafe { newstack(gp) };

            // Update the interrupted RSP/SP in the platform ucontext_t so the
            // faulting instruction retries on the new stack.
            unsafe { update_sp_in_context(ctx, delta) };
            return; // handler return → OS retries the faulting instruction
        }
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

/// Update the stack pointer saved in the signal `ucontext_t` by `delta`.
/// Platform-specific: Linux x86-64, Linux AArch64, macOS x86-64, macOS AArch64.
#[cfg(not(windows))]
#[allow(unused_variables)]
unsafe fn update_sp_in_context(ctx: *mut libc::c_void, delta: isize) {
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    unsafe {
        let uc  = ctx as *mut libc::ucontext_t;
        let rsp = (*uc).uc_mcontext.gregs[libc::REG_RSP as usize] as isize;
        (*uc).uc_mcontext.gregs[libc::REG_RSP as usize] = (rsp + delta) as libc::greg_t;
    }

    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    unsafe {
        let uc = ctx as *mut libc::ucontext_t;
        let sp = (*uc).uc_mcontext.sp as isize;
        (*uc).uc_mcontext.sp = (sp + delta) as u64;
    }

    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    unsafe {
        let uc  = ctx as *mut libc::ucontext_t;
        // uc_mcontext is *mut __darwin_mcontext64; __ss is __darwin_x86_thread_state64_t.
        let ss  = &mut (*(*uc).uc_mcontext).__ss;
        ss.__rsp = (ss.__rsp as isize + delta) as u64;
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    unsafe {
        let uc  = ctx as *mut libc::ucontext_t;
        let ss  = &mut (*(*uc).uc_mcontext).__ss;
        ss.__sp = (ss.__sp as isize + delta) as u64;
    }
}

// ---------------------------------------------------------------------------
// Grow stack at scheduler re-entry (checkpoint growth)
// ---------------------------------------------------------------------------

/// Check whether `gp`'s saved stack pointer is within STACK_GUARD bytes of
/// the guard page, and if so grow the stack proactively before resuming.
///
/// Called from `execute` in `sched.rs` (on g0, before every `gogo` call).
/// Handles the case where a goroutine exhausts most of its stack across
/// multiple scheduler quanta and the SIGSEGV handler is about to fire.
///
/// # Safety
/// Must be called from g0; `gp` must be about to be resumed via `gogo`.
pub(crate) unsafe fn grow_stack_if_needed(gp: *mut G) {
    let sp   = unsafe { (*gp).sched.sp };
    let lo   = unsafe { (*gp).stack.lo };

    // sp == 0 means the goroutine has never run yet (initial call).
    if sp == 0 || sp < lo + STACK_GUARD * 2 {
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
