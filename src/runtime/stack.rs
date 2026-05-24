//! Goroutine stack allocator — ported from `runtime/stack.go`.
//!
//! ## v1 constraints
//!
//! Go uses an initial stack of 8 KiB (`stackMin`) and grows it on demand via
//! `morestack` / `copystack`.  We skip that machinery and give every goroutine
//! a fixed **64 KiB** stack.  The trade-off: no risk of corrupting adjacent
//! memory on deep call stacks, at the cost of 8× more virtual memory per
//! goroutine than Go uses initially.  Virtual memory is cheap; revisit once
//! `copystack` is ported.
//!
//! ## Layout of each allocation
//!
//! ```text
//! base ──► ┌──────────────────────────────┐
//!          │  guard page  (PROT_NONE)     │  1 × page_size (4 KiB/16 KiB)
//!          ├──────────────────────────────┤ ◄── Stack.lo
//!          │                              │
//!          │  execution stack  (64 KiB)   │  SP starts at hi, grows ↓
//!          │                              │
//!          └──────────────────────────────┘ ◄── Stack.hi
//! ```
//!
//! The guard page turns stack overflow into a hard segfault rather than
//! silent memory corruption — our substitute for Go's `stackguard0` check.

use std::sync::OnceLock;

use libc::{MAP_ANON, MAP_FAILED, MAP_PRIVATE, PROT_NONE, PROT_READ, PROT_WRITE};

use super::g::Stack;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Usable bytes in each goroutine stack.
///
/// Go's initial size is 8 KiB (`stackMin = 2048` after rounding).  We use
/// 64 KiB because we don't implement stack growth — every goroutine gets the
/// same fixed headroom.
pub(crate) const GOROUTINE_STACK_BYTES: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// Page size
// ---------------------------------------------------------------------------

/// Returns the OS page size, queried once from `sysconf` and then cached.
///
/// On macOS/AArch64 (Apple Silicon) this is **16 KiB**; on Linux x86-64 it
/// is typically **4 KiB`.  Every `mmap`'d region is a multiple of this value,
/// so guard page placement and `stack.lo` alignment are automatically correct.
pub(crate) fn page_size() -> usize {
    static PAGE_SIZE: OnceLock<usize> = OnceLock::new();
    *PAGE_SIZE.get_or_init(|| {
        // SAFETY: sysconf is always safe to call with a valid name constant.
        let n = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        assert!(n > 0, "sysconf(_SC_PAGESIZE) returned {n}");
        n as usize
    })
}

// ---------------------------------------------------------------------------
// Allocation
// ---------------------------------------------------------------------------

/// Allocate a new goroutine stack.
///
/// The returned [`Stack`] describes the **usable** region `[lo, hi)`.  The
/// guard page immediately below `lo` is not included in those bounds but is
/// part of the same `mmap` allocation and is freed by [`stack_free`].
///
/// The initial stack pointer should be set to `stack.hi` and grows downward.
///
/// Ported from `stackalloc` in `runtime/stack.go` (fixed-size path only).
///
/// # Errors
/// Returns a static error string if `mmap` or `mprotect` fails.  The OS
/// error code is available via `std::io::Error::last_os_error()` at the
/// call site if more detail is needed.
pub(crate) unsafe fn stack_alloc() -> Result<Stack, &'static str> {
    let ps    = page_size();
    let total = GOROUTINE_STACK_BYTES + ps; // guard page + usable stack

    // Map the entire region as readable + writable anonymous memory.
    // SAFETY: all arguments are valid; MAP_FAILED is checked immediately.
    let base = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            total,
            PROT_READ | PROT_WRITE,
            MAP_ANON | MAP_PRIVATE,
            -1, // fd: must be -1 with MAP_ANON
            0,  // offset
        )
    };

    if base == MAP_FAILED {
        return Err("stack_alloc: mmap failed");
    }

    // Revoke all permissions on the lowest page to create a guard page.
    // Any access below stack.lo faults immediately rather than silently
    // reading or corrupting adjacent memory.
    // SAFETY: `base` is a valid mmap result; `ps` ≤ `total`.
    if unsafe { libc::mprotect(base, ps, PROT_NONE) } != 0 {
        // Clean up the mapping before returning the error.
        unsafe { libc::munmap(base, total) };
        return Err("stack_alloc: mprotect guard page failed");
    }

    let base_addr = base as usize;
    Ok(Stack {
        lo: base_addr + ps,              // first usable byte (above guard)
        hi: base_addr + total,           // one past the last byte
    })
}

// ---------------------------------------------------------------------------
// Deallocation
// ---------------------------------------------------------------------------

/// Free a goroutine stack previously returned by [`stack_alloc`].
///
/// Derives the original `mmap` base address from `stack.lo` by subtracting
/// the page size, then unmaps the whole region — guard page and usable stack
/// together.
///
/// Ported from `stackfree` in `runtime/stack.go`.
///
/// # Safety
/// `stack` must have been returned by [`stack_alloc`] on this process and
/// must not have been freed before.  Any pointers into the stack region
/// become dangling after this call.
pub(crate) unsafe fn stack_free(stack: &Stack) {
    let ps    = page_size();
    let base  = (stack.lo - ps) as *mut libc::c_void;
    let total = (stack.hi - stack.lo) + ps;
    // SAFETY: `base` and `total` reconstruct the original mmap region.
    unsafe { libc::munmap(base, total) };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Allocate a stack, write to its top word, read it back, then free it.
    /// Verifies that the memory is mapped R+W and that bounds are consistent.
    #[test]
    fn alloc_write_free() {
        unsafe {
            let stack = stack_alloc().expect("stack_alloc failed");
            let ps = page_size();

            // Bounds checks.
            assert_eq!(
                stack.hi - stack.lo,
                GOROUTINE_STACK_BYTES,
                "usable stack size mismatch"
            );
            assert_eq!(stack.lo % ps, 0, "stack.lo not page-aligned");
            assert_eq!(stack.hi % ps, 0, "stack.hi not page-aligned");

            // Write a sentinel to the top of the stack and read it back.
            // (SP starts at hi and grows down, so the first word used is at hi-8.)
            let top = (stack.hi - 8) as *mut u64;
            top.write(0xDEAD_BEEF_CAFE_BABE);
            assert_eq!(top.read(), 0xDEAD_BEEF_CAFE_BABE, "stack memory read mismatch");

            stack_free(&stack);
        }
    }

    /// Verify the page size is a power of two and at least 4 KiB.
    #[test]
    fn page_size_sanity() {
        let ps = page_size();
        assert!(
            ps.is_power_of_two(),
            "page size {ps} is not a power of two"
        );
        assert!(ps >= 4096, "page size {ps} is unexpectedly small");
        // macOS/AArch64 uses 16 KiB pages; log it for visibility.
        println!("page_size = {ps}");
    }

    /// Call page_size() from multiple threads to exercise the OnceLock path.
    #[test]
    fn page_size_concurrent() {
        let handles: Vec<_> = (0..8)
            .map(|_| std::thread::spawn(page_size))
            .collect();
        let sizes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(
            sizes.windows(2).all(|w| w[0] == w[1]),
            "page_size() returned different values across threads: {sizes:?}"
        );
    }
}
