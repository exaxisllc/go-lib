//! AArch64 context switch primitives — ported from `runtime/asm_arm64.s`.
//!
//! Three public entry points:
//! - [`gogo`]       — restore a saved `Gobuf` and resume a goroutine.
//! - [`mcall`]      — save current G, switch to g0's stack, call a fn.
//! - [`systemstack`] — run a closure on g0's stack (TODO: step 8).
//!
//! ## Design vs Go's approach
//!
//! Go stores the current G in a dedicated hardware register (`R28` / `x28` on
//! AArch64) via the OS's TLS mechanism and accesses it from assembly without
//! a function call.  We use a Rust `thread_local!` (`CURRENT_G` in `g.rs`)
//! instead, updating it in the Rust wrapper before entering the naked asm.
//! This avoids replicating Go's platform-specific TLS segment tricks while
//! keeping the hot-path assembly minimal.
//!
//! ## Calling convention (AArch64 AAPCS64)
//! Arguments: `x0`–`x7`.  Caller-saved: `x0`–`x18`.  Callee-saved: `x19`–`x28`,
//! `x29` (frame pointer), `x30` (link register / return address).
//!
//! ## Gobuf field offsets (verified by compile-time assertions in `g.rs`)
//! ```text
//!  0  sp
//!  8  pc
//! 16  g
//! 24  ctxt
//! 32  ret
//! 40  lr
//! 48  bp
//! ```

use std::ptr::addr_of_mut;

use super::g::{
    set_current_g, Gobuf, G,
    G0_SCHED,
    GOBUF_BP_OFFSET, GOBUF_G_OFFSET, GOBUF_LR_OFFSET,
    GOBUF_PC_OFFSET, GOBUF_SP_OFFSET,
};

// ---------------------------------------------------------------------------
// gogo — restore saved state and jump
// ---------------------------------------------------------------------------

/// Restore register state from `buf` and resume execution at `buf.pc`.
///
/// Ported from `runtime·gogo` in `runtime/asm_arm64.s`.
///
/// Register usage:
/// - `x0`  = buf (*mut Gobuf, argument)
/// - `x9`  = scratch (target pc)
/// - `x10` = scratch (sp value, cannot load sp from memory directly)
/// - `x29` = frame pointer (bp), `x30` = link register (lr) — restored
#[unsafe(naked)]
unsafe extern "C" fn gogo_asm(buf: *mut Gobuf) -> ! {
    core::arch::naked_asm!(
        "ldr  x9,  [x0, #{pc}]",   // x9  = gobuf.pc  (target instruction)
        "ldr  x29, [x0, #{bp}]",   // x29 = gobuf.bp  (frame pointer)
        "ldr  x30, [x0, #{lr}]",   // x30 = gobuf.lr  (link register)
        "ldr  x10, [x0, #{sp}]",   // x10 = gobuf.sp
        "mov  sp,  x10",            // SP  = gobuf.sp  (cannot be a load target)
        "br   x9",                  // jump to pc — never returns
        pc = const GOBUF_PC_OFFSET,
        bp = const GOBUF_BP_OFFSET,
        lr = const GOBUF_LR_OFFSET,
        sp = const GOBUF_SP_OFFSET,
    )
}

// ---------------------------------------------------------------------------
// mcall — save current G's state and switch to g0
// ---------------------------------------------------------------------------

/// Save the current goroutine's registers into `g_sched`, switch to g0's
/// stack, and call `fn_ptr(g)`.  Never returns.
///
/// Ported from `runtime·mcall` in `runtime/asm_arm64.s`.
///
/// AArch64 argument registers on entry:
/// - `x0` = g          (*mut G  — current goroutine)
/// - `x1` = g_sched    (*mut Gobuf — &(*g).sched, pre-computed by wrapper)
/// - `x2` = g0_gobuf   (*mut Gobuf — &(*g0).sched, from G0_SCHED TLS)
/// - `x3` = fn_ptr     (unsafe extern "C" fn(*mut G))
/// - `x30`= return address (caller's PC, saved as g_sched.pc)
/// - `sp` = caller's stack pointer (saved as g_sched.sp)
///
/// After the stack switch the call `blr x3` runs on g0's stack.  `fn_ptr`
/// must not return — it must tail into `gogo` or loop in `schedule()`.
/// The `brk #1` that follows is a hard trap for debug builds only.
#[unsafe(naked)]
unsafe extern "C" fn mcall_asm(
    _g:        *mut G,
    _g_sched:  *mut Gobuf,
    _g0_gobuf: *mut Gobuf,
    _fn_ptr:   unsafe extern "C" fn(*mut G),
) -> ! {
    core::arch::naked_asm!(
        // ── save current goroutine's context into g_sched (x1) ───────────
        // On AArch64 the return address is always in x30 (LR) on function
        // entry before any prologue — naked fns have no prologue, so x30
        // holds the true return address here.
        "str  x30, [x1, #{pc}]",   // g_sched.pc = return address
        "mov  x9,  sp",
        "str  x9,  [x1, #{sp}]",   // g_sched.sp = caller SP
        "str  x29, [x1, #{bp}]",   // g_sched.bp = frame pointer (x29)
        "str  x0,  [x1, #{g}]",    // g_sched.g  = g (keep field in sync)

        // ── switch to g0's stack (x2 = g0_gobuf) ────────────────────────
        // g0's stack must be 16-byte aligned at this point (ABI requirement
        // for bl/blr).  The invariant is maintained by M::new (step 6).
        "ldr  x9,  [x2, #{sp}]",   // x9 = g0.sp
        "mov  sp,  x9",
        "ldr  x29, [x2, #{bp}]",   // x29 = g0.bp

        // ── call fn_ptr(g) on g0's stack ─────────────────────────────────
        // x0 = g (first argument, untouched since function entry)
        // x3 = fn_ptr
        "blr  x3",

        // fn_ptr must never return.  If it does, execute an explicit trap
        // so the failure is obvious rather than silently corrupt.
        "brk  #0x1",

        pc = const GOBUF_PC_OFFSET,
        sp = const GOBUF_SP_OFFSET,
        bp = const GOBUF_BP_OFFSET,
        g  = const GOBUF_G_OFFSET,
    )
}

// ---------------------------------------------------------------------------
// Public wrappers
// ---------------------------------------------------------------------------

/// Resume goroutine `g` by restoring its saved register state and jumping.
///
/// Updates `CURRENT_G` before the context switch so any code that runs after
/// the switch sees the correct current goroutine.  The caller must have
/// initialised `g.sched.sp` and `g.sched.pc` before calling.
///
/// Ported from the `execute` → `gogo` path in `runtime/proc.go` +
/// `runtime/asm_arm64.s`.
pub(crate) unsafe fn gogo(g: *mut G) -> ! {
    unsafe {
        set_current_g(g);
        gogo_asm(addr_of_mut!((*g).sched))
    }
}

/// Save the current goroutine's state into `g.sched` and switch to g0's
/// stack, calling `fn_ptr(g)` there.  Never returns.
///
/// `fn_ptr` must loop back into the scheduler (call `schedule()`) or hand
/// off execution via `gogo()`.  It must not return to its caller.
///
/// Requires `G0_SCHED` to be initialised by `M::new` (step 6); panics in
/// debug builds if it has not been set yet.
///
/// Ported from `runtime·mcall` in `runtime/proc.go` + `runtime/asm_arm64.s`.
pub(crate) unsafe fn mcall(g: *mut G, fn_ptr: unsafe extern "C" fn(*mut G)) -> ! {
    unsafe {
        let g_sched  = addr_of_mut!((*g).sched);
        let g0_gobuf = G0_SCHED.with(|c| c.get());
        debug_assert!(
            !g0_gobuf.is_null(),
            "mcall: G0_SCHED is null — M::new must be called before spawning goroutines (step 6)",
        );
        mcall_asm(g, g_sched, g0_gobuf, fn_ptr)
    }
}

/// Run `f` on the M's system stack (g0) then return to the current G's stack.
///
/// Used by channel operations and the scheduler to ensure critical sections
/// always execute with sufficient stack headroom, regardless of how much of
/// the goroutine's own stack is already in use.
///
/// TODO(step 8): implement once `M::g0` is wired up.  The implementation will
/// save the current G's `Gobuf`, switch to g0's stack, call `f()`, then
/// switch back and restore the G's stack before returning.
#[allow(dead_code)]
pub(crate) unsafe fn systemstack<F: FnOnce()>(_f: F) {
    todo!("systemstack: requires M::g0 (step 6) and schedule() (step 8)")
}
