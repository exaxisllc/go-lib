//! AArch64 context switch primitives — ported from `runtime/asm_arm64.s` and
//! `runtime/preempt_arm64.s`.
//!
//! Public entry points:
//! - [`gogo`]                     — restore a saved `Gobuf` and resume a goroutine.
//! - [`mcall`]                    — save current G, switch to g0's stack, call a fn.
//! - [`async_preempt_trampoline`] — save all GPRs + d0–d31, call `async_preempt2`,
//!   restore, ret to interrupted PC.  *(v2.0 — Step 4)*
//! - [`systemstack`]              — run a closure on g0's stack (TODO).
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
/// stack, and call `fn_ptr(g)`.  Never returns via the normal path.
///
/// The return type is `()` (not `!`) deliberately: the Rust compiler must
/// generate a proper epilogue (`ldp x29,x30,[sp],#16; ret`) for `mcall()`
/// after the `blr mcall_asm` instruction.  `gogo_asm` resumes a goroutine
/// by jumping to `g_sched.pc`, which is the LR value saved here — i.e., the
/// address of that epilogue.  Executing it unwinds the `mcall` and caller
/// (`gosched`/`gopark`) frames normally, returning control to the goroutine's
/// user code — exactly the same sequence Go uses.
///
/// Ported from `runtime·mcall` in `runtime/asm_arm64.s`.
///
/// AArch64 argument registers on entry:
/// - `x0` = g          (*mut G  — current goroutine)
/// - `x1` = g_sched    (*mut Gobuf — &(*g).sched, pre-computed by wrapper)
/// - `x2` = g0_gobuf   (*mut Gobuf — &(*g0).sched, from G0_SCHED TLS)
/// - `x3` = fn_ptr     (unsafe extern "C" fn(*mut G))
/// - `x30`= LR / return address — the return address of `blr mcall_asm`
///          inside `mcall()`, i.e., the address of `mcall`'s epilogue.
///          Saved as `g_sched.pc` so `gogo` can resume there.
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
) {
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
/// stack, calling `fn_ptr(g)` there.
///
/// `fn_ptr` must eventually call `schedule()` or hand off via `gogo()` and
/// must not return to its caller.
///
/// The return type is `()` (not `!`) for the same reason as `mcall_asm`: the
/// compiler must emit an epilogue after `blr mcall_asm` so that `gogo` can
/// resume the goroutine by jumping to that epilogue and returning through the
/// call stack normally.
///
/// Requires `G0_SCHED` to be initialised by `M::new` (step 6); panics in
/// debug builds if it has not been set yet.
///
/// Ported from `runtime·mcall` in `runtime/proc.go` + `runtime/asm_arm64.s`.
pub(crate) unsafe fn mcall(g: *mut G, fn_ptr: unsafe extern "C" fn(*mut G)) {
    unsafe {
        let g_sched  = addr_of_mut!((*g).sched);
        let g0_gobuf = G0_SCHED.with(|c| c.get());
        debug_assert!(
            !g0_gobuf.is_null(),
            "mcall: G0_SCHED is null — M::new must be called before spawning goroutines (step 6)",
        );
        mcall_asm(g, g_sched, g0_gobuf, fn_ptr);
        // mcall_asm switches to g0 and calls fn_ptr (which calls schedule,
        // an infinite loop).  Execution never reaches here during normal
        // forward flow.  When gogo() later resumes this goroutine it jumps
        // directly to the epilogue of this function (the instruction after
        // `blr mcall_asm`), unwinding the frame chain back to the user code.
    }
}

/// Run `f` on the M's system stack (g0) then return to the current G's stack.
///
/// Used by channel operations and the scheduler to ensure critical sections
/// execute with sufficient stack headroom, regardless of how much of the
/// goroutine's own stack is already in use.  Currently unimplemented; goroutines
/// that need extra headroom should heap-allocate large temporaries instead.
#[allow(dead_code)]
pub(crate) unsafe fn systemstack<F: FnOnce()>(_f: F) {
    todo!("systemstack not yet implemented")
}

// ---------------------------------------------------------------------------
// async_preempt_trampoline — Step 4: async signal-based preemption
// ---------------------------------------------------------------------------

/// AArch64 async-preemption trampoline.
///
/// The SIGURG handler sets `x30` (LR) = original `PC` then redirects `PC` to
/// this function.  Execution resumes here with all registers intact except x30.
///
/// ## Frame layout (512 B, 16-byte aligned)
/// ```text
/// sp+0   .. sp+231  : x0–x28 (29 GPRs × 8 B)
/// sp+232 .. sp+239  : x29 (frame pointer)
/// sp+240 .. sp+247  : x30 (LR = original PC)
/// sp+248 .. sp+375  : d0–d15  (16 × 8 B double FP regs, caller-saved)
/// sp+376 .. sp+503  : d16–d31 (16 × 8 B double FP regs, callee-saved in AAPCS64)
///   ↑ 504 B used, padded to 512 B for 16-byte alignment
/// ```
///
/// After `bl async_preempt2` (which calls `mcall → schedule` and returns when
/// the goroutine is rescheduled), all registers are restored and `ret` returns
/// to the original PC (via restored x30).
///
/// Ported from the auto-generated `asyncPreempt` in `runtime/preempt_arm64.s`.
#[unsafe(naked)]
pub(crate) unsafe extern "C" fn async_preempt_trampoline() {
    core::arch::naked_asm!(
        // ── allocate frame (512 B) ────────────────────────────────────────────
        "sub sp, sp, #512",

        // ── save GPRs x0–x28, x29, x30 ───────────────────────────────────────
        "stp x0,  x1,  [sp, #0]",
        "stp x2,  x3,  [sp, #16]",
        "stp x4,  x5,  [sp, #32]",
        "stp x6,  x7,  [sp, #48]",
        "stp x8,  x9,  [sp, #64]",
        "stp x10, x11, [sp, #80]",
        "stp x12, x13, [sp, #96]",
        "stp x14, x15, [sp, #112]",
        "stp x16, x17, [sp, #128]",
        "stp x18, x19, [sp, #144]",
        "stp x20, x21, [sp, #160]",
        "stp x22, x23, [sp, #176]",
        "stp x24, x25, [sp, #192]",
        "stp x26, x27, [sp, #208]",
        "stp x28, x29, [sp, #224]",
        "str x30,      [sp, #240]",   // x30 = original PC (set by SIGURG handler)

        // ── save FP regs d0–d31 ───────────────────────────────────────────────
        "stp d0,  d1,  [sp, #248]",
        "stp d2,  d3,  [sp, #264]",
        "stp d4,  d5,  [sp, #280]",
        "stp d6,  d7,  [sp, #296]",
        "stp d8,  d9,  [sp, #312]",
        "stp d10, d11, [sp, #328]",
        "stp d12, d13, [sp, #344]",
        "stp d14, d15, [sp, #360]",
        "stp d16, d17, [sp, #376]",
        "stp d18, d19, [sp, #392]",
        "stp d20, d21, [sp, #408]",
        "stp d22, d23, [sp, #424]",
        "stp d24, d25, [sp, #440]",
        "stp d26, d27, [sp, #456]",
        "stp d28, d29, [sp, #472]",
        "stp d30, d31, [sp, #488]",

        // ── call async_preempt2 ────────────────────────────────────────────────
        // bl sets x30 = return address (inside this trampoline).  On return from
        // async_preempt2 (after the goroutine is rescheduled), sp is restored to
        // this frame by mcall/gogo.
        "bl {ap2}",

        // ── restore FP regs ────────────────────────────────────────────────────
        "ldp d0,  d1,  [sp, #248]",
        "ldp d2,  d3,  [sp, #264]",
        "ldp d4,  d5,  [sp, #280]",
        "ldp d6,  d7,  [sp, #296]",
        "ldp d8,  d9,  [sp, #312]",
        "ldp d10, d11, [sp, #328]",
        "ldp d12, d13, [sp, #344]",
        "ldp d14, d15, [sp, #360]",
        "ldp d16, d17, [sp, #376]",
        "ldp d18, d19, [sp, #392]",
        "ldp d20, d21, [sp, #408]",
        "ldp d22, d23, [sp, #424]",
        "ldp d24, d25, [sp, #440]",
        "ldp d26, d27, [sp, #456]",
        "ldp d28, d29, [sp, #472]",
        "ldp d30, d31, [sp, #488]",

        // ── restore GPRs ──────────────────────────────────────────────────────
        "ldr x30,      [sp, #240]",   // restore original PC into x30 (LR)
        "ldp x28, x29, [sp, #224]",
        "ldp x26, x27, [sp, #208]",
        "ldp x24, x25, [sp, #192]",
        "ldp x22, x23, [sp, #176]",
        "ldp x20, x21, [sp, #160]",
        "ldp x18, x19, [sp, #144]",
        "ldp x16, x17, [sp, #128]",
        "ldp x14, x15, [sp, #112]",
        "ldp x12, x13, [sp, #96]",
        "ldp x10, x11, [sp, #80]",
        "ldp x8,  x9,  [sp, #64]",
        "ldp x6,  x7,  [sp, #48]",
        "ldp x4,  x5,  [sp, #32]",
        "ldp x2,  x3,  [sp, #16]",
        "ldp x0,  x1,  [sp, #0]",

        // ── release frame and return to original PC ────────────────────────────
        "add sp, sp, #512",
        "ret",                          // branches to x30 = original PC

        ap2 = sym crate::runtime::sched::async_preempt2,
    )
}
