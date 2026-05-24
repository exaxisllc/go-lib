//! x86-64 context switch primitives — ported from `runtime/asm_amd64.s`.
//!
//! Three public entry points:
//! - [`gogo`]       — restore a saved `Gobuf` and resume a goroutine.
//! - [`mcall`]      — save current G, switch to g0's stack, call a fn.
//! - [`systemstack`] — run a closure on g0's stack (TODO: step 8).
//!
//! ## Design vs Go's approach
//!
//! Go uses the `FS` segment register (Linux) or `GS` (macOS) as a TLS pointer
//! to the current G, accessed from assembly via `get_tls`.  We use a Rust
//! `thread_local!` (`CURRENT_G` in `g.rs`) updated from the Rust wrapper,
//! keeping the naked asm free of OS-specific TLS segment tricks.
//!
//! ## Calling convention (System V AMD64 ABI)
//! Arguments: `rdi`, `rsi`, `rdx`, `rcx`, `r8`, `r9`.
//! Caller-saved: `rax`, `rcx`, `rdx`, `rsi`, `rdi`, `r8`–`r11`.
//! Callee-saved: `rbx`, `rbp`, `r12`–`r15`.
//! Stack alignment: 16-byte aligned before a `call` instruction.
//!
//! ## Gobuf field offsets (verified by compile-time assertions in `g.rs`)
//! ```text
//!  0  sp
//!  8  pc
//! 16  g
//! 24  ctxt
//! 32  ret
//! 40  lr  (unused on x86-64)
//! 48  bp
//! ```
//!
//! ## Assembly syntax
//! Rust's `naked_asm!` on x86-64 uses Intel syntax by default.

use std::ptr::addr_of_mut;

use super::g::{
    set_current_g, Gobuf, G,
    G0_SCHED,
    GOBUF_BP_OFFSET, GOBUF_G_OFFSET,
    GOBUF_PC_OFFSET, GOBUF_SP_OFFSET,
};

// ---------------------------------------------------------------------------
// gogo — restore saved state and jump
// ---------------------------------------------------------------------------

/// Restore register state from `buf` and resume execution at `buf.pc`.
///
/// Ported from `runtime·gogo` in `runtime/asm_amd64.s`.
///
/// Register usage:
/// - `rdi` = buf (*mut Gobuf, argument — System V first arg)
/// - `rax` = scratch (target pc, loaded before rsp changes)
/// - `rbp`, `rsp` — restored from Gobuf
#[unsafe(naked)]
unsafe extern "C" fn gogo_asm(buf: *mut Gobuf) -> ! {
    // rdi = buf (*mut Gobuf)
    // Load pc and bp before touching rsp — rdi remains valid throughout
    // because it is a general-purpose register, not stack-relative.
    core::arch::naked_asm!(
        "mov rax, [rdi + {pc}]",   // rax = gobuf.pc  (load before stack switch)
        "mov rbp, [rdi + {bp}]",   // rbp = gobuf.bp  (frame pointer)
        "mov rsp, [rdi + {sp}]",   // rsp = gobuf.sp  (stack switch — do last)
        "jmp rax",                  // jump to pc — never returns
        pc = const GOBUF_PC_OFFSET,
        bp = const GOBUF_BP_OFFSET,
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
/// generate a proper `leave; ret` epilogue for `mcall()` *after* the
/// `callq mcall_asm` instruction.  When `gogo` later resumes a goroutine it
/// jumps to `g_sched.pc`, which points at that epilogue.  Executing the
/// epilogue unwinds the `mcall` and caller (`gosched`/`gopark`) frames
/// normally, returning control to the goroutine's user code — exactly the
/// same sequence Go uses.
///
/// Ported from `runtime·mcall` in `runtime/asm_amd64.s`.
///
/// System V AMD64 argument registers on entry:
/// - `rdi` = g         (*mut G  — current goroutine)
/// - `rsi` = g_sched   (*mut Gobuf — &(*g).sched, pre-computed by wrapper)
/// - `rdx` = g0_gobuf  (*mut Gobuf — &(*g0).sched, from G0_SCHED TLS)
/// - `rcx` = fn_ptr    (unsafe extern "C" fn(*mut G))
/// - `[rsp]`= return address pushed by `call mcall_asm` — saved as
///            `g_sched.pc` so `gogo` can resume at `mcall`'s epilogue.
/// - `rsp` = stack pointer (pointing at the return address on entry)
///
/// Caller SP before the call is `rsp + 8` (the `call` instruction pushed the
/// 8-byte return address).  This is what we save as `g_sched.sp` so that
/// `gogo` can restore it and have the stack look exactly as it did before
/// `mcall_asm` was entered.
#[unsafe(naked)]
unsafe extern "C" fn mcall_asm(
    _g:        *mut G,
    _g_sched:  *mut Gobuf,
    _g0_gobuf: *mut Gobuf,
    _fn_ptr:   unsafe extern "C" fn(*mut G),
) {
    core::arch::naked_asm!(
        // ── save current goroutine's context into g_sched (rsi) ──────────
        // On x86-64 the `call` instruction pushes the return address at [rsp]
        // before entering the callee, so on entry: [rsp] = caller's PC.
        "mov rax,          [rsp]",         // rax = return address (caller PC)
        "mov [rsi + {pc}], rax",           // g_sched.pc = return address
        "lea rax,          [rsp + 8]",     // rax = caller SP (before call pushed ret addr)
        "mov [rsi + {sp}], rax",           // g_sched.sp = caller SP
        "mov [rsi + {bp}], rbp",           // g_sched.bp = frame pointer
        "mov [rsi + {g}],  rdi",           // g_sched.g  = g (keep field in sync)

        // ── switch to g0's stack (rdx = g0_gobuf) ────────────────────────
        // g0's sp must be 16-byte aligned before the `call` below (ABI).
        // The invariant is established by M::new (step 6).
        "mov rsp, [rdx + {sp}]",           // rsp = g0.sp (stack switch)
        "mov rbp, [rdx + {bp}]",           // rbp = g0.bp

        // ── call fn_ptr(g) on g0's stack ─────────────────────────────────
        // rdi = g (first argument, untouched since function entry)
        // rcx = fn_ptr
        // `call` pushes a return address onto g0's stack then jumps.
        "call rcx",

        // fn_ptr must never return.  Trap immediately to catch bugs.
        "ud2",

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
/// Updates `CURRENT_G` before the context switch so any code running after
/// the switch sees the correct current goroutine.  The caller must have
/// initialised `g.sched.sp` and `g.sched.pc` before calling.
///
/// Ported from the `execute` → `gogo` path in `runtime/proc.go` +
/// `runtime/asm_amd64.s`.
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
/// compiler must emit an epilogue (`leave; ret`) after `callq mcall_asm` so
/// that `gogo` can resume the goroutine by jumping to that epilogue and
/// returning through the call stack normally.
///
/// Requires `G0_SCHED` to be initialised by `M::new` (step 6); panics in
/// debug builds if it has not been set yet.
///
/// Ported from `runtime·mcall` in `runtime/proc.go` + `runtime/asm_amd64.s`.
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
        // directly to the `leave; ret` epilogue of this function (the
        // instruction after `callq mcall_asm`), unwinding the frame chain
        // back to the goroutine's user code.
    }
}

/// Run `f` on the M's system stack (g0) then return to the current G's stack.
///
/// TODO(step 8): implement once `M::g0` is wired up.
#[allow(dead_code)]
pub(crate) unsafe fn systemstack<F: FnOnce()>(_f: F) {
    todo!("systemstack: requires M::g0 (step 6) and schedule() (step 8)")
}
