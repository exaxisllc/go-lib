// SPDX-License-Identifier: Apache-2.0
//! x86-64 context switch primitives — ported from `runtime/asm_amd64.s` and
//! `runtime/preempt_amd64.s`.
//!
//! Public entry points:
//! - [`gogo`]                     — restore a saved `Gobuf` and resume a goroutine.
//! - [`mcall`]                    — save current G, switch to g0's stack, call a fn.
//! - [`async_preempt_trampoline`] — save all GPRs + XMMs, call `async_preempt2`,
//!   restore, ret to interrupted PC.  *(v0.2.0 — Step 4)*
//! - [`systemstack`]              — run a closure on g0's stack.
//!
//! ## Design vs Go's approach
//!
//! Go uses the `FS` segment register (Linux) or `GS` (macOS) as a TLS pointer
//! to the current G, accessed from assembly via `get_tls`.  We use a Rust
//! `thread_local!` (`CURRENT_G` in `g.rs`) updated from the Rust wrapper,
//! keeping the naked asm free of OS-specific TLS segment tricks.
//!
//! ## Calling convention
//!
//! **System V AMD64 (Linux, macOS)** — arguments in `rdi`, `rsi`, `rdx`, `rcx`, `r8`, `r9`.
//! Caller-saved: `rax`, `rcx`, `rdx`, `rsi`, `rdi`, `r8`–`r11`.
//! Callee-saved: `rbx`, `rbp`, `r12`–`r15`.
//! Stack: 16-byte aligned before a `call`.  No shadow space.
//!
//! **Microsoft x64 (Windows)** — arguments in `rcx`, `rdx`, `r8`, `r9`.
//! Caller-saved: `rax`, `rcx`, `rdx`, `r8`–`r11`.
//! Callee-saved: `rbx`, `rbp`, `rdi`, `rsi`, `r12`–`r15`, `xmm6`–`xmm15`.
//! Stack: 16-byte aligned before a `call`.  **Caller must allocate 32 bytes
//! of shadow space below RSP before any `call`** — the callee may write its
//! first four register arguments there.  Without it a callee that spills its
//! first argument (`rcx`) would write to `[rsp+8]`, which equals `g0.stack.hi+8`
//! — just past the end of the VirtualAlloc region → `STATUS_ACCESS_VIOLATION`.
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
    GOBUF_PC_OFFSET, GOBUF_REGS_OFFSET, GOBUF_SP_OFFSET,
};
#[cfg(windows)]
use super::g::{G_STACK_HI_OFFSET, G_STACK_LO_OFFSET};

// ---------------------------------------------------------------------------
// gogo — restore saved state and jump
// ---------------------------------------------------------------------------

/// Restore register state from `buf` and resume execution at `buf.pc`.
///
/// Ported from `runtime·gogo` in `runtime/asm_amd64.s`.
///
/// Register usage (System V AMD64 — Linux / macOS):
/// - `rdi` = buf (*mut Gobuf, first arg)
///
/// Register usage (Microsoft x64 — Windows):
/// - `rcx` = buf (*mut Gobuf, first arg)
///
/// Common: `rax` = scratch (target pc), `rbp` / `rsp` restored from Gobuf.
///
/// ## Callee-saved register restoration
///
/// `gogo_asm` resumes execution at `buf.pc`, which (for a `mcall`-yielded
/// goroutine) is the instruction *immediately after* `call mcall_asm`.  The
/// Rust function that called `mcall` follows the platform ABI, which means
/// it may hold live values in callee-saved registers across the call.  We
/// restore those slots here so the caller's frame sees the exact register
/// state it left behind, not whatever the scheduler happened to leave there.
///
/// System V AMD64 (Linux/macOS) callee-saved GPRs: RBX, R12, R13, R14, R15
/// (plus RBP, which we already restore from `bp`).  No callee-saved XMM/YMM.
// System V AMD64 ABI (Linux, macOS): first argument in rdi.
#[cfg(not(windows))]
#[unsafe(naked)]
pub(crate) unsafe extern "C" fn gogo_asm(buf: *mut Gobuf) -> ! {
    core::arch::naked_asm!(
        // Load callee-saved GPRs from gobuf.regs[..] BEFORE switching stacks.
        // Order matches mcall_asm's save order: [rbx, r12, r13, r14, r15].
        "mov rbx, [rdi + {regs} + 0]",
        "mov r12, [rdi + {regs} + 8]",
        "mov r13, [rdi + {regs} + 16]",
        "mov r14, [rdi + {regs} + 24]",
        "mov r15, [rdi + {regs} + 32]",
        "mov rax, [rdi + {pc}]",   // rax = gobuf.pc  (load before stack switch)
        "mov rbp, [rdi + {bp}]",   // rbp = gobuf.bp  (frame pointer)
        "mov rsp, [rdi + {sp}]",   // rsp = gobuf.sp  (stack switch — do last)
        "jmp rax",                  // jump to pc — never returns
        pc   = const GOBUF_PC_OFFSET,
        bp   = const GOBUF_BP_OFFSET,
        sp   = const GOBUF_SP_OFFSET,
        regs = const GOBUF_REGS_OFFSET,
    )
}

// Microsoft x64 ABI (Windows): first argument in rcx.
// Microsoft x64 callee-saved GPRs add RDI and RSI vs. System V.
// (Microsoft x64 also has callee-saved XMM6-15; not yet saved here — see
// the FIXME in mcall_asm below.)
#[cfg(windows)]
#[unsafe(naked)]
pub(crate) unsafe extern "C" fn gogo_asm(buf: *mut Gobuf) -> ! {
    core::arch::naked_asm!(
        // Restore callee-saved GPRs: [rbx, rdi, rsi, r12, r13, r14, r15].
        "mov rbx, [rcx + {regs} + 0]",
        "mov rdi, [rcx + {regs} + 8]",
        "mov rsi, [rcx + {regs} + 16]",
        "mov r12, [rcx + {regs} + 24]",
        "mov r13, [rcx + {regs} + 32]",
        "mov r14, [rcx + {regs} + 40]",
        "mov r15, [rcx + {regs} + 48]",
        "mov rax, [rcx + {pc}]",   // rax = gobuf.pc  (load before stack switch)
        "mov rbp, [rcx + {bp}]",   // rbp = gobuf.bp  (frame pointer)
        "mov rsp, [rcx + {sp}]",   // rsp = gobuf.sp  (stack switch — do last)
        "jmp rax",                  // jump to pc — never returns
        pc   = const GOBUF_PC_OFFSET,
        bp   = const GOBUF_BP_OFFSET,
        sp   = const GOBUF_SP_OFFSET,
        regs = const GOBUF_REGS_OFFSET,
    )
}

// ---------------------------------------------------------------------------
// mcall — save current G's state and switch to g0
// ---------------------------------------------------------------------------

/// Save the current goroutine's registers into `g_sched`, switch to g0's
/// stack, and call `fn_ptr(g)`.  Never returns via the normal path.
///
/// The return type is `()` (not `!`) deliberately: the Rust compiler must
/// generate a proper epilogue for `mcall()` *after* the `call mcall_asm`
/// instruction.  When `gogo` later resumes a goroutine it jumps to
/// `g_sched.pc`, which points at that epilogue.  Executing the epilogue
/// unwinds the `mcall` and caller (`gosched`/`gopark`) frames normally —
/// exactly the same sequence Go uses.
///
/// Ported from `runtime·mcall` in `runtime/asm_amd64.s`.
///
/// ## Calling conventions
///
/// **System V AMD64 (Linux, macOS)** — argument registers on entry:
/// - `rdi` = g, `rsi` = g_sched, `rdx` = g0_gobuf, `rcx` = fn_ptr
///
/// **Microsoft x64 (Windows)** — argument registers on entry:
/// - `rcx` = g, `rdx` = g_sched, `r8` = g0_gobuf, `r9` = fn_ptr
///
/// In both ABIs `[rsp]` on entry holds the return address pushed by the
/// `call mcall_asm` instruction.  Caller SP = `rsp + 8`.
// System V AMD64 ABI (Linux, macOS): args in rdi, rsi, rdx, rcx.
//
// ## Callee-saved register save
//
// `mcall_asm` is invoked by a Rust function that obeys the platform ABI: it
// expects callee-saved GPRs (RBX, R12–R15 on System V, plus RBP) to be
// preserved across the call.  But the goroutine is then yielded — the
// scheduler will run arbitrary code on this M and may clobber every register.
// Without saving the callee-saves here, the caller would resume with
// scheduler garbage in RBX/R12–R15 → corruption.
//
// We save them into `g_sched.regs[..]` (slots [0..5]).  `gogo_asm` restores
// them when the goroutine is resumed.  Order: [rbx, r12, r13, r14, r15].
#[cfg(not(windows))]
#[unsafe(naked)]
pub(crate) unsafe extern "C" fn mcall_asm(
    _g:        *mut G,
    _g_sched:  *mut Gobuf,
    _g0_gobuf: *mut Gobuf,
    _fn_ptr:   unsafe extern "C" fn(*mut G),
) {
    core::arch::naked_asm!(
        // ── save current goroutine's context into g_sched (rsi) ──────────
        "mov rax,          [rsp]",         // rax = return address (caller PC)
        "mov [rsi + {pc}], rax",           // g_sched.pc = return address
        "lea rax,          [rsp + 8]",     // rax = caller SP (before call pushed ret addr)
        "mov [rsi + {sp}], rax",           // g_sched.sp = caller SP
        "mov [rsi + {bp}], rbp",           // g_sched.bp = frame pointer
        "mov [rsi + {g}],  rdi",           // g_sched.g  = g

        // ── save callee-saved GPRs (System V AMD64): rbx, r12-r15 ────────
        "mov [rsi + {regs} + 0],  rbx",
        "mov [rsi + {regs} + 8],  r12",
        "mov [rsi + {regs} + 16], r13",
        "mov [rsi + {regs} + 24], r14",
        "mov [rsi + {regs} + 32], r15",

        // ── switch to g0's stack (rdx = g0_gobuf) ────────────────────────
        "mov rsp, [rdx + {sp}]",           // rsp = g0.sp (stack switch)
        "mov rbp, [rdx + {bp}]",           // rbp = g0.bp

        // ── call fn_ptr(g) on g0's stack ─────────────────────────────────
        // rdi = g (first argument, System V: first arg in rdi ✓)
        // rcx = fn_ptr
        "call rcx",
        "ud2",

        pc   = const GOBUF_PC_OFFSET,
        sp   = const GOBUF_SP_OFFSET,
        bp   = const GOBUF_BP_OFFSET,
        g    = const GOBUF_G_OFFSET,
        regs = const GOBUF_REGS_OFFSET,
    )
}

// Microsoft x64 ABI (Windows): args in rcx, rdx, r8, r9.
//
// Microsoft x64 callee-saved GPRs: RBX, RBP, RDI, RSI, R12, R13, R14, R15.
// We save all of them except RBP (already saved separately in `g_sched.bp`).
// FIXME: Microsoft x64 ALSO requires XMM6–15 to be callee-saved.  Not saved
// here yet — goroutines that hold SSE state across `mcall` may still corrupt.
// Tracked as a follow-up.
#[cfg(windows)]
#[unsafe(naked)]
pub(crate) unsafe extern "C" fn mcall_asm(
    _g:        *mut G,
    _g_sched:  *mut Gobuf,
    _g0_gobuf: *mut Gobuf,
    _fn_ptr:   unsafe extern "C" fn(*mut G),
) {
    core::arch::naked_asm!(
        // ── save current goroutine's context into g_sched (rdx) ──────────
        // rcx = g, rdx = g_sched, r8 = g0_gobuf, r9 = fn_ptr
        "mov rax,          [rsp]",         // rax = return address (caller PC)
        "mov [rdx + {pc}], rax",           // g_sched.pc = return address
        "lea rax,          [rsp + 8]",     // rax = caller SP
        "mov [rdx + {sp}], rax",           // g_sched.sp = caller SP
        "mov [rdx + {bp}], rbp",           // g_sched.bp = frame pointer
        "mov [rdx + {g}],  rcx",           // g_sched.g  = g

        // ── save callee-saved GPRs (Microsoft x64): rbx, rdi, rsi, r12-r15
        "mov [rdx + {regs} + 0],  rbx",
        "mov [rdx + {regs} + 8],  rdi",
        "mov [rdx + {regs} + 16], rsi",
        "mov [rdx + {regs} + 24], r12",
        "mov [rdx + {regs} + 32], r13",
        "mov [rdx + {regs} + 40], r14",
        "mov [rdx + {regs} + 48], r15",

        // ── switch to g0's stack (r8 = g0_gobuf) ─────────────────────────
        "mov rsp, [r8 + {sp}]",            // rsp = g0.sp (= g0.stack.hi — top of allocation)
        "mov rbp, [r8 + {bp}]",            // rbp = g0.bp

        // ── restore TEB stack bounds to g0's stack ────────────────────────
        // `gogo()` sets TEB to the goroutine's stack bounds before every
        // context switch.  Now that we're on g0's stack, we must update the
        // TEB to g0's bounds before any code runs on g0.  Without this,
        // Windows sees RSP outside [StackLimit, StackBase) and either raises
        // a spurious stack-overflow or attempts to auto-grow into unmapped
        // memory (STATUS_ACCESS_VIOLATION, fault-type write).
        //
        // G.stack.lo / G.stack.hi are at byte offsets G_STACK_LO_OFFSET (0)
        // and G_STACK_HI_OFFSET (8) because G is #[repr(C)] with `stack:
        // Stack` as its first field.  g0_gobuf.g (GOBUF_G_OFFSET = 16) is
        // the back-pointer to g0's G struct, set by G::new().
        //
        // r10 / r11 are caller-saved on Windows x64 — safe to clobber here.
        "mov r10, [r8 + {g}]",             // r10 = G0* (g0_gobuf.g)
        "mov r11, [r10 + {stack_lo}]",     // r11 = g0.stack.lo (new StackLimit)
        "mov r10, [r10 + {stack_hi}]",     // r10 = g0.stack.hi (new StackBase)
        "mov qword ptr gs:[0x10], r11",    // TEB.StackLimit = g0.stack.lo
        "mov qword ptr gs:[0x08], r10",    // TEB.StackBase  = g0.stack.hi

        // ── call fn_ptr(g) on g0's stack ─────────────────────────────────
        // Microsoft x64 ABI requires the *caller* to allocate 32 bytes of
        // "home space" (shadow space) before any CALL.  Without it the callee
        // prologue writes rcx → [rsp+8], which equals g0.stack.hi+8 — one
        // byte past the VirtualAlloc region → STATUS_ACCESS_VIOLATION.
        "sub rsp, 32",                     // allocate shadow space (keeps rsp 16-byte aligned)
        // rcx = g (still holds g — Microsoft x64 first arg in rcx ✓)
        // r9  = fn_ptr
        "call r9",
        "ud2",

        pc       = const GOBUF_PC_OFFSET,
        sp       = const GOBUF_SP_OFFSET,
        bp       = const GOBUF_BP_OFFSET,
        g        = const GOBUF_G_OFFSET,
        regs     = const GOBUF_REGS_OFFSET,
        stack_lo = const G_STACK_LO_OFFSET,
        stack_hi = const G_STACK_HI_OFFSET,
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
/// On Windows, the TEB `StackBase` / `StackLimit` fields are updated to
/// reflect the goroutine's custom stack bounds before the RSP switch.
/// Windows' exception dispatcher (`RtlDispatchException`) validates that
/// the faulting RSP is inside `[TEB.StackLimit, TEB.StackBase)` before
/// walking frame-based handlers.  Without this update, any `catch_unwind`
/// inside a goroutine is silently bypassed and the process terminates with
/// `0xe06d7363` (STATUS_CPP_EH_EXCEPTION).
///
/// Ported from the `execute` → `gogo` path in `runtime/proc.go` +
/// `runtime/asm_amd64.s`.
pub(crate) unsafe fn gogo(g: *mut G) -> ! {
    unsafe {
        set_current_g(g);

        // Windows only: tell the OS about the goroutine's stack region so
        // that SEH can find exception handlers while the goroutine runs.
        #[cfg(windows)]
        {
            // Read the fields directly — Stack doesn't implement Copy, and
            // moving out of a raw-pointer dereference requires Copy or
            // ptr::read.  The two usize fields are trivially readable.
            let stack_lo = (*g).stack.lo;
            let stack_hi = (*g).stack.hi;
            // GS:[0x08] = StackBase (exclusive high address of the stack).
            // GS:[0x10] = StackLimit (current lowest committed stack address).
            std::arch::asm!(
                "mov qword ptr gs:[0x08], {hi}",
                "mov qword ptr gs:[0x10], {lo}",
                lo = in(reg) stack_lo,
                hi = in(reg) stack_hi,
                options(nostack, preserves_flags),
            );
        }

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

// ---------------------------------------------------------------------------
// systemstack — run a closure on g0's stack
// ---------------------------------------------------------------------------

/// Low-level stack switch: save goroutine RSP/RBP, switch to `g0_sp`, call
/// `thunk(arg)` on g0's stack, then restore the goroutine's RSP/RBP and return.
///
/// ## Register layout on entry (System V AMD64 — Linux / macOS)
/// - `rdi` = g0_sp   (target stack pointer, aligned to 16 bytes inside)
/// - `rsi` = arg     (opaque closure pointer, forwarded to thunk)
/// - `rdx` = thunk   (function to call on g0's stack)
///
/// ## Register layout on entry (Microsoft x64 — Windows)
/// - `rcx` = g0_sp
/// - `rdx` = arg
/// - `r8`  = thunk
///
/// ## Safety
/// `g0_sp` must be a valid, accessible stack address for this OS thread's g0.
/// `thunk` must not panic or longjmp.
///
/// Ported from `runtime·systemstack` in `runtime/asm_amd64.s`.
#[cfg(not(windows))]
#[allow(dead_code)] // called by systemstack; no callers until systemstack is used
#[unsafe(naked)]
unsafe extern "C" fn systemstack_call(
    _g0_sp: usize,
    _arg:   *mut (),
    _thunk: unsafe extern "C" fn(*mut ()),
) {
    core::arch::naked_asm!(
        // Save goroutine frame pointer on goroutine's stack, then record the
        // goroutine's current RSP in RBP for restoration after the call.
        "push rbp",           // [goroutine stack] save old RBP
        "mov  rbp, rsp",      // RBP = goroutine SP (post-push)
        // Switch to g0 stack (rdi = g0_sp, already the arg).
        "and  rdi, -16",      // 16-byte-align the target SP
        "mov  rsp, rdi",      // RSP now on g0's stack
        "sub  rsp, 8",        // maintain 16-byte alignment before CALL
        // Call thunk(arg): arg is in rsi, thunk is in rdx.
        "mov  rdi, rsi",      // 1st arg: rdi = arg
        "call rdx",           // call thunk(arg) — ret addr lands on g0 stack
        // Restore goroutine stack.
        "mov  rsp, rbp",      // RSP = saved goroutine SP
        "pop  rbp",           // restore old RBP
        "ret",
    )
}

/// Windows x64 variant: rcx=g0_sp, rdx=arg, r8=thunk.
#[cfg(windows)]
#[allow(dead_code)] // called by systemstack; no callers until systemstack is used
#[unsafe(naked)]
unsafe extern "C" fn systemstack_call(
    _g0_sp: usize,
    _arg:   *mut (),
    _thunk: unsafe extern "C" fn(*mut ()),
) {
    core::arch::naked_asm!(
        "push rbp",
        "mov  rbp, rsp",
        "and  rcx, -16",
        "mov  rsp, rcx",
        // 32-byte shadow space + 8-byte alignment pad for CALL.
        "sub  rsp, 40",
        "mov  rcx, rdx",      // 1st arg: rcx = arg
        "call r8",            // call thunk(arg)
        "mov  rsp, rbp",
        "pop  rbp",
        "ret",
    )
}

/// Run `f` on the M's g0 (system) stack, then return to the current goroutine.
///
/// If already on g0 (scheduler context — `CURRENT_G` is null), `f` is called
/// directly without any stack switch.
///
/// ## How the switch works
///
/// `gogo` saves g0's stack pointer into the thread-local `G0_SCHED.sp` every
/// time it switches into a goroutine.  While the goroutine runs, g0 is idle —
/// its stack memory is allocated and valid, just not active.  `systemstack`
/// reads that saved SP, uses `systemstack_call` (a naked helper) to swap RSP,
/// calls `f` on g0's stack, and restores RSP before returning.
///
/// The closure `f` is stored in a `ManuallyDrop` slot on the goroutine's own
/// stack.  The goroutine stack memory remains valid throughout the switch (only
/// RSP changes), so the pointer passed to the thunk is always live.
///
/// Ported from `systemstack` in `runtime/asm_amd64.s`.
#[allow(dead_code)] // future callers: stack growth, signal handlers, GC hooks
pub(crate) unsafe fn systemstack<F: FnOnce()>(f: F) {
    // Already on g0 — call directly without switching stacks.
    if super::g::current_g().is_null() {
        f();
        return;
    }

    let g0_sp = unsafe { (*super::g::g0_sched()).sp };
    debug_assert!(g0_sp != 0, "systemstack: g0_sched.sp is 0 — M not initialised");

    // Keep `f` on the goroutine's stack; it stays accessible after the RSP
    // switch because the goroutine stack is allocated memory, not the
    // currently-active stack in any CPU sense.
    let mut slot = std::mem::ManuallyDrop::new(f);
    let arg = std::ptr::addr_of_mut!(slot) as *mut ();

    /// Thunk called on g0's stack: reads the closure out of `arg` and calls it.
    ///
    /// SAFETY: `arg` points to a `ManuallyDrop<F>` on the goroutine's stack,
    /// which is still valid memory even though RSP has been switched to g0.
    unsafe extern "C" fn thunk<F: FnOnce()>(arg: *mut ()) {
        let f = unsafe { std::ptr::read(arg as *mut F) };
        f();
    }

    unsafe { systemstack_call(g0_sp, arg, thunk::<F>) };
}

// ---------------------------------------------------------------------------
// async_preempt_trampoline — Step 4: async signal-based preemption
// ---------------------------------------------------------------------------

/// Trampoline injected by the SIGURG handler to preempt a running goroutine.
///
/// Windows has no POSIX signal mechanism so this trampoline is not compiled
/// there.  Async preemption is a no-op on Windows; goroutines yield only
/// cooperatively via `gosched` / channel operations.
#[cfg(not(windows))]
///
/// The SIGURG handler redirects the goroutine's `RIP` to this function and
/// pushes the original `RIP` onto the goroutine's stack (decrements `RSP` and
/// writes the original PC to `[RSP]`).  When the goroutine resumes after the
/// signal returns, execution begins here — exactly as if the goroutine had been
/// called with a normal `call` instruction.
///
/// ## Register layout on entry
/// - `[RSP]`   = original `RIP` (the preemption point; serves as the return address)
/// - `RSP+8..` = goroutine's live stack at the moment of preemption
/// - All other registers: unchanged (intact from the interrupted state)
///
/// ## Frame layout (built by this function)
/// ```text
/// [RSP+0  .. RSP+239]: 15 × 8 B general-purpose registers
///                       order: RBP, R15, R14, R13, R12, R11, R10, R9,
///                              R8,  RDI, RSI, RDX, RCX, RBX, RAX
/// [RSP-256 .. RSP-1]:  16 × 16 B XMM registers (XMM15 .. XMM0)
/// ```
///
/// Total frame: 15×8 + 16×16 = 120 + 256 = 376 B (RSP stays 16-byte aligned
/// before the `call async_preempt2` instruction).
///
/// ## Stack alignment
/// On entry `RSP % 16 == 8` (the "call" pushed one 8-byte return address).
/// After 15 pushes (120 B) `RSP % 16 == 8 - 120%16 == 8 - 8 == 0`.
/// After `sub rsp, 256` (256 B) `RSP % 16 == 0`.  Correct for a call site.
///
/// Ported from the auto-generated `asyncPreempt` in `runtime/preempt_amd64.s`.
#[unsafe(naked)]
pub(crate) unsafe extern "C" fn async_preempt_trampoline() {
    // NOTE: this attribute / function body is only compiled on non-Windows
    // (gated by the #[cfg(not(windows))] on the doc-comment block above).
    core::arch::naked_asm!(
        // ── save all general-purpose registers (15 pushes = 120 B) ──────────
        "push rbp",
        "push r15",
        "push r14",
        "push r13",
        "push r12",
        "push r11",
        "push r10",
        "push r9",
        "push r8",
        "push rdi",
        "push rsi",
        "push rdx",
        "push rcx",
        "push rbx",
        "push rax",
        // RSP % 16 == 0 here (15 pushes × 8 B from an 8-mod-16 base)

        // ── allocate XMM save area (256 B for 16 × 16 B XMM regs) ──────────
        "sub rsp, 256",
        // RSP % 16 == 0 — correct call-site alignment

        // ── save XMM registers ───────────────────────────────────────────────
        "movdqu [rsp + 0],   xmm0",
        "movdqu [rsp + 16],  xmm1",
        "movdqu [rsp + 32],  xmm2",
        "movdqu [rsp + 48],  xmm3",
        "movdqu [rsp + 64],  xmm4",
        "movdqu [rsp + 80],  xmm5",
        "movdqu [rsp + 96],  xmm6",
        "movdqu [rsp + 112], xmm7",
        "movdqu [rsp + 128], xmm8",
        "movdqu [rsp + 144], xmm9",
        "movdqu [rsp + 160], xmm10",
        "movdqu [rsp + 176], xmm11",
        "movdqu [rsp + 192], xmm12",
        "movdqu [rsp + 208], xmm13",
        "movdqu [rsp + 224], xmm14",
        "movdqu [rsp + 240], xmm15",

        // ── call async_preempt2 (yields via mcall → schedule) ───────────────
        // RSP is 16-byte aligned here; `call` pushes 8 → RSP%16==8 in callee.
        "call {ap2}",

        // ── restore XMM registers ────────────────────────────────────────────
        "movdqu xmm0,  [rsp + 0]",
        "movdqu xmm1,  [rsp + 16]",
        "movdqu xmm2,  [rsp + 32]",
        "movdqu xmm3,  [rsp + 48]",
        "movdqu xmm4,  [rsp + 64]",
        "movdqu xmm5,  [rsp + 80]",
        "movdqu xmm6,  [rsp + 96]",
        "movdqu xmm7,  [rsp + 112]",
        "movdqu xmm8,  [rsp + 128]",
        "movdqu xmm9,  [rsp + 144]",
        "movdqu xmm10, [rsp + 160]",
        "movdqu xmm11, [rsp + 176]",
        "movdqu xmm12, [rsp + 192]",
        "movdqu xmm13, [rsp + 208]",
        "movdqu xmm14, [rsp + 224]",
        "movdqu xmm15, [rsp + 240]",

        // ── release XMM save area ────────────────────────────────────────────
        "add rsp, 256",

        // ── restore general-purpose registers ────────────────────────────────
        "pop rax",
        "pop rbx",
        "pop rcx",
        "pop rdx",
        "pop rsi",
        "pop rdi",
        "pop r8",
        "pop r9",
        "pop r10",
        "pop r11",
        "pop r12",
        "pop r13",
        "pop r14",
        "pop r15",
        "pop rbp",

        // ── return to the original interrupted PC ─────────────────────────────
        // [RSP] holds the original RIP (placed there by the SIGURG handler).
        "ret",

        ap2 = sym crate::runtime::sched::async_preempt2,
    )
}
