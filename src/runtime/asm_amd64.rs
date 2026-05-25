//! x86-64 context switch primitives — ported from `runtime/asm_amd64.s` and
//! `runtime/preempt_amd64.s`.
//!
//! Public entry points:
//! - [`gogo`]                     — restore a saved `Gobuf` and resume a goroutine.
//! - [`mcall`]                    — save current G, switch to g0's stack, call a fn.
//! - [`async_preempt_trampoline`] — save all GPRs + XMMs, call `async_preempt2`,
//!   restore, ret to interrupted PC.  *(v2.0 — Step 4)*
//! - [`systemstack`]              — run a closure on g0's stack (TODO).
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
/// Register usage (System V AMD64 — Linux / macOS):
/// - `rdi` = buf (*mut Gobuf, first arg)
///
/// Register usage (Microsoft x64 — Windows):
/// - `rcx` = buf (*mut Gobuf, first arg)
///
/// Common: `rax` = scratch (target pc), `rbp` / `rsp` restored from Gobuf.

// System V AMD64 ABI (Linux, macOS): first argument in rdi.
#[cfg(not(windows))]
#[unsafe(naked)]
unsafe extern "C" fn gogo_asm(buf: *mut Gobuf) -> ! {
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

// Microsoft x64 ABI (Windows): first argument in rcx.
#[cfg(windows)]
#[unsafe(naked)]
unsafe extern "C" fn gogo_asm(buf: *mut Gobuf) -> ! {
    core::arch::naked_asm!(
        "mov rax, [rcx + {pc}]",   // rax = gobuf.pc  (load before stack switch)
        "mov rbp, [rcx + {bp}]",   // rbp = gobuf.bp  (frame pointer)
        "mov rsp, [rcx + {sp}]",   // rsp = gobuf.sp  (stack switch — do last)
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
#[cfg(not(windows))]
#[unsafe(naked)]
unsafe extern "C" fn mcall_asm(
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

        // ── switch to g0's stack (rdx = g0_gobuf) ────────────────────────
        "mov rsp, [rdx + {sp}]",           // rsp = g0.sp (stack switch)
        "mov rbp, [rdx + {bp}]",           // rbp = g0.bp

        // ── call fn_ptr(g) on g0's stack ─────────────────────────────────
        // rdi = g (first argument, System V: first arg in rdi ✓)
        // rcx = fn_ptr
        "call rcx",
        "ud2",

        pc = const GOBUF_PC_OFFSET,
        sp = const GOBUF_SP_OFFSET,
        bp = const GOBUF_BP_OFFSET,
        g  = const GOBUF_G_OFFSET,
    )
}

// Microsoft x64 ABI (Windows): args in rcx, rdx, r8, r9.
#[cfg(windows)]
#[unsafe(naked)]
unsafe extern "C" fn mcall_asm(
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

        // ── switch to g0's stack (r8 = g0_gobuf) ─────────────────────────
        "mov rsp, [r8 + {sp}]",            // rsp = g0.sp (stack switch)
        "mov rbp, [r8 + {bp}]",            // rbp = g0.bp

        // ── call fn_ptr(g) on g0's stack ─────────────────────────────────
        // rcx = g (still holds g — Microsoft x64 first arg in rcx ✓)
        // r9  = fn_ptr
        "call r9",
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
/// Used by channel operations and the scheduler to ensure critical sections
/// execute with sufficient stack headroom.  Currently unimplemented; goroutines
/// that need extra headroom should heap-allocate large temporaries instead.
#[allow(dead_code)]
pub(crate) unsafe fn systemstack<F: FnOnce()>(_f: F) {
    todo!("systemstack not yet implemented")
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
