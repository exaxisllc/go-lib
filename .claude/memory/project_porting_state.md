---
name: project-porting-state
description: Current implementation state of the go-lib Go→Rust runtime port — which steps are done, what the key design decisions were, and what comes next.
metadata:
  type: project
---

Porting Go's M:N scheduler and concurrency primitives to Rust with no async
runtime (no Tokio). All code is in `/Users/dberry/project/rust/go-lib`.

**Why:** Build goroutines, channels, select, WaitGroup natively in Rust by
porting Go's runtime from <https://github.com/golang/go>.

**Key design decisions:**
- No Tokio / no async — stackful coroutines via ported scheduler.
- `std::sync::{Mutex, RwLock}` re-exported (skip porting Go's semaphore); the
  `entersyscall`/`exitsyscall` shim (step 15.5) makes them scheduler-safe.
- Channels MUST be ported (they call `gopark`); std channels would park the OS thread.
- Fixed 64 KiB stacks via `mmap` (no `copystack` growth in v1).
- `naked_asm!` (Rust 1.85 stable) for `gogo`/`mcall` context switches.
- Thread-local `CURRENT_G` / `G0_SCHED` instead of Go's FS/GS segment trick.

**Steps completed (as of 2026-05-23):**
- Step 1: Crate scaffolding — full module tree, Cargo.toml (libc dep), go.rs removed.
- Step 2+5: `Gobuf`, `Stack`, `G`, status constants, `WaitReason` in `src/runtime/g.rs`.
  GOBUF_*_OFFSET compile-time assertions in place.
- Step 3: `gogo` / `mcall` assembly in `asm_arm64.rs` + `asm_amd64.rs`.
  `CURRENT_G` and `G0_SCHED` thread-locals in `g.rs`.
- Step 4: Stack allocator in `src/runtime/stack.rs` — mmap + guard page, 3 tests pass.
- Step 6: `M` struct + `Note` park primitive in `src/runtime/m.rs` — 6 tests pass.

**Next steps (in order):**
- Step 7: Port `P` and 256-slot lock-free run queue (`runqput`/`runqget`/`runqsteal`).
- Step 8: Scheduler core (`schedule`, `findrunnable`, `execute`, `goexit0`, `gopark`/`goready`).
- Step 9: Bootstrap (`schedinit`, `run(f)` entry point, M spawning).
- Steps 12-14: `sudog`, channels (`chan.rs`), `select` (`select.rs`).
- Step 15.5: `entersyscall`/`exitsyscall` shim.
- Step 16: `WaitGroup`.

**How to apply:** When resuming, check `docs/porting-plan.md` for the full
step-by-step plan and look at whichever step is next.
