# go-lib v2 Roadmap — Addressing Known Limitations

This document is the step-by-step implementation plan for the eight known
limitations listed in `README.md`.  Each step includes a scope description,
sub-tasks, and any inter-step prerequisites.

---

## Suggested release sequencing

```
v0.2.0  Steps 1, 2, 6, 7   — pure library / scheduler API, low risk
v0.2.0  Step 8 (Option A)   — loom integration, CI hardening
v0.2.0  Steps 3, 4, 5       — assembly + kernel APIs, ship together
v0.3.1  Step 8 (Option B)   — TSan CI pass post v0.2.0
```

Steps 3 → 4 → 5 form a natural dependency chain:

- Stack growth (Step 3) must land before async preemption (Step 4), because
  the signal handler injects a preemption frame onto the live goroutine stack
  and requires `g.stackguard0` to be valid.
- Async preemption (Step 4) should land before netpoll (Step 5) so that an
  I/O-ready callback is never delayed by a CPU-bound goroutine hogging an M.

---

## Step 1 — `go_lib::sync::Cond`

**Effort:** small  
**Release:** v0.2.0

Port Go's `sync.Cond` as a goroutine-aware condition variable.  Using
`std::sync::Condvar` directly would block an OS thread (M), starving other
goroutines sharing that M.

### Sub-tasks

1. Add `src/sync/cond.rs`.
2. Define `Cond { waitq: Mutex<WaitQueue>, ... }` where `WaitQueue` is a
   `VecDeque<*mut G>`.
3. Implement `wait(guard: MutexGuard<T>)`:
   - Enqueue `current_g()` into `waitq`.
   - Release `guard`.
   - Call `gopark(WaitReason::CondWait)` to park the goroutine without blocking
     the M.
   - Re-acquire `guard` on return.
4. Implement `notify_one()`: pop one G from `waitq`, call `goready(gp)`.
5. Implement `notify_all()`: drain `waitq`, call `goready` on each.
6. Export as `go_lib::sync::Cond`.
7. Add unit tests (single-waiter, multi-waiter, spurious-wakeup resilience).
8. Add an example `examples/cond.rs` (producer / consumer with bounded buffer).

---

## Step 2 — Runtime-adjustable `GOMAXPROCS`

**Effort:** medium  
**Release:** v0.2.0

Allow the number of Ps (and thus the degree of true parallelism) to be changed
at runtime, matching Go's `runtime.GOMAXPROCS(n)`.

### Sub-tasks

1. Add `GOMAXPROCS: AtomicUsize` field to `Sched`.
2. Implement `pub fn set_gomaxprocs(n: usize) -> usize`:
   - Atomically swap the value; return the old value.
   - If *increasing* by `delta`: allocate `delta` new `P` objects via `P::new`,
     push each to the idle-P list, call `startm(ptr::null_mut())` for each new P.
   - If *decreasing* by `delta`: mark `delta` surplus Ps `Pidle`; their M-threads
     will call `stopm` on the next `findrunnable` cycle — no forced kill needed.
3. In `schedinit`, read the `GOMAXPROCS` environment variable (parse as `usize`,
   clamp to `[1, 256]`) before falling back to `available_parallelism`.
4. Update `findrunnable` to consult the live `GOMAXPROCS` value when iterating
   Ps for work-stealing so newly added Ps are visible immediately.
5. Tests:
   - Verify P count before and after an increase.
   - Verify P count before and after a decrease.
   - Stress-test: spawn 64 goroutines while toggling GOMAXPROCS between 1 and 8.

---

## Step 3 — Goroutine stack growth (`morestack` / `copystack`)

**Effort:** large  
**Release:** v0.2.0  
**Prerequisite for:** Step 4

This is the most invasive change.  Every goroutine currently gets a fixed 64 KiB
`mmap` region.  Stack growth requires emitting stack-check prologues in the
context-switch assembly, a `morestack` trampoline, and a `copystack` routine
that relocates live stack frames.

### Sub-tasks

1. **Reduce initial stack size** — change the allocation in `stack.rs` from
   64 KiB to 8 KiB.  Update `g.stackguard0 = stack.lo + STACK_GUARD` where
   `STACK_GUARD` is 256 bytes (matches Go's `_StackGuard`).

2. **Stack-check prologue** — in `asm_amd64.rs` (and `asm_arm64.rs`), add a
   check at the top of `gogo` and at each `mcall` return:
   ```asm
   ; AMD64
   cmp rsp, [rdi + offsetof(G, stackguard0)]
   jbe  morestack_trampoline
   ```
   The offset of `stackguard0` must be a compile-time constant exported from
   `g.rs` via a `const OFFSET_STACKGUARD0: usize`.

3. **`morestack` trampoline** (naked fn, Rust + inline asm):
   - Save all caller-saved registers to the current goroutine stack.
   - Switch to g0 via `mcall`.
   - Call `newstack()` on g0's stack.
   - Restore registers; return to the function that triggered the check.

4. **`newstack()`** (in `stack.rs`):
   - Compute `new_size = old_size * 2`, capped at 1 GiB.
   - `mmap` a new region with a guard page at the low end (`mprotect PROT_NONE`).
   - Call `copystack(gp, new_stack)`:
     - Walk the frame chain from `g.sched.sp` up to `g.stack.hi`.
     - For each pointer-sized word in the old stack that falls within
       `[old_lo, old_hi)`, adjust it by `delta = new_lo - old_lo`.
     - Copy all bytes from `old_lo` to `old_hi` into the corresponding position
       in the new stack.
   - Update `g.stack`, `g.stackguard0`, `g.sched.sp`, `g.sched.bp`.
   - `munmap` the old region.

5. **Frame-pointer requirement** — compile with `-C force-frame-pointers=yes`
   (add to `.cargo/config.toml`) so `copystack` can reliably walk frames.

6. **Adjust `G::new`** — allocate 8 KiB, set `stackguard0` correctly.

7. **Remove the 64 KiB constant** from `stack.rs`; replace with `STACK_MIN =
   8 * 1024` and `STACK_MAX = 1 * 1024 * 1024 * 1024`.

8. Tests:
   - A goroutine that recurses 100 000 levels deep must not segfault.
   - Verify `g.stack.hi - g.stack.lo` grows across recursive calls.
   - Verify the old stack region is unmapped after growth (try reading from old
     address; should SIGSEGV — use `catch_unwind` + `signal` in the test).

---

## Step 4 — Async (signal-based) preemption

**Effort:** large  
**Release:** v0.2.0  
**Prerequisite:** Step 3

Currently `sysmon` sets `g.preempt = true` but a goroutine is not actually
preempted until it voluntarily calls `gosched()` or blocks.  Async preemption
injects a signal into the OS thread carrying the goroutine.

### Sub-tasks

1. **Signal handler registration** — in `schedinit`, install a `SIGURG` handler
   via `libc::sigaction`.  `SIGURG` is what Go uses; it is ignored by default
   and not used by debuggers.  The handler must be async-signal-safe.

2. **`asyncPreempt` assembly stub** (`asm_amd64.rs` / `asm_arm64.rs`):
   - The signal frame already saves integer registers (the OS does this).
   - Save FP / SIMD registers to the goroutine stack (the OS does not).
   - Call `asyncPreempt2()` (a Rust function) via an indirect call so the return
     address is on the goroutine's stack.

3. **`asyncPreempt2()`** (in `sched.rs`):
   - Check `g.preempt`; if false, return immediately (spurious signal).
   - Clear `g.preempt`.
   - Call `mcall(preempt_fn)` → `schedule()`, yielding the goroutine.

4. **Signal delivery** — in `sysmon`, when a goroutine has run ≥ 10 ms:
   ```rust
   libc::pthread_kill((*mp).pthread_id, libc::SIGURG);
   ```
   Add `pthread_id: libc::pthread_t` to the `M` struct; set it in the M-thread
   body via `libc::pthread_self()`.

5. **Guard against signal-in-g0** — check `current_g().is_null()` at the top
   of the signal handler; if on g0 (or in `entersyscall`), return immediately.

6. Tests:
   - A CPU-bound goroutine with no `gosched()` calls must be preempted within
     ~20 ms (measure with `Instant::now()`).
   - A goroutine blocked in `gopark` must not receive spurious wakeups from
     the signal.

---

## Step 5 — Netpoll / async I/O

**Effort:** large  
**Release:** v0.2.0  
**Prerequisite:** Step 4 recommended (not strictly required)

Add a `netpoll` subsystem so goroutines can wait on file-descriptor readiness
without blocking an OS thread.  This removes the need for `with_syscall` on
network operations.

### Sub-tasks

1. **`src/runtime/netpoll.rs`**:
   - `netpoll_init()` — create the `epoll` fd (Linux) or `kqueue` fd (macOS).
   - `netpoll_arm(fd, mode, gp)` — register `fd` for read (`mode=0`) or write
     (`mode=1`), storing `gp` in `epoll_data` / `kevent.udata`.
   - `netpoll_unarm(fd)` — deregister.
   - `netpoll_wait(timeout_ns: i64) -> Vec<*mut G>` — call `epoll_wait` /
     `kevent` with the given timeout; return the list of ready Gs.

2. **Integrate into `findrunnable`**:
   ```rust
   // After failing all steal attempts:
   let ready = netpoll_wait(0);   // non-blocking poll
   for gp in ready { goready(gp); }
   ```

3. **Integrate into `sysmon`** — when the run queues are empty, call
   `netpoll_wait(sleep_ns)` so the process can sleep efficiently on I/O.

4. **`go_lib::net` facade** (`src/net.rs`):
   - `TcpListener::bind(addr)` — wraps `std::net::TcpListener`; sets `O_NONBLOCK`.
   - `TcpListener::accept() -> TcpStream` — calls `libc::accept4`; on
     `EAGAIN`, calls `netpoll_arm(fd, READ, current_g())` then `gopark`.
   - `TcpStream::read(&mut self, buf) -> usize` — same pattern for `EAGAIN`.
   - `TcpStream::write(&self, buf) -> usize` — same pattern for `EAGAIN` on write.

5. **`with_syscall` remains** for file I/O (no `epoll` support for regular files
   on Linux; use `io_uring` in a future step if needed).

6. Tests:
   - Echo server + client goroutines exchange 10 000 messages without blocking
     any OS thread beyond the M-threads.
   - Verify that a goroutine waiting on `TcpStream::read` does not hold its P
     (another goroutine on the same P must make progress).

---

## Step 6 — `panic` / `recover` across goroutine boundaries

**Effort:** medium  
**Release:** v0.2.0

Currently a Rust panic inside a goroutine aborts the process.  This step gives
each goroutine its own panic boundary and exposes a `recover()`-like API.

### Sub-tasks

1. **Wrap goroutine bodies in `catch_unwind`** — in `execute` (the entry point
   called by `gogo`), wrap the goroutine closure:
   ```rust
   let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
   if let Err(payload) = result {
       handle_goroutine_panic(payload);
   }
   ```

2. **`handle_goroutine_panic`**:
   - Store `payload` in a thread-local `GOROUTINE_PANIC: Cell<Option<Box<dyn Any>>>`.
   - Call the user-set panic handler (see below), then clear the cell.
   - Continue into `goexit0` / `schedule` so the scheduler keeps running.

3. **`go_lib::set_panic_handler(f: fn(&dyn Any))`** — registers a process-wide
   handler called for every unrecovered goroutine panic.  Default: print the
   payload to stderr (mirrors Go's behaviour of printing the stack trace and
   continuing).

4. **`go_lib::recover() -> Option<Box<dyn Any>>`** — if called from within the
   panic handler or a deferred closure registered *before* the panic, drains
   `GOROUTINE_PANIC` and returns `Some(payload)`; otherwise returns `None`.
   This matches Go's `recover()` semantics closely enough for practical use.

5. Tests:
   - A panicking goroutine does not abort the process.
   - Other goroutines continue to run after a peer panics.
   - `recover()` returns the correct payload.
   - A panic in the main goroutine still propagates (no silent swallow).

---

## Step 7 — `go_lib::context`

**Effort:** small  
**Release:** v0.2.0

A pure-library port of Go's `context` package.  No scheduler changes required.

### Sub-tasks

1. Add `src/context.rs`.

2. Define `ContextInner`:
   ```rust
   struct ContextInner {
       deadline: Option<Instant>,
       done_tx:  Option<Sender<()>>,
       done_rx:  Receiver<()>,
       parent:   Option<Arc<ContextInner>>,
       err:      Mutex<Option<ContextError>>,
   }
   ```

3. **`Context`** — a newtype `Arc<ContextInner>` with these methods:
   - `done(&self) -> &Receiver<()>` — usable directly in `select!`.
   - `deadline(&self) -> Option<Instant>`
   - `err(&self) -> Option<ContextError>` — `None | Cancelled | DeadlineExceeded`.

4. **Constructors**:
   - `context::background()` — root; never cancels.
   - `context::with_cancel(parent: &Context) -> (Context, CancelFn)` — `CancelFn`
     sends `()` on the done channel and sets `err = Cancelled`.
   - `context::with_deadline(parent: &Context, t: Instant) -> (Context, CancelFn)` —
     spawns a goroutine that calls `go_lib::sleep(t - now)` then cancels.
   - `context::with_timeout(parent: &Context, d: Duration) -> (Context, CancelFn)` —
     sugar over `with_deadline`.

5. **Parent propagation** — when a parent is cancelled, all children must also be
   cancelled.  Implement via a `children: Mutex<Vec<Weak<ContextInner>>>` list on
   each `ContextInner`; `CancelFn` walks the list and cancels each child.

6. Export as `go_lib::context::{background, with_cancel, with_deadline, with_timeout, Context, CancelFn, ContextError}`.

7. Tests:
   - Cancel propagates from parent to child.
   - `done()` channel receives after cancel.
   - Deadline fires automatically via `sleep`.
   - `with_cancel` from a done-channel example in `select!`.

---

## Step 8 — Race detector

**Effort:** medium  
**Release:** v0.2.0 (Option A) / v0.3.1 (Option B)

### Option A — `loom` integration (near-term, nightly not required)

1. Add `loom` as a dev-dependency under `[dev-dependencies]`.
2. Add a Cargo feature `loom` that replaces `std::sync::atomic`, `std::sync::Mutex`,
   and `std::sync::Arc` with `loom::sync::atomic`, etc., via `cfg` re-exports
   in a `src/loom_shim.rs` module.
3. The raw-pointer and assembly paths (`gopark`, `goready`, context switches)
   cannot be modelled by `loom` — annotate them with `loom::skip` or keep them
   outside the loom boundary.
4. Add a CI job:
   ```yaml
   - run: cargo test --cfg loom
   ```
5. Run `loom` with `LOOM_MAX_PERMUTATIONS=10000` on the channel, WaitGroup, and
   Cond tests.

### Option B — ThreadSanitizer (v0.2.0, requires nightly)

1. Add a CI matrix entry using the nightly toolchain:
   ```yaml
   - run: cargo +nightly test -Zsanitizer=thread --target x86_64-unknown-linux-gnu
   ```
2. TSan instruments the assembly context switches and catches happens-before
   violations across goroutine boundaries that `loom` cannot model.
3. Annotate known-benign intentional races (e.g., the `g.preempt` flag written
   by sysmon without a lock) with `__tsan_acquire` / `__tsan_release` annotations
   via `libc` bindings, matching how the Go runtime annotates its own races.
4. Fix any real races TSan surfaces before shipping v0.2.0.

---

## File map — new / changed files per step

| Step | New files | Changed files |
|---|---|---|
| 1 | `src/sync/cond.rs`, `examples/cond.rs` | `src/sync/mod.rs`, `src/lib.rs` |
| 2 | — | `src/runtime/sched.rs`, `src/runtime/p.rs`, `src/lib.rs` |
| 3 | — | `src/runtime/stack.rs`, `src/runtime/asm_amd64.rs`, `src/runtime/asm_arm64.rs`, `src/runtime/g.rs` |
| 4 | — | `src/runtime/sched.rs`, `src/runtime/sysmon.rs`, `src/runtime/m.rs`, `src/runtime/asm_amd64.rs`, `src/runtime/asm_arm64.rs` |
| 5 | `src/runtime/netpoll.rs`, `src/net.rs` | `src/runtime/sched.rs`, `src/runtime/sysmon.rs`, `src/lib.rs` |
| 6 | — | `src/runtime/sched.rs`, `src/lib.rs` |
| 7 | `src/context.rs` | `src/lib.rs` |
| 8A | `src/loom_shim.rs` | `Cargo.toml`, `.github/workflows/ci.yml` |
| 8B | — | `.github/workflows/ci.yml` |
