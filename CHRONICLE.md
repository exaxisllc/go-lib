# Building go-lib: A Chronicle

*How a faithful port of Go's M:N scheduler came to life in Rust.*

---

## The Idea

The Rust language doesn't pick a single concurrency model, instead, it provides primitives and ownership rules that make any model safe to implement. Unlike Java (virtual threads), Go (goroutines + channels) or Erlang (actor model), Rust ships without a runtime or a preferred style. The async working group and the async-std libraries were unsuccessful attempts to add concurrency to the language. The async-std library has been abandoned, in favor of smol and the async working group has not met in years.

The standard library gives you:
- std::thread — OS threads, nothing more
- std::sync::{Mutex, RwLock, Condvar, Barrier} — shared-state primitives
- std::sync::mpsc — a single-producer/multi-consumer channel
That's it. Everything else — async/await, actors, work-stealing executors, lock-free data structures — lives in the ecosystem (tokio, rayon, crossbeam, actix, etc.).

Go has one of the most elegant concurrency models in any systems language: goroutines that start tiny and grow, channels that block without burning threads, a work-stealing scheduler that squeezes every CPU core. The question was whether that model could be brought to Rust — not via `async/await`, not by wrapping Tokio, but by porting Go's actual runtime, source file by source file, into safe-ish Rust.

The answer became **go-lib**: a crate that lets you write:

```rust
go_lib::run(|| {
    let (tx, rx) = go_lib::chan::chan::<String>(0);
    for i in 0..5 {
        let tx = tx.clone();
        go!(move || tx.send(format!("hello from goroutine {i}")));
    }
    drop(tx);
    while let Some(msg) = rx.recv() { println!("{msg}"); }
});
```

No `async`, no executor, no Tokio — just goroutines.

---

## Phase 1: The Scheduler Core (v0.1.0)

The build started from the bottom of the Go runtime stack and worked upward, one step at a time.

**Step 1 — Workspace scaffold.**  
The Cargo workspace was created with two members: the main `go-lib` crate and a `macros` sub-crate for procedural macros. The `edition = "2024"` was chosen from the start. A CI workflow skeleton was added to give every subsequent step a green-or-red signal.

**Step 2 — `G` and `Gobuf`.**  
The goroutine descriptor (`G`) and its register save area (`Gobuf`) were ported from `runtime/runtime2.go`. `Gobuf` holds `sp`, `pc`, `bp`, `g`, `ctxt`, `ret`, and `lr` — everything needed to freeze and restore a goroutine's execution state. `G` embeds `Gobuf` directly and is marked `#[repr(C)]` because the assembly in the next step addresses its fields by byte offset. The goroutine status constants (`GRUNNABLE`, `GRUNNING`, `GWAITING`, `GDEAD`, and the rest) were defined as `u32` constants alongside an `AtomicU32` `atomicstatus` field, exactly matching Go's design.

Goroutine lifecycle infrastructure was also laid down here: thread-local `CURRENT_G`, `current_g()` / `set_current_g()`, the `WaitReason` enum, and the compile-time offset assertions (`GOBUF_SP_OFFSET`, `GOBUF_PC_OFFSET`, etc.) that the assembly relies on to address struct fields without unsafe Rust pointer arithmetic.

**Step 3 — Assembly context switch primitives.**  
`gogo_asm` and `mcall_asm` were written as naked functions in `asm_amd64.rs` and `asm_arm64.rs` using Rust's `naked_asm!`. Each function was written twice — once for System V AMD64 (Linux/macOS, arguments in `rdi`/`rsi`/`rdx`/`rcx`) and once for Microsoft x64 (Windows, arguments in `rcx`/`rdx`/`r8`/`r9` with a 32-byte shadow-space requirement).

`gogo_asm` restores `pc`, `sp`, and `bp` from a `Gobuf` and jumps. `mcall_asm` saves the caller's `pc` (the return address at `[rsp]`), `sp` (= `rsp + 8`), and `bp`, switches RSP to g0's stack, and calls the scheduler function. Both functions end with `ud2` — a deliberate illegal instruction that fires if execution ever reaches code that "cannot be reached" (the scheduler function called by `mcall_asm` must never return).

The g0 stack for each M was also introduced here: a 512 KiB `mmap`-backed region set aside for the scheduler loop, separate from the goroutine stacks that users run on. 512 KiB provides enough headroom for the `schedule → findrunnable → stopm → Condvar::wait` call chain even in debug builds.

**Step 4 — Async preemption and the alternate signal stack.**  
`M` gained two fields: `pthread_id` (the OS thread's `libc::pthread_t`, captured via `pthread_self()` in `M::start()`) and `sigaltstack` (a 64 KiB `mmap`-backed alternate stack installed via `sigaltstack(2)`). Both are needed for async preemption.

The `async_preempt_trampoline` was written in naked assembly. When `sysmon` sends `SIGURG` to an M-thread, the signal handler redirects the goroutine's PC to the trampoline. The trampoline saves all live GPRs and XMM registers onto the goroutine's stack, calls `async_preempt2` (which `mcall`s into `preemptm`), and restores everything on the way back out. On x86-64, a `no-redzone` compilation flag is required to prevent the trampoline from clobbering the 128-byte red zone that the ABI permits leaf functions to use without adjusting RSP.

SIGURG was chosen by the Go team (and reused here) because it is not used by any standard C library and is almost never sent by the OS, making it a safe "private" signal.

**Step 5 — Netpoll and `WaitReason::IOWait`.**  
Three platform backends were implemented: `epoll` (Linux), `kqueue` (macOS), and IOCP (Windows). Each backend registers non-blocking file descriptors and returns a list of ready goroutines when polled. `findrunnable()` calls `netpoll_wait(0)` (non-blocking) before giving up and parking the M, and the sysmon thread calls `netpoll_wait(timeout)` to wake goroutines whose I/O has completed. `WaitReason::IOWait` was added to the `WaitReason` enum to explain why a goroutine is parked.

`net::TcpListener` and `net::TcpStream` were built on top of the netpoll layer, giving goroutines goroutine-aware blocking TCP I/O.

**Step 6 — `M::new` and g0 initialisation.**  
`M::new()` became the single place that allocates a g0 stack, initialises the `G0_SCHED` thread-local (the `Gobuf` that `mcall` uses to return to g0's stack), and records the M in `CURRENT_M`. `spawn_m()` wraps `std::thread::spawn` and calls `M::new()` inside the new thread before entering `schedule()`. The thread-local `G0_SCHED` uses a raw pointer (not `Arc`) because g0 outlives the thread and is cleaned up only when the scheduler tears down — which in the current design is never (the M-threads run forever).

**Step 7 — The P (logical processor) struct.**  
Go's scheduler uses three abstractions: G (goroutine), M (OS thread), and P (logical processor). P owns a 256-slot lock-free run queue — the goroutines it can run without touching a global lock. The ring buffer uses monotonically-increasing `runqhead`/`runqtail` counters and `AtomicUsize` slots, exactly matching Go's `p.runq` design.

**Step 8 — The scheduler core.**  
`schedule()`, `findrunnable()`, `execute()`, `goexit0()`, `gopark()`, and `goready()` were ported from `runtime/proc.go`. The critical invariant: every goroutine transition goes through `casgstatus`, and `schedule()` runs on g0's stack forever — it never returns.

Context switching is done with naked assembly (`gogo_asm` on x86-64 and AArch64) that loads `gobuf.pc`, `gobuf.sp`, and `gobuf.bp` directly into the CPU registers, then jumps. `mcall_asm` does the reverse — saves the running goroutine's registers into `gobuf`, switches to g0's stack, and calls the scheduler function.

**Step 9 — Bootstrap.**  
`schedinit()` initializes the singleton scheduler, allocates Ps, and spawns one M-thread per P. `new_goroutine()` allocates a goroutine stack via `mmap`, wires `goexit_trampoline` as the return address (pushed below `stack.hi` on x86-64, placed in `gobuf.lr` on AArch64), and sets the initial PC to `goroutine_entry`. `run_impl()` wraps the whole thing: spawn the user's closure as a goroutine, `park()` the calling thread, `unpark()` when it's done.

**Step 10 — sysmon.**  
A background OS thread runs `sysmon()`: checks for goroutines that have been running too long and calls `pthread_kill(m.pthread_id, SIGURG)` to preempt them. It also retakes Ps from threads that have been in a syscall too long.

**Step 11 — Cooperative preemption + `gosched()`.**  
`gosched()` yields the current goroutine back to the global run queue via `mcall(gp, gosched_m)`. CPU-bound goroutines can call it to be polite neighbors.

**Step 12 — sudog records.**  
`sudog` (short for "suspended goroutine") represents a goroutine waiting on a channel operation. Each sudog links the waiting goroutine to the channel's sender or receiver queue.

**Steps 13–20 — Channels, select!, sync, timers, macros, tests, docs.**  
The full channel implementation (`hchan`, `chansend`, `chanrecv`, `closechan`) was ported from `runtime/chan.go`. Unbuffered channels use a direct copy between sender and receiver stacks when they meet; buffered channels use a circular ring. `select!` was ported from Go's `selectgo` — it shuffles case indices, checks for ready channels, and parks if none are ready.

`WaitGroup` and `Cond` were added. A global timer heap runs a background thread that fires `goready()` on sleeping goroutines. The `go!` macro was introduced to match Go's `go func()` syntax.

---

## Phase 2: Hardening (v1.1 → v2.0)

With the core working, the next phase was making it robust.

**v1.1** added `sync::Cond` (a goroutine-aware condition variable), runtime `GOMAXPROCS` control, a goroutine panic handler (so a panicking goroutine doesn't abort the process), and a `context` package for cancellation and deadline propagation.

**v2.0** was the big one: **dynamic goroutine stack growth, async preemption, and netpoll.**

- **Dynamic stack growth** — Goroutines start with a small stack (initially 64 KiB, later reduced to Go's actual 2 KiB in release builds). A guard page below `stack.lo` is marked `PROT_NONE`. When the goroutine overflows, the OS delivers a signal. The signal handler calls `newstack()`: allocates a double-sized stack, copies the live region with a conservative pointer scan (any word in `[old_lo, old_hi)` that looks like a stack address is adjusted by `delta = new_hi - old_hi`), updates `G.stack` and `G.stackguard0`, then patches the saved stack pointer in the `ucontext_t` so the OS retries the faulting instruction on the new stack.

- **Async preemption via SIGURG** — On Unix, `sysmon` sends `SIGURG` to an M-thread when its goroutine has been running for more than 10 ms. The signal handler saves all live registers onto the goroutine's stack (the "async preempt trampoline"), calls `mcall(gp, preemptm)`, and resumes after being rescheduled. On Windows, cooperative preemption is used instead.

- **Netpoll** — An `epoll`/`kqueue`/IOCP integration layer lets goroutines block on I/O without blocking their OS thread. `net::TcpListener` and `net::TcpStream` were implemented on top.

---

## Phase 3: The Scope Feature (v0.4.0)

Goroutines are great, but they require `'static` closures. Sometimes you want to spawn short-lived helpers that borrow data from the current stack — exactly what `std::thread::scope` does for threads.

**`go_lib::scope`** was added as a goroutine equivalent:

```rust
go_lib::run(|| {
    let data: Vec<i64> = (1..=100).collect();
    let sum = go_lib::scope(|s| {
        let mid = data.len() / 2;
        let h1 = s.go(|| data[..mid].iter().sum::<i64>());
        let h2 = s.go(|| data[mid..].iter().sum::<i64>());
        h1.join().unwrap() + h2.join().unwrap()
    });
    assert_eq!(sum, 5050);
});
```

The implementation uses a `WaitGroup` inside the scope, `unsafe { transmute }` to erase the `'scope` lifetime to `'static` (sound because `scope` blocks until all goroutines finish), and `ScopedJoinHandle<'scope, R>` for result collection. Panics in scoped goroutines are routed through a one-shot buffered channel to `join()`, not via `resume_unwind` — crossing a scheduling boundary with an in-flight C++ unwind is undefined behavior.

The API was renamed from `Scope::spawn` to `Scope::go` to match Go's keyword.

---

## Debugging Sessions

Building a scheduler from scratch means debugging problems that don't exist in ordinary Rust code. Here are the most memorable ones.

### The macOS Sleep Hang

The timer thread wakes goroutines by calling `goready()`. On macOS, `SIGURG` (the async preemption signal) can arrive between the moment a goroutine inserts its timer entry and the moment it calls `gopark()`. In that window the goroutine is `GRUNNING`, not `GWAITING`.

When the timer fires, `goready()` would spin waiting for `GWAITING`. But the goroutine was preempted mid-sleep and was now `GRUNNABLE`. The original code had a `debug_assert!` that fired for `GRUNNABLE`, panicking the timer thread — after which every subsequent `sleep()` call parked forever.

**Fix:** `goready()` returns early if the goroutine is already `GRUNNABLE`. `fire_expired()` in the timer module re-inserts a 5 ms retry timer when it encounters a `GRUNNABLE` goroutine (still mid-transition to `GWAITING`). The `sleep_short_duration` and `concurrent_sleepers` tests were rewritten to use `WaitGroup` instead of `std::thread::sleep` — using the OS sleep inside a goroutine blocks the M-thread's P, starving timer-woken goroutines that need an idle P to run on.

### The WaitGroup Race (CI Ubuntu)

The multi-producer single-consumer channel test was failing on Ubuntu CI with the wrong sum. The `wg.add(1)` call was happening *inside* the spawned goroutine, so `wg.wait()` could see a counter of 0 before any producer had registered itself and return early.

**Fix:** `wg.add(1)` must happen *before* `spawn_goroutine`, not inside it.

### The SIGURG/Mutex Deadlock

After async preemption was enabled, Mutexes inside the scheduler started deadlocking. A goroutine holding a `Mutex` guard could be interrupted by SIGURG mid-critical-section. The trampoline called `mcall`, which itself needed to lock a scheduler Mutex — instant deadlock.

**Fix:** SIGURG is blocked (via `pthread_sigmask`) for the duration of any critical scheduler Mutex section. The `goexit_trampoline` path was also guarded — if SIGURG fires while the goroutine is returning, it must not redirect to the async preempt trampoline (which would clobber the return-address slot).

### The Red Zone (macOS x86-64)

On System V AMD64, the 128 bytes below RSP are the "red zone" — the ABI permits leaf functions to use them without adjusting RSP. The async preempt trampoline was writing saved registers below RSP and corrupting whatever the interrupted function had stored there.

**Fix:** `-C no-redzone=yes` was added to the project-wide `rustflags` in `.cargo/config.toml`. With this flag the compiler never generates code that relies on the region below RSP, so the trampoline's writes are always into dead space. The flag applies to the entire build graph, including `core` and `std`.

### The RFLAGS Corruption

After async preemption was re-enabled (post red-zone fix), certain tests would intermittently fail with iterators stopping early and returning wrong sums. Saving and restoring general-purpose registers but *not* RFLAGS meant the condition codes from the interrupted instruction were lost. Code that resumed after preemption and immediately tested a flag would branch the wrong way.

The specific trigger: `RangeInclusive::spec_next` calls `Step::forward_unchecked`, which in debug builds does:

```asm
addl  %ecx, %edi      ; sets OF (overflow flag)
movl  %edi, -N(%rbp)  ; does not touch flags
seto  %al             ; reads OF  ← WRONG after preemption
```

If SIGURG fired between the `addl` and `seto`, the scheduler's Rust code (which does arithmetic freely) clobbered OF. On resume, `seto` read the stale flag, the iterator's termination branch was taken early, and the sum was wrong. With small goroutine counts the probability was negligible; at 75 000 workers it was ~30%.

**Fix:** `async_preempt_trampoline` now saves RFLAGS with `pushfq` as its *first* instruction (before any flag-modifying code) and restores with `popfq` just before `ret`. The frame grew from 376 B to 392 B to accommodate RFLAGS and the required 16-byte alignment realignment.

### The Double-Decrement Bug (Signal Stack Growth)

Reducing the initial goroutine stack size to 8 KiB (matching Go's `stackMin`) surfaced a subtle bug in the SIGBUS/SIGSEGV growth path on macOS.

When the guard page is accessed, the OS saves the faulting CPU state in a `ucontext_t`. For instructions like `push rbp` or `stp x29,x30,[sp,#-16]!`, RSP/SP has *already been decremented* before the store faults. The growth handler was adjusting RSP by `delta` (the new stack's offset from the old), then the OS retried the instruction — which decremented RSP a *second* time, placing the frame 8 or 16 bytes too low. Every subsequent `mov rbp, rsp` and `sub rsp, N` was off, and the function's frame was in the wrong place.

**Fix:** `sp_predecrement_at_pc()` reads the faulting instruction at PC and returns the pre-decrement amount: 8 for any x86-64 `push` opcode, or the magnitude of the signed imm7/imm9 offset for AArch64 pre-indexed STP/STR. That correction is added to RSP *in addition* to `delta`, so the retry instruction lands at the right position.

A second bug: after the old stack was freed, any general-purpose register that still held an old-stack address would fault on the next dereference. `update_sp_in_context()` was extended to adjust *all* GPRs whose values fall within `[old_guard_lo, old_hi)` — not just RSP and RBP.

**Remaining limitation:** Signal-based growth cannot safely handle a single function frame larger than the initial stack size. When the fault occurs at the very bottom of the old stack, resuming mid-prologue in the new stack leaves only `old_size` bytes below the resume point. A frame larger than that skips past the new guard page into unmapped memory (SIGSEGV), which the handler cannot intercept. This is why debug builds keep larger initial stacks. The correct long-term fix is Go-style `morestack` checks inserted at every function entry by the compiler — future work.

---

## Phase 4: Making Async Preemption Actually Work

The theory was correct: SIGURG fires, trampoline saves registers, scheduler yields, trampoline restores, goroutine resumes. The reality was a cascade of independent bugs, each invisible at small goroutine counts and each requiring a different fix. The `many_goroutines` test became the stress harness: 75 000 workers each computing `(0_i64..=i).sum::<i64>()`, all in parallel. Any scheduler invariant that could be violated would be.

### The stopm Lost-Wakeup

The first confirmed hang: lldb attached to a stuck process showed one M parked in `Note::sleep` with the global run queue non-empty and the wrapper goroutine still alive. Classic lost-wakeup.

The sequence: goroutine A calls `findrunnable`, sees an empty queue, decides to sleep. Before it enqueues itself on `idle_m`, goroutine B calls `goready` and pushes work. B checks `idle_m` — empty — calls `startm` — no idle M found. A then enqueues on `idle_m` and parks. Nobody wakes it.

**Fix:** `stopm` re-checks both the global queue and the local P queue *under the scheduler inner lock*, after enqueueing on `idle_m`. If work has appeared, it undoes the enqueue and returns to `findrunnable`.

### The Mutex Self-Deadlock Series

A more insidious class of hang: an M parked in `__psynch_mutexwait` inside a goroutine that should be running. The `std::sync::Mutex` backing `WaitGroup`, `goready`, and the global run queue is a pthread mutex — non-recursive. If SIGURG fires while the M holds the mutex, the trampoline calls `async_preempt2` → `mcall` → `preemptm` → `push_batch`, which tries to re-acquire the same mutex on the same OS thread. Deadlock.

Three independent sites required the same fix pattern. Each one was found by capturing a hung process under lldb and reading the backtrace.

- **Global run queue** — `spawn_goroutine` and `goready` enqueue goroutines under a Mutex. SIGURG was blocked with `m_lock()` around those critical sections.
- **WaitGroup** — `WaitGroup::add` and `WaitGroup::wait` hold a `std::sync::Mutex`. Same fix.
- **Channel spinlock** — `RawMutex::LockGuard` for the channel's spinlock now bumps `m.locks` before acquiring, so SIGURG cannot preempt a goroutine holding a channel lock.

The shared mechanism: `m_lock()` increments `(*m).locks`; `sigurg_handler`'s Guard 0 checks `locks > 0` and skips preemption.

### Guard 0.5: Foreign Library Code

A separate crash class: SIGABRT from `_os_unfair_lock_recursive_abort` on macOS. The guard-page expansion of `m_lock` didn't cover code interrupted *inside* `libsystem_malloc.dylib`. When a goroutine was preempted mid-`free_tiny` (holding malloc's `os_unfair_lock`) and the next goroutine allocated memory, malloc detected the recursive lock acquisition and aborted.

**Fix:** `sigurg_handler` gained Guard 0.5: if the interrupted PC falls outside the binary's TEXT segment (checked via `|pc − goroutine_entry| < 256 MiB`), preemption is deferred. The goroutine continues through the library call, exits back into our code, and the next SIGURG attempt preempts it safely.

### The getaddrinfo Stack Overflow (release-only)

Surfaced by the go-http port: any client connecting to a *hostname* (`TcpStream::connect("example.com:80")`) crashed with `[go-lib SIGSEGV] non-stack fault` — but only in **release** builds, and only for names that needed the real resolver (numeric IPs and `localhost` were fine). Debug builds never crashed.

The culprit was stack size, not optimization. `TcpStream::connect` / `TcpListener::bind` resolve their argument with `std::net::ToSocketAddrs::to_socket_addrs`, which calls the platform `getaddrinfo` — a C function that loads the system resolver configuration and allocates tens of KiB of stack in a single deep call chain. Goroutine stacks are fixed and small (macOS debug 64 KiB, **release 32 KiB**), and without compiler-emitted `morestack` checks the guard page only catches incremental frame growth. `getaddrinfo`'s prologue jumps clean past the guard into unmapped memory, so the SIGSEGV handler classifies it as a non-stack fault and cannot grow-and-retry. 64 KiB happened to be enough for the resolver, 32 KiB was not — hence debug-passes / release-crashes.

**Fix:** resolution moved off the goroutine stack entirely. `resolve_first_addr` runs `to_socket_addrs` on a freshly spawned OS thread (full ≈8 MiB system stack) via `std::thread::scope`, mirroring Go's use of a dedicated thread for blocking `cgo`/resolver calls. The calling goroutine's M blocks in `join()` for the brief lookup. Regression test: `net_connect_hostname_does_not_overflow_stack` drives a `*.invalid` name (offline per RFC 6761) through `connect` and asserts a clean error instead of a crash.

### The Windows Accept That Pinned a Thread

Also surfaced by the go-http port: its server/client integration tests **hung** on Windows (10-minute CI timeout) while passing on macOS and Linux. The unit tests passed; the hang was in the goroutine-per-connection integration binaries, and it took the *whole* test binary down — even simple tests stalled — which pointed at a scheduler-wide deadlock rather than one bad test.

Bisecting in go-lib's own `tests/net.rs` ruled the suspects out one by one (each validated on the Windows CI runner, since the failure never reproduced on macOS/Linux): single-connection `try_clone` splits passed, keep-alive re-clone loops passed, `try_clone` splits under concurrent load passed. The reproduction needed one specific shape: servers that run an **accept loop forever** and are never shut down — exactly go-http's `listen_and_serve`, whose accept goroutine is leaked when a test returns. A minimal probe (leaked forever-`accept()` goroutines, no `select!`, no `try_clone`) hung; the same servers bounded to N accepts did not.

The culprit was the Windows `accept` itself. Unlike `recv`/`send`/`connect` — which issue overlapped `WSARecv`/`WSASend`/`ConnectEx` and **park** the goroutine via IOCP — `TcpListener::accept` wrapped the *synchronous* Winsock `accept()` in `with_syscall`, blocking the OS thread (M) for as long as the goroutine sat in accept. On Unix `accept` parks through the netpoll (epoll/kqueue) and costs no thread, so the leak was harmless there. On Windows each leaked accept loop pinned an M forever; with several concurrent listeners the M pool drained and the scheduler had no threads left to run anything — global deadlock.

**Fix:** `accept` now issues an overlapped `AcceptEx` (loaded once via `WSAIoctl`/`SIO_GET_EXTENSION_FUNCTION_POINTER`) into a pre-created socket and `gopark`s until the IOCP completion fires, mirroring the overlapped `recv`/`send` path — so it consumes no M while waiting. The listener is associated with the IOCP in `bind`, and the accepted socket gets `SO_UPDATE_ACCEPT_CONTEXT`. Regression tests: `net_leaked_select_dispatch_servers` (the full go-http server model) and `net_leaked_forever_accept` (the minimal form).

### The Callee-Saved Register False Positive

With async preemption working, the stress test at 75 000 workers surfaced a new crash pattern: `drop_in_place<Box<dyn Any>>` crashing because the vtable pointer was actually the bytes of an `i64` partial sum. The `Result<i64, Box<dyn Any>>` in a channel buffer had its discriminant flipped from 0 (Ok) to non-zero (Err).

Captured under lldb with the old stack quarantined via `mprotect(PROT_NONE)` instead of `munmap`: confirmed that stale stack-address values were leaking through register adjustment. `update_sp_in_context` adjusted registers whose values fell in `[old_lo, old_hi)` — the full old usable stack range. At scale, callee-saved registers (RBX, R12–R15) commonly held heap pointers whose values *coincidentally* fell in some other goroutine's old stack range, and adjusting them corrupted the heap data they pointed to.

**Fix:** Callee-saved registers (RBX, R12–R15 and AArch64 x19–x28) were moved to the narrow `[old_guard_lo, old_lo)` range — the guard page only. RSP and RBP remain on the full range since they are definitively frame-chain pointers.

### Guard 3: Preemption Inside Scheduler ASM

Async preemption also had a narrow but real window where it would fire while RIP was inside one of the naked-asm trampolines (`gogo_asm`, `mcall_asm`, or `async_preempt_trampoline` itself). In those windows, a second `mcall_asm` run overwrites `g.sched.regs[]` with the wrong values — the scheduler path's callee-saves instead of the user code's. Resuming from the corrupted `g.sched` produced wild dereferences.

**Fix:** `sigurg_handler` gained Guard 3: if the interrupted PC falls within 4 KiB of any scheduler-asm function's start address, preemption is deferred.

### The goexit SIGURG Race

A 2% SIGABRT rate persisted after all other fixes. The crash message was always "thread caused non-unwinding panic. aborting." and the backtrace showed:

```
frame #11: core::hint::unreachable_unchecked::precondition_check
frame #12: core::hint::unreachable_unchecked
frame #13: go_lib::runtime::sched::goexit0_handler at sched.rs:1648
frame #14: go_lib::runtime::sched::goexit_trampoline + 5
```

`goexit0_handler` calls `mcall(gp, goexit0)`. `mcall_asm`'s first action is saving `gp.sched.pc` (the resume address). If SIGURG fires *after* `goexit0_handler` is entered but *before* `goexit0` transitions the goroutine to GDEAD, the async preempt's own `mcall` run **overwrites `gp.sched.pc`** with the trampoline's recovery address. When the goroutine is later re-scheduled, `gogo` jumps to the wrong PC — the instruction after `mcall(gp, goexit0)` in `goexit0_handler` — which is `unsafe { std::hint::unreachable_unchecked() }`. The debug precondition check fires, and the process aborts.

**Fix:** `goexit0_handler` (and the AArch64 `goexit_trampoline`) acquires `m_lock()` at entry. Guard 0 in `sigurg_handler` sees `m.locks > 0` and skips preemption for the entire goexit path. The guard never needs to be released — once `goexit0` is called, the M re-enters `schedule()` and the dead goroutine's lock counter is irrelevant.

### The Campaign in Numbers

Starting from the first `many_goroutines` hang report, eleven pull requests (PRs #16–#26) were needed to achieve a clean 120/120 run at WORKERS=75 000 with async preemption fully enabled:

| PR | Fix |
|---|---|
| #16 | stopm lost-wakeup; SIGURG-during-Mutex self-deadlock |
| #17 | WaitGroup and channel spinlock SIGURG protection; double `catch_unwind` |
| #18 | Nested `Box<dyn Any>` panic payload unwrapping |
| #19 | Goroutine panic forwarding from `run_impl`; stack size band-aid |
| #20 | Temporarily disabled async preemption (workaround, later removed) |
| #21 | Fixed `i32` overflow in test at WORKERS ≥ 46 341 |
| #22 | Guard 3: bail SIGURG when PC is inside scheduler ASM |
| #23 | Narrow callee-saved register adjustment to guard-page range only |
| #24 | **RFLAGS save/restore in trampoline** (`pushfq`/`popfq`); re-enable async preemption |
| #25 | **Red zone** (`-C no-redzone=yes`); **goexit race** (`m_lock` in goexit) |
| #26 | Docs: updated to match all fixes; restored original debug stack sizes |

The total wall-clock time from first hang report to clean green was two sessions spanning several days of investigation, lldb captures, assembly reading, and stress testing.

---

## The G State Machine

Go's goroutine state machine has a dozen states: `Gidle`, `Grunnable`, `Grunning`, `Gsyscall`, `Gwaiting`, `Gcopystack`, `Gpreempted`, `Gdead`, and the `Gscan` bitmask overlay. Each transition is validated by `casgstatus()`, which spins while the `Gscan` bit is set (the GC is scanning the stack) before doing a CAS on `atomicstatus`.

All state constants were wired up:

- `GSYSCALL` — goroutine transitions to this state in `entersyscall()` and back in `exitsyscall()`. The P is released for other goroutines while the syscall blocks.
- `GCOPYSTACK` — brackets the stack copy in `copystack()` so a future GC scanner knows not to walk a half-copied stack.
- `GPREEMPTED` — async preemption lands here as a stable scan point between `Grunning` and `Grunnable`.
- `GSCAN` — the GC bitmask that freezes a goroutine's status while its stack is being scanned.

The blanket `#![allow(dead_code)]` on the runtime module was removed and replaced with targeted annotations on the two GC-gated processor states (`PGCSTOP`, `PDEAD`) that await a garbage collector.

`systemstack()` was implemented: it switches execution to the M's g0 stack (via a naked assembly RSP swap), runs a closure there, and returns. This is how the Go runtime performs operations that must not grow the goroutine's stack.

---

## The API Surface

Over 80 commits and five minor versions, the public API grew to cover:

| Feature | Status |
|---|---|
| `go!` macro — spawn a goroutine | ✅ |
| `go_lib::run<F, R>` — scheduler entry point with return value | ✅ |
| `#[go_lib::run]` — attribute macro for `main` | ✅ |
| Unbuffered and buffered channels | ✅ |
| `tx.close()` — Go-compatible close semantics | ✅ |
| `select!` with recv, send, default | ✅ |
| `WaitGroup` | ✅ |
| `Cond` — goroutine-aware condition variable | ✅ |
| `context` — cancellation and deadline propagation | ✅ |
| `scope` / `ScopedJoinHandle` — safe short-lived borrows | ✅ |
| `sleep(Duration)` | ✅ |
| `gosched()` — cooperative yield | ✅ |
| `with_syscall` — P hand-off during blocking I/O | ✅ |
| `GOMAXPROCS` env var + `set_gomaxprocs()` | ✅ |
| Goroutine panic handler | ✅ |
| Dynamic stack growth (32 KiB → 1 GiB) | ✅ |
| Async preemption via SIGURG | ✅ |
| Netpoll — epoll/kqueue/IOCP integration | ✅ |
| `net::TcpListener` / `net::TcpStream` | ✅ |
| Loom concurrency model checker integration | ✅ |
| Full G state machine (casgstatus, GSYSCALL, GCOPYSTACK, etc.) | ✅ |
| `systemstack` — run closure on M's g0 stack | ✅ |

---

## Lessons Learned

**Ports are not rewrites.** The discipline of staying faithful to the original Go source — same function names, same variable names, same algorithm structure — made debugging tractable. When something went wrong, the Go source code was a reliable oracle.

**Naked assembly is a commitment.** Every naked function is invisible to the Rust compiler's unwind machinery. A Rust panic that escapes through a naked frame causes undefined behavior. Every `unwrap()` and `expect()` in a code path reachable from a naked frame is a latent footgun.

**Signal handlers are brutal.** They have no unwind tables. They share a 64 KiB alternate stack. They cannot call non-async-signal-safe functions without risking deadlock or corruption. And on macOS, `PROT_NONE` raises `SIGBUS`, not `SIGSEGV` — a detail that took real investigation to discover.

**Race conditions live at scheduler quanta boundaries.** The hardest bugs were the ones where behavior depended on whether a goroutine was interrupted between two specific instructions: between `wg.add(1)` and `spawn_goroutine`, between `gopark` and the channel lock release, between `sleep()` inserting the timer and calling `gopark()`. Many integration tests were rewritten to use `WaitGroup` instead of polling loops to eliminate these races.

**CI is the ground truth.** macOS AArch64 (Apple Silicon) in CI caught bugs that never appeared on the development machine (Intel x86-64). The SIGBUS vs SIGSEGV distinction, the 16 KiB page size, the different calling conventions — all surfaced only on the CI runners.

---

## Where It Stands

go-lib v0.4.3 is a working, tested implementation of Go-style concurrency in Rust with no `async` machinery. The runtime passes CI across Ubuntu x86-64, macOS AArch64, and Windows x86-64 in both standard and loom model-check configurations.

Async preemption via SIGURG is fully operational. The full chain — sysmon fires, signal handler redirects RIP, trampoline saves RFLAGS + all GPRs + XMM registers, scheduler yields, trampoline restores, goroutine resumes — is correct, tested at 75 000 concurrent goroutines, and free of known crash modes.

The stack growth path works correctly for the common case (frames smaller than the initial stack size). The correct long-term fix — morestack-style compiler-generated stack checks at every function entry — remains future work; the current guard-page approach cannot intercept a single frame larger than the initial stack size.

Goroutines work. Channels work. Select works. Scope works. The scheduler steals work. Goroutines preempt. Stacks grow. RFLAGS round-trips correctly across a yield.

It runs.
