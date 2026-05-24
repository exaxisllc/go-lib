# Porting Plan: Go Runtime → Rust

Porting Go's M:N scheduler and concurrency primitives to Rust with no async
runtime. The scheduler, channels, and parking primitives are ported from
[`src/runtime/`](https://github.com/golang/go/tree/master/src/runtime) in the
Go repo. `std::sync::{Mutex, RwLock}` are used in place of porting Go's
semaphore-backed versions — their uncontended path is a single atomic CAS,
and the contended path is made scheduler-safe via `entersyscall`/`exitsyscall`
(step 15.5). No `async`, no Tokio, no external runtime.

## Go → Rust primitive mapping

| Go construct | Rust construct |
|---|---|
| `go func() { … }` | `go!(…)` macro → runtime spawn |
| `chan T` (buffered) | `chan::Chan<T>` ported from `chan.go` |
| `chan T` (unbuffered) | same, `dataqsiz == 0` |
| `close(ch)` | drop all senders; receiver sees disconnected |
| `select { case … }` | `select!(…)` macro → `selectgo` port |
| `sync.WaitGroup` | `sync::WaitGroup` on `Mutex<u64> + Condvar` |
| `sync.Mutex` | re-export `std::sync::Mutex` + syscall shim |
| `sync.RWMutex` | re-export `std::sync::RwLock` + syscall shim |
| Go runtime scheduler | `runtime::{sched, g, m, p}` ported directly |
| `runtime.Gosched()` | `gosched()` — cooperative yield |
| `context.Context` | deferred (v2) |

## Skipped (deferred)

- **Stack growth** (`copystack`, `morestack`) — v1 uses fixed 64 KiB `mmap`'d stacks with a guard page.
- **Async preemption** — signal-based preemption (`preemptone`, `signalM`) is deferred; v1 is cooperative only (`gosched()`).
- **GC / write barriers / finalizers** — irrelevant; Rust owns memory.
- **Netpoll** — deferred.
- **`defer` / `recover` / `panic`** — deferred.
- **`context.Context`** — deferred (v2).
- **`sync.Cond`** — deferred.
- **`GOMAXPROCS` at runtime** — fixed at startup in v1.

---

## Phase A — Scaffolding ✅

### Step 1 — Crate layout `[DONE]`

Module tree mirroring Go's `src/runtime/` layout. `libc` added as the only
external dependency (used for `mmap`/`mprotect`). `std::sync::{Mutex, RwLock}`
re-exported from `sync::`. `go.rs` stub replaced by the full module structure.

**Source mapping:**

| Rust module | Go source |
|---|---|
| `runtime::g` | `runtime/runtime2.go` |
| `runtime::m` | `runtime/runtime2.go` |
| `runtime::p` | `runtime/runtime2.go`, `runtime/proc.go` |
| `runtime::sched` | `runtime/proc.go` |
| `runtime::stack` | `runtime/stack.go` |
| `runtime::park` | `runtime/proc.go` (gopark / goready) |
| `runtime::sudog` | `runtime/runtime2.go`, `runtime/proc.go` |
| `runtime::syscall` | `runtime/proc.go` (entersyscall / exitsyscall) |
| `runtime::sysmon` | `runtime/proc.go` (sysmon) |
| `runtime::time` | `runtime/time.go` |
| `runtime::asm_arm64` | `runtime/asm_arm64.s` |
| `runtime::asm_amd64` | `runtime/asm_amd64.s` |
| `chan` | `runtime/chan.go` |
| `select` | `runtime/select.go` |
| `sync::waitgroup` | `sync/waitgroup.go` |

---

## Phase B — Low-level primitives

### Step 2 — `Gobuf` / register save area

Port `Gobuf` from `runtime/runtime2.go`. Layout must be `#[repr(C)]` and match
assembly offsets used in step 3 exactly.

```rust
#[repr(C)]
pub(crate) struct Gobuf {
    pub sp:   usize,
    pub pc:   usize,
    pub g:    *mut G,
    pub ctxt: *mut u8,
    pub ret:  usize,
    pub lr:   usize,   // AArch64 link register / x86_64 unused
    pub bp:   usize,   // frame pointer
}
```

**File:** `src/runtime/g.rs`

---

### Step 3 — `gogo` / `mcall` / `systemstack` (assembly)

Port context-switch primitives from `runtime/asm_arm64.s` (and `asm_amd64.s`)
using `core::arch::global_asm!` or `#[unsafe(naked)]` functions.

- `gogo(buf: *mut Gobuf)` — load sp/pc/callee-saved registers from `Gobuf`, jump.
- `mcall(fn_ptr)` — save current G's `Gobuf`, switch to `g0`'s stack, call `fn_ptr(g)`.
- `systemstack(fn_ptr)` — run a closure on the M's system stack (`g0`).

On AArch64 the link register (`x30`) is also saved. Both platforms must
save/restore all callee-saved registers (ABI-defined).

**Files:** `src/runtime/asm_arm64.rs`, `src/runtime/asm_amd64.rs`

---

### Step 4 — Stack allocator

Port from `runtime/stack.go`. v1 allocates each goroutine a fixed 64 KiB stack:

1. `mmap(NULL, 64K + PAGE_SIZE, PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE)`
2. `mprotect(base, PAGE_SIZE, PROT_NONE)` — guard page.
3. Return `Stack { lo: base + PAGE_SIZE, hi: base + PAGE_SIZE + 64K }`.

`stackfree` calls `munmap`. Stack size-classes and `copystack` growth are
deferred — document as known limitation.

**File:** `src/runtime/stack.rs`

---

## Phase B — G/M/P scheduler

### Step 5 — Port `G`

Port from `runtime/runtime2.go`. Required fields only:

```rust
pub(crate) struct G {
    pub sched:       Gobuf,
    pub stack:       Stack,
    pub atomicstatus: AtomicU32,  // Gidle/Grunnable/Grunning/Gwaiting/Gdead
    pub schedlink:   *mut G,      // intrusive list link
    pub waitreason:  WaitReason,
    pub param:       *mut u8,     // channel: passed value pointer
    pub m:           *mut M,
}
```

Skip GC/defer/panic fields.

**File:** `src/runtime/g.rs`

---

### Step 6 — Port `M`

Each `M` is an OS thread (`std::thread::spawn`). Holds `g0` (scheduler
goroutine running on the system stack), `curg` (currently running G), a
pinned `P`, and a park primitive.

Go uses futex-based `note`; portable equivalent is `Mutex<bool> + Condvar`.
Port `notesleep`/`notewakeup` semantics on top of that pair
(ref: `runtime/lock_sema.go`).

**File:** `src/runtime/m.rs`

---

### Step 7 — Port `P` and run queue

Port from `runtime/runtime2.go` and `runtime/proc.go`.

- 256-slot lock-free local run queue: `runqhead`/`runqtail` as `AtomicU32`
  over a `[*mut G; 256]` ring buffer.
- Port `runqput`, `runqget`, `runqsteal` **verbatim** — the memory ordering
  (`Acquire`/`Release`) is load-bearing; do not paraphrase.
- Global run queue: linked list guarded by `sched.lock` (a `Mutex`).

**File:** `src/runtime/p.rs`

---

### Step 8 — Port scheduler core

Port five functions from `runtime/proc.go` in this order:

1. `schedule()` — main loop on `g0`; calls `findrunnable` then `execute`.
2. `findrunnable()` — local rq → global rq (steal 1/GOMAXPROCS) → work-steal
   from a random P (4 tries) → park M.
3. `execute(g)` — set status to `Grunning`, call `gogo(&g.sched)`.
4. `goexit0(g)` — runs on `g0` after a G returns; recycles G, calls `schedule`.
5. `gopark(reason)` / `goready(g)` — set status to `Gwaiting`, `mcall(schedule)`;
   and re-enqueue a G on a P's run queue respectively.

**Files:** `src/runtime/sched.rs`, `src/runtime/park.rs`

---

### Step 9 — Bootstrap (`schedinit` / `main`)

Port `runtime·rt0_go` minimally:

1. Create `GOMAXPROCS` Ps (default: `num_cpus`).
2. Spawn one M per P via `std::thread::spawn`; each M calls `schedule()` on
   its `g0`.
3. Expose `go_lib::run(f: impl FnOnce() + Send + 'static)` — allocates the
   first G running `f` and blocks the calling thread (via `std::thread::park`)
   until it returns.

**File:** `src/runtime/sched.rs`

---

### Step 10 — Port `sysmon`

Detached `std::thread` running a trimmed `sysmon` loop (ref: `runtime/proc.go`):

- Fire expired timers (step 17).
- Retake Ps stuck in syscalls — calls `handoffp` (step 15.5).
- No async preemption in v1.

Sleeps for 20 µs initially, backs off to 10 ms max, same as Go.

**File:** `src/runtime/sysmon.rs`

---

### Step 11 — Cooperative preemption only

Expose `gosched()` — puts current G on the global run queue and calls
`schedule()`. Document that CPU-bound loops must call `gosched()` periodically;
without signal-based preemption there is no other mechanism.

**File:** `src/runtime/sched.rs`

---

## Phase C — Channels and synchronization

### Step 12 — Port `sudog`

Port waiter record from `runtime/runtime2.go` and the per-P sudog cache
(`acquireSudog`/`releaseSudog`) from `runtime/proc.go`. Every channel
send/receive that blocks allocates a sudog; pooling matters.

```rust
pub(crate) struct Sudog {
    pub g:        *mut G,
    pub next:     *mut Sudog,
    pub prev:     *mut Sudog,
    pub elem:     *mut u8,   // pointer to data being sent/received
    pub isselect: bool,
    pub c:        *mut HChan<()>,
}
```

**File:** `src/runtime/sudog.rs`

---

### Step 13 — Port `hchan` / channels

Port `hchan`, `chansend`, `chanrecv`, `closechan` from `runtime/chan.go`.

Key points:
- `Chan<T>` wraps `HChan` behind an `Arc` for shared ownership.
- Unbuffered = `dataqsiz == 0`; same code path as buffered.
- **Direct handoff path**: sender copies value into a waiting receiver's
  `sudog.elem` then calls `goready` — this is the core performance win;
  do not simplify.
- `closechan` wakes all parked senders (with panic) and receivers (with zero value).
- Use `std::sync::Mutex` for `hchan.lock` — wrap acquire with `entersyscall`
  shim (step 15.5) to keep contended cases scheduler-safe.

**File:** `src/chan.rs`

---

### Step 14 — Port `selectgo`

Port from `runtime/select.go`.

1. Build `scase` array from macro arms.
2. Shuffle with `fastrand` for uniform fairness (do not use `tokio::select!`'s
   biased ordering).
3. Pass 1: lock all channels in address order (deadlock prevention), try each
   non-blocking.
4. Pass 2: enqueue self on all channels' `sendq`/`recvq`, `gopark`.
5. Pass 3: on wake, dequeue from non-winning channels, unlock.

Expose as `select!` macro in `src/go_macro.rs`.

**File:** `src/select.rs`, `src/go_macro.rs`

---

### Step 15.5 — `entersyscall` / `exitsyscall` *(new — replaces mutex port)*

Port from `runtime/proc.go`. Used as a wrapper around any `std` call that may
park the OS thread.

- `entersyscall`: save G's `Gobuf`, set M status to `Msyscall`, call `handoffp`
  if the global run queue is non-empty or there are idle Ps.
- `exitsyscall`: try to reacquire a P; if none available, put G on global queue
  and park M.
- `handoffp`: give the M's P to another M (or wake a parked M).

This replaces the need to port Go's `sync.Mutex`, `sync.RWMutex`, and
`runtime.sema` (~600 lines). `std::sync::Mutex` + this shim achieves equivalent
scheduler safety.

**File:** `src/runtime/syscall.rs`

---

### Step 16 — `WaitGroup`

Implement on `Mutex<(u64, u64)> + Condvar` (counter, waiter-count) rather than
porting `runtime.sema`. Wrap `wait()` with `entersyscall`/`exitsyscall` so a
waiting M releases its P.

- `add(delta)`: lock, update counter, panic if negative; if counter == 0,
  `notify_all`.
- `done()`: `add(-1)`.
- `wait()`: lock, increment waiter-count; `entersyscall`; loop on `condvar.wait`
  until counter == 0; `exitsyscall`.

**File:** `src/sync/waitgroup.rs`

---

## Phase D — Timers, public API, tests

### Step 17 — Timer heap

Single global 4-ary min-heap behind a `Mutex` (per-P heaps are an optimization
for later). `sysmon` fires expired timers by calling `goready` on their
parked Gs.

Enables `time::sleep(Duration)` and timeout arms in `select!`.

**File:** `src/runtime/time.rs`

---

### Step 18 — Public API and macros

Re-export the full public surface from `src/lib.rs`:

```rust
// Macros
go!(|| { ... })          // spawn a goroutine
select! { ... }          // Go-style arms

// Channels
chan::make<T>()          // unbuffered
chan::make_buffered<T>(n) // buffered

// Sync
sync::WaitGroup
sync::Mutex              // re-export of std::sync::Mutex
sync::RwLock             // re-export of std::sync::RwLock

// Runtime
run(f)                   // entry point
gosched()                // cooperative yield
```

**Files:** `src/lib.rs`, `src/go_macro.rs`

---

### Step 19 — Tests and examples

Required coverage before calling the scheduler correct:

| Test | What it validates |
|---|---|
| Ping-pong over unbuffered channel | Direct handoff path, gopark/goready |
| Producer/consumer + WaitGroup | Buffered channel, WaitGroup |
| `select!` with timeout | Timer heap, select arm fairness |
| Work-stealing stress | N goroutines > M Ps all make progress |
| Contended mutex | `entersyscall` shim correctness |

Put under `examples/` so they double as documentation.

---

### Step 20 — Known deferred items (document, don't implement)

Record clearly in crate docs:

- Stack growth (`morestack` / `copystack`) — all goroutines have a fixed 64 KiB stack.
- Async preemption — long CPU-bound loops must call `gosched()`.
- `defer` / `recover` / `panic` across goroutine boundaries.
- `context.Context` / cancellation.
- Netpoll / I/O integration.
- `GOMAXPROCS` adjustment at runtime.
- Race detector.
- `sync.Cond`.

---

## Critical path

The scheduler must be able to run a trivial G that calls `gosched()` and exits
before channels, select, or WaitGroup can be meaningfully tested. Suggested
first milestone (steps 2–9):

```
Gobuf → gogo/mcall → stack alloc → G/M/P structs → schedule() → bootstrap
```

Land that as the first PR. Channels and sync follow as independent PRs.
