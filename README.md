# go-lib

[![CI](https://github.com/exaxisllc/go-lib/actions/workflows/ci.yml/badge.svg)](https://github.com/exaxisllc/go-lib/actions/workflows/ci.yml)

Go-style concurrency for Rust — goroutines, channels, `select!`, `WaitGroup`, `Cond`, and a `context` package — built on a rust-native direct port of the Go M:N scheduler.

```rust
go_lib::run(|| {
    let (tx, rx) = go_lib::chan::chan::<String>(0);

    for i in 0..5 {
        let tx = tx.clone();
        go!(move || tx.send(format!("hello from goroutine {i}")));
    }
    drop(tx);

    for _ in 0..5 {
        if let Some(msg) = rx.recv() {
            println!("{msg}");
        }
    }
});
```

No `async`, no Tokio, no executor. Every goroutine starts with a **2 KiB stack** in release builds (matching Go's `stackMin = 2048`; 16 KiB on Linux debug, 64 KiB on macOS and Windows debug) that grows automatically on demand (up to 1 GiB). The `G` descriptor is **128 B** — total per-goroutine memory is **~6 KiB** on Linux/Windows x86-64, ~18 KiB on macOS AArch64 (16 KiB OS guard page). The runtime is a work-stealing M:N scheduler ported verbatim from [`src/runtime/`](https://github.com/golang/go/tree/master/src/runtime) in the Go GitHub repository.

---

## Contents

- [Features](#features)
- [Quick start](#quick-start)
- [API reference](#api-reference)
  - [Entry point](#entry-point)
  - [Goroutines](#goroutines)
  - [scope — safe short-lived borrows](#scope--safe-short-lived-borrows)
  - [Channels](#channels)
  - [select!](#select)
  - [WaitGroup](#waitgroup)
  - [Cond](#cond)
  - [context](#context)
  - [sleep and gosched](#sleep-and-gosched)
  - [with_syscall](#with_syscall)
  - [net — async TCP](#net--async-tcp)
  - [GOMAXPROCS](#gomaxprocs)
  - [Panic handler](#panic-handler)
- [Examples](#examples)
- [Testing & CI](#testing--ci)
- [Architecture](#architecture)
- [Go → Rust mapping](#go--rust-mapping)
- [Known limitations](#known-limitations)

---

## Features

| Capability | Status |
|---|---|
| M:N goroutine scheduler (G/M/P) | ✅ |
| Unbuffered channels | ✅ |
| Buffered channels | ✅ |
| Channel close + drain | ✅ |
| `select!` with recv, send, default | ✅ |
| `WaitGroup` | ✅ |
| `Cond` — goroutine-aware condition variable | ✅ |
| `context` — cancellation and deadline propagation | ✅ |
| `sleep(Duration)` | ✅ |
| `gosched()` cooperative yield | ✅ |
| `with_syscall` — P hand-off during blocking calls | ✅ |
| Work-stealing across Ps | ✅ |
| `GOMAXPROCS` env var + runtime adjustment | ✅ |
| Goroutine panic handler (process does not abort) | ✅ |
| Dynamic goroutine stack growth (2 KiB → 1 GiB) | ✅ v0.2.0 |
| Async preemption via `SIGURG` | ✅ v0.2.0 |
| Netpoll — `epoll`/`kqueue`/IOCP I/O integration | ✅ v0.3.0 |
| `net::TcpListener` / `net::TcpStream` | ✅ v0.2.0 |
| Loom concurrency model checker integration | ✅ v0.2.0 |
| CI — standard + loom jobs on every push/PR | ✅ v0.2.0 |
| G state machine — `casgstatus`, `GSYSCALL`, `GCOPYSTACK`, `GPREEMPTED`, `GSCAN` | ✅ v0.3.1 |
| `systemstack` — run closure on M's g0 stack (naked-asm RSP/SP switch) | ✅ v0.3.1 |
| `scope` — scoped goroutines with safe short-lived borrows | ✅ v0.4.0 |

---

## Quick start

Add to `Cargo.toml`:

```toml
[dependencies]
go-lib = { path = "…" }   # local path until crates.io publication
```

Every program entry point wraps its body in `go_lib::run`.
Use the `#[go_lib::run]` attribute macro to do it automatically:

```rust
use go_lib::{chan::chan, go};

#[go_lib::run]
fn main() {
    let (tx, rx) = chan::<i32>(0); // unbuffered

    go!(move || tx.send(42));

    if let Some(n) = rx.recv() {
        println!("received {n}");
    }
}
```

Or call `go_lib::run` explicitly when you need more control:

```rust
fn main() {
    go_lib::run(|| {
        // body
    });
}
```

Run the bundled examples:

```sh
cargo run --example hello
cargo run --example pipeline
cargo run --example select_fanin
cargo run --example cond
cargo run --example scope           # scoped goroutines, safe borrows
cargo run --example scope_channel   # scope + channel producer/consumer
cargo run --example main_exitcode   # main() -> ExitCode
cargo run --example main_result     # main() -> Result<(), E>
cargo run --example attr_run        # all three #[go_lib::run] patterns
```

---

## API reference

### Entry point

There are two equivalent ways to start the scheduler.

#### Attribute macro (recommended)

```rust
#[go_lib::run]
fn main() { … }

#[go_lib::run]
fn main() -> ExitCode { … }

#[go_lib::run]
fn main() -> Result<(), MyError> { … }
```

The macro expands the function body into `go_lib::run(move || [-> ReturnType] { … })`.
It works on any function, not only `main`, and captures parameters via `move`.

An `async` function is rejected at compile time with a clear error message.

#### Direct call

```rust
pub fn run<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
```

Initialises the scheduler (one M-thread per logical CPU, or the value of the `GOMAXPROCS` environment variable), runs `f` as the first goroutine, and blocks until `f` returns — propagating `f`'s return value directly to the caller. The scheduler threads remain alive in the background; subsequent calls to `run` reuse them.

**Parameters** are passed via `move` closures:

```rust
let threshold = 42_i32;
go_lib::run(move || { /* threshold is in scope */ });
```

**Return values** propagate back to the caller:

```rust
let n: i32      = go_lib::run(|| compute_answer());
let ok: bool    = go_lib::run(|| all_tasks_pass());
```

If the first goroutine panics before returning, `run` re-panics on the calling thread with a clear message.

---

### Goroutines

```rust
go!(closure)
```

Spawns `closure` as a new goroutine. Must be called from inside `run`. Equivalent to Go's `go f()`.

```rust
go_lib::run(|| {
    go!(|| println!("I run concurrently"));
    go!(move || {
        // captured variables are moved in
    });
});
```

---

### scope — safe short-lived borrows

```rust
go_lib::scope(|s| {
    let h1 = s.go(|| /* closure that may borrow from outer scope */);
    let h2 = s.go(|| /* … */);
    h1.join().unwrap() + h2.join().unwrap()
})
```

Scoped goroutines work exactly like `std::thread::scope`: goroutines spawned inside the closure can borrow data from the enclosing stack frame because `scope` guarantees all spawned goroutines complete before it returns.  No `Arc`, no channels, and no `.clone()` are needed for read-only shared data.

```rust
go_lib::run(|| {
    // `data` lives on the goroutine's stack.  Both goroutines borrow slices
    // of it directly — no Arc required.  `scope` enforces the lifetime at
    // compile time; go_lib::run requires a 'static closure, so data that needs
    // to be shared lives inside run (or is moved in).
    let data = vec![1_i64, 2, 3, 4, 5, 6, 7, 8];

    let sum = go_lib::scope(|s| {
        let mid = data.len() / 2;
        let h1 = s.go(|| data[..mid].iter().sum::<i64>());
        let h2 = s.go(|| data[mid..].iter().sum::<i64>());
        h1.join().unwrap() + h2.join().unwrap()
    });

    assert_eq!(sum, 36);
});
```

`s.go(f)` returns a `ScopedJoinHandle<'scope, R>`:

| Method | Description |
|---|---|
| `h.join()` | Block until the goroutine finishes; returns `std::thread::Result<R>` — `Ok(R)` on success, `Err(payload)` if the goroutine panicked |

The `Err` payload from a panicking goroutine is the same `Box<dyn Any + Send>` you would receive from `std::panic::catch_unwind`.  Dropping a `ScopedJoinHandle` without calling `join` is safe — the goroutine still runs to completion; its result is simply discarded.

Channels work normally inside `s.go()` closures.  The scope lifetime guarantee means both goroutines finish before `scope` returns — no `Arc` or `WaitGroup` needed.  Call `tx.close()` after the last send so the consumer's `while let Some` loop terminates:

```rust
go_lib::run(|| {
    let (tx, rx) = go_lib::chan::chan::<i32>(0); // unbuffered

    let sum = go_lib::scope(|s| {
        // Producer: send 0..10 then close to signal end-of-stream.
        s.go(move || {
            for i in 0..10 {
                tx.send(i);
            }
            tx.close();
        });

        // Consumer: drain until the channel is closed and empty.
        s.go(move || {
            let mut total = 0_i32;
            while let Some(v) = rx.recv() {
                total += v;
            }
            total
        })
        .join()
        .expect("consumer panicked")
    });
    // scope() returns here — both goroutines have finished.

    assert_eq!(sum, 45); // 0 + 1 + … + 9
});
```

**When to prefer `scope` over `go!` + channel:**

| Pattern | Use when |
|---|---|
| `scope` | Goroutines are short-lived helpers that read (or write exclusively to) local data |
| `scope` + channel | Paired producer/consumer with a bounded lifetime and a single collected result |
| `go!` + channel | Long-running goroutines, or when results need to be streamed/merged as they arrive |
| `WaitGroup` | Fire-and-forget goroutines that do side effects but produce no return value |

---

### Channels

```rust
use go_lib::chan::chan;

let (tx, rx) = chan::<T>(capacity);  // capacity=0 → unbuffered
```

| Operation | Blocks when… |
|---|---|
| `tx.send(val)` | buffer full / no receiver (unbuffered) |
| `rx.recv() -> Option<T>` | buffer empty / no sender; returns `None` on close |
| `tx.try_send(val) -> bool` | never blocks; returns false if would block |
| `rx.try_recv() -> Option<T>` | never blocks; returns `None` if empty |
| `tx.close()` | — wakes all blocked receivers with `None` |

`Sender<T>` and `Receiver<T>` are both `Clone`. Closing happens automatically when the last `Sender` clone is dropped, or explicitly via `tx.close()`. Sending on a closed channel panics (matching Go semantics).

```rust
go_lib::run(|| {
    let (tx, rx) = chan::<u64>(8); // buffered, capacity 8

    go!(move || {
        for i in 0..8 { tx.send(i); }
        tx.close();
    });

    while let Some(n) = rx.recv() {
        println!("{n}");
    }
});
```

---

### select!

Multiplexes channel operations, picking the first ready case at random (Go's fairness guarantee). Without `default` it blocks until a case fires; with `default` it is non-blocking.

**Syntax:**

```rust
select! {
    recv(rx)       -> v => { /* v: Option<T> */ }
    recv(rx2)      -> v => { /* … */            }
    send(tx, expr)     => { /* sent */           }
    default            => { /* nothing ready */  }
}
```

- Recv arms: `v` is `Some(T)` on success, `None` if the channel is closed.
- Send arms: the expression is evaluated once; consumed on win, dropped on loss.
- Up to 4 recv arms + 2 send arms per invocation.
- Arms may appear in any order.

```rust
go_lib::run(|| {
    let (tx1, rx1) = chan::<i32>(0);
    let (tx2, rx2) = chan::<i32>(0);

    go!(move || tx1.send(1));
    go!(move || tx2.send(2));

    select! {
        recv(rx1) -> v => println!("from rx1: {:?}", v),
        recv(rx2) -> v => println!("from rx2: {:?}", v),
    }
});
```

**Nonblocking poll:**

```rust
select! {
    recv(rx) -> v => println!("{:?}", v),
    default       => println!("nothing ready"),
}
```

---

### WaitGroup

```rust
use go_lib::sync::WaitGroup;
use std::sync::Arc;

go_lib::run(|| {
    let wg = Arc::new(WaitGroup::new());

    for i in 0..8 {
        wg.add(1);
        let wg = Arc::clone(&wg);
        go!(move || {
            println!("worker {i}");
            wg.done();
        });
    }

    wg.wait(); // blocks until counter reaches 0
});
```

`WaitGroup` is reusable: `add` / `done` / `wait` may be called in multiple rounds on the same instance. Calling `done` when the counter is already 0 panics (matching Go semantics).

---

### Cond

A goroutine-aware condition variable. `wait` parks the calling goroutine via the scheduler (instead of blocking an OS thread), so other goroutines sharing the same M continue to run while waiting.

```rust
use go_lib::sync::Cond;
use std::sync::{Arc, Mutex};

go_lib::run(|| {
    let mu  = Arc::new(Mutex::new(false));
    let cnd = Arc::new(Cond::new());

    let mu2  = Arc::clone(&mu);
    let cnd2 = Arc::clone(&cnd);

    go!(move || {
        // Producer: set the flag and signal.
        *mu2.lock().unwrap() = true;
        cnd2.notify_one();
    });

    // Consumer: wait until the flag is set.
    let mut guard = mu.lock().unwrap();
    while !*guard {
        guard = cnd.wait(&mu, guard);
    }
    println!("flag is set");
});
```

| Method | Description |
|---|---|
| `Cond::new()` | Create a new condition variable |
| `cnd.wait(mu, guard)` | Release `guard`, park goroutine, re-acquire on wakeup; returns new guard |
| `cnd.notify_one()` | Wake one waiting goroutine |
| `cnd.notify_all()` | Wake all waiting goroutines |

Always re-check the predicate in a loop — spurious wakeups are possible.

---

### context

A port of Go's `context` package. A `Context` carries a cancellation signal and optional deadline. Cancellation propagates from parent to all descendants.

```rust
use go_lib::context;
use std::time::Duration;

go_lib::run(|| {
    let bg = context::background(); // root — never cancels

    // Derived context with explicit cancel
    let (ctx, cancel) = context::with_cancel(&bg);

    go!(move || {
        loop {
            select! {
                recv(ctx.done()) -> _v => { break }   // cancelled
                default => { /* do work */ go_lib::gosched(); }
            }
        }
        println!("worker stopped");
    });

    go_lib::sleep(Duration::from_millis(10));
    cancel.cancel(); // signal all workers
});
```

| Constructor | Description |
|---|---|
| `context::background()` | Root context; never cancelled |
| `context::with_cancel(parent)` | Returns `(Context, CancelFn)`; `cancel.cancel()` cancels |
| `context::with_deadline(parent, instant)` | Auto-cancels at `instant`; also returns `CancelFn` |
| `context::with_timeout(parent, duration)` | Sugar over `with_deadline` |

| Method | Description |
|---|---|
| `ctx.done()` | `&Receiver<()>` — fires (`None`) when cancelled; use in `select!` |
| `ctx.err()` | `Option<ContextError>` — `None`, `Cancelled`, or `DeadlineExceeded` |
| `ctx.deadline()` | `Option<Instant>` |
| `ctx.is_done()` | `bool` — shorthand for `ctx.err().is_some()` |
| `cancel.cancel()` | Cancel the context; idempotent, safe to call multiple times |

`Context` and `CancelFn` are both `Clone`. `with_deadline` / `with_timeout` spawn a timer goroutine and must be called from within `run`.

---

### sleep and gosched

```rust
go_lib::sleep(Duration::from_millis(100)); // park goroutine; let others run
go_lib::gosched();                         // cooperative yield to scheduler
```

`sleep` parks the calling goroutine and inserts a timer; the background timer thread calls `goready` when the duration elapses. `gosched` is the equivalent of Go's `runtime.Gosched()`.

Both must be called from inside `run`.

---

### with_syscall

```rust
go_lib::run(|| {
    let contents = go_lib::with_syscall(|| std::fs::read("data.bin"));
});
```

Wraps a potentially-blocking operation so the scheduler can hand the current M's P to another M while the OS thread is blocked. Use this around any call that may park an OS thread (file I/O, blocking network, `std::thread::sleep`, etc.).

---

### net — async TCP

`go_lib::net` provides goroutine-aware TCP sockets integrated with the scheduler on all supported platforms:

| Platform | Backend | I/O model |
|---|---|---|
| Linux | `epoll` | readiness-based (`EAGAIN` → park → fd ready → resume) |
| macOS | `kqueue` | readiness-based |
| Windows | IOCP | completion-based (`WSARecv`/`WSASend` → park → operation done → resume) |

```rust
use go_lib::net::{TcpListener, TcpStream};

go_lib::run(|| {
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();

    loop {
        let mut stream = listener.accept().unwrap(); // parks until connection arrives
        go!(move || {
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).unwrap(); // parks if no data yet
            stream.write(&buf[..n]).unwrap();        // parks if send buffer full
        });
    }
});
```

| Type | Method | Description |
|---|---|---|
| `TcpListener` | `bind(addr)` | Bind a server socket |
| `TcpListener` | `accept()` | Accept; parks goroutine until a connection arrives |
| `TcpStream` | `connect(addr)` | Connect; parks until the connection is established |
| `TcpStream` | `read(&mut buf)` | Read; parks if no data available |
| `TcpStream` | `write(&buf)` | Write; parks if send buffer full |

The park/resume flow is integrated into the scheduler: `findrunnable` checks `netpoll_wait(0)` on every scheduling iteration, and `sysmon` polls it during idle periods.

---

### GOMAXPROCS

The number of logical processors defaults to `available_parallelism()` but can be overridden:

**Environment variable** — set before starting the process:

```sh
GOMAXPROCS=4 cargo run
```

**Runtime adjustment** — from inside `run`:

```rust
let old = go_lib::set_gomaxprocs(4);
println!("was {old}, now {}", go_lib::gomaxprocs());
```

Increasing GOMAXPROCS immediately spawns new Ps and M-threads. Decreasing updates the counter; surplus Ms park on their next idle cycle.

---

### Panic handler

By default, a goroutine panic prints the payload to stderr and the scheduler continues — the process does **not** abort. Install a custom handler to log, record metrics, or recover state:

```rust
go_lib::set_panic_handler(|payload| {
    if let Some(msg) = payload.downcast_ref::<String>() {
        eprintln!("[goroutine panic] {msg}");
    } else if let Some(msg) = payload.downcast_ref::<&str>() {
        eprintln!("[goroutine panic] {msg}");
    }
});

go_lib::run(|| {
    go!(|| panic!("oops")); // caught; other goroutines keep running
    go_lib::sleep(std::time::Duration::from_millis(10));
    println!("still running");
});
```

---

## Examples

### attr\_main — `#[go_lib::run]` attribute

The attribute macro is the most concise entry point.  It rewrites the
function body in-place, so the three `main` signatures work without any
boilerplate:

```rust
// examples/attr_run.rs  (illustrative — see the real file for the full driver)

// 1. Plain — no return value
#[go_lib::run]
fn main() {
    let (tx, rx) = chan::<&str>(0);
    go!(move || tx.send("hello from #[go_lib::run]"));
    println!("{}", rx.recv().unwrap());
}

// 2. main() -> ExitCode
#[go_lib::run]
fn main() -> ExitCode {
    let ok = run_all_tasks();
    if ok { ExitCode::SUCCESS } else { ExitCode::FAILURE }
}

// 3. main() -> Result<(), E>   (? works inside the closure)
#[go_lib::run]
fn main() -> Result<(), ParseIntError> {
    let (tx, rx) = chan::<Result<i64, _>>(4);
    for s in ["1","2","3","4"] {
        let tx = tx.clone();
        go!(move || tx.send(s.parse::<i64>()));
    }
    let mut sum = 0i64;
    for _ in 0..4 { sum += rx.recv().unwrap()?; }
    println!("sum = {sum}");
    Ok(())
}
```

The macro expands each of these into `go_lib::run(move || [-> R] { … })` —
identical to writing it by hand, but without the wrapping ceremony.

---

### hello — goroutines and channels

```rust
// examples/hello.rs
use go_lib::{chan::chan, go};

fn main() {
    const N: usize = 5;
    go_lib::run(|| {
        let (tx, rx) = chan::<String>(0);

        for i in 0..N {
            let tx = tx.clone();
            go!(move || tx.send(format!("hello from goroutine {i}")));
        }
        drop(tx);

        for _ in 0..N {
            if let Some(msg) = rx.recv() { println!("{msg}"); }
        }
    });
}
```

```
hello from goroutine 2
hello from goroutine 0
hello from goroutine 4
hello from goroutine 1
hello from goroutine 3
```

### pipeline — three-stage concurrent pipeline

```rust
// examples/pipeline.rs  (generate → square → print)
use go_lib::{chan::chan, go};

fn main() {
    go_lib::run(|| {
        let (gen_tx, gen_rx) = chan::<u64>(0);
        go!(move || {
            for n in 1..=8 { gen_tx.send(n); }
            gen_tx.close();
        });

        let (sq_tx, sq_rx) = chan::<u64>(0);
        go!(move || {
            loop {
                match gen_rx.recv() {
                    Some(n) => sq_tx.send(n * n),
                    None    => { sq_tx.close(); break; }
                }
            }
        });

        let mut sum = 0u64;
        while let Some(sq) = sq_rx.recv() {
            println!("{sq}");
            sum += sq;
        }
        assert_eq!(sum, 204); // 1+4+9+16+25+36+49+64
    });
}
```

### select\_fanin — fan-in with select!

Two producers at different rates merged into one consumer:

```rust
// examples/select_fanin.rs
use go_lib::{chan::chan, go, select};
use std::time::Duration;

fn main() {
    go_lib::run(|| {
        let (fast_tx, fast_rx) = chan::<i32>(8);
        let (slow_tx, slow_rx) = chan::<i32>(4);

        go!(move || { for i in 0..6   { go_lib::sleep(Duration::from_millis(5));  fast_tx.send(i); } });
        go!(move || { for i in 10..13 { go_lib::sleep(Duration::from_millis(15)); slow_tx.send(i); } });

        let mut received = Vec::new();
        for _ in 0..9 {
            select! {
                recv(fast_rx) -> v => { if let Some(n) = v { received.push(('F', n)); } }
                recv(slow_rx) -> v => { if let Some(n) = v { received.push(('S', n)); } }
            }
        }
        println!("{received:?}");
    });
}
```

### cond — bounded producer/consumer queue

```rust
// examples/cond.rs  (abridged)
use go_lib::sync::{Cond, WaitGroup};
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

struct BoundedQueue<T> {
    buf:       Mutex<VecDeque<T>>,
    cap:       usize,
    not_full:  Cond,
    not_empty: Cond,
}

impl<T: Send + 'static> BoundedQueue<T> {
    fn push(&self, val: T) {
        let mut buf = self.buf.lock().unwrap();
        while buf.len() >= self.cap {
            buf = self.not_full.wait(&self.buf, buf);
        }
        buf.push_back(val);
        drop(buf);
        self.not_empty.notify_one();
    }

    fn pop(&self) -> T {
        let mut buf = self.buf.lock().unwrap();
        while buf.is_empty() {
            buf = self.not_empty.wait(&self.buf, buf);
        }
        let val = buf.pop_front().unwrap();
        drop(buf);
        self.not_full.notify_one();
        val
    }
}
```

### scope — parallel reduction with safe borrows

```rust
// examples/scope.rs
fn main() {
    let sum = go_lib::run(|| {
        // `data` lives on the goroutine's stack.  Spawned goroutines borrow
        // chunks of it — no Arc or Clone needed.
        let data: Vec<i64> = (1..=100).collect();

        go_lib::scope(|s| {
            let chunks: Vec<&[i64]> = data.chunks(data.len() / 4 + 1).collect();

            let handles: Vec<_> = chunks
                .into_iter()
                .map(|chunk| s.go(move || chunk.iter().sum::<i64>()))
                .collect();

            handles
                .into_iter()
                .map(|h| h.join().expect("chunk goroutine panicked"))
                .sum::<i64>()
        })
    });

    println!("sum 1..=100 = {sum}");  // 5050
    assert_eq!(sum, 5050);
}
```

The goroutines borrow slices of `data` directly from the enclosing goroutine's stack frame — no `Arc` or channel needed.

---

### scope\_channel — producer/consumer inside a scope

When goroutines need to stream values between each other, channels work normally inside `s.go()` closures.  Calling `tx.close()` after the last send signals the consumer that the stream is finished — the same semantics as `close(ch)` in Go.

```rust
// examples/scope_channel.rs
fn main() {
    let sum = go_lib::run(|| {
        let (tx, rx) = go_lib::chan::chan::<i32>(0); // unbuffered

        go_lib::scope(|s| {
            // Producer: send 0..10, then close so the consumer terminates.
            s.go(move || {
                for i in 0..10 {
                    tx.send(i);
                }
                tx.close();
            });

            // Consumer: drain until the channel is closed and empty.
            s.go(move || {
                let mut total = 0_i32;
                while let Some(v) = rx.recv() {
                    total += v;
                }
                total
            })
            .join()
            .expect("consumer goroutine panicked")
        })
        // scope() blocks here until both goroutines have finished.
    });

    println!("sum 0..10 = {sum}");
    assert_eq!(sum, 45); // 0 + 1 + … + 9 = 45
}
```

---

### main\_exitcode — `main() -> ExitCode`

`go_lib::scope` runs the tasks concurrently and collects their `(id, bool)` results through join handles — no `Arc`, `WaitGroup`, or channel required:

```rust
// examples/main_exitcode.rs
use std::process::ExitCode;

fn main() -> ExitCode {
    let all_passed = go_lib::run(|| {
        const N: usize = 5;

        let results = go_lib::scope(|s| {
            let handles: Vec<_> = (0..N)
                .map(|id| s.go(move || (id, run_task(id))))
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("task goroutine panicked"))
                .collect::<Vec<_>>()
        });

        let mut failures = 0_usize;
        for (id, ok) in results {
            println!("  task {id}: {}", if ok { "ok" } else { "FAIL" });
            if !ok { failures += 1; }
        }
        println!("{}/{N} tasks passed", N - failures);
        failures == 0   // ← return value of run()
    });

    if all_passed { ExitCode::SUCCESS } else { ExitCode::FAILURE }
}
```

```
  task 0: ok
  task 1: FAIL
  task 2: ok
  task 3: FAIL
  task 4: ok
3/5 tasks passed
```
Exit code: `1`

### main\_result — `main() -> Result<(), E>`

The closure can return `Result`; `main` returns it verbatim.  Rust's
`Termination` trait prints the error and sets exit code `1` on `Err`.
`go_lib::scope` lets each goroutine borrow its `&str` directly — no channel needed:

```rust
// examples/main_result.rs
use std::num::ParseIntError;

fn main() -> Result<(), ParseIntError> {
    go_lib::run(|| -> Result<(), ParseIntError> {
        let inputs = ["3", "1", "4", "1", "5", "9"];

        // Parse concurrently; goroutines borrow `inputs` directly.
        let sum: i64 = go_lib::scope(|scope| -> Result<i64, ParseIntError> {
            let handles: Vec<_> = inputs
                .iter()
                .map(|s| scope.go(move || s.parse::<i64>()))
                .collect();
            // h.join().unwrap() strips the panic wrapper; ? propagates ParseIntError.
            handles
                .into_iter()
                .try_fold(0_i64, |acc, h| Ok(acc + h.join().unwrap()?))
        })?;

        println!("sum = {sum}");   // sum = 23
        Ok(())
    })
}
```

```
sum = 23
```

If one of the strings were not a valid integer (e.g. `"abc"`), `main` would
print `Error: invalid digit found in string` and exit with code `1`.

---

## Testing & CI

### Standard tests

```sh
cargo test
```

Runs 106 unit tests, 15 integration tests, and 23 doc tests.  All tests use
`std::sync` primitives and the real go-lib scheduler.

### Loom concurrency model checker

[loom](https://docs.rs/loom) explores every possible thread interleaving of a
concurrent program and checks for data races, deadlocks, and broken invariants.

```sh
RUSTFLAGS="--cfg loom" LOOM_MAX_PERMUTATIONS=10000 cargo test -- --test-threads 1
```

Under `--cfg loom` the `loom_shim` module swaps `std::sync::Mutex` and
`std::sync::Condvar` for `loom::sync` equivalents in the data structures under
test (`GlobalRunQueue`, `WaitGroup`).  Every test wrapped in `loom::model(||
{ … })` is exercised across all valid interleavings.

Tests that are coupled to the live scheduler (`chan`, `select`, `context`,
`sync::Cond`, and the scheduler itself) are excluded from the loom run via
`#[cfg(all(test, not(loom)))]` because the scheduler uses assembly-level
primitives (`gopark`, `goready`, `gogo`, `mcall`) that loom cannot model.

To increase the search depth for local exploration:

```sh
# Unlimited permutations (slow but exhaustive for small models)
RUSTFLAGS="--cfg loom" LOOM_MAX_PERMUTATIONS=0 cargo test -- --test-threads 1
```

### CI

The GitHub Actions workflow (`.github/workflows/ci.yml`) runs two jobs on every
push and pull request targeting `main`:

| Job | Command | Checks |
|---|---|---|
| `test` | `cargo test` | Build, all unit/integration/doc tests |
| `loom` | `RUSTFLAGS="--cfg loom" cargo test -- --test-threads 1` | Concurrent data structure correctness |

---

## Architecture

### Per-goroutine memory overhead

| Platform        | Initial stack | OS guard page | `G` descriptor | Total per goroutine |
|-----------------|---------------|---------------|----------------|---------------------|
| Linux x86-64    | 2 KiB         | 4 KiB         | 128 B          | **~6.1 KiB**        |
| Linux AArch64   | 2 KiB         | 4 KiB         | 128 B          | **~6.1 KiB**        |
| macOS x86-64    | 2 KiB         | 4 KiB         | 128 B          | **~6.1 KiB**        |
| macOS AArch64   | 2 KiB         | 16 KiB        | 128 B          | **~18 KiB**         |
| Windows x86-64  | 2 KiB         | 4 KiB         | 128 B          | **~6.1 KiB**        |

The initial stack matches Go's `stackMin = 2048` and grows on demand (up to 1 GiB) via the SIGSEGV/SIGBUS guard-page handler.  The `G` descriptor is 128 B — smaller than Go's `g` (≈480 B) because GC, defer/panic chain, and tracer fields are omitted.

The remaining gap to Go's ~2.4 KiB minimum (2 KiB stack + 392 B descriptor, no OS guard page) is the OS guard page itself.  Closing it would require compiler-generated `morestack` checks in every Rust function — not feasible without compiler changes; without them, severe stack overflows would silently corrupt adjacent memory rather than crashing cleanly.

Debug builds use larger initial sizes (Linux 16 KiB, macOS 64 KiB, Windows 64 KiB) because non-optimised frames are 3–5× wider and would otherwise trigger the growth handler on every new goroutine.

```
go_lib::run(f)
    │
    ├─ schedinit()          create GOMAXPROCS Ps; spawn one M per P
    │                       install SIGSEGV + SIGURG handlers
    │                       start sysmon thread; start timer thread
    │
    ├─ spawn_goroutine(f)   allocate 2 KiB stack + 128 B G; push to global run queue
    │
    └─ thread::park()       calling thread sleeps until f() returns

Each M-thread (M::start → schedule → findrunnable → execute → goexit0 → schedule):

    M::start()
      pthread_id = pthread_self()  capture for SIGURG delivery
      setup_sigaltstack()          per-thread 64 KiB alternate signal stack

    findrunnable()
      1. local P run queue  (256-slot lock-free ring, no lock on get)
      2. global run queue   (Mutex-protected linked list)
      3. work-steal         (up to 4 attempts, random victim P)
      4. netpoll_wait(0)    non-blocking poll; goready() each ready/completed G
                             (epoll on Linux, kqueue on macOS, IOCP on Windows)
      5. stopm()            park M on idle list; wake on goready/startm

    execute(gp)
      grow_stack_if_needed(gp)   checkpoint: proactively double stack if nearly full
      gogo(gp)                   context switch onto goroutine's stack (naked asm)

    goexit0(gp)
      → schedule()               re-enter scheduler loop

Stack growth (Step 3):
    goroutine touches guard page → SIGSEGV (Linux/Windows) or SIGBUS (macOS)
    sigsegv_handler / sigbus_handler     identify guard-page fault
    newstack(gp)                 double stack (capped at 1 GiB); copystack
                                 brackets the copy with
                                 casgstatus(GRUNNING→GCOPYSTACK→GRUNNING)
                                 and rewrites all pointer-sized words in
                                 [old_guard_lo, old_hi)
    update_sp_in_context(ucontext, delta)   rewrite SP + FP + callee-saved
                                 and argument GPRs so the retried instruction
                                 lands on the new stack (two-range scheme)
    grow_stack_if_needed         proactive checkpoint on every gogo: doubles
                                 the stack when saved SP is within STACK_GUARD
                                 (928 B) of the guard page

Async preemption (Step 4):
    sysmon: goroutine running > 10 ms
      gp.preempt = true; gp.stackguard0 = STACK_PREEMPT
      pthread_kill(m.pthread_id, SIGURG)   → sigurg_handler
    sigurg_handler
      redirect_to_async_preempt(gp, ucontext)
        push original RIP → [RSP]; set RIP = async_preempt_trampoline
    async_preempt_trampoline (naked asm)
      save all GPRs + XMM/FP regs → goroutine stack
      call async_preempt2()
    async_preempt2()
      mcall(preemptm):
        casgstatus(GRUNNING→GPREEMPTED→GRUNNABLE)   [two-step Go 1.14+ protocol]
        schedule()   [G re-queued; resumes later via gogo]
      (returns after gogo re-schedules this G)
    async_preempt_trampoline (resumed)
      restore all regs; ret → original interrupted PC

Netpoll (Step 5):
    Unix:
      TcpStream::read: EAGAIN → netpoll_arm(fd, POLL_READ, gp) → gopark
      sysmon/findrunnable: netpoll_wait(0) → goready(gp) for each ready fd
    Windows (IOCP):
      TcpStream::read: alloc IocpOp{gp} → WSARecv(overlapped) → gopark
      sysmon/findrunnable: netpoll_wait(0) → GetQueuedCompletionStatusEx
                           → fill IocpOp.{bytes,ntstatus} → goready(gp)
```

### Source map

| Rust module | Go source | Purpose |
|---|---|---|
| `runtime::g` | `runtime/runtime2.go` | G struct, goroutine status constants |
| `runtime::m` | `runtime/runtime2.go` | M struct, Note park/unpark primitive |
| `runtime::p` | `runtime/runtime2.go`, `proc.go` | P struct, 256-slot run queue |
| `runtime::sched` | `runtime/proc.go`, `runtime/preempt.go` | schedule, findrunnable, execute, goexit0, async_preempt2, SIGURG handler, GOMAXPROCS |
| `runtime::park` | `runtime/proc.go` | gopark, goready |
| `runtime::stack` | `runtime/stack.go`, `runtime/signal_unix.go` | 2 KiB→1 GiB dynamic stack allocator, newstack, copystack, SIGSEGV/SIGBUS handler |
| `runtime::netpoll` | `runtime/netpoll_epoll.go`, `runtime/netpoll_kqueue.go`, `runtime/netpoll_windows.go` | epoll (Linux) / kqueue (macOS) / IOCP (Windows) |
| `runtime::sudog` | `runtime/runtime2.go` | Sudog waiter records + per-P pool |
| `runtime::syscall` | `runtime/proc.go` | entersyscall, exitsyscall, handoffp |
| `runtime::sysmon` | `runtime/proc.go` | sysmon, retake, async preemption via `pthread_kill(SIGURG)` |
| `runtime::time` | `runtime/time.go` | 4-ary min-heap timer, goroutine_sleep |
| `runtime::asm_amd64` | `runtime/asm_amd64.s`, `runtime/preempt_amd64.s` | gogo, mcall, systemstack, async_preempt_trampoline (AMD64) |
| `runtime::asm_arm64` | `runtime/asm_arm64.s`, `runtime/preempt_arm64.s` | gogo, mcall, systemstack, async_preempt_trampoline (AArch64) |
| `net` | `net/tcpsock.go`, `net/fd_*.go` | TcpListener, TcpStream — goroutine-aware non-blocking TCP |
| `chan` | `runtime/chan.go` | hchan, chansend, chanrecv, closechan |
| `select` | `runtime/select.go` | selectgo, type-erased vtable |
| `scope` | *(new — mirrors `std::thread::scope`)* | Scoped goroutines; safe short-lived borrows; `ScopedJoinHandle` |
| `sync::waitgroup` | `sync/waitgroup.go` | WaitGroup |
| `sync::cond` | `sync/cond.go` | Cond — goroutine-aware condition variable |
| `context` | `context/context.go` | background, with_cancel, with_deadline, with_timeout |
| `loom_shim` | *(new)* | Conditional re-export: `loom::sync` under `--cfg loom`, `std::sync` otherwise |

---

## Go → Rust mapping

| Go | Rust |
|---|---|
| `go func() { … }` | `go!(closure)` |
| `make(chan T)` | `chan::chan::<T>(0)` |
| `make(chan T, n)` | `chan::chan::<T>(n)` |
| `close(ch)` | `tx.close()` (or drop last `Sender`) |
| `select { case … }` | `select! { … }` |
| `sync.WaitGroup` | `sync::WaitGroup` |
| `sync.Cond` | `sync::Cond` |
| `context.Background()` | `context::background()` |
| `context.WithCancel(ctx)` | `context::with_cancel(&ctx)` |
| `context.WithDeadline(ctx, t)` | `context::with_deadline(&ctx, t)` |
| `context.WithTimeout(ctx, d)` | `context::with_timeout(&ctx, d)` |
| `runtime.Gosched()` | `go_lib::gosched()` |
| `time.Sleep(d)` | `go_lib::sleep(d)` |
| `runtime.GOMAXPROCS(n)` | `go_lib::set_gomaxprocs(n)` |
| `errgroup.Group` / `golang.org/x/sync/errgroup` | `go_lib::scope` (with `h.join().unwrap()?`) |

---

## Known limitations

**No `defer`/`recover` across goroutine boundaries** — Goroutine panics are caught and routed to the panic handler; the process does not abort. However, Go's `recover()` (intercepting a panic mid-stack and returning a value to the caller) has no direct equivalent. Use `std::panic::catch_unwind` inside the goroutine body for fine-grained recovery.

**GOMAXPROCS decrease is best-effort** — Increasing GOMAXPROCS immediately adds capacity. Decreasing updates the counter but does not forcibly retire excess Ms; they park on their next idle cycle and are re-recruited if GOMAXPROCS rises again.

**No race detector** — The Go race detector is a compiler/runtime feature with no Rust equivalent in this crate. Use the [loom model checker](#testing--ci) (`cargo test --cfg loom`) for systematic concurrency testing of the data structures that are within loom's boundary.  Scheduler-level primitives (goroutine stacks, context switches) are outside loom's scope.

**Conservative `copystack` pointer scan** — Stack growth copies the live stack region and adjusts every pointer-sized word that falls within the old stack bounds. Values that coincidentally equal a stack address (e.g. integer constants, heap pointers above the stack range) are not adjusted. Return addresses (code segment pointers) are far outside the stack range and are never touched. In practice, Rust's borrow checker makes stack-address escapes nearly impossible, so false adjustments are vanishingly rare.

