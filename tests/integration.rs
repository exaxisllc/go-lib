// SPDX-License-Identifier: Apache-2.0
//! Integration tests — exercise the full public API end-to-end.
//!
//! Each test runs the go-lib scheduler via `go_lib::run`, spawns goroutines
//! with `go!`, communicates through channels, and synchronises with
//! `WaitGroup` or atomic flags.  No `pub(crate)` symbols are used.

use std::sync::{
    atomic::{AtomicI32, AtomicI64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use go_lib::{
    chan::chan,
    go,
    scope,
    scope::ScopedJoinHandle,
    select,
    sync::WaitGroup,
};

// ---------------------------------------------------------------------------
// 1. Hello goroutine — spawn and receive a single value
// ---------------------------------------------------------------------------

#[test]
fn hello_goroutine() {
    let (tx, rx) = chan::<&'static str>(0);
    go_lib::run(move || {
        go!(move || { tx.send("hello"); });
        assert_eq!(rx.recv(), Some("hello"));
    });
}

// ---------------------------------------------------------------------------
// 2. Fan-out — N goroutines each send one value; main collects all N
// ---------------------------------------------------------------------------

#[test]
fn fan_out() {
    const N: i32 = 8;
    let sum = Arc::new(AtomicI32::new(0));

    let sum2 = Arc::clone(&sum);
    go_lib::run(move || {
        let (tx, rx) = chan::<i32>(N as usize);

        for i in 1..=N {
            let tx = tx.clone();
            go!(move || { tx.send(i); });
        }

        // Collect all N values.
        let mut total = 0_i32;
        for _ in 0..N {
            total += rx.recv().unwrap();
        }
        sum2.store(total, Ordering::Relaxed);
    });

    // 1+2+…+N = N*(N+1)/2
    assert_eq!(sum.load(Ordering::Acquire), N * (N + 1) / 2);
}

// ---------------------------------------------------------------------------
// 3. Pipeline — generator → square → accumulate (three-stage)
// ---------------------------------------------------------------------------

#[test]
fn pipeline_three_stage() {
    let result = Arc::new(AtomicI64::new(0));
    let result2 = Arc::clone(&result);

    go_lib::run(move || {
        // Stage 1: emit 1..=5
        let (gen_tx, gen_rx) = chan::<i64>(0);
        go!(move || {
            for i in 1_i64..=5 {
                gen_tx.send(i);
            }
            gen_tx.close();
        });

        // Stage 2: square each value
        let (sq_tx, sq_rx) = chan::<i64>(0);
        go!(move || {
            loop {
                match gen_rx.recv() {
                    Some(v) => sq_tx.send(v * v),
                    None    => { sq_tx.close(); break; }
                }
            }
        });

        // Stage 3: accumulate
        let mut sum = 0_i64;
        while let Some(v) = sq_rx.recv() {
            sum += v;
        }
        // 1+4+9+16+25 = 55
        result2.store(sum, Ordering::Relaxed);
    });

    assert_eq!(result.load(Ordering::Acquire), 55);
}

// ---------------------------------------------------------------------------
// 4. WaitGroup fan-out — N workers, each increments a counter then calls Done
// ---------------------------------------------------------------------------

#[test]
fn waitgroup_fan_out() {
    const N: i32 = 16;
    let counter = Arc::new(AtomicI32::new(0));
    let counter2 = Arc::clone(&counter);

    go_lib::run(move || {
        let wg = Arc::new(WaitGroup::new());

        for _ in 0..N {
            wg.add(1);
            let wg2 = Arc::clone(&wg);
            let c    = Arc::clone(&counter2);
            go!(move || {
                c.fetch_add(1, Ordering::Relaxed);
                wg2.done();
            });
        }

        wg.wait();
        // All goroutines finished before wait() returned.
        assert_eq!(counter2.load(Ordering::Acquire), N);
    });

    assert_eq!(counter.load(Ordering::Acquire), N);
}

// ---------------------------------------------------------------------------
// 5. Ping-pong — two goroutines exchange a value 20 times
// ---------------------------------------------------------------------------

#[test]
fn ping_pong() {
    let hops = Arc::new(AtomicI32::new(0));
    let hops2 = Arc::clone(&hops);

    go_lib::run(move || {
        let (a_tx, a_rx) = chan::<i32>(0);
        let (b_tx, b_rx) = chan::<i32>(0);

        // Keep a clone for goroutine B; the original kicks off the exchange.
        let a_tx_b = a_tx.clone();

        // Goroutine A: receive on a, send on b.
        go!(move || {
            while let Some(v) = a_rx.recv() {
                b_tx.send(v + 1);
            }
        });

        // Goroutine B: receive on b, send on a (until done).
        let h = Arc::clone(&hops2);
        go!(move || {
            while let Some(v) = b_rx.recv() {
                h.fetch_add(1, Ordering::Relaxed);
                if v < 20 {
                    a_tx_b.send(v + 1);
                } else {
                    a_tx_b.close();
                }
            }
        });

        // Kick off the ping-pong.
        a_tx.send(0);
        // Wait until goroutine B closes a_tx (indirectly, via a_rx draining).
        // The main goroutine yields until both workers finish.
        let deadline = Instant::now() + Duration::from_millis(500);
        loop {
            if hops2.load(Ordering::Acquire) >= 10 { break; }
            if Instant::now() > deadline { panic!("ping-pong timed out"); }
            go_lib::gosched();
        }
    });

    assert!(hops.load(Ordering::Acquire) >= 10);
}

// ---------------------------------------------------------------------------
// 6. Select fan-in — two senders, one receiver; both values arrive
// ---------------------------------------------------------------------------

#[test]
fn select_fan_in() {
    let sum = Arc::new(AtomicI32::new(0));
    let sum2 = Arc::clone(&sum);

    go_lib::run(move || {
        let (tx1, rx1) = chan::<i32>(0);
        let (tx2, rx2) = chan::<i32>(0);

        go!(move || { tx1.send(10); });
        go!(move || { tx2.send(20); });

        let mut total = 0_i32;
        for _ in 0..2 {
            select! {
                recv(rx1) -> v => { total += v.unwrap(); }
                recv(rx2) -> v => { total += v.unwrap(); }
            }
        }
        sum2.store(total, Ordering::Relaxed);
    });

    assert_eq!(sum.load(Ordering::Acquire), 30);
}

// ---------------------------------------------------------------------------
// 7. Done channel — goroutine loops until signalled to stop
// ---------------------------------------------------------------------------

#[test]
fn done_channel_cancels_goroutine() {
    let ticks = Arc::new(AtomicI32::new(0));
    let ticks2 = Arc::clone(&ticks); // moved into worker goroutine
    let ticks3 = Arc::clone(&ticks); // used by polling loop inside run()

    go_lib::run(move || {
        let (done_tx, done_rx) = chan::<()>(0);
        let (tick_tx, tick_rx) = chan::<()>(4);

        // Worker: keep ticking until done.
        go!(move || {
            loop {
                select! {
                    recv(done_rx) -> _v => { break; }
                    recv(tick_rx) -> _v => { ticks2.fetch_add(1, Ordering::Relaxed); }
                    default            => { go_lib::gosched(); }
                }
            }
        });

        // Send 3 ticks, then signal done.
        for _ in 0..3 { tick_tx.send(()); }
        // Wait until the worker has processed all 3 ticks before sending done.
        // A fixed gosched-loop is not deterministic under parallel test load;
        // polling on the atomic counter is race-free and works on every platform.
        let deadline = Instant::now() + Duration::from_millis(500);
        loop {
            if ticks3.load(Ordering::Acquire) >= 3 { break; }
            if Instant::now() > deadline { panic!("ticks not all processed in time"); }
            go_lib::gosched();
        }
        done_tx.send(());

        // Give the worker time to observe done and exit its loop.
        for _ in 0..50 { go_lib::gosched(); }
    });

    assert_eq!(ticks.load(Ordering::Acquire), 3);
}

// ---------------------------------------------------------------------------
// 8. Buffered channel saturation — sender never blocks if buffer has room
// ---------------------------------------------------------------------------

#[test]
fn buffered_never_blocks_sender() {
    const N: usize = 64;
    let received  = Arc::new(AtomicI32::new(0));
    let received2 = Arc::clone(&received);

    go_lib::run(move || {
        let wg = Arc::new(WaitGroup::new());
        let (tx, rx) = chan::<i32>(N);

        // Fill the buffer from the main goroutine — should never block.
        for i in 0..N as i32 { tx.send(i); }

        // Drain in a separate goroutine.
        wg.add(1);
        let wg2 = Arc::clone(&wg);
        go!(move || {
            let mut count = 0_i32;
            for _ in 0..N { rx.recv(); count += 1; }
            received2.store(count, Ordering::Relaxed);
            wg2.done();
        });

        wg.wait();
    });

    assert_eq!(received.load(Ordering::Acquire), N as i32);
}

// ---------------------------------------------------------------------------
// 9. sleep — goroutine wakes after a short delay
// ---------------------------------------------------------------------------

#[test]
fn sleep_completes() {
    let elapsed_ms  = Arc::new(AtomicI64::new(-1));
    let e2          = Arc::clone(&elapsed_ms);
    let elapsed_ms3 = Arc::clone(&elapsed_ms); // kept for the assert after run()

    go_lib::run(move || {
        // Use WaitGroup so the main goroutine parks (gopark) rather than
        // blocking the OS thread.  std::thread::sleep inside a goroutine
        // holds the M+P without releasing them; under high scheduling
        // pressure (many integration tests running concurrently) the timer
        // that fires for the sleeper may find no idle M and starve past the
        // timeout, causing an unrecoverable hang via the panic path.
        let wg  = Arc::new(WaitGroup::new());
        let wg2 = Arc::clone(&wg);
        wg.add(1);

        go!(move || {
            let t0 = Instant::now();
            go_lib::sleep(Duration::from_millis(10));
            e2.store(t0.elapsed().as_millis() as i64, Ordering::Relaxed);
            wg2.done();
        });

        wg.wait(); // parks this goroutine; the M is free to run the sleeper
    });

    let ms = elapsed_ms3.load(Ordering::Acquire);
    assert!(ms >= 8, "slept too short: {ms} ms"); // allow ±2 ms slack
}

// ---------------------------------------------------------------------------
// 10. select! nonblocking send — value dropped correctly when buffer full
// ---------------------------------------------------------------------------

#[test]
fn select_send_drops_on_full_buffer() {
    let dropped = Arc::new(AtomicI32::new(0));
    let d2 = Arc::clone(&dropped);

    go_lib::run(move || {
        let (tx, rx) = chan::<String>(1);
        tx.send("first".to_string()); // fill the buffer

        let val = "second".to_string();
        select! {
            send(tx, val) => { panic!("buffer was full, should have taken default"); }
            default       => { d2.store(1, Ordering::Relaxed); }
        }

        assert_eq!(rx.recv().unwrap(), "first");
    });

    assert_eq!(dropped.load(Ordering::Acquire), 1);
}

// ---------------------------------------------------------------------------
// 11. Multiple WaitGroup reuse — same WaitGroup used in two rounds
// ---------------------------------------------------------------------------

#[test]
fn waitgroup_reuse() {
    const ROUNDS: i32 = 3;
    const WORKERS: i32 = 4;
    let total = Arc::new(AtomicI32::new(0));
    let total2 = Arc::clone(&total);

    go_lib::run(move || {
        let wg = Arc::new(WaitGroup::new());

        for _round in 0..ROUNDS {
            for _ in 0..WORKERS {
                wg.add(1);
                let wg2 = Arc::clone(&wg);
                let t    = Arc::clone(&total2);
                go!(move || {
                    t.fetch_add(1, Ordering::Relaxed);
                    wg2.done();
                });
            }
            wg.wait();
        }
    });

    assert_eq!(total.load(Ordering::Acquire), ROUNDS * WORKERS);
}

// ---------------------------------------------------------------------------
// 12. with_syscall — blocking operation releases P; scheduler keeps running
// ---------------------------------------------------------------------------

#[test]
fn with_syscall_unblocks_scheduler() {
    let other_ran = Arc::new(AtomicI32::new(0));
    let other2    = Arc::clone(&other_ran);

    go_lib::run(move || {
        // Spawn a goroutine that increments a counter.
        go!(move || { other2.store(1, Ordering::Release); });

        // This thread briefly "blocks" in a syscall.  The spawned goroutine
        // should be able to run while we're in here.
        go_lib::with_syscall(|| {
            std::thread::sleep(Duration::from_millis(5));
        });

        // After exiting the syscall, yield until the spawned goroutine is
        // observed.  Use a wall-clock deadline so a slow macOS CI runner
        // does not cause a panic that deadlocks run_impl.
        let deadline = Instant::now() + Duration::from_secs(5);
        while other_ran.load(Ordering::Acquire) != 1 && Instant::now() < deadline {
            go_lib::gosched();
        }
        assert_eq!(
            other_ran.load(Ordering::Acquire),
            1,
            "spawned goroutine should have run during with_syscall"
        );
    });
}

// ---------------------------------------------------------------------------
// 13. scope — parallel reduction with safe short-lived borrows
// ---------------------------------------------------------------------------

#[test]
fn scope_parallel_reduction() {
    // `data` is inside `run` so the closure is 'static; scope then lets
    // goroutines borrow halves of it without Arc or channels.
    let sum = go_lib::run(|| {
        let data: Vec<i64> = (1..=100).collect();
        scope(|s| {
            let mid = data.len() / 2;
            let h1 = s.go(|| data[..mid].iter().sum::<i64>());
            let h2 = s.go(|| data[mid..].iter().sum::<i64>());
            h1.join().unwrap() + h2.join().unwrap()
        })
    });

    assert_eq!(sum, 5050); // 1 + 2 + … + 100
}

// ---------------------------------------------------------------------------
// 14. scope panic — panicking goroutine surfaces as Err from join
// ---------------------------------------------------------------------------

#[test]
fn scope_goroutine_panic_surfaces_via_join() {
    go_lib::run(|| {
        let result = scope(|s| {
            let h = s.go(|| -> i32 { panic!("intentional scope panic") });
            h.join() // should be Err, not a process abort
        });
        assert!(result.is_err(), "expected Err from panicking goroutine");
    });
}

// ---------------------------------------------------------------------------
// 15. scope + channel — producer closes channel; consumer drains to None
// ---------------------------------------------------------------------------

#[test]
fn scope_channel_producer_consumer() {
    // A scoped producer/consumer pair: the producer closes tx after its last
    // send so the consumer's `while let Some` loop terminates.  `scope`
    // guarantees both goroutines finish before it returns — no Arc or
    // WaitGroup needed to coordinate the outer code.
    let sum = go_lib::run(|| {
        let (tx, rx) = chan::<i32>(0); // unbuffered

        scope(|s| {
            s.go(move || {
                for i in 0..10 {
                    tx.send(i);
                }
                tx.close(); // signals receiver: no more values
            });

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
    });

    assert_eq!(sum, 45); // 0 + 1 + … + 9 = 45
}

// ---------------------------------------------------------------------------
// 16. run return value — result propagates to caller
// ---------------------------------------------------------------------------
#[test]
fn run_returns_value() {
    // Scalar return: the sum computed inside the scheduler reaches the caller.
    let sum = go_lib::run(|| {
        let (tx, rx) = chan::<i32>(4);
        for i in 1..=4 {
            let t = tx.clone();
            go!(move || { t.send(i); });
        }
        (0..4).filter_map(|_| rx.recv()).sum::<i32>()
    });
    assert_eq!(sum, 10);

    // Move-capture: parameters reach the goroutine via closure capture.
    let base = 7_i32;
    let doubled = go_lib::run(move || base * 2);
    assert_eq!(doubled, 14);

    // String return: heap-allocated value crosses the goroutine boundary.
    let s: String = go_lib::run(|| "hello from goroutine".to_string());
    assert_eq!(s, "hello from goroutine");
}

// ---------------------------------------------------------------------------
// 17. create many more goroutines than OS threads
//
// Regression notes:
//   - WORKERS was originally 25,000, which creates ~50,000 mmap'd regions
//     (stack + guard page per goroutine).  macOS limits each process to
//     ~65,536 VM regions; approaching that limit caused stack_alloc() to
//     fail inside grow_stack_if_needed(), which runs on g0's stack (entered
//     through the naked mcall_asm frame).  A panic from .expect() on g0
//     tried to unwind through the naked frame (no DWARF tables) → SIGILL.
//     Reduced to 5,000 (~10,000 mmap regions) to stay well within limits
//     on all supported platforms while still exercising the scheduler under
//     meaningful goroutine pressure.
//
//   - The original loop used `1..i` and asserted `sum > 0`.  For i=0 the
//     range is empty (sum=0) and for i=1 it is also empty (sum=0), so the
//     assertion fired for the first two goroutines.  Changed to `0..=i` so
//     every goroutine computes the expected triangular number i*(i+1)/2,
//     and the assertion checks the exact value rather than just sign.
// ---------------------------------------------------------------------------
#[test]
fn many_goroutines() {
    const WORKERS: i32 = 75_000;
    go_lib::run(|| {
        go_lib::scope(|s| {
            // Use i64 throughout so the assertion arithmetic does not
            // overflow at WORKERS ≥ 46_341 (where `i * (i + 1)` first
            // exceeds `i32::MAX = 2_147_483_647`).  The final triangular
            // number `i*(i+1)/2` for `i = 49_999` is 1,249,975,000 — well
            // within i64.
            let handles: Vec<ScopedJoinHandle<i64>> = (0..WORKERS)
                .map(|i| s.go(move || {
                    // Compute the triangular number i*(i+1)/2.
                    // Range 0..=i is never empty: every goroutine does real work.
                    (0_i64..=i as i64).sum::<i64>()
                }))
                .collect();

            for (i, handle) in handles.into_iter().enumerate() {
                let sum = handle.join().expect("goroutine panicked");
                let i   = i as i64;
                assert_eq!(
                    sum,
                    i * (i + 1) / 2,
                    "goroutine {i}: expected triangular number, got {sum}"
                );
            }
        });
    });
}

// ---------------------------------------------------------------------------
// 18. select under an async-preemption storm — commit-park lost-wakeup guard
//
// Mirrors `many_goroutines`: thousands of CPU-bound goroutines so sysmon's
// SIGURG async preemption fires inside the blocking `select` park window.
//
// Each worker pair shares an unbuffered channel: a producer computes a value
// and sends it; a consumer blocks in a two-case `select!` (no default → it
// parks via `selectgo`'s commit-park path) and must receive exactly that
// value.  The second case is a channel that is never written, so the only way
// `select` can return is the real rendezvous — if a wakeup were lost (the bug
// this test guards), the consumer would park forever and `scope`'s implicit
// join would hang instead of completing.
//
// The CPU-bound loops on both sides create async-safe points so the SIGURG
// preemption lands in the unlock→park window that the commit-park protocol
// closes.  Before the conversion this hung intermittently in debug builds.
// ---------------------------------------------------------------------------
#[test]
fn select_preemption_storm() {
    const WORKERS: i64 = 5_000;
    let done  = Arc::new(AtomicI64::new(0));
    let done2 = Arc::clone(&done);

    go_lib::run(move || {
        scope(|s| {
            for i in 0..WORKERS {
                let (tx, rx)            = chan::<i64>(0);
                // A channel whose sender is never used: its recv case in the
                // select below is never ready, so it cannot satisfy the
                // select.  Keep the sender alive (no close) by moving it into
                // the consumer so the channel is never reported closed.
                let (idle_tx, idle_rx)  = chan::<i64>(0);
                let done = Arc::clone(&done2);

                // Producer: CPU-bound work, then send the triangular number.
                s.go(move || {
                    let mut acc = 0_i64;
                    for k in 0..=i { acc = acc.wrapping_add(k); }
                    tx.send(acc);
                });

                // Consumer: CPU-bound work, then block in a 2-case select.
                s.go(move || {
                    let _keep = idle_tx; // hold the idle sender; never send/close
                    let mut acc = 0_i64;
                    for k in 0..=i { acc = acc.wrapping_add(k); }
                    let got = select! {
                        recv(rx) -> v => { v.expect("rx closed unexpectedly") }
                        recv(idle_rx) -> _v => { -1_i64 }
                    };
                    assert_eq!(got, acc, "select worker {i}: wrong value (lost wakeup?)");
                    done.fetch_add(1, Ordering::Relaxed);
                });
            }
        });
    });

    assert_eq!(
        done.load(Ordering::Acquire),
        WORKERS,
        "every select consumer must complete (no lost wakeups)"
    );
}

// ---------------------------------------------------------------------------
// 19. Cond under an async-preemption storm — commit-park lost-wakeup guard
//
// Thousands of CPU-bound goroutine *pairs*, each with its own `(Mutex, Cond)`:
// a waiter parks in `Cond::wait`, and a notifier sets the predicate and fires
// exactly ONE `notify_one`.  Both sides do CPU-bound work first so SIGURG
// async preemption lands in the waiter's release-lock→park window that the
// commit-park protocol closes.
//
// Why per-pair (not one shared Cond + a re-notifying broadcaster):
//   * A single shared `std::sync::Mutex` hammered by thousands of goroutines
//     triggers an UNRELATED, pre-existing hazard — a goroutine preempted while
//     holding the pthread mutex blocks every M that then calls `lock()`,
//     starving the scheduler of an M to resume the holder (a contended-mutex
//     deadlock, not a lost wakeup).  Per-pair mutexes keep contention at two
//     goroutines, so a preempted holder only ever blocks its single partner.
//   * Firing exactly ONE `notify_one` per waiter makes this a STRICT test of
//     commit-park: a re-notifying broadcaster would paper over a dropped
//     `goready`.  Here, if the single wake is lost (waiter observed by the
//     notifier before it committed to GWAITING), that waiter parks forever and
//     `scope`'s implicit join hangs.
//
// Correctness of a single notify_one: the predicate is set under the same
// per-pair mutex the waiter checks it under, and `Cond::wait` enqueues on the
// cond's queue *before* releasing that mutex — so by the time the notifier
// acquires the mutex to set the predicate, either the waiter already parked
// (notify_one wakes it) or it has not yet read the predicate (it will see
// `true` and never park).  Commit-park guarantees the enqueued waiter is
// GWAITING before notify_one can pop it.
// ---------------------------------------------------------------------------
#[test]
fn cond_preemption_storm() {
    use go_lib::sync::Cond;
    use std::sync::Mutex;

    const WORKERS: i64 = 5_000;
    let woke  = Arc::new(AtomicI64::new(0));
    let woke2 = Arc::clone(&woke);

    go_lib::run(move || {
        scope(|s| {
            for i in 0..WORKERS {
                let mu  = Arc::new(Mutex::new(false));
                let cnd = Arc::new(Cond::new());

                // Waiter: CPU-bound work, then park until the predicate holds.
                {
                    let mu   = Arc::clone(&mu);
                    let cnd  = Arc::clone(&cnd);
                    let woke = Arc::clone(&woke2);
                    s.go(move || {
                        let mut acc = 0_i64;
                        for k in 0..=(i % 512) { acc = acc.wrapping_add(k); }
                        std::hint::black_box(acc);

                        let mut guard = mu.lock().unwrap();
                        while !*guard {
                            guard = cnd.wait(&mu, guard);
                        }
                        drop(guard);
                        woke.fetch_add(1, Ordering::Relaxed);
                    });
                }

                // Notifier: CPU-bound work, set predicate, one notify_one.
                s.go(move || {
                    let mut acc = 0_i64;
                    for k in 0..=(i % 512) { acc = acc.wrapping_add(k); }
                    std::hint::black_box(acc);

                    *mu.lock().unwrap() = true;
                    cnd.notify_one();
                });
            }
        });
    });

    assert_eq!(
        woke.load(Ordering::Acquire),
        WORKERS,
        "every cond waiter must wake (no lost wakeups)"
    );
}

// TCP networking tests live in tests/net.rs — a separate integration test
// binary with its own process and scheduler/netpoll instance, avoiding
// interference with the many_goroutines test above.
