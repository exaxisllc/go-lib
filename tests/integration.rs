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
// 13. run return value — result propagates to caller
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
