//! select_fanin — fan-in multiplexing with `select!`.
//!
//! Two producers send values at different rates.  A single consumer uses
//! `select!` to receive from whichever producer is ready first, merging
//! both streams into one.  This is the "fan-in" pattern from Go's concurrency
//! tutorial.
//!
//! Run with:
//!   cargo run --example select_fanin

use std::time::Duration;
use go_lib::{chan::chan, go, select};

fn main() {
    go_lib::run(|| {
        // Two buffered channels — producers fill them at their own pace.
        let (fast_tx, fast_rx) = chan::<i32>(8);
        let (slow_tx, slow_rx) = chan::<i32>(4);

        // Fast producer: 6 values, 5 ms apart.
        go!(move || {
            for i in 0..6_i32 {
                go_lib::sleep(Duration::from_millis(5));
                fast_tx.send(i);
            }
        });

        // Slow producer: 3 values, 15 ms apart.
        go!(move || {
            for i in 10..13_i32 {
                go_lib::sleep(Duration::from_millis(15));
                slow_tx.send(i);
            }
        });

        // Consumer: receive exactly 9 values from whichever channel is ready.
        let mut received = Vec::new();
        for _ in 0..9 {
            select! {
                recv(fast_rx) -> v => {
                    if let Some(n) = v { received.push(('F', n)); }
                }
                recv(slow_rx) -> v => {
                    if let Some(n) = v { received.push(('S', n)); }
                }
            }
        }

        println!("received {} values in arrival order:", received.len());
        for (src, val) in &received {
            println!("  {src}: {val}");
        }

        let fast_count = received.iter().filter(|(s, _)| *s == 'F').count();
        let slow_count = received.iter().filter(|(s, _)| *s == 'S').count();
        println!("fast={fast_count} slow={slow_count}");
        assert_eq!(fast_count, 6);
        assert_eq!(slow_count, 3);
    });
}
