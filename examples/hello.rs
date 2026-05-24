//! hello — goroutines and channels in ten lines.
//!
//! Demonstrates the basic pattern: spawn goroutines with `go!`, communicate
//! through unbuffered channels, and receive in the main goroutine.
//!
//! Run with:
//!   cargo run --example hello

use go_lib::{chan::chan, go};

fn main() {
    const N: usize = 5;

    go_lib::run(|| {
        let (tx, rx) = chan::<String>(0); // unbuffered (synchronous)

        // Spawn N goroutines, each sending a greeting.
        for i in 0..N {
            let tx = tx.clone();
            go!(move || {
                tx.send(format!("hello from goroutine {i}"));
            });
        }
        drop(tx); // drop the original; goroutine clones are still live

        // Receive exactly N greetings (order is non-deterministic).
        for _ in 0..N {
            if let Some(msg) = rx.recv() {
                println!("{msg}");
            }
        }
        println!("all goroutines finished");
    });
}
