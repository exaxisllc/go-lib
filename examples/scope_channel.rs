// SPDX-License-Identifier: Apache-2.0
//! scope\_channel — producer/consumer inside a scoped goroutine pair.
//!
//! `go_lib::scope` lets you pair goroutines that communicate over a channel
//! while still getting the scope's lifetime guarantee: both goroutines finish
//! before `scope` returns, so no `Arc` or `WaitGroup` is needed to coordinate
//! the outer code.
//!
//! The producer closes the channel after its last send so the consumer's
//! `while let Some(v) = rx.recv()` loop terminates cleanly — the same
//! semantics as `close(ch)` in Go.
//!
//! ```sh
//! cargo run --example scope_channel
//! ```

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
    assert_eq!(sum, 45); // 0+1+…+9 = 45
}
