// SPDX-License-Identifier: Apache-2.0
//! scope — parallel slice reduction with safe short-lived borrows.
//!
//! `go_lib::scope` works exactly like `std::thread::scope`: goroutines spawned
//! inside the closure can borrow data from the enclosing *goroutine's* stack
//! frame because the scheduler guarantees every spawned goroutine finishes
//! before `scope` returns.  No `Arc`, no channels, no `.clone()` required for
//! shared read-only data.
//!
//! The `#[go_lib::main]` body runs as the first goroutine; data that needs to
//! be shared is defined there.  `scope` then lets helper goroutines borrow
//! slices of it — the lifetime is enforced at compile time, not at runtime.
//!
//! ```sh
//! cargo run --example scope
//! ```

#[go_lib::main]
fn main() {
    // `data` lives on the goroutine's stack.  Spawned goroutines borrow
    // chunks of it — no Arc or Clone needed.  The scheduler guarantees all
    // goroutines complete before `scope` returns, so the borrow is safe.
    let data: Vec<i64> = (1..=100).collect(); // 1 + 2 + … + 100 = 5 050

    let sum = go_lib::scope(|s| {
        let chunks: Vec<&[i64]> = data.chunks(data.len() / 4 + 1).collect();

        let handles: Vec<_> = chunks
            .into_iter()
            .map(|chunk| s.go(move || chunk.iter().sum::<i64>()))
            .collect();

        handles
            .into_iter()
            .map(|h| h.join().expect("chunk goroutine panicked"))
            .sum::<i64>()
    });

    println!("sum 1..=100 = {sum}");
    assert_eq!(sum, 5050);
}
