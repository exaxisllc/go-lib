// SPDX-License-Identifier: Apache-2.0
//! pipeline — a three-stage concurrent pipeline.
//!
//! ```
//! generate(1..=8) → square → print
//! ```
//!
//! Each stage is a goroutine.  Values flow through unbuffered channels so
//! back-pressure is automatic: the generator can only produce as fast as the
//! squarer consumes, and so on.
//!
//! Run with:
//!   cargo run --example pipeline

use go_lib::{chan::chan, go};

#[go_lib::main]
fn main() {
    // Stage 1 — emit integers 1..=8 then close.
    let (gen_tx, gen_rx) = chan::<u64>(0);
    go!(move || {
        for n in 1..=8_u64 {
            gen_tx.send(n);
        }
        gen_tx.close();
    });

    // Stage 2 — square each value.
    let (sq_tx, sq_rx) = chan::<u64>(0);
    go!(move || {
        loop {
            match gen_rx.recv() {
                Some(n) => sq_tx.send(n * n),
                None    => { sq_tx.close(); break; }
            }
        }
    });

    // Stage 3 (main goroutine) — print and accumulate.
    let mut sum = 0_u64;
    while let Some(sq) = sq_rx.recv() {
        println!("{sq}");
        sum += sq;
    }
    println!("sum of squares 1..=8: {sum}");
    // 1+4+9+16+25+36+49+64 = 204
    assert_eq!(sum, 204);
}
