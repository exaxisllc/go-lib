// SPDX-License-Identifier: Apache-2.0
/// Demonstrates `sync::Cond` with a bounded producer / consumer queue.
///
/// Three producers push items onto a shared queue; three consumers pop them.
/// `Cond` is used to:
///   - signal consumers when items are available (`not_empty.notify_one()`).
///   - signal producers when space opens up (`not_full.notify_one()`).
///
/// Run with:
///   cargo run --example cond
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use go_lib::sync::{Cond, WaitGroup};

const PRODUCERS: usize = 3;
const CONSUMERS: usize = 3;
const ITEMS_PER_PRODUCER: usize = 5;
const CAPACITY: usize = 4;

struct BoundedQueue<T> {
    buf:      Mutex<VecDeque<T>>,
    cap:      usize,
    not_full:  Cond,
    not_empty: Cond,
}

impl<T: Send + 'static> BoundedQueue<T> {
    fn new(cap: usize) -> Self {
        Self {
            buf:       Mutex::new(VecDeque::new()),
            cap,
            not_full:  Cond::new(),
            not_empty: Cond::new(),
        }
    }

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

#[go_lib::main]
fn main() {
    let queue: Arc<BoundedQueue<String>> =
        Arc::new(BoundedQueue::new(CAPACITY));
    let total = PRODUCERS * ITEMS_PER_PRODUCER;

    let wg = Arc::new(WaitGroup::new());

    // Producers
    for p in 0..PRODUCERS {
        let q   = Arc::clone(&queue);
        let wg2 = Arc::clone(&wg);
        wg.add(1);
        go_lib::go!(move || {
            for i in 0..ITEMS_PER_PRODUCER {
                go_lib::sleep(Duration::from_millis(2));
                let msg = format!("p{p}:item{i}");
                println!("  produce {msg}");
                q.push(msg);
            }
            wg2.done();
        });
    }

    // Consumers
    let received = Arc::new(Mutex::new(Vec::<String>::new()));
    let cwg = Arc::new(WaitGroup::new());
    for _ in 0..CONSUMERS {
        let q         = Arc::clone(&queue);
        let received2 = Arc::clone(&received);
        let cwg2      = Arc::clone(&cwg);
        cwg.add(1);
        go_lib::go!(move || {
            // Each consumer takes `total / CONSUMERS` items.
            for _ in 0..total / CONSUMERS {
                let item = q.pop();
                println!("consume {item}");
                received2.lock().unwrap().push(item);
            }
            cwg2.done();
        });
    }

    wg.wait();   // all producers done
    cwg.wait();  // all consumers done

    let r = received.lock().unwrap();
    println!("\nTotal items consumed: {}", r.len());
    assert_eq!(r.len(), total, "all items must be consumed exactly once");
    println!("OK — all {total} items passed through the bounded queue.");
}
