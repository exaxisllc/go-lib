// SPDX-License-Identifier: Apache-2.0
//! Network integration tests — exercises the `std::io` trait implementations
//! and new helpers added to `go_lib::net::TcpStream` and `TcpListener`.
//!
//! ## Why a separate test file
//!
//! go-lib's netpoll backend (kqueue on macOS, epoll on Linux) is a
//! process-global part of the singleton scheduler.  Since the singleton-Rt
//! refactor, concurrent scheduler entries share one scheduler and tag netpoll
//! registrations per invocation, so cross-run pointer collisions are no longer
//! possible — but keeping the networking tests in their own binary (and thus
//! their own OS process) still isolates them from the resource pressure of
//! `tests/integration.rs` (whose `many_goroutines` test spawns 75,000
//! goroutines) and keeps port usage independent.
//!
//! Each test carries `#[go_lib::main]`, so its body runs as the first
//! goroutine on the process-wide scheduler; the tests still run concurrently
//! (one thread per CPU), each driving the netpoll from inside goroutine
//! context.

use std::io::{BufRead, BufReader, Read, Write};
use std::sync::{Arc, Mutex};

use go_lib::{
    chan::chan,
    go,
    net::{TcpListener, TcpStream},
    sync::WaitGroup,
};


// ---------------------------------------------------------------------------
// 1. TcpListener::local_addr — bind to port 0, confirm OS assigned a port
// ---------------------------------------------------------------------------
#[test]
#[go_lib::main]
fn net_listener_local_addr() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    assert_eq!(addr.ip().to_string(), "127.0.0.1");
    assert_ne!(addr.port(), 0, "OS must assign a non-zero port");
}

// ---------------------------------------------------------------------------
// 2. impl Read / Write for &mut TcpStream — echo one message
//
// Exercises: impl Read for TcpStream (&mut path), impl Write for TcpStream
// ---------------------------------------------------------------------------
#[test]
#[go_lib::main]
fn net_read_write_mut_ref() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr     = listener.local_addr().unwrap();

    let (done_tx, done_rx) = chan::<()>(1);
    go!(move || {
        let mut conn = listener.accept().unwrap();
        let mut buf  = [0u8; 64];
        // Drive impl Read via &mut TcpStream.
        let n = conn.read(&mut buf).unwrap();
        // Drive impl Write via &mut TcpStream.
        conn.write_all(&buf[..n]).unwrap();
        done_tx.send(());
    });

    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(b"hello").unwrap();

    let mut resp = [0u8; 5];
    client.read_exact(&mut resp).unwrap();
    assert_eq!(&resp, b"hello");

    done_rx.recv();
}

// ---------------------------------------------------------------------------
// 3. impl Read / Write for &TcpStream — shared-reference path
//
// Exercises: impl Read for &TcpStream, impl Write for &TcpStream
// ---------------------------------------------------------------------------
#[test]
#[go_lib::main]
fn net_read_write_shared_ref() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr     = listener.local_addr().unwrap();

    let (done_tx, done_rx) = chan::<()>(1);
    go!(move || {
        let conn     = listener.accept().unwrap();
        let mut buf  = [0u8; 64];
        // Drive impl Read via &TcpStream.
        let n = (&conn).read(&mut buf).unwrap();
        // Drive impl Write via &TcpStream.
        (&conn).write_all(&buf[..n]).unwrap();
        done_tx.send(());
    });

    let client = TcpStream::connect(addr).unwrap();
    (&client).write_all(b"shared").unwrap();

    let mut resp = [0u8; 6];
    (&client).read_exact(&mut resp).unwrap();
    assert_eq!(&resp, b"shared");

    done_rx.recv();
}

// ---------------------------------------------------------------------------
// 4. TcpStream::try_clone — split into read / write halves in one goroutine
//
// Exercises: try_clone, Read on one half, Write on other half
// ---------------------------------------------------------------------------
#[test]
#[go_lib::main]
fn net_try_clone_split_halves() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr     = listener.local_addr().unwrap();

    let (done_tx, done_rx) = chan::<()>(1);
    go!(move || {
        let stream     = listener.accept().unwrap();
        let mut writer = stream.try_clone().expect("try_clone failed");

        // stream = read half (via &TcpStream), writer = write half (&mut).
        let mut buf = [0u8; 64];
        let n = (&stream).read(&mut buf).unwrap();
        writer.write_all(&buf[..n]).unwrap();
        done_tx.send(());
    });

    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(b"cloned").unwrap();

    let mut resp = [0u8; 6];
    client.read_exact(&mut resp).unwrap();
    assert_eq!(&resp, b"cloned");

    done_rx.recv();
}

// ---------------------------------------------------------------------------
// 5. TcpStream::try_clone — read and write halves in separate goroutines
//
// Exercises: try_clone, concurrent goroutine access to dup'd fds
// ---------------------------------------------------------------------------
#[test]
#[go_lib::main]
fn net_try_clone_separate_goroutines() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr     = listener.local_addr().unwrap();

    let (done_tx, done_rx) = chan::<()>(1);
    go!(move || {
        let stream = listener.accept().unwrap();
        let writer = stream.try_clone().expect("try_clone failed");

        let (relay_tx, relay_rx) = chan::<Vec<u8>>(1);

        // Reader goroutine — owns the original stream (read half).
        go!(move || {
            let mut buf = [0u8; 64];
            let n = (&stream).read(&mut buf).unwrap();
            relay_tx.send(buf[..n].to_vec());
        });

        // Writer goroutine — owns the cloned stream (write half).
        go!(move || {
            let data = relay_rx.recv().unwrap();
            (&writer).write_all(&data).unwrap();
            done_tx.send(());
        });
    });

    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(b"split").unwrap();

    let mut resp = [0u8; 5];
    client.read_exact(&mut resp).unwrap();
    assert_eq!(&resp, b"split");

    done_rx.recv();
}

// ---------------------------------------------------------------------------
// 6. TcpStream::peer_addr / local_addr + TcpStream::local_addr
//
// Exercises: peer_addr, local_addr on TcpStream; already-covered local_addr
// on TcpListener via test 1.
// ---------------------------------------------------------------------------
#[test]
#[go_lib::main]
fn net_peer_and_local_addr() {
    let listener    = TcpListener::bind("127.0.0.1:0").unwrap();
    let listen_addr = listener.local_addr().unwrap();

    let (addr_tx, addr_rx) = chan::<std::net::SocketAddr>(1);
    go!(move || {
        let conn = listener.accept().unwrap();

        // Server-side local_addr must match the listener port.
        let local = conn.local_addr().expect("local_addr failed");
        assert_eq!(local.port(), listen_addr.port());

        // peer_addr is the client's ephemeral port — must be non-zero.
        let peer = conn.peer_addr().expect("peer_addr failed");
        assert_ne!(peer.port(), 0);

        addr_tx.send(peer);
    });

    let client       = TcpStream::connect(listen_addr).unwrap();
    let client_local = client.local_addr().expect("client local_addr failed");
    let reported     = addr_rx.recv().unwrap();

    // Server's view of the peer address == client's local address.
    assert_eq!(reported.port(), client_local.port());
}

// ---------------------------------------------------------------------------
// 7. BufReader<TcpStream> — verify impl Read works with std I/O adapters
//
// Exercises: BufReader wrapping TcpStream, read_line via impl Read
// ---------------------------------------------------------------------------
#[test]
#[go_lib::main]
fn net_bufreader_adapter() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr     = listener.local_addr().unwrap();

    let (done_tx, done_rx) = chan::<()>(1);
    go!(move || {
        let conn   = listener.accept().unwrap();
        let mut br = BufReader::new(conn);

        // BufReader calls impl Read internally in its line-buffering logic.
        let mut line = String::new();
        br.read_line(&mut line).unwrap();
        assert_eq!(line.trim_end(), "ping");

        // Access the underlying TcpStream to write back.
        br.get_mut().write_all(b"pong\n").unwrap();
        done_tx.send(());
    });

    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(b"ping\n").unwrap();

    let mut resp = String::new();
    BufReader::new(client).read_line(&mut resp).unwrap();
    assert_eq!(resp.trim_end(), "pong");

    done_rx.recv();
}

// ---------------------------------------------------------------------------
// 8. Multiple concurrent connections — N clients connect simultaneously
//
// Exercises: concurrent goroutine-per-connection pattern, read_exact and
// write_all on &TcpStream under real scheduling pressure.
// ---------------------------------------------------------------------------
#[test]
#[go_lib::main]
fn net_concurrent_connections() {
    const N: usize = 8;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr     = listener.local_addr().unwrap();

    let server_wg = Arc::new(WaitGroup::new());

    // Server: accept N connections, echo each in its own goroutine.
    let wg2 = Arc::clone(&server_wg);
    go!(move || {
        for _ in 0..N {
            let conn = listener.accept().unwrap();
            let wg3  = Arc::clone(&wg2);
            wg3.add(1);
            go!(move || {
                let mut buf = [0u8; 4];
                (&conn).read_exact(&mut buf).unwrap();
                (&conn).write_all(&buf).unwrap();
                wg3.done();
            });
        }
    });

    // Clients: N goroutines each open a connection and verify the echo.
    let results   = Arc::new(Mutex::new(Vec::<bool>::new()));
    let client_wg = Arc::new(WaitGroup::new());

    for i in 0..N {
        client_wg.add(1);
        let results2   = Arc::clone(&results);
        let client_wg2 = Arc::clone(&client_wg);
        go!(move || {
            let mut conn = TcpStream::connect(addr).unwrap();
            let tag      = [i as u8; 4];
            conn.write_all(&tag).unwrap();
            let mut resp = [0u8; 4];
            conn.read_exact(&mut resp).unwrap();
            results2.lock().unwrap().push(resp == tag);
            client_wg2.done();
        });
    }

    client_wg.wait();
    server_wg.wait();

    let ok = results.lock().unwrap();
    assert_eq!(ok.len(), N, "wrong number of results");
    assert!(ok.iter().all(|&b| b), "some echo checks failed");
}

// ---------------------------------------------------------------------------
// 9. write_all / read_exact — large payload (128 KiB) spanning many chunks
//
// Exercises: multi-call write_all and read_exact via impl Write / impl Read
// on &mut TcpStream for payloads that don't fit in a single kernel buffer.
// ---------------------------------------------------------------------------
#[test]
#[go_lib::main]
fn net_large_payload() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr     = listener.local_addr().unwrap();

    const SIZE: usize = 128 * 1024;
    let payload: Vec<u8> = (0..SIZE).map(|i| (i % 251) as u8).collect();
    let payload = Arc::new(payload);

    let (done_tx, done_rx) = chan::<()>(1);
    let payload2 = Arc::clone(&payload);
    go!(move || {
        let mut conn = listener.accept().unwrap();
        let mut buf  = vec![0u8; SIZE];
        // read_exact drives impl Read in a loop until the buffer is full.
        conn.read_exact(&mut buf).unwrap();
        // write_all drives impl Write across as many write() calls as needed.
        conn.write_all(&buf).unwrap();
        done_tx.send(());
    });

    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(&payload).unwrap();

    let mut received = vec![0u8; SIZE];
    client.read_exact(&mut received).unwrap();
    assert_eq!(received, *payload2, "large payload echo mismatch");

    done_rx.recv();
}
