// SPDX-License-Identifier: Apache-2.0
//! Goroutine-aware TCP networking — Windows IOCP backend.
//!
//! `TcpListener` and `TcpStream` integrate with the go-lib scheduler via
//! Windows I/O Completion Ports (IOCP).
//!
//! ## I/O model
//!
//! - **`read` / `write`**: issue an overlapped `WSARecv` / `WSASend`, then
//!   call `gopark(IOWait)`.  The background `sysmon` / `findrunnable` loop
//!   drains the IOCP with `GetQueuedCompletionStatusEx` and calls `goready`
//!   when the operation completes.  The goroutine resumes and reads the result
//!   from the heap-allocated [`IocpOp`] it passed to the operation.
//!
//! - **`accept` / `connect`**: use blocking Winsock calls wrapped in
//!   [`with_syscall`][crate::with_syscall] so the scheduler can hand the P
//!   off to another M while the OS thread waits.  After the connection is
//!   established, the socket is associated with the IOCP for subsequent
//!   overlapped reads/writes.
//!
//! ## Usage
//!
//! ```no_run
//! go_lib::run(|| {
//!     let listener = go_lib::net::TcpListener::bind("127.0.0.1:8080").unwrap();
//!     loop {
//!         let mut stream = listener.accept().unwrap();
//!         go_lib::go!(move || {
//!             let mut buf = [0u8; 1024];
//!             let n = stream.read(&mut buf).unwrap();
//!             stream.write(&buf[..n]).unwrap();
//!         });
//!     }
//! });
//! ```

use std::ffi::c_void;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, ToSocketAddrs};

use crate::runtime::g::WaitReason;
use crate::runtime::netpoll::{netpoll_iocp_associate, IocpOp, WinOverlapped};
use crate::runtime::park::gopark;

// ---------------------------------------------------------------------------
// Windows socket type and constants
// ---------------------------------------------------------------------------

/// Windows `SOCKET` (pointer-sized unsigned integer).
type Socket = usize;

const INVALID_SOCKET: Socket = usize::MAX;
const SOCKET_ERROR: i32 = -1;
/// Overlapped operation successfully posted; completion will arrive via IOCP.
const WSA_IO_PENDING: i32 = 997;

const WSA_FLAG_OVERLAPPED: u32 = 0x01;

const AF_INET: i32 = 2;
const AF_INET6: i32 = 23;
const SOCK_STREAM: i32 = 1;
const IPPROTO_TCP: i32 = 6;

const SOL_SOCKET: i32 = 0xffff;
const SO_REUSEADDR: i32 = 0x0004;

// ---------------------------------------------------------------------------
// Socket address structures
// ---------------------------------------------------------------------------

#[repr(C)]
struct SockAddrIn {
    sin_family: u16,
    sin_port: u16,
    sin_addr: u32,
    sin_zero: [u8; 8],
}

#[repr(C)]
struct SockAddrIn6 {
    sin6_family: u16,
    sin6_port: u16,
    sin6_flowinfo: u32,
    sin6_addr: [u8; 16],
    sin6_scope_id: u32,
}

/// Generic, maximally-aligned socket-address storage (128 bytes).
#[repr(C, align(8))]
struct SockAddrStorage {
    _data: [u8; 128],
}

impl SockAddrStorage {
    fn zeroed() -> Self {
        SockAddrStorage { _data: [0u8; 128] }
    }
}

// ---------------------------------------------------------------------------
// WSABUF — scatter/gather buffer descriptor
// ---------------------------------------------------------------------------

/// Windows `WSABUF`: a (length, pointer) pair describing one I/O buffer.
#[repr(C)]
struct WsaBuf {
    len: u32,
    buf: *mut u8,
}

// SAFETY: used only within a single goroutine that is parked for the duration
// of the overlapped operation; the goroutine's stack (where the pointed-to
// buffer lives) remains allocated and pinned while the goroutine is parked.
unsafe impl Send for WsaBuf {}

// ---------------------------------------------------------------------------
// Winsock2 FFI
// ---------------------------------------------------------------------------

#[link(name = "ws2_32")]
unsafe extern "system" {
    fn WSASocketW(
        af: i32,
        ty: i32,
        protocol: i32,
        lp_protocol_info: *mut c_void,
        g: u32,
        dw_flags: u32,
    ) -> Socket;
    fn bind(s: Socket, name: *const c_void, namelen: i32) -> i32;
    fn listen(s: Socket, backlog: i32) -> i32;
    fn accept(s: Socket, addr: *mut c_void, addrlen: *mut i32) -> Socket;
    fn connect(s: Socket, name: *const c_void, namelen: i32) -> i32;
    fn closesocket(s: Socket) -> i32;
    fn setsockopt(
        s: Socket,
        level: i32,
        optname: i32,
        optval: *const c_void,
        optlen: i32,
    ) -> i32;
    fn getsockname(s: Socket, name: *mut c_void, namelen: *mut i32) -> i32;
    fn getpeername(s: Socket, name: *mut c_void, namelen: *mut i32) -> i32;
    fn WSAGetLastError() -> i32;
    fn WSARecv(
        s: Socket,
        lp_buffers: *mut WsaBuf,
        dw_buffer_count: u32,
        lp_number_of_bytes_recvd: *mut u32,
        lp_flags: *mut u32,
        lp_overlapped: *mut WinOverlapped,
        lp_completion_routine: *mut c_void,
    ) -> i32;
    fn WSASend(
        s: Socket,
        lp_buffers: *mut WsaBuf,
        dw_buffer_count: u32,
        lp_number_of_bytes_sent: *mut u32,
        dw_flags: u32,
        lp_overlapped: *mut WinOverlapped,
        lp_completion_routine: *mut c_void,
    ) -> i32;
}

#[link(name = "ntdll")]
unsafe extern "system" {
    /// Convert an NTSTATUS code to the corresponding Win32 error code.
    fn RtlNtStatusToDosError(status: u32) -> u32;
}

#[link(name = "kernel32")]
unsafe extern "system" {
    fn GetCurrentProcess() -> *mut c_void;
    fn DuplicateHandle(
        h_source_process: *mut c_void,
        h_source:         *mut c_void,
        h_target_process: *mut c_void,
        lp_target_handle: *mut *mut c_void,
        dw_desired_access: u32,
        b_inherit_handle:  i32,
        dw_options:        u32,
    ) -> i32;
}

const DUPLICATE_SAME_ACCESS: u32 = 0x0002;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn addr_family(addr: SocketAddr) -> i32 {
    match addr {
        SocketAddr::V4(_) => AF_INET,
        SocketAddr::V6(_) => AF_INET6,
    }
}

/// Convert a `SocketAddr` to a `SockAddrStorage` + length pair.
fn to_sockaddr(addr: SocketAddr) -> (SockAddrStorage, i32) {
    let mut storage = SockAddrStorage::zeroed();
    match addr {
        SocketAddr::V4(v4) => {
            let sa = unsafe { &mut *(storage._data.as_mut_ptr() as *mut SockAddrIn) };
            sa.sin_family = AF_INET as u16;
            sa.sin_port = v4.port().to_be();
            sa.sin_addr = u32::from_ne_bytes(v4.ip().octets());
            (storage, std::mem::size_of::<SockAddrIn>() as i32)
        }
        SocketAddr::V6(v6) => {
            let sa = unsafe { &mut *(storage._data.as_mut_ptr() as *mut SockAddrIn6) };
            sa.sin6_family = AF_INET6 as u16;
            sa.sin6_port = v6.port().to_be();
            sa.sin6_addr = v6.ip().octets();
            (storage, std::mem::size_of::<SockAddrIn6>() as i32)
        }
    }
}

/// Decode a `SockAddrStorage` returned by `getsockname`/`getpeername` into a
/// Rust `SocketAddr`.
fn sockaddr_to_socketaddr(storage: &SockAddrStorage, len: i32) -> io::Result<SocketAddr> {
    let family = u16::from_ne_bytes([storage._data[0], storage._data[1]]);
    if family == AF_INET as u16 {
        let sa = unsafe { &*(storage._data.as_ptr() as *const SockAddrIn) };
        let ip   = std::net::Ipv4Addr::from(sa.sin_addr.to_ne_bytes());
        let port = u16::from_be(sa.sin_port);
        Ok(SocketAddr::new(std::net::IpAddr::V4(ip), port))
    } else if family == AF_INET6 as u16 {
        let sa = unsafe { &*(storage._data.as_ptr() as *const SockAddrIn6) };
        let ip   = std::net::Ipv6Addr::from(sa.sin6_addr);
        let port = u16::from_be(sa.sin6_port);
        Ok(SocketAddr::new(std::net::IpAddr::V6(ip), port))
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("unsupported address family: {family}"),
        ))
    }
}

/// Create a Winsock overlapped TCP socket for the given address family.
fn create_overlapped_socket(af: i32) -> io::Result<Socket> {
    let s = unsafe {
        WSASocketW(
            af,
            SOCK_STREAM,
            IPPROTO_TCP,
            std::ptr::null_mut(),
            0,
            WSA_FLAG_OVERLAPPED,
        )
    };
    if s == INVALID_SOCKET {
        return Err(wsa_last_error());
    }
    Ok(s)
}

/// Return an `io::Error` for the most recent Winsock error on this thread.
#[inline]
fn wsa_last_error() -> io::Error {
    io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
}

/// Convert an NTSTATUS completion code to an `io::Error`.
fn ntstatus_to_error(ntstatus: u32) -> io::Error {
    let win32 = unsafe { RtlNtStatusToDosError(ntstatus) };
    io::Error::from_raw_os_error(win32 as i32)
}

// ---------------------------------------------------------------------------
// Overlapped I/O helpers
// ---------------------------------------------------------------------------

/// Issue an overlapped `WSARecv` on `s`, park the goroutine, and return the
/// number of bytes received after the completion fires.
///
/// # Safety
/// Must be called from a live goroutine context.
unsafe fn overlapped_recv(s: Socket, buf: &mut [u8]) -> io::Result<usize> {
    let gp = crate::runtime::g::current_g();
    debug_assert!(!gp.is_null(), "overlapped_recv: not running on a goroutine");

    // Allocate the per-operation state on the heap so it outlives the park.
    let mut op: Box<IocpOp> = Box::new(unsafe { std::mem::zeroed() });
    op.gp = gp;
    let op_ptr = Box::into_raw(op);

    let mut wsa_buf = WsaBuf {
        len: buf.len() as u32,
        buf: buf.as_mut_ptr(),
    };
    let mut flags: u32 = 0;
    let mut recvd: u32 = 0;

    let rc = unsafe {
        WSARecv(
            s,
            &mut wsa_buf,
            1,
            &mut recvd,
            &mut flags,
            // overlapped is at offset 0 in IocpOp (repr(C)); the cast is valid.
            &mut (*op_ptr).overlapped,
            std::ptr::null_mut(),
        )
    };

    let wsa_err = if rc == SOCKET_ERROR {
        unsafe { WSAGetLastError() }
    } else {
        0
    };

    if rc == SOCKET_ERROR && wsa_err != WSA_IO_PENDING {
        // Operation was not started — free op and return the error immediately.
        drop(unsafe { Box::from_raw(op_ptr) });
        return Err(io::Error::from_raw_os_error(wsa_err));
    }

    // Operation successfully initiated.  Both the immediate-success (rc == 0)
    // and WSA_IO_PENDING paths queue an IOCP completion, so we always park.
    gopark(WaitReason::IOWait);

    // Goroutine resumed — netpoll_wait has filled bytes_transferred and ntstatus.
    let op = unsafe { Box::from_raw(op_ptr) };
    if op.ntstatus != 0 {
        return Err(ntstatus_to_error(op.ntstatus));
    }
    Ok(op.bytes_transferred as usize)
}

/// Issue an overlapped `WSASend` on `s`, park the goroutine, and return the
/// number of bytes sent after the completion fires.
///
/// # Safety
/// Must be called from a live goroutine context.
unsafe fn overlapped_send(s: Socket, buf: &[u8]) -> io::Result<usize> {
    let gp = crate::runtime::g::current_g();
    debug_assert!(!gp.is_null(), "overlapped_send: not running on a goroutine");

    let mut op: Box<IocpOp> = Box::new(unsafe { std::mem::zeroed() });
    op.gp = gp;
    let op_ptr = Box::into_raw(op);

    let mut wsa_buf = WsaBuf {
        len: buf.len() as u32,
        buf: buf.as_ptr() as *mut u8,
    };
    let mut sent: u32 = 0;

    let rc = unsafe {
        WSASend(
            s,
            &mut wsa_buf,
            1,
            &mut sent,
            0, // dwFlags
            &mut (*op_ptr).overlapped,
            std::ptr::null_mut(),
        )
    };

    let wsa_err = if rc == SOCKET_ERROR {
        unsafe { WSAGetLastError() }
    } else {
        0
    };

    if rc == SOCKET_ERROR && wsa_err != WSA_IO_PENDING {
        drop(unsafe { Box::from_raw(op_ptr) });
        return Err(io::Error::from_raw_os_error(wsa_err));
    }

    gopark(WaitReason::IOWait);

    let op = unsafe { Box::from_raw(op_ptr) };
    if op.ntstatus != 0 {
        return Err(ntstatus_to_error(op.ntstatus));
    }
    Ok(op.bytes_transferred as usize)
}

// ---------------------------------------------------------------------------
// TcpListener
// ---------------------------------------------------------------------------

/// A goroutine-aware TCP server socket (Windows IOCP backend).
///
/// Calls to [`accept`][TcpListener::accept] hand the P off to another M
/// while the OS thread blocks in the Winsock `accept` call.
pub struct TcpListener {
    socket: Socket,
}

impl TcpListener {
    /// Bind a TCP listener to `addr`.
    ///
    /// Equivalent to `net.Listen("tcp", addr)` in Go.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no address given"))?;

        let s = create_overlapped_socket(addr_family(addr))?;

        // SO_REUSEADDR — allow fast listener restart.
        let one: i32 = 1;
        unsafe {
            setsockopt(
                s,
                SOL_SOCKET,
                SO_REUSEADDR,
                &one as *const i32 as *const c_void,
                std::mem::size_of::<i32>() as i32,
            )
        };

        let (sa, sa_len) = to_sockaddr(addr);
        if unsafe { bind(s, sa._data.as_ptr() as *const c_void, sa_len) } == SOCKET_ERROR {
            unsafe { closesocket(s) };
            return Err(wsa_last_error());
        }
        if unsafe { listen(s, 128) } == SOCKET_ERROR {
            unsafe { closesocket(s) };
            return Err(wsa_last_error());
        }

        Ok(TcpListener { socket: s })
    }

    /// Accept the next incoming connection.
    ///
    /// Wraps the blocking Winsock `accept` in
    /// [`with_syscall`][crate::with_syscall] so the scheduler can hand the P
    /// off to another M while the OS thread waits.  The accepted socket is
    /// then associated with the global IOCP for overlapped reads/writes.
    pub fn accept(&self) -> io::Result<TcpStream> {
        let listener = self.socket;
        let accepted = crate::with_syscall(|| {
            let s = unsafe { accept(listener, std::ptr::null_mut(), std::ptr::null_mut()) };
            if s == INVALID_SOCKET {
                Err(wsa_last_error())
            } else {
                Ok(s)
            }
        })?;

        if !netpoll_iocp_associate(accepted) {
            unsafe { closesocket(accepted) };
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "failed to associate accepted socket with IOCP",
            ));
        }

        Ok(TcpStream { socket: accepted })
    }

    /// Return the local address this listener is bound to.
    ///
    /// Useful for obtaining the OS-assigned port when the listener was bound
    /// with port 0.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let mut storage = SockAddrStorage::zeroed();
        let mut len = std::mem::size_of::<SockAddrStorage>() as i32;
        if unsafe {
            getsockname(self.socket, storage._data.as_mut_ptr() as *mut c_void, &mut len)
        } == SOCKET_ERROR
        {
            return Err(wsa_last_error());
        }
        sockaddr_to_socketaddr(&storage, len)
    }

    /// Return the underlying Windows `SOCKET` handle.
    pub fn as_raw_socket(&self) -> usize {
        self.socket
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        unsafe { closesocket(self.socket) };
    }
}

// ---------------------------------------------------------------------------
// TcpStream
// ---------------------------------------------------------------------------

/// A goroutine-aware TCP stream socket (Windows IOCP backend).
///
/// Blocking reads and writes issue overlapped `WSARecv`/`WSASend` operations
/// and park the goroutine until the IOCP completion fires.
///
/// `TcpStream` implements [`std::io::Read`] and [`std::io::Write`] (for both
/// `&mut TcpStream` and `&TcpStream`), so it works with any Rust I/O adapter
/// without unsafe wrapper code.  Use [`try_clone`][TcpStream::try_clone] to
/// split a connection into independent read and write halves.
pub struct TcpStream {
    socket: Socket,
}

impl TcpStream {
    /// Connect to `addr`.
    ///
    /// The blocking Winsock `connect` is wrapped in
    /// [`with_syscall`][crate::with_syscall].  After the connection is
    /// established the socket is associated with the IOCP for subsequent
    /// overlapped reads/writes.
    ///
    /// Equivalent to `net.Dial("tcp", addr)` in Go.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no address given"))?;

        let s = create_overlapped_socket(addr_family(addr))?;
        let (sa, sa_len) = to_sockaddr(addr);

        crate::with_syscall(|| {
            if unsafe { connect(s, sa._data.as_ptr() as *const c_void, sa_len) } == SOCKET_ERROR
            {
                Err(wsa_last_error())
            } else {
                Ok(())
            }
        })?;

        if !netpoll_iocp_associate(s) {
            unsafe { closesocket(s) };
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "failed to associate connected socket with IOCP",
            ));
        }

        Ok(TcpStream { socket: s })
    }

    /// Read bytes from the stream into `buf`.
    ///
    /// Issues an overlapped `WSARecv` and parks the goroutine until the
    /// operation completes via IOCP.
    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // SAFETY: called from a goroutine context.
        unsafe { overlapped_recv(self.socket, buf) }
    }

    /// Write bytes from `buf` to the stream.
    ///
    /// Issues an overlapped `WSASend` and parks the goroutine until the
    /// operation completes via IOCP.  Returns the number of bytes written
    /// (may be less than `buf.len()`).
    pub fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // SAFETY: called from a goroutine context.
        unsafe { overlapped_send(self.socket, buf) }
    }

    /// Duplicate this stream, yielding a second `TcpStream` that refers to
    /// the same TCP connection.
    ///
    /// Uses `DuplicateHandle` to copy the underlying `SOCKET` handle within
    /// the current process, then associates the duplicate with the IOCP.
    /// Both streams have independent lifetimes; closing one does not close
    /// the other.
    pub fn try_clone(&self) -> io::Result<TcpStream> {
        let proc = unsafe { GetCurrentProcess() };
        let mut new_handle: *mut c_void = std::ptr::null_mut();
        let ok = unsafe {
            DuplicateHandle(
                proc,
                self.socket as *mut c_void,
                proc,
                &mut new_handle,
                0,
                0,
                DUPLICATE_SAME_ACCESS,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let s = new_handle as Socket;
        if !netpoll_iocp_associate(s) {
            unsafe { closesocket(s) };
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "failed to associate cloned socket with IOCP",
            ));
        }
        Ok(TcpStream { socket: s })
    }

    /// Return the remote address of the peer this stream is connected to.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        let mut storage = SockAddrStorage::zeroed();
        let mut len = std::mem::size_of::<SockAddrStorage>() as i32;
        if unsafe {
            getpeername(self.socket, storage._data.as_mut_ptr() as *mut c_void, &mut len)
        } == SOCKET_ERROR
        {
            return Err(wsa_last_error());
        }
        sockaddr_to_socketaddr(&storage, len)
    }

    /// Return the local address this stream is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let mut storage = SockAddrStorage::zeroed();
        let mut len = std::mem::size_of::<SockAddrStorage>() as i32;
        if unsafe {
            getsockname(self.socket, storage._data.as_mut_ptr() as *mut c_void, &mut len)
        } == SOCKET_ERROR
        {
            return Err(wsa_last_error());
        }
        sockaddr_to_socketaddr(&storage, len)
    }

    /// Return the underlying Windows `SOCKET` handle.
    pub fn as_raw_socket(&self) -> usize {
        self.socket
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        // Closing the socket cancels any pending overlapped I/O.  The IOCP
        // completion fires with STATUS_CANCELLED, waking the parked goroutine
        // (if any) so it returns an error rather than leaking.
        unsafe { closesocket(self.socket) };
    }
}

// ---------------------------------------------------------------------------
// std::io trait implementations for TcpStream (Windows)
// ---------------------------------------------------------------------------

impl Read for TcpStream {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        TcpStream::read(self, buf)
    }
}

/// `&TcpStream` Read: the overlapped operation uses the socket handle by
/// value, so no mutation of the `TcpStream` struct is required.
impl Read for &TcpStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        unsafe { overlapped_recv(self.socket, buf) }
    }
}

impl Write for TcpStream {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        TcpStream::write(self, buf)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// `&TcpStream` Write: same reasoning as the `Read` impl above.
impl Write for &TcpStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        unsafe { overlapped_send(self.socket, buf) }
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::runtime::sched::{run_impl, spawn_goroutine};
    use std::sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    };

    /// Bind to port 0 and verify the OS assigns a non-zero port.
    #[test]
    fn bind_port_zero() {
        run_impl(|| {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            assert_ne!(addr.port(), 0, "OS should assign a non-zero port");
        });
    }

    /// A goroutine connects; the listener accepts; they exchange a payload.
    #[test]
    fn connect_accept_echo() {
        use std::time::{Duration, Instant};

        const PAYLOAD: &[u8] = b"hello-iocp";
        let done = Arc::new(AtomicU8::new(0));
        let done2 = Arc::clone(&done);

        run_impl(move || {
            // Bind the listener on the loopback interface.
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            // Spawn a client goroutine.
            spawn_goroutine(move || {
                let mut stream = TcpStream::connect(addr).unwrap();
                stream.write(PAYLOAD).unwrap();

                let mut buf = [0u8; 64];
                let n = stream.read(&mut buf).unwrap();
                assert_eq!(&buf[..n], PAYLOAD, "echo mismatch");

                done2.store(1, Ordering::Release);
            });

            // Accept the connection and echo the payload back.
            let mut stream = listener.accept().unwrap();
            let mut buf = [0u8; 64];
            let n = stream.read(&mut buf).unwrap();
            stream.write(&buf[..n]).unwrap();

            // Wait for the client goroutine to confirm the echo.
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if done.load(Ordering::Acquire) == 1 {
                    break;
                }
                assert!(Instant::now() < deadline, "echo test timed out");
                crate::gosched();
                std::thread::sleep(Duration::from_millis(5));
            }
        });
    }

    /// Multiple concurrent goroutines each sleep via overlapped I/O and
    /// all complete within the expected window.
    #[test]
    fn concurrent_connections() {
        use std::time::{Duration, Instant};

        const N: u8 = 4;
        let awoke = Arc::new(AtomicU8::new(0));
        let awoke2 = Arc::clone(&awoke);

        run_impl(move || {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            // Spawn N client goroutines.
            for _ in 0..N {
                let awoke3 = Arc::clone(&awoke2);
                spawn_goroutine(move || {
                    let mut stream = TcpStream::connect(addr).unwrap();
                    let mut buf = [0u8; 1];
                    stream.read(&mut buf).unwrap(); // wait for server byte
                    awoke3.fetch_add(1, Ordering::Relaxed);
                });
            }

            // Accept N connections and send one byte each.
            for _ in 0..N {
                let mut stream = listener.accept().unwrap();
                spawn_goroutine(move || {
                    stream.write(&[42u8]).unwrap();
                });
            }

            // Wait for all clients.
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if awoke2.load(Ordering::Acquire) == N {
                    break;
                }
                assert!(Instant::now() < deadline, "concurrent_connections timed out");
                crate::gosched();
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        assert_eq!(awoke.load(Ordering::Acquire), N);
    }
}
