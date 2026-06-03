// SPDX-License-Identifier: Apache-2.0
//! Goroutine-aware TCP networking.
//!
//! `TcpListener` and `TcpStream` wrap non-blocking OS sockets and integrate
//! with the go-lib scheduler via the netpoll backend (`epoll` on Linux,
//! `kqueue` on macOS).  When a socket operation would block (`EAGAIN` /
//! `EWOULDBLOCK`), the goroutine is parked via `gopark` and re-enqueued by
//! the netpoll machinery when the socket becomes ready.
//!
//! ## Usage
//!
//! ```no_run
//! use std::io::{Read, Write};
//!
//! go_lib::run(|| {
//!     let listener = go_lib::net::TcpListener::bind("127.0.0.1:8080").unwrap();
//!     loop {
//!         let mut stream = listener.accept().unwrap();
//!         go_lib::go!(move || {
//!             let mut buf = [0u8; 1024];
//!             let n = stream.read(&mut buf).unwrap();
//!             stream.write_all(&buf[..n]).unwrap();
//!         });
//!     }
//! });
//! ```
//!
//! ## `std::io` trait implementations
//!
//! `TcpStream` implements [`std::io::Read`] and [`std::io::Write`], so it
//! works directly with any code that accepts `impl Read` or `impl Write` —
//! including `BufReader`, `BufWriter`, and Rust's I/O adapters — without
//! any unsafe wrapper or raw-fd manipulation.
//!
//! [`TcpStream::try_clone`] duplicates the underlying fd via `dup(2)`,
//! yielding an independent stream that shares the same TCP connection.  This
//! is useful for splitting a connection into separate read and write halves:
//!
//! ```no_run
//! use std::io::{Read, Write};
//!
//! go_lib::run(|| {
//!     let listener = go_lib::net::TcpListener::bind("127.0.0.1:9000").unwrap();
//!     let stream = listener.accept().unwrap();
//!     let mut writer = stream.try_clone().unwrap();
//!     go_lib::go!(move || {
//!         // `stream` is the read half; `writer` is the write half.
//!         let mut buf = [0u8; 512];
//!         let n = (&stream).read(&mut buf).unwrap();   // via &TcpStream impl
//!         writer.write_all(&buf[..n]).unwrap();
//!     });
//! });
//! ```
//!
//! ## Porting note
//!
//! Go's `net` package calls `runtime.poll.pollDesc.waitRead` / `waitWrite`
//! which translate directly to `netpoll_arm(fd, POLL_READ/WRITE, gp)` +
//! `gopark`.  The same protocol is used here.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, ToSocketAddrs};
use std::os::unix::io::RawFd;

use libc;

use crate::runtime::g::WaitReason;
use crate::runtime::netpoll::{netpoll_arm, netpoll_unarm, POLL_READ, POLL_WRITE};
use crate::runtime::park::gopark;

// ---------------------------------------------------------------------------
// Helpers — non-blocking socket creation and address conversion
// ---------------------------------------------------------------------------

/// Create a non-blocking `SOCK_STREAM` socket for the given address family.
///
/// On Linux, `SOCK_NONBLOCK` is passed directly to `socket(2)`.
/// On macOS (which lacks `SOCK_NONBLOCK`), `O_NONBLOCK` is set via `fcntl`.
fn nonblocking_tcp_socket(family: libc::c_int) -> io::Result<RawFd> {
    #[cfg(target_os = "linux")]
    let fd = unsafe { libc::socket(family, libc::SOCK_STREAM | libc::SOCK_NONBLOCK, 0) };

    #[cfg(not(target_os = "linux"))]
    let fd = unsafe { libc::socket(family, libc::SOCK_STREAM, 0) };

    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    // On platforms where SOCK_NONBLOCK is not available, set O_NONBLOCK via fcntl.
    #[cfg(not(target_os = "linux"))]
    {
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0
            || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
        {
            unsafe { libc::close(fd) };
            return Err(io::Error::last_os_error());
        }
    }

    Ok(fd)
}

fn set_reuseaddr(fd: RawFd) -> io::Result<()> {
    let one: libc::c_int = 1;
    let ret = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Convert a `SocketAddr` to a `libc::sockaddr_storage` + length.
fn to_sockaddr(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    match addr {
        SocketAddr::V4(v4) => {
            let sa: &mut libc::sockaddr_in =
                unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in) };
            sa.sin_family = libc::AF_INET as libc::sa_family_t;
            sa.sin_port   = v4.port().to_be();
            sa.sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            (storage, std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t)
        }
        SocketAddr::V6(v6) => {
            let sa: &mut libc::sockaddr_in6 =
                unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in6) };
            sa.sin6_family   = libc::AF_INET6 as libc::sa_family_t;
            sa.sin6_port     = v6.port().to_be();
            sa.sin6_addr.s6_addr = v6.ip().octets();
            (storage, std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t)
        }
    }
}

fn addr_family(addr: SocketAddr) -> libc::c_int {
    match addr {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    }
}

/// Park the calling goroutine until `fd` is ready for `mode`
/// (`POLL_READ` or `POLL_WRITE`).
///
/// # Safety
/// Must be called from a live goroutine context.
unsafe fn park_on_fd(fd: RawFd, mode: u32) {
    let gp = crate::runtime::g::current_g();
    debug_assert!(!gp.is_null(), "park_on_fd: not running on a goroutine");
    unsafe {
        netpoll_arm(fd, mode, gp);
        gopark(WaitReason::IOWait);
        // gopark suspends this goroutine; execution resumes after goready()
        // is called by the netpoll machinery.
    }
}

// ---------------------------------------------------------------------------
// TcpListener
// ---------------------------------------------------------------------------

/// A goroutine-aware TCP server socket.
///
/// Calls to [`accept`][TcpListener::accept] park the current goroutine when no
/// connection is immediately available and resume it when one arrives.
pub struct TcpListener {
    fd: RawFd,
}

impl TcpListener {
    /// Bind a non-blocking TCP listener to `addr`.
    ///
    /// Equivalent to `net.Listen("tcp", addr)` in Go.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no address given"))?;

        let fd = nonblocking_tcp_socket(addr_family(addr))?;
        set_reuseaddr(fd)?;

        let (sa, sa_len) = to_sockaddr(addr);
        let ret = unsafe {
            libc::bind(fd, &sa as *const _ as *const libc::sockaddr, sa_len)
        };
        if ret < 0 {
            unsafe { libc::close(fd) };
            return Err(io::Error::last_os_error());
        }

        let ret = unsafe { libc::listen(fd, 128) };
        if ret < 0 {
            unsafe { libc::close(fd) };
            return Err(io::Error::last_os_error());
        }

        Ok(TcpListener { fd })
    }

    /// Accept the next incoming connection.
    ///
    /// Parks the goroutine if no connection is immediately available, resuming
    /// it when the OS delivers one.
    pub fn accept(&self) -> io::Result<TcpStream> {
        loop {
            let cfd = unsafe {
                libc::accept(self.fd, std::ptr::null_mut(), std::ptr::null_mut())
            };
            if cfd >= 0 {
                // Set O_NONBLOCK on the accepted socket.
                let flags = unsafe { libc::fcntl(cfd, libc::F_GETFL) };
                if flags >= 0 {
                    unsafe { libc::fcntl(cfd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
                }
                return Ok(TcpStream { fd: cfd });
            }
            let err = io::Error::last_os_error();
            match err.raw_os_error().unwrap_or(0) {
                libc::EAGAIN => {
                    // No connection yet — park until the listener fd is readable.
                    unsafe { park_on_fd(self.fd, POLL_READ) };
                    // After wakeup, retry accept().
                }
                _ => return Err(err),
            }
        }
    }

    /// Return the underlying raw file descriptor.
    pub fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        netpoll_unarm(self.fd);
        unsafe { libc::close(self.fd) };
    }
}

// ---------------------------------------------------------------------------
// TcpStream
// ---------------------------------------------------------------------------

/// A goroutine-aware TCP stream socket.
///
/// Blocking reads and writes park the calling goroutine (via the netpoll
/// backend) when the operation would block, resuming it when data is
/// available or the send buffer has space.
///
/// `TcpStream` implements [`std::io::Read`] and [`std::io::Write`] (for both
/// `&mut TcpStream` and `&TcpStream`), so it works with any Rust I/O adapter
/// without unsafe wrapper code.  Use [`try_clone`][TcpStream::try_clone] to
/// split a connection into independent read and write halves.
pub struct TcpStream {
    fd: RawFd,
}

impl TcpStream {
    /// Connect to `addr`.
    ///
    /// Parks the goroutine until the connection completes if it does not
    /// complete immediately (which is typical for non-blocking `connect`).
    ///
    /// Equivalent to `net.Dial("tcp", addr)` in Go.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no address given"))?;

        let fd = nonblocking_tcp_socket(addr_family(addr))?;
        let (sa, sa_len) = to_sockaddr(addr);

        let ret = unsafe {
            libc::connect(fd, &sa as *const _ as *const libc::sockaddr, sa_len)
        };

        if ret < 0 {
            let err = io::Error::last_os_error();
            match err.raw_os_error().unwrap_or(0) {
                libc::EINPROGRESS | libc::EAGAIN => {
                    // Connection in progress — park until the socket is writable.
                    unsafe { park_on_fd(fd, POLL_WRITE) };
                    // Check for connect error via SO_ERROR.
                    let mut so_err: libc::c_int = 0;
                    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
                    unsafe {
                        libc::getsockopt(
                            fd,
                            libc::SOL_SOCKET,
                            libc::SO_ERROR,
                            &mut so_err as *mut _ as *mut libc::c_void,
                            &mut len,
                        )
                    };
                    if so_err != 0 {
                        unsafe { libc::close(fd) };
                        return Err(io::Error::from_raw_os_error(so_err));
                    }
                }
                _ => {
                    unsafe { libc::close(fd) };
                    return Err(err);
                }
            }
        }

        Ok(TcpStream { fd })
    }

    /// Read bytes from the stream into `buf`.
    ///
    /// Parks the goroutine if no data is immediately available.
    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let n = unsafe {
                libc::read(self.fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len())
            };
            if n >= 0 {
                return Ok(n as usize);
            }
            let err = io::Error::last_os_error();
            match err.raw_os_error().unwrap_or(0) {
                libc::EAGAIN => {
                    unsafe { park_on_fd(self.fd, POLL_READ) };
                }
                _ => return Err(err),
            }
        }
    }

    /// Write `buf` to the stream.
    ///
    /// Parks the goroutine if the send buffer is full.  Returns the number of
    /// bytes written (may be less than `buf.len()`).
    pub fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        loop {
            let n = unsafe {
                libc::write(self.fd, buf.as_ptr() as *const libc::c_void, buf.len())
            };
            if n >= 0 {
                return Ok(n as usize);
            }
            let err = io::Error::last_os_error();
            match err.raw_os_error().unwrap_or(0) {
                libc::EAGAIN => {
                    unsafe { park_on_fd(self.fd, POLL_WRITE) };
                }
                _ => return Err(err),
            }
        }
    }

    /// Duplicate this stream, creating a second `TcpStream` that refers to the
    /// same underlying TCP connection.
    ///
    /// The duplicate is an independent `TcpStream` with its own fd (via
    /// `dup(2)`).  Both streams share the same socket; reads and writes on
    /// either half see the same data stream.  Closing one does not close the
    /// other.
    ///
    /// The typical use-case is splitting a connection into a dedicated read
    /// half and a dedicated write half for use in separate goroutines.
    pub fn try_clone(&self) -> io::Result<TcpStream> {
        let new_fd = unsafe { libc::dup(self.fd) };
        if new_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(TcpStream { fd: new_fd })
    }

    /// Return the remote address of the peer this stream is connected to.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        sockaddr_of(self.fd, /* peer = */ true)
    }

    /// Return the local address this stream is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        sockaddr_of(self.fd, /* peer = */ false)
    }

    /// Return the underlying raw file descriptor.
    pub fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        netpoll_unarm(self.fd);
        unsafe { libc::close(self.fd) };
    }
}

// ---------------------------------------------------------------------------
// std::io trait implementations for TcpStream
// ---------------------------------------------------------------------------

/// Implements [`std::io::Read`] by delegating to [`TcpStream::read`].
///
/// This allows `TcpStream` to be used with any Rust I/O adapter that accepts
/// `impl Read`, such as `BufReader`, `Read::read_to_string`, etc., without
/// any unsafe wrapper or raw-fd manipulation.
impl Read for TcpStream {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        TcpStream::read(self, buf)
    }
}

/// Implements [`std::io::Read`] on a shared reference by issuing a raw
/// `libc::read` call.  The fd is non-blocking; EAGAIN causes the goroutine
/// to park via netpoll exactly as the owned-`&mut self` path does.
///
/// This enables using the same `TcpStream` for both reading and writing from
/// two separate code sites within the same goroutine (e.g. after splitting
/// into read/write halves conceptually without calling `try_clone`).
impl Read for &TcpStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let n = unsafe {
                libc::read(self.fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len())
            };
            if n >= 0 {
                return Ok(n as usize);
            }
            let err = io::Error::last_os_error();
            match err.raw_os_error().unwrap_or(0) {
                libc::EAGAIN => unsafe { park_on_fd(self.fd, POLL_READ) },
                _ => return Err(err),
            }
        }
    }
}

/// Implements [`std::io::Write`] by delegating to [`TcpStream::write`].
/// `flush` is a no-op because the kernel TCP stack handles buffering.
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

/// Implements [`std::io::Write`] on a shared reference.
impl Write for &TcpStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        loop {
            let n = unsafe {
                libc::write(self.fd, buf.as_ptr() as *const libc::c_void, buf.len())
            };
            if n >= 0 {
                return Ok(n as usize);
            }
            let err = io::Error::last_os_error();
            match err.raw_os_error().unwrap_or(0) {
                libc::EAGAIN => unsafe { park_on_fd(self.fd, POLL_WRITE) },
                _ => return Err(err),
            }
        }
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// std::io trait implementations for TcpListener
// ---------------------------------------------------------------------------

impl TcpListener {
    /// Return the local address the listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        sockaddr_of(self.fd, /* peer = */ false)
    }
}

// ---------------------------------------------------------------------------
// Address helpers
// ---------------------------------------------------------------------------

/// Query the local or peer address of `fd`.
fn sockaddr_of(fd: RawFd, peer: bool) -> io::Result<SocketAddr> {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    let ret = unsafe {
        if peer {
            libc::getpeername(fd, &mut storage as *mut _ as *mut libc::sockaddr, &mut len)
        } else {
            libc::getsockname(fd, &mut storage as *mut _ as *mut libc::sockaddr, &mut len)
        }
    };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            let sa: &libc::sockaddr_in =
                unsafe { &*(&storage as *const _ as *const libc::sockaddr_in) };
            let ip = std::net::Ipv4Addr::from(u32::from_be(sa.sin_addr.s_addr));
            let port = u16::from_be(sa.sin_port);
            Ok(SocketAddr::from((ip, port)))
        }
        libc::AF_INET6 => {
            let sa: &libc::sockaddr_in6 =
                unsafe { &*(&storage as *const _ as *const libc::sockaddr_in6) };
            let ip = std::net::Ipv6Addr::from(sa.sin6_addr.s6_addr);
            let port = u16::from_be(sa.sin6_port);
            Ok(SocketAddr::from((ip, port)))
        }
        family => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("unsupported address family: {family}"),
        )),
    }
}
