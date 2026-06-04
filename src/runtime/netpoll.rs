// SPDX-License-Identifier: Apache-2.0
//! Network poller — integrates non-blocking I/O with the goroutine scheduler.
//!
//! Ported from `runtime/netpoll_epoll.go` (Linux), `runtime/netpoll_kqueue.go`
//! (macOS), and `runtime/netpoll_windows.go` (Windows).
//!
//! ## Architecture
//!
//! ### Unix (readiness-based — Linux epoll, macOS kqueue)
//!
//! A goroutine that would block on a non-blocking socket calls [`netpoll_arm`]
//! with its file descriptor and `*mut G`, then calls `gopark`.  When the fd
//! becomes readable/writable the OS notifies the epoll/kqueue fd; a background
//! call to [`netpoll_wait`] (from `findrunnable` or `sysmon`) collects the
//! ready fds, looks up the waiting goroutines, and returns them.  The caller
//! calls [`goready`][super::park::goready] on each.
//!
//! ### Windows (completion-based — IOCP)
//!
//! `net_windows.rs` creates each socket with `WSA_FLAG_OVERLAPPED` and calls
//! [`netpoll_iocp_associate`] to attach it to the process-wide I/O Completion
//! Port.  For each read/write it allocates a heap [`IocpOp`] (OVERLAPPED at
//! offset 0 + `*mut G` + result fields), passes the overlapped pointer to
//! `WSARecv`/`WSASend`, then calls `gopark`.  [`netpoll_wait`] calls
//! `GetQueuedCompletionStatusEx`, casts `LPOVERLAPPED` back to `*mut IocpOp`,
//! fills the result fields, and returns the goroutine pointers.  The goroutine
//! resumes, reads the result, and drops the `Box<IocpOp>`.
//!
//! [`netpoll_arm`] is a no-op on Windows — `POLL_FD` remains -1, so the early
//! `pfd < 0` guard returns immediately.  Socket I/O is initiated directly from
//! `net_windows.rs` rather than through the arm/wait protocol.
//!
//! ## Platform support
//!
//! | Platform    | Backend  | I/O model   |
//! |-------------|----------|-------------|
//! | Linux       | `epoll`  | readiness   |
//! | macOS       | `kqueue` | readiness   |
//! | Windows     | IOCP     | completion  |
//!
//! ## Safety
//!
//! `*mut G` raw pointers are stored in the registration table wrapped in
//! `GRaw(usize)` so they cross Mutex / HashMap ownership boundaries without
//! triggering `Send` trait violations.  All access is serialised by the Mutex.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering::*};
use std::sync::Mutex;

// `RawFd` is a Unix concept (file descriptor = signed integer).  On Windows we
// use the same `i32` representation but don't pull in the unix-specific module.
#[cfg(not(windows))]
use std::os::unix::io::RawFd;
#[cfg(windows)]
type RawFd = i32;

use super::g::G;

// ---------------------------------------------------------------------------
// Registration table
// ---------------------------------------------------------------------------

/// Raw G pointer stored as `usize` to satisfy `Send` (HashMap needs it).
struct GRaw(usize);
// SAFETY: scheduling operations that hand off *mut G pointers are always
// serialised by either the scheduler Mutex or the Mutex below.
unsafe impl Send for GRaw {}

/// Global registration table: maps file descriptor → (G pointer, poll mode).
///
/// Mode bit 1 = read; bit 2 = write.
static REG: Mutex<Option<HashMap<RawFd, (GRaw, u32)>>> = Mutex::new(None);

fn with_reg<F, R>(f: F) -> R
where
    F: FnOnce(&mut HashMap<RawFd, (GRaw, u32)>) -> R,
{
    let mut guard = REG.lock().unwrap();
    f(guard.get_or_insert_with(HashMap::new))
}

// ---------------------------------------------------------------------------
// Poll mode constants
// ---------------------------------------------------------------------------

/// Register the file descriptor for read readiness.
pub(crate) const POLL_READ:  u32 = 1;
/// Register the file descriptor for write readiness.
pub(crate) const POLL_WRITE: u32 = 2;

// ---------------------------------------------------------------------------
// Global poll fd (epoll on Linux, kqueue on macOS)
// ---------------------------------------------------------------------------

/// The process-wide epoll / kqueue file descriptor (Unix only).
///
/// Remains `-1` on Windows, where the IOCP handle in `WIN_IOCP` is used
/// instead.
static POLL_FD: AtomicI32 = AtomicI32::new(-1);

/// Initialise the netpoll backend.  Idempotent — subsequent calls are no-ops.
///
/// On Unix, creates the epoll / kqueue fd and stores it in `POLL_FD`.
/// On Windows, initialises Winsock 2.2 and creates the process-wide IOCP.
pub(crate) fn netpoll_init() {
    // Unix: create the epoll / kqueue fd.
    #[cfg(not(windows))]
    {
        if POLL_FD.load(Relaxed) >= 0 {
            return;
        }
        let fd = create_poll_fd();
        assert!(fd >= 0, "netpoll_init: could not create poll fd");
        // Only one caller wins the CAS; the rest discard their fd.
        if POLL_FD
            .compare_exchange(-1, fd, AcqRel, Relaxed)
            .is_err()
        {
            unsafe { libc::close(fd) };
        }
    }
    // Windows: initialise Winsock and create the I/O completion port.
    #[cfg(windows)]
    iocp_win_init();
}

// ---------------------------------------------------------------------------
// netpoll_arm — register a goroutine for fd readiness notification (Unix)
// ---------------------------------------------------------------------------

/// Register `fd` with the epoll / kqueue backend; wake `gp` when ready.
///
/// `mode` is a bitmask of [`POLL_READ`] and/or [`POLL_WRITE`].
///
/// `gp` must be the goroutine that is about to call `gopark`; it must remain
/// alive until [`netpoll_unarm`] or [`netpoll_wait`] removes it.
///
/// **Windows**: this function is a no-op.  Windows sockets use IOCP; I/O is
/// initiated directly from `net_windows.rs` without going through
/// `netpoll_arm`.  `POLL_FD` is always -1 on Windows, so the early return
/// below fires immediately.
///
/// # Safety
/// `gp` must point to a live `G` that is about to be parked.
pub(crate) unsafe fn netpoll_arm(fd: RawFd, mode: u32, gp: *mut G) {
    let pfd = POLL_FD.load(Acquire);
    if pfd < 0 {
        // Unix: netpoll not yet initialised, or Windows (POLL_FD unused).
        return;
    }

    with_reg(|reg| {
        reg.insert(fd, (GRaw(gp as usize), mode));
    });

    #[cfg(not(windows))]
    unsafe { poll_add(pfd, fd, mode) };
}

// ---------------------------------------------------------------------------
// netpoll_unarm — deregister a file descriptor
// ---------------------------------------------------------------------------

/// Remove `fd` from the netpoll backend.  Safe to call even if `fd` was never
/// armed.
pub(crate) fn netpoll_unarm(fd: RawFd) {
    with_reg(|reg| { reg.remove(&fd); });
    #[cfg(not(windows))]
    {
        let pfd = POLL_FD.load(Acquire);
        if pfd >= 0 {
            unsafe { poll_del(pfd, fd) };
        }
    }
}

// ---------------------------------------------------------------------------
// netpoll_clear_reg — purge all stale registrations after go_lib::run() exits
// ---------------------------------------------------------------------------

/// Deregister every file descriptor from the epoll/kqueue backend and clear
/// the registration table.
///
/// ## When to call
///
/// Call this once, from the caller's thread, immediately after `run_impl`'s
/// `std::thread::park()` returns — i.e. after the main goroutine has finished
/// but before `run_impl` returns to the user.
///
/// ## Why this is necessary
///
/// `go_lib::run()` can be called multiple times in the same process (e.g.
/// each `#[test]` function calls it independently).  When the main closure
/// exits, background goroutines spawned with `go!()` — such as HTTP server
/// goroutines blocked in `listen_and_serve()` — continue running: they are
/// parked in `gopark` with their `(fd, *mut G)` entry still live in `REG`
/// and their fd still registered with epoll/kqueue.
///
/// The next `go_lib::run()` call in the same process reuses:
/// * the same `POLL_FD` (epoll/kqueue fd, created once via `netpoll_init`)
/// * the same `REG` global registration table
///
/// Without cleanup, `netpoll_wait` can then return stale `*mut G` pointers
/// from the previous run.  `goready` dereferences those pointers, causing:
/// * **Linux/epoll**: SIGSEGV (invalid memory reference)
/// * **Windows/IOCP**: hang (IOCP threads from run N conflict with run N+1)
///
/// macOS/kqueue silently discards events for abandoned goroutines, so no
/// crash is observed there, but the stale table entries are still incorrect.
///
/// ## What happens to abandoned goroutines
///
/// Goroutines left running after this call can never be woken by netpoll
/// again (their fd is removed from epoll/kqueue and their entry is gone from
/// `REG`).  They remain parked forever — a memory leak.  Fixing this properly
/// requires a "stop the world" mechanism (out of scope for this patch).
///
/// ## Safety
///
/// Must be called from outside any goroutine (i.e. from the OS thread that
/// called `run_impl`, after `park()` returns).  No M thread must be executing
/// `netpoll_arm` or `netpoll_wait` concurrently; this is guaranteed because
/// the main goroutine has already exited and no new goroutine can be spawned
/// once `park()` has returned.
pub(crate) fn netpoll_clear_reg() {
    let pfd = POLL_FD.load(Acquire);
    with_reg(|reg| {
        // Deregister every fd from epoll/kqueue before clearing the map.
        // Without this, stale kernel-level registrations can accumulate.
        // When an old fd number is reused by a new socket, EPOLL_CTL_ADD
        // would fail with EEXIST (the kernel sees the fd as already
        // registered), falling through to EPOLL_CTL_MOD — subtle but
        // harmless.  Explicit removal keeps the kernel poll table clean.
        #[cfg(not(windows))]
        if pfd >= 0 {
            for (fd, _) in reg.iter() {
                unsafe { poll_del(pfd, *fd) };
            }
        }
        reg.clear();
    });
}

// ---------------------------------------------------------------------------
// netpoll_wait — collect goroutines whose I/O is ready or complete
// ---------------------------------------------------------------------------

/// Collect goroutines whose pending I/O has become ready (Unix) or whose
/// overlapped operations have completed (Windows), and return them.
///
/// `timeout_ms < 0`  → block indefinitely.
/// `timeout_ms == 0` → non-blocking poll (used by `findrunnable`).
/// `timeout_ms > 0`  → block up to `timeout_ms` ms (used by `sysmon`).
///
/// **Unix**: drains the epoll/kqueue fd; removes each ready fd from the
/// registration table and returns the associated goroutine.
///
/// **Windows**: calls `GetQueuedCompletionStatusEx`; fills
/// `IocpOp.bytes_transferred` and `IocpOp.ntstatus` for each completed
/// operation and returns the goroutine that was waiting on it.
///
/// The caller must call [`goready`][super::park::goready] on each returned
/// goroutine.
///
/// # Safety
/// Must not be called concurrently with itself on the same poll handle.
pub(crate) unsafe fn netpoll_wait(timeout_ms: i32) -> Vec<*mut G> {
    // Windows: drain the IOCP.
    #[cfg(windows)]
    return unsafe { iocp_win_wait(timeout_ms) };

    // Unix: drain epoll / kqueue.
    #[cfg(not(windows))]
    {
        let pfd = POLL_FD.load(Acquire);
        if pfd < 0 {
            return Vec::new();
        }
        let ready_fds = unsafe { poll_wait(pfd, timeout_ms) };
        let mut goroutines = Vec::with_capacity(ready_fds.len());
        with_reg(|reg| {
            for fd in &ready_fds {
                if let Some((g_raw, _mode)) = reg.remove(fd) {
                    goroutines.push(g_raw.0 as *mut G);
                    unsafe { poll_del(pfd, *fd) };
                }
            }
        });
        goroutines
    }
}

// ---------------------------------------------------------------------------
// Linux epoll backend
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn create_poll_fd() -> RawFd {
    unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) }
}

#[cfg(target_os = "linux")]
unsafe fn poll_add(epfd: RawFd, fd: RawFd, mode: u32) {
    let mut ev = libc::epoll_event {
        events: {
            let mut e = libc::EPOLLONESHOT as u32;
            if mode & POLL_READ  != 0 { e |= libc::EPOLLIN as u32; }
            if mode & POLL_WRITE != 0 { e |= libc::EPOLLOUT as u32; }
            e
        },
        u64: fd as u64,
    };
    unsafe {
        let ret = libc::epoll_ctl(epfd, libc::EPOLL_CTL_ADD, fd, &mut ev);
        if ret < 0 {
            // fd may already be added; try MOD instead.
            libc::epoll_ctl(epfd, libc::EPOLL_CTL_MOD, fd, &mut ev);
        }
    }
}

#[cfg(target_os = "linux")]
unsafe fn poll_del(epfd: RawFd, fd: RawFd) {
    unsafe {
        libc::epoll_ctl(epfd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut());
    }
}

#[cfg(target_os = "linux")]
unsafe fn poll_wait(epfd: RawFd, timeout_ms: i32) -> Vec<RawFd> {
    const MAX_EVENTS: usize = 128;
    let mut events = [libc::epoll_event { events: 0, u64: 0 }; MAX_EVENTS];
    let n = unsafe {
        libc::epoll_wait(epfd, events.as_mut_ptr(), MAX_EVENTS as i32, timeout_ms)
    };
    if n <= 0 {
        return Vec::new();
    }
    (0..n as usize).map(|i| events[i].u64 as RawFd).collect()
}

// ---------------------------------------------------------------------------
// macOS kqueue backend
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
fn create_poll_fd() -> RawFd {
    unsafe { libc::kqueue() }
}

#[cfg(target_os = "macos")]
unsafe fn poll_add(kq: RawFd, fd: RawFd, mode: u32) {
    let mut changes: [libc::kevent; 2] = unsafe { std::mem::zeroed() };
    let mut n = 0usize;

    if mode & POLL_READ != 0 {
        changes[n] = libc::kevent {
            ident:  fd as libc::uintptr_t,
            filter: libc::EVFILT_READ,
            flags:  libc::EV_ADD | libc::EV_ONESHOT,
            fflags: 0,
            data:   0,
            udata:  std::ptr::null_mut(),
        };
        n += 1;
    }
    if mode & POLL_WRITE != 0 {
        changes[n] = libc::kevent {
            ident:  fd as libc::uintptr_t,
            filter: libc::EVFILT_WRITE,
            flags:  libc::EV_ADD | libc::EV_ONESHOT,
            fflags: 0,
            data:   0,
            udata:  std::ptr::null_mut(),
        };
        n += 1;
    }

    if n > 0 {
        unsafe {
            libc::kevent(
                kq,
                changes.as_ptr(),
                n as libc::c_int,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            );
        }
    }
}

#[cfg(target_os = "macos")]
unsafe fn poll_del(kq: RawFd, fd: RawFd) {
    let changes = [
        libc::kevent {
            ident:  fd as libc::uintptr_t,
            filter: libc::EVFILT_READ,
            flags:  libc::EV_DELETE,
            fflags: 0,
            data:   0,
            udata:  std::ptr::null_mut(),
        },
        libc::kevent {
            ident:  fd as libc::uintptr_t,
            filter: libc::EVFILT_WRITE,
            flags:  libc::EV_DELETE,
            fflags: 0,
            data:   0,
            udata:  std::ptr::null_mut(),
        },
    ];
    unsafe {
        libc::kevent(
            kq,
            changes.as_ptr(),
            2,
            std::ptr::null_mut(),
            0,
            std::ptr::null(),
        );
    }
}

#[cfg(target_os = "macos")]
unsafe fn poll_wait(kq: RawFd, timeout_ms: i32) -> Vec<RawFd> {
    const MAX_EVENTS: usize = 128;
    let mut events: [libc::kevent; MAX_EVENTS] = unsafe { std::mem::zeroed() };

    let ts;
    let ts_ptr;
    if timeout_ms < 0 {
        ts_ptr = std::ptr::null();
    } else {
        ts = libc::timespec {
            tv_sec:  (timeout_ms / 1000) as libc::time_t,
            tv_nsec: ((timeout_ms % 1000) * 1_000_000) as libc::c_long,
        };
        ts_ptr = &ts as *const libc::timespec;
    }

    let n = unsafe {
        libc::kevent(
            kq,
            std::ptr::null(),
            0,
            events.as_mut_ptr(),
            MAX_EVENTS as libc::c_int,
            ts_ptr,
        )
    };
    if n <= 0 {
        return Vec::new();
    }
    (0..n as usize)
        .map(|i| events[i].ident as RawFd)
        .collect()
}

// ---------------------------------------------------------------------------
// Windows IOCP backend
// ---------------------------------------------------------------------------
//
// POLL_FD remains -1 on Windows; the readiness-based helpers (create_poll_fd,
// poll_add, poll_del, poll_wait) are not defined.  Instead, goroutine-aware
// sockets registered with the global I/O Completion Port drive all async I/O.
//
// Flow:
//   1. `netpoll_init` calls `iocp_win_init` which runs `WSAStartup` and
//      `CreateIoCompletionPort`, storing the handle in `WIN_IOCP`.
//   2. `net_windows.rs` creates each socket with `WSA_FLAG_OVERLAPPED`, then
//      calls `netpoll_iocp_associate` to attach it to the IOCP.
//   3. For each read/write, `net_windows.rs` allocates a heap `IocpOp`,
//      passes `&mut op.overlapped` to `WSARecv`/`WSASend`, then calls
//      `gopark(IOWait)`.
//   4. `netpoll_wait` (called from `findrunnable` / `sysmon`) drains the IOCP
//      via `GetQueuedCompletionStatusEx`, fills `IocpOp.bytes_transferred` and
//      `IocpOp.ntstatus`, and returns the goroutine pointers.
//   5. The goroutine resumes, reads the result, and drops the `Box<IocpOp>`.

#[cfg(windows)]
use std::sync::OnceLock;

/// Process-wide IOCP handle (stored as `usize`; 0 = not yet initialised).
#[cfg(windows)]
static WIN_IOCP: OnceLock<usize> = OnceLock::new();

/// Alias for Windows `HANDLE` — a nullable pointer-sized value.
#[cfg(windows)]
type WinHandle = *mut std::ffi::c_void;

// ── Per-operation state ─────────────────────────────────────────────────────

/// Per-overlapped-I/O state block.
///
/// **Must** be `repr(C)` with `overlapped` at offset 0: Windows delivers
/// `LPOVERLAPPED` in the completion entry and we recover the full struct by
/// casting.
///
/// Allocated with `Box::into_raw` before issuing `WSARecv`/`WSASend`;
/// recovered and freed by the goroutine after it resumes.
#[cfg(windows)]
#[repr(C)]
pub(crate) struct IocpOp {
    /// Windows `OVERLAPPED` — **must be the first field** (offset 0).
    pub overlapped:        WinOverlapped,
    /// Goroutine waiting for this operation.
    pub gp:                *mut G,
    /// Bytes transferred; written by `netpoll_wait` on completion.
    pub bytes_transferred: u32,
    /// NTSTATUS completion code (0 = STATUS_SUCCESS).
    pub ntstatus:          u32,
}

// SAFETY: IocpOp is owned by exactly one goroutine (which is parked) plus
// the kernel writing into it; no concurrent Rust access occurs.
#[cfg(windows)]
unsafe impl Send for IocpOp {}

/// Rust layout of Windows `OVERLAPPED` (32 bytes on 64-bit Windows).
#[cfg(windows)]
#[repr(C)]
pub(crate) struct WinOverlapped {
    pub internal:      usize,     // ULONG_PTR Internal
    pub internal_high: usize,     // ULONG_PTR InternalHigh
    pub offset:        u32,       // union { struct { Offset, OffsetHigh }
    pub offset_high:   u32,       //         | PVOID Pointer }
    pub h_event:       WinHandle, // HANDLE hEvent
}

/// Windows `OVERLAPPED_ENTRY` — one slot from `GetQueuedCompletionStatusEx`.
#[cfg(windows)]
#[repr(C)]
struct OverlappedEntry {
    completion_key:    usize,              // ULONG_PTR lpCompletionKey
    overlapped:        *mut WinOverlapped, // LPOVERLAPPED
    internal:          usize,              // ULONG_PTR Internal (NTSTATUS)
    bytes_transferred: u32,                // DWORD dwNumberOfBytesTransferred
    // 4 bytes implicit trailing padding to reach 8-byte struct alignment
}

// ── Opaque WSADATA (always 400 bytes on all Windows targets) ────────────────

#[cfg(windows)]
#[repr(C)]
struct WsaData([u8; 400]);

// ── Windows FFI ─────────────────────────────────────────────────────────────

#[cfg(windows)]
#[link(name = "ws2_32")]
unsafe extern "system" {
    fn WSAStartup(w_version_required: u16, lp_wsa_data: *mut WsaData) -> i32;
}

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn CreateIoCompletionPort(
        file_handle:                  WinHandle,
        existing_completion_port:     WinHandle,
        completion_key:               usize,
        number_of_concurrent_threads: u32,
    ) -> WinHandle;

    fn GetQueuedCompletionStatusEx(
        completion_port:            WinHandle,
        lp_completion_port_entries: *mut OverlappedEntry,
        ul_count:                   u32,
        ul_num_entries_removed:     *mut u32,
        dw_milliseconds:            u32,
        f_alertable:                i32,
    ) -> i32;
}

// ── Initialisation ──────────────────────────────────────────────────────────

/// Initialise Winsock 2.2 and create the process-wide IOCP.  Idempotent.
#[cfg(windows)]
pub(crate) fn iocp_win_init() {
    if WIN_IOCP.get().is_some() {
        return;
    }
    let mut data = WsaData([0u8; 400]);
    let rc = unsafe { WSAStartup(0x0202u16, &mut data) };
    assert_eq!(rc, 0, "iocp_win_init: WSAStartup failed (error {rc})");

    // INVALID_HANDLE_VALUE = (HANDLE)(LONG_PTR)-1
    let invalid: WinHandle = usize::MAX as WinHandle;
    let h = unsafe { CreateIoCompletionPort(invalid, std::ptr::null_mut(), 0, 0) };
    assert!(!h.is_null(), "iocp_win_init: CreateIoCompletionPort failed");

    // If another thread raced us, we discard our handle (minor one-time leak).
    let _ = WIN_IOCP.set(h as usize);
}

/// Return the global IOCP handle, or null if not yet initialised.
#[cfg(windows)]
#[inline]
pub(crate) fn netpoll_iocp_handle() -> WinHandle {
    WIN_IOCP.get().map(|&h| h as WinHandle).unwrap_or(std::ptr::null_mut())
}

/// Associate `socket` (a Windows `SOCKET` value) with the global IOCP so that
/// overlapped operations on it complete via `GetQueuedCompletionStatusEx`.
///
/// Call once immediately after creating or accepting a socket.
/// Returns `true` on success.
#[cfg(windows)]
pub(crate) fn netpoll_iocp_associate(socket: usize) -> bool {
    let iocp = netpoll_iocp_handle();
    if iocp.is_null() {
        return false;
    }
    let result = unsafe {
        CreateIoCompletionPort(socket as WinHandle, iocp, socket, 0)
    };
    !result.is_null()
}

// ── Completion drain ────────────────────────────────────────────────────────

/// Drain the IOCP and return all goroutines whose operations completed.
///
/// Follows the same `timeout_ms` convention as `netpoll_wait`.
#[cfg(windows)]
unsafe fn iocp_win_wait(timeout_ms: i32) -> Vec<*mut G> {
    let iocp = netpoll_iocp_handle();
    if iocp.is_null() {
        return Vec::new();
    }

    const MAX_ENTRIES: usize = 64;
    let mut entries: [OverlappedEntry; MAX_ENTRIES] = unsafe { std::mem::zeroed() };
    let mut removed: u32 = 0;
    // timeout_ms: -1 → INFINITE (0xFFFF_FFFF), 0 → non-blocking, >0 → ms
    let timeout_dw: u32 = if timeout_ms < 0 { u32::MAX } else { timeout_ms as u32 };

    let ok = unsafe {
        GetQueuedCompletionStatusEx(
            iocp,
            entries.as_mut_ptr(),
            MAX_ENTRIES as u32,
            &mut removed,
            timeout_dw,
            0, // fAlertable = FALSE
        )
    };
    if ok == 0 || removed == 0 {
        return Vec::new();
    }

    let mut goroutines = Vec::with_capacity(removed as usize);
    for i in 0..removed as usize {
        let lp = entries[i].overlapped;
        if lp.is_null() {
            continue;
        }
        // SAFETY: `lp` points to the `overlapped` field (offset 0) of a live
        // `IocpOp` allocated by `net_windows.rs`.
        let op: *mut IocpOp = lp as *mut IocpOp;
        unsafe {
            (*op).bytes_transferred = entries[i].bytes_transferred;
            (*op).ntstatus          = entries[i].internal as u32;
            goroutines.push((*op).gp);
        }
    }
    goroutines
}
