//! Network poller — integrates non-blocking I/O with the goroutine scheduler.
//!
//! Ported from `runtime/netpoll_epoll.go` (Linux) and
//! `runtime/netpoll_kqueue.go` (macOS).
//!
//! ## Architecture
//!
//! A goroutine that would block on a non-blocking socket calls
//! [`netpoll_arm`] with its file descriptor and the current `*mut G` pointer,
//! then calls `gopark` to voluntarily deschedule itself.
//!
//! A background call to [`netpoll_wait`] (from `findrunnable` or `sysmon`)
//! retrieves the set of file descriptors that became ready and returns the
//! goroutines that were waiting on them.  The caller is responsible for calling
//! [`goready`][super::park::goready] on each returned goroutine.
//!
//! ## Platform support
//!
//! | Platform    | Backend  |
//! |-------------|----------|
//! | Linux       | `epoll`  |
//! | macOS       | `kqueue` |
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

/// The process-wide epoll / kqueue file descriptor.
static POLL_FD: AtomicI32 = AtomicI32::new(-1);

/// Initialise the global netpoll fd.  Idempotent — subsequent calls are no-ops.
///
/// On Windows, netpoll is not implemented (epoll/kqueue are Unix-specific).
/// `POLL_FD` remains `-1` and all `netpoll_wait` calls return an empty vec.
pub(crate) fn netpoll_init() {
    // Windows: no epoll/kqueue backend — leave POLL_FD at -1.
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
}

// ---------------------------------------------------------------------------
// netpoll_arm — register a goroutine for fd readiness notification
// ---------------------------------------------------------------------------

/// Register `fd` with the netpoll backend; wake `gp` when the fd is ready.
///
/// `mode` is a bitmask of [`POLL_READ`] and/or [`POLL_WRITE`].
///
/// `gp` must be the goroutine that is about to call `gopark`; it must remain
/// alive until [`netpoll_unarm`] or [`netpoll_wait`] removes it.
///
/// # Safety
/// `gp` must point to a live `G` that is about to be parked.
pub(crate) unsafe fn netpoll_arm(fd: RawFd, mode: u32, gp: *mut G) {
    let pfd = POLL_FD.load(Acquire);
    if pfd < 0 {
        // Netpoll not initialised (e.g. called before schedinit) — ignore.
        return;
    }

    with_reg(|reg| {
        reg.insert(fd, (GRaw(gp as usize), mode));
    });

    unsafe { poll_add(pfd, fd, mode) };
}

// ---------------------------------------------------------------------------
// netpoll_unarm — deregister a file descriptor
// ---------------------------------------------------------------------------

/// Remove `fd` from the netpoll backend.  Safe to call even if `fd` was never
/// armed.
pub(crate) fn netpoll_unarm(fd: RawFd) {
    let pfd = POLL_FD.load(Acquire);
    with_reg(|reg| { reg.remove(&fd); });
    if pfd >= 0 {
        unsafe { poll_del(pfd, fd) };
    }
}

// ---------------------------------------------------------------------------
// netpoll_wait — collect goroutines whose fds are ready
// ---------------------------------------------------------------------------

/// Poll for ready file descriptors and return the corresponding goroutines.
///
/// `timeout_ms < 0`  → block indefinitely.
/// `timeout_ms == 0` → non-blocking (used by `findrunnable`).
/// `timeout_ms > 0`  → block for up to `timeout_ms` milliseconds (used by
///                     `sysmon`).
///
/// The returned goroutines have been removed from the registration table;
/// the caller must call [`goready`][super::park::goready] on each one.
///
/// # Safety
/// Must not be called concurrently with itself on the same `POLL_FD`.
pub(crate) unsafe fn netpoll_wait(timeout_ms: i32) -> Vec<*mut G> {
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
                // Remove from the OS poll set to avoid spurious wakeups.
                unsafe { poll_del(pfd, *fd) };
            }
        }
    });
    goroutines
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
// Windows stubs — no epoll/kqueue; netpoll is a no-op
// ---------------------------------------------------------------------------
// POLL_FD stays -1; netpoll_wait always returns empty; goroutines that need
// I/O readiness must use platform threads or async runtimes on Windows.

#[cfg(windows)]
fn create_poll_fd() -> RawFd { -1 }

#[cfg(windows)]
unsafe fn poll_add(_pfd: RawFd, _fd: RawFd, _mode: u32) {}

#[cfg(windows)]
unsafe fn poll_del(_pfd: RawFd, _fd: RawFd) {}

#[cfg(windows)]
unsafe fn poll_wait(_pfd: RawFd, _timeout_ms: i32) -> Vec<RawFd> { Vec::new() }
