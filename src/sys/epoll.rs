use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::time::Duration;
use std::{cmp, i32};

use libc::{self, c_int};
use libc::{EPOLLERR, EPOLLHUP};
use libc::{EPOLLET, EPOLLOUT, EPOLLIN, EPOLLPRI};
use libc::{EPOLLRDHUP, EPOLLONESHOT};

use sys::{io, cvt};

use {Token, Ready, PollOpt, Event};

static NEXT_ID: AtomicUsize = ATOMIC_USIZE_INIT;

pub struct Epoll {
    id: usize,
    epfd: RawFd
}

impl Epoll {
    pub fn new() -> io::Result<Epoll> {
        let epfd = unsafe {
            cvt(libc::epoll_create1(libc::EPOLL_CLOEXEC))?
        };

        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed) + 1;

        Ok(Epoll {
            id: id,
            epfd: epfd
        })
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn select(&self, evts: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        let timeout_ms = timeout
            .map(|to| cmp::min(millis(to), i32::MAX as u64) as i32)
            .unwrap_or(-1);

        unsafe {
            let cnt = cvt(libc::epoll_wait(
                self.epfd,
                evts.events.as_mut_ptr(),
                evts.events.capacity() as i32,
                timeout_ms    
            ))?;

            let cnt = cnt as usize;
            evts.events.set_len(cnt);

            return Ok(());
        }
    }

    pub fn register(&self, fd: RawFd, token: Token, interests: Ready, opts: PollOpt) -> io::Result<()> {
        let mut info = libc::epoll_event {
            events: ioevent_to_epoll(interests, opts),
            u64: usize::from(token) as u64
        };

        unsafe {
            cvt(libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_ADD, fd, &mut info))?;
            Ok(())
        }
    }

    pub fn reregister(&self, fd: RawFd, token: Token, interests: Ready, opts: PollOpt) -> io::Result<()> {
        let mut info = libc::epoll_event {
            events: ioevent_to_epoll(interests, opts),
            u64: usize::from(token) as u64
        };

        unsafe {
            cvt(libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_MOD, fd, &mut info))?;
            Ok(())
        }
    }

    pub fn deregister(&self, fd: RawFd) -> io::Result<()> {
        let mut info = libc::epoll_event {
            events: 0,
            u64: 0,
        };

        unsafe {
            cvt(libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_DEL, fd, &mut info))?;
            Ok(())
        }
    }
}

fn ioevent_to_epoll(interest: Ready, opts: PollOpt) -> u32 {
    let mut kind = 0;

    if interest.is_readable() {
        kind |= EPOLLIN;
    }

    if interest.is_writable() {
        kind |= EPOLLOUT;
    }

    if interest.is_hup() {
        kind |= EPOLLRDHUP;
    }

    if opts.is_edge() {
        kind |= EPOLLET;
    }

    if opts.is_oneshot() {
        kind |= EPOLLONESHOT;
    }

    if opts.is_level() {
        kind &= !EPOLLET;
    }

    kind as u32
}

impl AsRawFd for Epoll {
    fn as_raw_fd(&self) -> RawFd {
        self.epfd
    }
}

impl Drop for Epoll {
    fn drop(&mut self) {
        unsafe {
            let _ = libc::close(self.epfd);
        }
    }
}

pub struct Events {
    events: Vec<libc::epoll_event>,
}

impl Events {
    pub fn with_capacity(u: usize) -> Events {
        Events {
            events: Vec::with_capacity(u)
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.events.capacity()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    #[inline]
    pub fn get(&self, idx: usize) -> Option<Event> {
        self.events.get(idx).map(|event| {
            let epoll = event.events as c_int;
            let mut kind = Ready::empty();

            if (epoll & EPOLLIN) != 0 || (epoll & EPOLLPRI) != 0 {
                kind = kind | Ready::readable();
            }

            if (epoll & EPOLLOUT) != 0 {
                kind = kind | Ready::writable();
            }

            // EPOLLHUP - Usually means a socket error happened
            if (epoll & EPOLLERR) != 0 {
                kind = kind | Ready::error();
            }

            if (epoll & EPOLLRDHUP) != 0 || (epoll & EPOLLHUP) != 0 {
                kind = kind | Ready::hup();
            }

            let token = self.events[idx].u64;

            Event::new(kind, Token(token as usize))
        })
    }
}

const NANOS_PER_MILLI: u32 = 1_000_000;
const MILLIS_PER_SEC: u64 = 1_000;

pub fn millis(duration: Duration) -> u64 {
    let millis = (duration.subsec_nanos() + NANOS_PER_MILLI - 1) / NANOS_PER_MILLI;
    duration.as_secs().saturating_mul(MILLIS_PER_SEC).saturating_add(millis as u64)
}
