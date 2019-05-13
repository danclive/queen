use std::fmt;
use std::os::unix::io::RawFd;

use libc::{POLLERR, POLLHUP};
use libc::{POLLIN, POLLOUT, POLLPRI};

use super::Ready;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct Event {
    fd: RawFd,
    kind: Ready,
}

impl Event {
    #[inline]
    pub fn new(fd: RawFd, readiness: Ready) -> Event {
        Event {
            fd,
            kind: readiness
        }
    }

    #[inline]
    pub fn fd(self) -> RawFd {
        self.fd
    }

    #[inline]
    pub fn readiness(self) -> Ready {
        self.kind
    }
}

#[derive(Default)]
pub struct Events {
    pub events: Vec<libc::pollfd>
}

impl Events {
    #[inline]
    pub fn new() -> Events {
        Events {
            events: Vec::new()
        }
    }

    #[inline]
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
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    #[inline]
    pub fn put(&mut self, fd: RawFd, interest: Ready) {
        let pollfd = libc::pollfd {
            fd,
            events: ioevent_to_poll(interest),
            revents: 0
        };

        self.events.push(pollfd);
    }

    #[inline]
    pub fn clear(&mut self) {
        self.events.clear();
    }

    #[inline]
    pub fn get(&self, idx: usize) -> Option<Event> {
        self.events.get(idx).map(|event| {
            let revents = event.revents;
            let mut kind = Ready::empty();

            if (revents & POLLIN) != 0 || (revents & POLLPRI) != 0 {
                kind = kind | Ready::readable();
            }

            if (revents & POLLOUT) != 0 {
                kind = kind | Ready::writable();
            }

            if (revents & POLLERR) != 0 {
                kind = kind | Ready::error();
            }

            if (revents & POLLHUP) != 0 {
                kind = kind | Ready::hup();
            }

            Event::new(event.fd, kind)
        })
    }

    pub fn iter(&self) -> Iter {
        Iter {
            inner: self,
            pos: 0
        }
    }
}

impl fmt::Debug for Events {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Events")
            .field("len", &self.len())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct Iter<'a> {
    inner: &'a Events,
    pos: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        let ret = self.inner.get(self.pos);
        self.pos += 1;
        ret
    }
}

impl<'a> IntoIterator for &'a Events {
    type Item = Event;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug)]
pub struct IntoIter {
    inner: Events,
    pos: usize
}

impl Iterator for IntoIter {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        let ret = self.inner.get(self.pos);
        self.pos += 1;
        ret
    }
}

impl IntoIterator for Events {
    type Item = Event;
    type IntoIter = IntoIter;

    fn into_iter(self) -> self::IntoIter {
        IntoIter {
            inner: self,
            pos: 0
        }
    }
}

fn ioevent_to_poll(interest: Ready) -> i16 {
    let mut kind = 0;

    if interest.is_readable() {
        kind |= POLLIN;
    }

    if interest.is_writable() {
        kind |= POLLOUT;
    }

    if interest.is_hup() {
        kind |= POLLHUP;
    }

    kind
}
