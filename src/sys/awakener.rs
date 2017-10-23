use std::os::unix::io::FromRawFd;

use libc;

use sys::cvt;
use sys::io::{self, Io, Read, Write};

use {Poll, Token, Ready, PollOpt, Evented};

pub struct Awakener {
    inner: Io
}

impl Awakener {
    pub fn new() -> io::Result<Awakener> {
        let flags = libc::EFD_CLOEXEC | libc::EFD_NONBLOCK;
        let eventfd = unsafe { cvt(libc::eventfd(0, flags))? };

        Ok(Awakener {
            inner: unsafe { Io::from_raw_fd(eventfd) }
        })
    }

    pub fn wakeup(&self) -> io::Result<()> {
        match (&self.inner).write(&[1, 0, 0, 0, 0, 0, 0, 0]) {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    pub fn cleanup(&self) {
        let mut buf = [0, 0, 0, 0, 0, 0, 0, 0];

        match (&self.inner).read(&mut buf) {
            Ok(i) if i > 0 => {},
            _ => return
        }
    }
}

impl Evented for Awakener {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        self.inner.register(poll, token, interest, opts)
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        self.inner.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.inner.deregister(poll)
    }
}
