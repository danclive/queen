use std::os::unix::io::FromRawFd;

use nix;

use sys::io::{self, Io, Read, Write};

use poll::Poll;
use token::Token;
use ready::Ready;
use poll_opt::PollOpt;
use evented::Evented;

pub struct Awakener {
    inner: Io
}

impl Awakener {
    pub fn new() -> io::Result<Awakener> {
        let flags = nix::sys::eventfd::EFD_CLOEXEC | nix::sys::eventfd::EFD_NONBLOCK;

        let eventfd = nix::sys::eventfd::eventfd(0, flags).unwrap();

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
