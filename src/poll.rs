use std::time::Duration;
use std::fmt;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;

use sys::{self, io};
use {Token, Ready, PollOpt, Events, Evented};

pub struct Poll {
    epoll: sys::Epoll
}

impl Poll {
    pub fn new() -> io::Result<Poll> {
        is_send::<Poll>();
        is_sync::<Poll>();

        Ok(Poll {
            epoll: sys::Epoll::new()?
        })
    }

    pub fn poll(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<usize> {
        self.epoll.select(&mut events.inner, timeout)?;
        Ok(events.len())
    }

    pub fn register<E: ?Sized>(&self, handle: &E, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()>
        where E: Evented
    {
        validate_args(token, interest)?;

        // Register interests for this socket
        handle.register(self, token, interest, opts)?;

        Ok(())
    }

    pub fn reregister<E: ?Sized>(&self, handle: &E, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()>
        where E: Evented
    {
        validate_args(token, interest)?;

        // Register interests for this socket
        handle.reregister(self, token, interest, opts)?;

        Ok(())
    }

    pub fn deregister<E: ?Sized>(&self, handle: &E) -> io::Result<()>
        where E: Evented
    {
        // Deregister interests for this socket
        handle.deregister(self)?;

        Ok(())
    }

    pub fn inner(&self) -> &sys::Epoll {
        &self.epoll
    }
}

impl AsRawFd for Poll {
    fn as_raw_fd(&self) -> RawFd {
        self.epoll.as_raw_fd()
    }
}

impl fmt::Debug for Poll {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Poll")
    }
}

fn validate_args(_token: Token, interest: Ready) -> io::Result<()> {
    if !interest.is_readable() && !interest.is_writable() {
        return Err(io::Error::new(io::ErrorKind::Other, "interest must include readable or writable"));
    }

    Ok(())
}

fn is_send<T: Send>() {}
fn is_sync<T: Sync>() {}
