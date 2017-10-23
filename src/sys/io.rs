use std::fs::File;
use std::os::unix::io::{IntoRawFd, AsRawFd, FromRawFd, RawFd};
pub use std::io::{Read, Write, Result, Error, ErrorKind};

use libc;

use sys::cvt;

use {Poll, Token, Ready, PollOpt, Evented};

pub fn set_nonblock(fd: libc::c_int) -> Result<()> {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        cvt(libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK)).map(|_| ())
    }
}

pub fn set_cloexec(fd: libc::c_int) -> Result<()> {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFD);
        cvt(libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC)).map(|_| ())
    }
}

#[derive(Debug)]
pub struct Io {
    fd: File
}

impl Io {
    pub fn try_clone(&self) -> Result<Io> {
        Ok(Io { fd: self.fd.try_clone()? })
    }
}

impl FromRawFd for Io {
    unsafe fn from_raw_fd(fd: RawFd) -> Io {
        Io { fd: File::from_raw_fd(fd) }
    }
}

impl IntoRawFd for Io {
    fn into_raw_fd(self) -> RawFd {
        self.fd.into_raw_fd()
    }
}

impl AsRawFd for Io {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl Read for Io {
    fn read(&mut self, dst: &mut [u8]) -> Result<usize> {
        (&self.fd).read(dst)
    }
}

impl<'a> Read for &'a Io {
    fn read(&mut self, dst: &mut [u8]) -> Result<usize> {
        (&self.fd).read(dst)
    }
}

impl Write for Io {
    fn write(&mut self, src: &[u8]) -> Result<usize> {
        (&self.fd).write(src)
    }

    fn flush(&mut self) -> Result<()> {
        (&self.fd).flush()
    }
}

impl<'a> Write for &'a Io {
    fn write(&mut self, src: &[u8]) -> Result<usize> {
        (&self.fd).write(src)
    }

    fn flush(&mut self) -> Result<()> {
        (&self.fd).flush()
    }
}

impl Evented for Io {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> Result<()> {
        poll.inner().register(self.as_raw_fd(), token, interest, opts)
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> Result<()> {
        poll.inner().reregister(self.as_raw_fd(), token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> Result<()> {
        poll.inner().deregister(self.as_raw_fd())
    }
}
