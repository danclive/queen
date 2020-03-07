use std::os::unix::io::AsRawFd;
use std::io;

use queen_io::sys::socket::{setsockopt, getsockopt};
use queen_io::tcp::TcpStream;

use libc;

pub trait TcpExt: AsRawFd {
    fn set_keep_alive(&self, enabled: bool) -> io::Result<()> {
        let en: i32 = if enabled { 1 } else { 0 };

        setsockopt(self.as_raw_fd(), libc::SOL_SOCKET, libc::SO_KEEPALIVE, en)
    }

    fn keep_alive(&self) -> io::Result<bool> {
        let ret: i32 = getsockopt(self.as_raw_fd(), libc::SOL_SOCKET, libc::SO_KEEPALIVE)?;

        if ret == 1 { Ok(true) } else { Ok(false) }
    }

    fn set_keep_idle(&self, secs: i32) -> io::Result<()> {
        setsockopt(self.as_raw_fd(), libc::SOL_TCP, libc::TCP_KEEPIDLE, secs)
    }

    fn keep_idle(&self) -> io::Result<i32> {
        getsockopt(self.as_raw_fd(), libc::SOL_TCP, libc::TCP_KEEPIDLE)
    }

    fn set_keep_intvl(&self, secs: i32) -> io::Result<()> {
        setsockopt(self.as_raw_fd(), libc::SOL_TCP, libc::TCP_KEEPINTVL, secs)
    }

    fn keep_intvl(&self) -> io::Result<i32> {
        getsockopt(self.as_raw_fd(), libc::SOL_TCP, libc::TCP_KEEPINTVL)
    }

    fn set_keep_cnt(&self, count: i32) -> io::Result<()> {
        setsockopt(self.as_raw_fd(), libc::SOL_TCP, libc::TCP_KEEPCNT, count)
    }

    fn keep_cnt(&self) -> io::Result<i32> {
        getsockopt(self.as_raw_fd(), libc::SOL_TCP, libc::TCP_KEEPCNT)
    }
}

impl TcpExt for TcpStream {}
