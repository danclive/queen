use std::os::unix::io::AsRawFd;
use std::io;

use queen_io::sys::socket::{setsockopt, getsockopt};
use queen_io::tcp::TcpStream;

pub trait TcpExt: AsRawFd {
    fn set_keep_alive(&self, enabled: bool) -> io::Result<()> {
        let en: i32 = if enabled { 1 } else { 0 };

        setsockopt(self.as_raw_fd(), libc::SOL_SOCKET, libc::SO_KEEPALIVE, en)
    }

    fn keep_alive(&self) -> io::Result<bool> {
        let ret: i32 = getsockopt(self.as_raw_fd(), libc::SOL_SOCKET, libc::SO_KEEPALIVE)?;

        if ret == 1 { Ok(true) } else { Ok(false) }
    }

    /// Send first probe after `interval' seconds
    /// 设置连接上如果没有数据发送的话，多久后发送 keepalive 探测分组，单位是秒
    fn set_keep_idle(&self, secs: i32) -> io::Result<()> {
        setsockopt(self.as_raw_fd(), libc::SOL_TCP, libc::TCP_KEEPIDLE, secs)
    }

    fn keep_idle(&self) -> io::Result<i32> {
        getsockopt(self.as_raw_fd(), libc::SOL_TCP, libc::TCP_KEEPIDLE)
    }

    /// Send next probes after the specified interval. Note that we set the
    /// delay as interval / 3, as we send three probes before detecting
    /// an error (see the next setsockopt call). */
    /// 前后两次探测之间的时间间隔，单位是秒
    fn set_keep_intvl(&self, secs: i32) -> io::Result<()> {
        setsockopt(self.as_raw_fd(), libc::SOL_TCP, libc::TCP_KEEPINTVL, secs)
    }

    fn keep_intvl(&self) -> io::Result<i32> {
        getsockopt(self.as_raw_fd(), libc::SOL_TCP, libc::TCP_KEEPINTVL)
    }

    /// Consider the socket in error state after three we send three ACK
    /// probes without getting a reply. */
    /// 关闭一个非活跃连接之前的最大重试次数
    fn set_keep_cnt(&self, count: i32) -> io::Result<()> {
        setsockopt(self.as_raw_fd(), libc::SOL_TCP, libc::TCP_KEEPCNT, count)
    }

    fn keep_cnt(&self) -> io::Result<i32> {
        getsockopt(self.as_raw_fd(), libc::SOL_TCP, libc::TCP_KEEPCNT)
    }
}

impl TcpExt for TcpStream {}
