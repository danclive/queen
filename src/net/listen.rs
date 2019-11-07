use std::io::{self};
use std::os::unix::io::{AsRawFd, RawFd};

use libc;

use queen_io::tcp::TcpListener;
use queen_io::unix::UnixListener;

use super::Addr;
use super::NetStream;

#[derive(Debug)]
pub enum Listen {
    Tcp(TcpListener),
    Uds(UnixListener)
}

const KEEP_ALIVE: i32 = 1; // 开启keepalive属性
const KEEP_IDLE: i32 = 60; // 如该连接在60秒内没有任何数据往来,则进行探测 
const KEEP_INTERVAL: i32 = 5; // 探测时发包的时间间隔为5秒
const KEEP_COUNT: i32 = 3; // 探测尝试的次数.如果第1次探测包就收到响应了,则后2次的不再发.

impl Listen {
    pub fn accept(&self) -> io::Result<(NetStream, Addr)> {
        match self {
            Listen::Tcp(tcp) => {
                tcp.accept().and_then(|(socket, addr)| {
                    socket.set_nodelay(true)?;

                    let fd = socket.as_raw_fd();

                    unsafe {
                        libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, &KEEP_ALIVE as *const i32 as *const _, 4);
                        libc::setsockopt(fd, libc::SOL_TCP, libc::TCP_KEEPIDLE, &KEEP_IDLE as *const i32 as *const _, 4);
                        libc::setsockopt(fd, libc::SOL_TCP, libc::TCP_KEEPINTVL, &KEEP_INTERVAL as *const i32 as *const _, 4);
                        libc::setsockopt(fd, libc::SOL_TCP, libc::TCP_KEEPCNT, &KEEP_COUNT as *const i32 as *const _, 4);
                    }

                    Ok((NetStream::Tcp(socket), Addr::Tcp(addr)))
                })
            }
            Listen::Uds(unix) => {
                unix.accept().and_then(|(socket, addr)| {
                    let addr = addr.as_pathname().map(|p| p.display().to_string()).unwrap_or_else(|| "unnamed".to_string());

                    Ok((NetStream::Uds(socket), Addr::Uds(addr)))
                })
            }
        }
    }
}

impl AsRawFd for Listen {
    fn as_raw_fd(&self) -> RawFd {
        match self {
            Listen::Tcp(tcp) => tcp.as_raw_fd(),
            Listen::Uds(unix) => unix.as_raw_fd()
        }
    }
}
