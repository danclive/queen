use std::net::{SocketAddr, ToSocketAddrs};
use std::io::{self, Read, Write};
use std::os::unix::io::{AsRawFd, RawFd};

use libc;

use queen_io::tcp::TcpListener;
use queen_io::unix::UnixListener;
use queen_io::epoll::{Epoll, Token, Ready, EpollOpt};
use queen_io::tcp::TcpStream;
use queen_io::unix::UnixStream;

#[derive(Debug, Clone)]
pub enum Addr {
    Tcp(SocketAddr),
    Uds(String)
}

impl Addr {
    pub fn tcp<A: ToSocketAddrs>(addr: A) -> io::Result<Addr> {
        let mut addr = addr.to_socket_addrs()?;
        Ok(Addr::Tcp(addr.next().expect("can't paser addr!")))
    }

    pub fn uds(path: String) -> Addr {
        Addr::Uds(path)
    }

    pub fn is_tcp(&self) -> bool {
        if let Addr::Tcp(_) = self {
            return true
        }

        false
    }

    pub fn bind(&self) -> io::Result<Listen> {
        match self {
            Addr::Tcp(addr) => Ok(Listen::Tcp(TcpListener::bind(addr)?)),
            Addr::Uds(addr) => Ok(Listen::Uds(UnixListener::bind(addr)?))
        }
    }

    pub fn connect(&self) -> io::Result<Stream> {
        match self {
            Addr::Tcp(addr) => {
                let socket = TcpStream::connect(addr)?;
                socket.set_nodelay(true)?;
                Ok(Stream::Tcp(socket))
            }
            Addr::Uds(path) => {
                let socket = UnixStream::connect(path)?;
                Ok(Stream::Uds(socket))
            }
        }
    }
}

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
    pub fn epoll_add(&self, epoll: &Epoll, token: Token, interest: Ready, opts: EpollOpt) -> io::Result<()> {
        match self {
            Listen::Tcp(tcp) => epoll.add(tcp, token, interest, opts),
            Listen::Uds(unix) => epoll.add(unix, token, interest, opts)
        }
    }

    pub fn accept(&self) -> io::Result<(Stream, Addr)> {
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

                    Ok((Stream::Tcp(socket), Addr::Tcp(addr)))
                })
            }
            Listen::Uds(unix) => {
                unix.accept().and_then(|(socket, addr)| {
                    let addr = addr.as_pathname().map(|p| p.display().to_string()).unwrap_or_else(|| "unnamed".to_string());

                    Ok((Stream::Uds(socket), Addr::Uds(addr)))
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

#[derive(Debug)]
pub enum Stream {
    Tcp(TcpStream),
    Uds(UnixStream)
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(tcp) => tcp.read(buf),
            Stream::Uds(unix) => unix.read(buf)
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(tcp) => tcp.write(buf),
            Stream::Uds(unix) => unix.write(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Tcp(tcp) => tcp.flush(),
            Stream::Uds(unix) => unix.flush()
        }
    }
}

impl AsRawFd for Stream {
    fn as_raw_fd(&self) -> RawFd {
        match self {
            Stream::Tcp(tcp) => tcp.as_raw_fd(),
            Stream::Uds(unix) => unix.as_raw_fd()
        }
    }
}
