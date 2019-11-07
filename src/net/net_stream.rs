use std::io::{self, Read, Write};
use std::os::unix::io::{AsRawFd, RawFd};

use queen_io::epoll::{Epoll, Token, Ready, EpollOpt, Evented};
use queen_io::tcp::TcpStream;
use queen_io::unix::UnixStream;

#[derive(Debug)]
pub enum NetStream {
    Tcp(TcpStream),
    Uds(UnixStream)
}

impl NetStream {
    // pub fn peer_addr(&self) -> io::Result<SocketAddr> {
    //     // match self {
    //     //     NetStream::Tcp(tcp) => tcp.peer_addr(),
    //     //     NetStream::Uds(unix) => unix.peer_addr()
    //     // }
    // }
}

impl Read for NetStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            NetStream::Tcp(tcp) => tcp.read(buf),
            NetStream::Uds(unix) => unix.read(buf)
        }
    }
}

impl Write for NetStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            NetStream::Tcp(tcp) => tcp.write(buf),
            NetStream::Uds(unix) => unix.write(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            NetStream::Tcp(tcp) => tcp.flush(),
            NetStream::Uds(unix) => unix.flush()
        }
    }
}

impl AsRawFd for NetStream {
    fn as_raw_fd(&self) -> RawFd {
        match self {
            NetStream::Tcp(tcp) => tcp.as_raw_fd(),
            NetStream::Uds(unix) => unix.as_raw_fd()
        }
    }
}

impl Evented for NetStream {
    fn add(&self, epoll: &Epoll, token: Token, interest: Ready, opts: EpollOpt) -> io::Result<()> {
        match self {
            NetStream::Tcp(tcp) => tcp.add(epoll, token, interest, opts),
            NetStream::Uds(unix) => unix.add(epoll, token, interest, opts)
        }
    }

    fn modify(&self, epoll: &Epoll, token: Token, interest: Ready, opts: EpollOpt) -> io::Result<()> {
        match self {
            NetStream::Tcp(tcp) => tcp.modify(epoll, token, interest, opts),
            NetStream::Uds(unix) => unix.modify(epoll, token, interest, opts)
        }
    }

    fn delete(&self, epoll: &Epoll) -> io::Result<()> {
        match self {
            NetStream::Tcp(tcp) => tcp.delete(epoll),
            NetStream::Uds(unix) => unix.delete(epoll)
        }
    }
}
