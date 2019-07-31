use std::net::{SocketAddr, ToSocketAddrs};
use std::io::{self, Read, Write};
use std::os::unix::io::{AsRawFd, RawFd};

use queen_io::tcp::TcpStream;
use queen_io::unix::UnixStream;

#[derive(Debug, Clone)]
pub enum Addr {
    Tcp(SocketAddr),
    Unix(String)
}

impl Addr {
    pub fn tcp<A: ToSocketAddrs>(addr: A) -> io::Result<Addr> {
        let mut addr = addr.to_socket_addrs()?;
        Ok(Addr::Tcp(addr.next().expect("can't paser addr!")))
    }

    pub fn unix(path: String) -> Addr {
        Addr::Unix(path)
    }

    pub fn is_tcp(&self) -> bool {
        if let Addr::Tcp(_) = self {
            return true
        }

        return false
    }
}

#[derive(Debug)]
pub enum Stream {
    Tcp(TcpStream),
    Unix(UnixStream)
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(tcp) => tcp.read(buf),
            Stream::Unix(unix) => unix.read(buf)
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(tcp) => tcp.write(buf),
            Stream::Unix(unix) => unix.write(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Tcp(tcp) => tcp.flush(),
            Stream::Unix(unix) => unix.flush()
        }
    }
}

impl AsRawFd for Stream {
    fn as_raw_fd(&self) -> RawFd {
        match self {
            Stream::Tcp(tcp) => tcp.as_raw_fd(),
            Stream::Unix(unix) => unix.as_raw_fd()
        }
    }
}


