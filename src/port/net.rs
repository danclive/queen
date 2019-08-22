use std::io;

use queen_io::tcp::TcpStream;
use queen_io::unix::UnixStream;

use crate::net::{Addr, Stream};

impl Addr {
    pub fn connect(&self) -> io::Result<Stream> {
        match self {
            Addr::Tcp(addr) => {
                let socket = TcpStream::connect(addr)?;
                socket.set_nodelay(true)?;
                Ok(Stream::Tcp(socket))
            }
            Addr::Uds(path) => {
                let socket = UnixStream::connect(path)?;
                Ok(Stream::Unix(socket))
            }
        }
    }
}
