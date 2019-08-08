use std::io;

use queen_io::tcp::TcpStream;
use queen_io::unix::UnixStream;

use crate::net::{Addr, Stream};

use super::conn::Connection;

impl Addr {
    pub fn connect(&self) -> io::Result<Connection> {
        match self {
            Addr::Tcp(addr) => {
                let socket = TcpStream::connect(addr)?;
                socket.set_nodelay(true)?;
                Ok(Connection::new(Stream::Tcp(socket)))
            }
            Addr::Uds(path) => {
                let socket = UnixStream::connect(path)?;
                Ok(Connection::new(Stream::Unix(socket)))
            }
        }
    }
}
