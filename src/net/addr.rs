use crate::net::Listen;
use crate::net::NetStream;
use std::net::{SocketAddr, ToSocketAddrs};
use std::io::{self};

use queen_io::tcp::TcpListener;
use queen_io::unix::UnixListener;

use queen_io::tcp::TcpStream;
use queen_io::unix::UnixStream;

use nson::{Message, msg};

use crate::dict::*;

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

    pub fn connect(&self) -> io::Result<NetStream> {
        match self {
            Addr::Tcp(addr) => {
                let socket = TcpStream::connect(addr)?;
                socket.set_nodelay(true)?;
                Ok(NetStream::Tcp(socket))
            }
            Addr::Uds(path) => {
                let socket = UnixStream::connect(path)?;
                Ok(NetStream::Uds(socket))
            }
        }
    }

    pub fn to_message(&self) -> Message {
        match self {
            Addr::Tcp(addr) => {
                msg!{
                    ADDR_TYPE: "TCP",
                    ADDR: addr.to_string()
                }
            }
            Addr::Uds(addr) => {
                msg!{
                    ADDR_TYPE: "UDS",
                    ADDR: addr
                }
            }
        }

    }
}
