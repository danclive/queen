use std::net::{ToSocketAddrs, SocketAddr};
use std::io::{self, Error, ErrorKind};

use crate::crypto::Method;

pub use codec::{Codec, NsonCodec};
pub use network::{Packet, NetWork};
pub use keepalive::KeepAlive;

mod codec;
mod network;
mod keepalive;
pub mod tcp_ext;

#[derive(Debug, Clone)]
pub struct CryptoOptions {
    pub method: Method,
    pub secret: String
}

pub fn parse_addr<A: ToSocketAddrs>(addr: A) -> io::Result<SocketAddr> {
    addr.to_socket_addrs()?.next().ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Address is not valid"))
}
