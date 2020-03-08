use std::net::{ToSocketAddrs, SocketAddr};
use std::io::{self, Error, ErrorKind};

use crate::crypto::Method;

#[cfg(target_os = "linux")]
pub use network::{AccessFn, Packet, NetWork};
#[cfg(target_os = "linux")]
mod network;
#[cfg(target_os = "linux")]
pub mod tcp_ext;

#[derive(Debug, Clone)]
pub struct CryptoOptions {
    pub method: Method,
    pub access: String,
    pub secret: String
}

pub fn parse_addr<A: ToSocketAddrs>(addr: A) -> io::Result<SocketAddr> {
    addr.to_socket_addrs()?.next().ok_or(Error::new(ErrorKind::InvalidInput, "Address is not valid"))
}
