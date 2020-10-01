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

impl CryptoOptions {
    pub fn new(method: Method, secret: &str) -> CryptoOptions {
        CryptoOptions {
            method,
            secret: secret.to_string()
        }
    }
}
