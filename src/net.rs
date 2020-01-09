use crate::crypto::Method;

#[cfg(target_os = "linux")]
pub use network::{AccessFn, Packet, NetWork};
#[cfg(target_os = "linux")]
mod network;

#[derive(Debug, Clone)]
pub struct CryptoOptions {
    pub method: Method,
    pub access: String,
    pub secret: String
}
