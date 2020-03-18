use std::net::{SocketAddr};
use std::time::Duration;

use crate::nson::{Message, MessageId};
use crate::net::CryptoOptions;

#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub addr: SocketAddr,
    pub crypto_options: Option<CryptoOptions>,
    pub auth_message: Option<Message>,
    pub works: usize
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:8888".parse().unwrap(),
            crypto_options: None,
            auth_message: None,
            works: 2
        }
    }
}

#[derive(Debug)]
pub struct SendOptions {
    pub label: Option<Vec<String>>,
    pub ack: bool,
    pub to: Option<Vec<MessageId>>,
    pub timeout: Duration
}

impl Default for SendOptions {
    fn default() -> Self {
        Self {
            label: None,
            ack: true,
            to: None,
            timeout: Duration::from_secs(10)
        }
    }
}

pub struct RecvOptions {
    pub label: Option<Vec<String>>,
    pub timeout: Duration
}

impl Default for RecvOptions {
    fn default() -> Self {
        Self {
            label: None,
            timeout: Duration::from_secs(10)
        }
    }
}

pub struct CallOptions {
    pub label: Option<Vec<String>>,
    pub to: Option<MessageId>,
    pub timeout: Duration
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            label: None,
            to: None,
            timeout: Duration::from_secs(10)
        }
    }
}

pub struct AddOptions {
    pub label: Option<Vec<String>>,
    pub timeout: Duration
}

impl Default for AddOptions {
    fn default() -> Self {
        Self {
            label: None,
            timeout: Duration::from_secs(10)
        }
    }
}
