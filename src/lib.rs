pub mod socket;
pub mod wire;
pub mod node;
pub mod net;
pub mod port;
pub mod crypto;
pub mod dict;
pub mod util;
pub mod error;

pub use nson;

pub const MAX_MESSAGE_LEN: usize = 64 * 1024 * 1024; // 64 MB

pub use crate::socket::{Socket, Slot, Client, Hook};
pub use crate::wire::Wire;
pub use crate::node::Node;
pub use crate::port::Port;
