pub mod queen;
pub mod queen2;
pub mod node;
pub mod stream;
pub mod net;
pub mod port;
pub mod crypto;
pub mod dict;
pub mod util;
pub mod error;

pub use nson;

pub const MAX_MESSAGE_LEN: usize = 64 * 1024 * 1024; // 64 MB

pub use crate::queen::{Queen, Sessions, Session, Hook};
pub use crate::stream::Stream;
pub use crate::node::Node;
pub use crate::port::Port;
