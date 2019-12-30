pub mod queen;
pub mod node;
pub mod port;
pub mod stream;
pub mod net;
pub mod connector;
pub mod crypto;
pub mod bus;
pub mod dict;
pub mod util;
pub mod error;

pub use nson;

pub const MAX_MESSAGE_LEN: usize = 32 * 1024 * 1024; // 32 MB

pub use crate::queen::{Queen, Session, Callback};
pub use crate::stream::Stream;
pub use crate::node::Node;
pub use crate::connector::Connector;
pub use crate::port::{Port, Recv, AsyncRecv};
