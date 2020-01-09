#[cfg(target_os = "linux")]
pub mod queen;
#[cfg(target_os = "linux")]
pub mod node;
#[cfg(target_os = "linux")]
pub mod stream;
#[cfg(target_os = "linux")]
pub mod bus;
pub mod net;
pub mod client;
pub mod crypto;
pub mod dict;
pub mod util;
pub mod error;

pub use nson;

pub const MAX_MESSAGE_LEN: usize = 32 * 1024 * 1024; // 32 MB

#[cfg(target_os = "linux")]
pub use crate::queen::{Queen, Session, Callback};
#[cfg(target_os = "linux")]
pub use crate::stream::Stream;
#[cfg(target_os = "linux")]
pub use crate::node::Node;
