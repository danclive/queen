pub mod node;
pub mod port;
pub mod register;
pub mod net;
pub mod oneshot;
pub mod lock;
pub mod crypto;
pub mod bus;
pub mod dict;
pub mod util;
pub mod error;

pub use nson;
pub use node::Node;

pub const MAX_MESSAGE_LEN: usize = 16_777_216; // 16 MB
