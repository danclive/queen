pub mod poll;
pub mod queen;
pub mod node;
pub mod center;
pub mod bridge;
pub mod channel;
pub mod net;
pub mod util;
pub mod error;

pub use nson;
pub use node::Node;
pub use crate::queen::{Queen, Context};

pub const MAX_MESSAGE_LEN: usize = 16777216; // 16 MB
