use nson::Message;

use crate::Queen;
use crate::net::Addr;
use crate::crypto::Method;

pub enum Connector {
    Net(Addr, Option<(Method, String)>),
    Queen(Queen, Message)
}
