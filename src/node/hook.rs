use queen_io::net::tcp::TcpStream;

use nson::{Message, MessageId};

use crate::Wire;

pub trait Hook: Send + 'static {
    fn enable_secure(&self) -> bool { false }

    fn accept(&self, _: &mut TcpStream) -> bool { true }

    fn start(&self, _slot_id: MessageId, _: &mut Message) -> bool { true }

    fn access(&self, _slot_id: MessageId, _: &mut Message) -> Option<String> { None }

    fn finish(&self, _slot_id: MessageId, _: &mut Message, _: &Wire<Message>) { }
}

pub struct NonHook;

impl Hook for NonHook {}
impl Hook for () {}
