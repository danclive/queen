use queen_io::net::tcp::TcpStream;

use nson::Message;

use crate::Wire;

pub trait Hook {
    fn enable_secure(&self) -> bool { false }

    fn accept(&self, _: &mut TcpStream) -> bool { true }

    fn start(&self, _: &mut Message) -> bool { true }

    fn access(&self, _: &mut Message) -> Option<String> { None }

    fn finish(&self, _: &mut Message, _: &Wire<Message>) { }
}

pub struct NonHook;

impl Hook for NonHook {}
impl Hook for () {}
