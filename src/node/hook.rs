use queen_io::tcp::TcpStream;

use nson::Message;

use crate::Wire;

pub trait Hook {
    fn enable_secure(&self) -> bool { false }

    fn accept(&self, _: &mut TcpStream) -> bool { true }

    fn start(&self, _: &mut Message) -> bool { true }

    fn access(&self, _: &mut Message) -> Option<String> { None }

    fn finish(&self, _: &mut Message, _: &Wire<Message>) { }
}

impl Hook for () {}
