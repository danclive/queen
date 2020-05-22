use queen_io::tcp::TcpStream;

use nson::Message;

pub trait Hook {
    fn enable_secure(&self) -> bool { false }

    fn accept(&self, _: &mut TcpStream) -> bool { true }

    fn hand(&self, _: &mut Message) -> bool { true }

    fn access(&self, _: &str) -> Option<String> { None }
}

impl Hook for () {}
