use queen_io::tcp::TcpStream;

use crate::error::{Result, Error};

pub trait Hook {
    fn enable_secure(&self) -> bool { false }

    fn accept(&self, _: &mut TcpStream) -> bool { true }

    fn access(&self, _: &str) -> Result<String> {
        Err(Error::PermissionDenied("".to_string()))
    }
}

impl Hook for () {}
