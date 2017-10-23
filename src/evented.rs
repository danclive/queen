use sys::io;
use {Poll, Token, Ready, PollOpt};

pub trait Evented {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()>;

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()>;

    fn deregister(&self, poll: &Poll) -> io::Result<()>;
}