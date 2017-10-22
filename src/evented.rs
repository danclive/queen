use poll::Poll;
use token::Token;
use ready::Ready;
use poll_opt::PollOpt;
use sys::io;

pub trait Evented {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()>;

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()>;

    fn deregister(&self, poll: &Poll) -> io::Result<()>;
}