use std::sync::Arc;

use sys::io;
use sys::awakener;

use poll::Poll;
use token::Token;
use ready::Ready;
use poll_opt::PollOpt;
use evented::Evented;

#[derive(Clone)]
pub struct Registration {
    pub awakener: Arc<awakener::Awakener>
}

impl Registration {
    pub fn new() -> io::Result<Registration>{
        let awakener = Arc::new(awakener::Awakener::new()?);

        let registration = Registration {
                awakener: awakener
        };

        Ok(registration)
    }

    pub fn set_readiness(&self, ready: Ready) -> io::Result<()> {
        if ready == Ready::readable() || ready == Ready::writable() {
            self.awakener.wakeup()?;
        }

        Ok(())
    }

    pub fn finish(&self) {
        self.awakener.cleanup()
    }
}

impl Evented for Registration {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        self.awakener.register(poll, token, interest, opts)
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        self.awakener.register(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.awakener.deregister(poll)
    }
}
