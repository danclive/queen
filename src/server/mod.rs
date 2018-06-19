use std::io;
use std::thread;

use queen_io::*;

use service::{Service, Command};
use protocol::Message;
use util::both_queue::BothQueue;

use self::session::Session;

const MSG: Token = Token(1);
const CMD: Token = Token(2);
const TIME: Token = Token(3);

mod session;

#[allow(dead_code)]
pub struct Queen {
    poll: Poll,
    events: Events,
    msg_queue: BothQueue<(usize, Message)>,
    cmd_queue: BothQueue<Command>,
    sessions: Session
}

impl Queen {
    pub fn new() -> io::Result<Queen> {
        let mut service = Service::new()?;

        let queen = Queen {
            poll: Poll::new()?,
            events: Events::with_capacity(128),
            msg_queue: service.msg_queue().clone(),
            cmd_queue: service.cmd_queue().clone(),
            sessions: Session::new(service.msg_queue().clone(), service.cmd_queue().clone())
        };

        queen.poll.register(&queen.msg_queue.tx, MSG, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;
        queen.poll.register(&queen.cmd_queue.tx, CMD, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;

        thread::Builder::new().name("service".to_owned()).spawn(move || {
            service.run()
        }).unwrap();

        Ok(queen)
    }

    fn msg_handle(&mut self) -> io::Result<()> {
        loop {
            let msg = match self.msg_queue.tx.try_pop()? {
                Some(msg) => msg,
                None => break
            };

            self.sessions.handle(msg.0, msg.1)?;
        }

        self.poll.reregister(&self.msg_queue.tx, MSG, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;

        Ok(())
    }

    fn cmd_handle(&mut self) -> io::Result<()> {
        loop {
            let cmd = match self.cmd_queue.tx.try_pop()? {
                Some(cmd) => cmd,
                None => break
            };

            match cmd {
                Command::CloseConn { id } => {
                    self.sessions.remove_client(id)
                }
                _ => ()
            }
        }

        self.poll.reregister(&self.cmd_queue.tx, CMD, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;

        Ok(())
    }

    pub fn listen(&self, addr: &str) -> io::Result<()> {
        let cmd = Command::Listen {
            id: 0,
            addr: addr.to_owned()
        };


        self.cmd_queue.rx.push(cmd)?;

        Ok(())
    }

    pub fn link_to(&self, addr: &str) -> io::Result<()> {
        let cmd = Command::Connent {
            id: 0,
            addr: addr.to_owned()
        };

        self.cmd_queue.rx.push(cmd)?;

        Ok(())
    }

    pub fn run(&mut self) -> io::Result<()> {
        loop {

            let size = self.poll.poll(&mut self.events, None)?;

            for i in 0..size {
                let event = self.events.get(i).unwrap();

                match event.token() {
                    MSG => {
                        self.msg_handle()?
                    }
                    CMD => {
                        self.cmd_handle()?
                    }
                    TIME => {

                    }
                    _ => ()
                }
            }
        }
    }
}
