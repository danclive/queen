use std::collections::HashMap;
use std::io;
use std::io::ErrorKind::WouldBlock;
use std::cell::Cell;
use std::error::Error;

use queen_io::Poll;
use queen_io::Token;
use queen_io::Ready;
use queen_io::PollOpt;
use queen_io::Evented;
use queen_io::Events;
use queen_io::Event;
use queen_io::tcp::TcpListener;
use queen_io::tcp::TcpStream;

use protocol::Message;
use util::both_queue::BothQueue;

use self::connection::Connection;

pub mod connection;

const SOCKET: Token = Token(0);
const MSG: Token = Token(1);
const CMD: Token = Token(2);

pub struct Service {
    poll: Poll,
    events: Events,
    conns: HashMap<Token, Connection>,
    token_counter: Cell<u32>,
    msg_queue: BothQueue<(usize, Message)>,
    cmd_queue: BothQueue<Command>,
    socket: Option<TcpListener>,
    run: bool
}

#[derive(Debug, Clone)]
pub enum Command {
    Shoutdown,
    CloseConn {
        id: usize
    },
    Listen {
        id: usize,
        addr: String
    },
    ListenReply {
        id: usize,
        addr: String,
        success: bool,
        msg: String
    },
    Connent {
        id: usize,
        addr: String,
    },
    ConnentReply {
        id: usize,
        token: usize,
        addr: String,
        success: bool,
        msg: String
    }

}

impl Service {
    pub fn new() -> io::Result<Service> {
        let msg_queue = BothQueue::new(1024)?;
        let cmd_queue = BothQueue::new(1024)?;

        let service = Service {
            poll: Poll::new()?,
            events: Events::with_capacity(256),
            conns: HashMap::with_capacity(128),
            token_counter: Cell::new(8),
            msg_queue,
            cmd_queue,
            socket: None,
            run: true

        };

        service.poll.register(&service.msg_queue.rx, MSG, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;
        service.poll.register(&service.cmd_queue.rx, CMD, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;

        Ok(service)
    }

    pub fn msg_queue(&self) -> &BothQueue<(usize, Message)> {
        &self.msg_queue
    }

    pub fn cmd_queue(&self) -> &BothQueue<Command> {
        &self.cmd_queue
    }

    fn next_token(&self) -> Token {
        let next_token = self.token_counter.get() + 1;
        Token(self.token_counter.replace(next_token) as usize)
    }
    fn msg_handle(&mut self) -> io::Result<()> {
        // loop {
        //     let msg = match self.msg_queue.rx.try_pop()? {
        //         Some(msg) => msg,
        //         None => break
        //     };

        //     if let Some(conn) = self.conns.get_mut(&Token(msg.0)) {
        //         conn.set_message(&self.poll, msg.1)?;
        //     }
        // }
        while let Some(msg) = self.msg_queue.rx.try_pop()? {
            if let Some(conn) = self.conns.get_mut(&Token(msg.0)) {
                conn.set_message(&self.poll, &msg.1)?;
            }
        }

        self.poll.reregister(&self.msg_queue.rx, MSG, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;

        Ok(())
    }

    fn cmd_handle(&mut self) -> io::Result<()> {
        while let Some(cmd) = self.cmd_queue.rx.try_pop()? {
            match cmd {
                Command::Shoutdown => {
                    self.run = false
                }
                Command::CloseConn { id } => {
                    self.remove_connent(Token(id))?;
                }
                Command::Listen { id, addr } => {
                    let socket = match TcpListener::bind(addr.clone()) {
                        Ok(socket) => socket,
                        Err(err) => {
                            self.cmd_queue.tx.push(
                                Command::ListenReply {
                                    id,
                                    addr,
                                    success: false,
                                    msg: err.description().to_owned()
                                }
                            )?;

                            break;
                        }
                    };

                    self.poll.register(&socket, SOCKET, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;

                    self.socket = Some(socket);

                    self.cmd_queue.tx.push(
                        Command::ListenReply {
                            id,
                            addr,
                            success: true,
                            msg: String::default()
                        }
                    )?;
                }
                Command::Connent { id, addr } => {
                    let socket = match TcpStream::connect(addr.clone()) {
                        Ok(socket) => socket,
                        Err(err) => {
                            self.cmd_queue.tx.push(
                                Command::ConnentReply {
                                    id,
                                    token: 0,
                                    addr,
                                    success: false,
                                    msg: err.description().to_owned()
                                }
                            )?;

                            break;
                        }
                    };

                    socket.set_nodelay(true)?;

                    let token = self.next_token();

                    let conn = Connection::new(socket, token)?;
                    conn.register_insterest(&self.poll);

                    self.conns.insert(token, conn);

                    self.cmd_queue.tx.push(
                        Command::ConnentReply {
                            id,
                            token: token.into(),
                            addr,
                            success: true,
                            msg: String::default()
                        }
                    )?;
                }
                _ => ()
            }
        }

        self.poll.reregister(&self.cmd_queue.rx, CMD, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;

        Ok(())
    }

    fn connect_handle(&mut self, event: Event, token: Token) -> io::Result<()> {
        if event.readiness().is_hup() || event.readiness().is_error() {
            self.remove_connent(token)?;
            return Ok(())
        }

        let mut close = false;

        if event.readiness().is_readable() {
            if let Some(conn) = self.conns.get_mut(&token) {
                close = conn.reader(&self.poll, &self.msg_queue).is_err();
            }
        }

        if event.readiness().is_writable() {
            if let Some(conn) = self.conns.get_mut(&token) {
                close = conn.writer(&self.poll).is_err();
            }
        }

        if close {
            self.remove_connent(token)?;
        }

        Ok(())
    }

    fn remove_connent(&mut self, token: Token) -> io::Result<()> {
        if let Some(conn) = self.conns.remove(&token) {
            conn.deregister(&self.poll).unwrap();
            self.cmd_queue.tx.push(
                Command::CloseConn { id: token.into() }
            )?;
        }

        Ok(())
    }

    fn dispatch(&mut self, event: Event) -> io::Result<()> {
        match event.token() {
            SOCKET => {
                if let Some(ref socket) = self.socket {
                    loop {
                        let socket = match socket.accept().map(|s| s.0) {
                            Ok(socket) => socket,
                            Err(err) => {
                                if let WouldBlock = err.kind() {
                                    break;
                                } else {
                                    return Err(err)
                                }
                            }
                        };

                        let token = self.next_token();

                        socket.set_nodelay(true)?;

                        let conn = Connection::new(socket, token)?;
                        conn.register_insterest(&self.poll);

                        self.conns.insert(token, conn);
                    }

                    self.poll.reregister(socket, SOCKET, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;
                }
 
                Ok(())
            }
            MSG => self.msg_handle(),
            CMD => self.cmd_handle(),
            token => self.connect_handle(event, token)
        }
    }

    fn run_once(&mut self) -> io::Result<()> {
        let size = self.poll.poll(&mut self.events, None)?;

        for i in 0..size {
            let event = self.events.get(i).unwrap();
            self.dispatch(event)?;
        }

        Ok(())
    }

    pub fn run(&mut self) -> io::Result<()> {
        while self.run {
            self.run_once()?;
        }

        Ok(())
    }
}
