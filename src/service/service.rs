use std::collections::HashMap;
use std::io::ErrorKind::WouldBlock;
use std::cell::Cell;

use queen_io::*;
use queen_io::channel::{self, Receiver, Sender};
use queen_io::tcp::TcpListener;
use queen_io::tcp::TcpStream;

use wire_protocol::message::Message;

use super::connection::Connection;

const SOCKET: Token = Token(0);
const CHANNEL: Token = Token(1);

pub struct Service {
    poll: Poll,
    events: Events,
    conns: HashMap<Token, Connection>,
    token_counter: Cell<usize>,
    rx_in: Receiver<ServiceEvent>,
    tx_out: Sender<ServiceEvent>,
    socket: Option<TcpListener>,
    run: bool
}

#[derive(Debug)]
pub enum ServiceEvent {
    Command(Command),
    Error(CommandError),
    Message(usize, Message)
}

#[derive(Debug)]
pub enum Command {
    Shutdown,
    SetSocket(String),
    NewConn(String)
}

#[derive(Debug)]
pub enum CommandError {
    SetSocket(io::Error),
    NewConn(io::Error)
}

impl Service {
    pub fn new() -> io::Result<(Service, Sender<ServiceEvent>, Receiver<ServiceEvent>)> {
        let (tx_in, rx_in) = channel::channel()?;
        let (tx_out, rx_out) = channel::channel()?;

        let service = Service {
            poll: Poll::new()?,
            events: Events::with_capacity(1024),
            conns: HashMap::with_capacity(128),
            token_counter: Cell::new(8),
            rx_in: rx_in,
            tx_out: tx_out,
            socket: None,
            run: true

        };

        service.poll.register(&service.rx_in, CHANNEL, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;

        Ok((service, tx_in, rx_out))
    }

    fn next_token(&self) -> Token {
        let next_token = self.token_counter.get() + 1;

        Token(self.token_counter.replace(next_token))
    }

    fn channel_process(&mut self) -> io::Result<()> {
        for event in self.rx_in.try_iter() {
            match event {
                ServiceEvent::Command(command) => {
                    match command {
                        Command::Shutdown => {
                            self.run = false
                        }
                        Command::SetSocket(addr) => {
                            let socket = match TcpListener::bind(addr) {
                                Ok(socket) => socket,
                                Err(err) => {
                                    self.tx_out.send(ServiceEvent::Error(CommandError::SetSocket(err))).unwrap();

                                    continue
                                }
                            };

                            self.poll.register(&socket, SOCKET, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;

                            self.socket = Some(socket);
                        }
                        Command::NewConn(addr) => {
                            let socket = match TcpStream::connect(addr) {
                                Ok(socket) => socket,
                                Err(err) => {
                                    self.tx_out.send(ServiceEvent::Error(CommandError::NewConn(err))).unwrap();

                                    continue
                                }
                            };

                            let token = self.next_token();

                            let conn = Connection::new(socket, token)?;
                            conn.register_insterest(&self.poll);

                            self.conns.insert(
                                token,
                                conn
                            );
                        }
                    }
                }
                ServiceEvent::Message(id, message) => {
                    if let Some(conn) = self.conns.get_mut(&Token(id)) {
                        conn.recv_message(&self.poll, message)?;
                    }
                }
                _ => ()
            }
        }

        self.poll.reregister(&self.rx_in, CHANNEL, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;

        Ok(())
    }

    fn connent_process(&mut self, event: Event, token: Token) -> io::Result<()> {
        if event.readiness().is_hup() || event.readiness().is_error() {
            self.remove_connent(token);
            return Ok(())
        }

        let mut close = false;

        if event.readiness().is_readable() {
            if let Some(conn) = self.conns.get_mut(&token) {
                close = conn.reader(&self.poll, &self.tx_out).is_err();
            }
        }

        if event.readiness().is_writable() {
            if let Some(conn) = self.conns.get_mut(&token) {
                close = conn.writer(&self.poll).is_err();
            }
        }

        if close {
            self.remove_connent(token);
        }

        Ok(())
    }

    fn remove_connent(&mut self, token: Token) {
        if let Some(conn) = self.conns.remove(&token) {
            conn.deregister(&self.poll).unwrap();
        }
    }

    fn dispatch(&mut self, event: Event) -> io::Result<()> {
        match event.token() {
            SOCKET => {
                match self.socket {
                    Some(ref socket) => {
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

                            let conn = Connection::new(socket, token)?;
                            conn.register_insterest(&self.poll);

                            self.conns.insert(
                                token,
                                conn
                            );
                        }

                        self.poll.reregister(socket, SOCKET, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;
                    }
                    None => ()
                }

                Ok(())
            }
            CHANNEL => self.channel_process(),
            token => self.connent_process(event, token)
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
