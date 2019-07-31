use std::collections::{VecDeque, HashSet};
use std::io::{self, Read, Write, ErrorKind::{WouldBlock, BrokenPipe, InvalidData}};
use std::os::unix::io::AsRawFd;
use std::time::Duration;
use std::thread::sleep;

use queen_io::tcp::TcpStream;
use queen_io::unix::UnixStream;

use nson::{Message, msg};

use crate::poll::{poll, Ready, Events};
use crate::net::{Addr, Stream};
use crate::util::slice_msg;

pub struct Bridge {
    session_a: Session,
    session_b: Session,
    read_buffer: VecDeque<Message>,
    white_list: HashSet<String>,
    run: bool,
}

struct Session {
    conn: Option<(i32, Connection)>,
    state: SessionState,
    addr: Addr,
    auth_msg: Message
}

#[derive(Debug, Eq, PartialEq)]
enum SessionState {
    UnAuth,
    Authing,
    Authed
}

#[derive(Clone)]
pub struct LinkConfig {
    pub addr: Addr,
    pub auth_msg: Message
}

#[derive(Clone)]
pub enum ConfigAddr {
    Tcp(String),
    Unix(String)
}

pub struct BridgeConfig {
    pub addr1: Addr,
    pub auth_msg1: Message,
    pub addr2: Addr,
    pub auth_msg2: Message,
    pub white_list: HashSet<String>
}

impl Bridge {
    pub fn link(config: BridgeConfig) -> Bridge {
        let bridge = Bridge {
            session_a: Session {
                conn: None,
                state: SessionState::UnAuth,
                addr: config.addr1,
                auth_msg: config.auth_msg1
            },
            session_b: Session {
                conn: None,
                state: SessionState::UnAuth,
                addr: config.addr2,
                auth_msg: config.auth_msg2
            },
            read_buffer: VecDeque::new(),
            white_list: config.white_list,
            run: true
        };

        bridge
    }

    pub fn run(&mut self) -> io::Result<()> {
        while self.run {
            self.run_once()?;
        }

        Ok(())
    }

    pub fn run_once(&mut self) -> io::Result<()> {
        {
            macro_rules! link {
                ($session:ident) => {
                    if self.$session.conn.is_none() {
                        let conn = match self.$session.addr.connect() {
                            Ok(conn) => conn,
                            Err(err) => {
                                println!("link: {:?} err: {}", self.$session.addr, err);

                                sleep(Duration::from_secs(1));

                                return Ok(())
                            }
                        };

                        let fd = conn.fd();

                        self.$session.conn = Some((fd, conn));
                    }
                };
            }

            link!(session_a);
            link!(session_b);
        }

        {
            macro_rules! auth {
                ($session:ident) => {
                    if self.$session.state == SessionState::UnAuth {
                        let mut msg = msg!{
                            "_chan": "_auth",
                            "_brge": true
                        };

                        msg.extend(self.$session.auth_msg.clone());

                        self.$session.conn
                            .as_mut().unwrap()
                            .1.write_buffer.push_back(msg.to_vec().unwrap());

                        self.$session.state = SessionState::Authing;
                    }
                };
            }

            auth!(session_a);
            auth!(session_b);
        }

        let mut events = Events::new();

        {
            macro_rules! event_put {
                ($session:ident) => {
                    let (fd, conn) = self.$session.conn.as_ref().unwrap();

                    let mut interest = Ready::readable() | Ready::hup();

                    if !conn.write_buffer.is_empty() {
                        interest = interest | Ready::writable();
                    }

                    events.put(*fd, interest);
                };
            }
            event_put!(session_a);
            event_put!(session_b);
        }

        if poll(&mut events, Some(<Duration>::from_secs(1)))? > 0 {
            for event in &events {
                macro_rules! event {
                    ($session:ident) => {
                        if self.$session.conn.as_ref().map(|(id, _)| { *id == event.fd() }).unwrap_or(false) {
                            let readiness = event.readiness();

                            if readiness.is_hup() || readiness.is_error() {
                                self.$session.conn = None;
                                self.$session.state = SessionState::UnAuth;

                                continue;
                            }

                            if readiness.is_readable() {
                                if let Some((_, conn)) = &mut self.$session.conn {
                                    if conn.read(&mut self.read_buffer).is_err() {
                                        self.$session.conn = None;
                                        self.$session.state = SessionState::UnAuth;
                                    }

                                    if !self.read_buffer.is_empty() {
                                        self.handle_message_from_conn(stringify!($session))?;
                                    }
                                }
                            }

                            if readiness.is_writable() {
                                if let Some((_, conn)) = &mut self.$session.conn {
                                    if conn.write().is_err() {
                                        self.$session.conn = None;
                                        self.$session.state = SessionState::UnAuth;
                                    }
                                }
                            }
                        }
                    };
                }

                event!(session_a);
                event!(session_b);
            }
        }

        Ok(())
    }

    fn handle_message_from_conn(&mut self, s: &str) -> io::Result<()> {
        macro_rules! handle_message {
            ($session_a:ident, $session_b:ident) => {
                while let Some(message) = self.read_buffer.pop_front() {
                    if let Ok(chan) = message.get_str("_chan") {
                        if chan.starts_with("_") {
                            match chan {
                                "_auth" => {
                                    if let Ok(ok) = message.get_i32("ok") {
                                        if ok == 0 {

                                            self.$session_a.state = SessionState::Authed;

                                            for chan in &self.white_list {
                                                let msg = msg!{
                                                    "_chan": "_atta",
                                                    "_valu": chan
                                                };

                                                self.$session_a.conn
                                                    .as_mut().unwrap()
                                                    .1.write_buffer
                                                    .push_back(msg.to_vec().unwrap());
                                            }
                                            continue;
                                        }
                                    }

                                    self.$session_a.state = SessionState::UnAuth;
                                }
                                "_atta" => {
                                    println!("_atta: {:?}", message);
                                }
                                _ => ()
                            }
                        } else {
                            if self.$session_a.state != SessionState::Authed || self.$session_b.state != SessionState::Authed {
                                continue;
                            }

                            if let Some((_, conn)) = &mut self.$session_b.conn {            
                                conn.write_buffer.push_back(message.to_vec().unwrap());
                            }
                        }
                    }
                }

            };
        }

        if s == "session_a" {
            handle_message!(session_a, session_b);
        } else if s == "session_b" {
            handle_message!(session_b, session_a);
        }

        Ok(())
    }
}

impl Addr {
    fn connect(&self) -> io::Result<Connection> {
        match self {
            Addr::Tcp(addr) => {
                let socket = TcpStream::connect(addr)?;
                socket.set_nodelay(true)?;
                Ok(Connection::new(Stream::Tcp(socket)))
            }
            Addr::Unix(path) => {
                let socket = UnixStream::connect(path)?;
                Ok(Connection::new(Stream::Unix(socket)))
            }
        }
    }
}

struct Connection {
    stream: Stream,
    read_buffer: Vec<u8>,
    write_buffer: VecDeque<Vec<u8>>,
}

impl Connection {
    fn new(stream: Stream) -> Connection {
        let conn = Connection {
            stream,
            read_buffer: Vec::new(),
            write_buffer: VecDeque::new(),
        };

        conn
    }

    fn fd(&self) -> i32 {
        self.stream.as_raw_fd()
    }

    fn read(&mut self, read_buffer: &mut VecDeque<Message>) -> io::Result<()> {
        loop {
            let mut buf = [0; 4 * 1024];

            match self.stream.read(&mut buf) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
                    } else {
                        let messages = slice_msg(&mut self.read_buffer, &buf[..size])?;

                        for message in messages {
                            match Message::from_slice(&message) {
                                Ok(message) => read_buffer.push_back(message),
                                Err(_err) => {
                                    return Err(io::Error::new(InvalidData, "InvalidData"))
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    if let WouldBlock = err.kind() {
                        break;
                    } else {
                        return Err(err)
                    }
                }
            }
        }

        Ok(())
    }

    fn write(&mut self) -> io::Result<()> {
        while let Some(front) = self.write_buffer.front_mut() {
            match self.stream.write(front) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
                    } else if size == front.len() {
                        self.write_buffer.pop_front();
                    } else if size < front.len() {
                        // size < front.len()
                        // assert!(size > front.len());
                        *front = front[size..].to_vec();
                    }
                }
                Err(err) => {
                    if let WouldBlock = err.kind() {
                        break;
                    } else {
                        return Err(err)
                    }
                }
            }
        }

        Ok(())
    }
}
