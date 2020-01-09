use std::collections::{VecDeque};
use std::io::{self, Write};
use std::io::ErrorKind::{WouldBlock, BrokenPipe, InvalidData, PermissionDenied};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::str::FromStr;
use std::time::Duration;

use queen_io::epoll::{Epoll, Event, Events, Token, Ready, EpollOpt};
use queen_io::queue::spsc::Queue;
use queen_io::tcp::TcpStream;

use nson::{Message, msg};

use slab::Slab;

use crate::Stream;
use crate::crypto::{Method, Crypto};
use crate::util::message::read_nonblock;
use crate::dict::*;

use super::CryptoOptions;

pub type AccessFn = Arc<Box<dyn Fn(String) -> Option<String> + Send + Sync>>;

pub enum Packet {
    NewConn {
        stream: Stream,
        net_stream: TcpStream,
        options: Option<CryptoOptions>
    },
    NewServ {
        stream: Stream,
        net_stream: TcpStream,
        access_fn: Option<AccessFn>
    }
}

pub struct NetWork {
    epoll: Epoll,
    events: Events,
    pub queue: Queue<Packet>,
    streams: Slab<StreamConn>,
    nets: Slab<NetConn>,
    pub run: Arc<AtomicBool>
}

impl NetWork {
    const START_TOKEN: usize = 2;
    const QUEUE_TOKEN: usize = 0;

    pub fn new(queue: Queue<Packet>, run: Arc<AtomicBool>) -> io::Result<Self> {
        Ok(Self {
            epoll: Epoll::new()?,
            events: Events::with_capacity(1024),
            queue,
            streams: Slab::new(),
            nets: Slab::new(),
            run
        })
    }

    pub fn run(&mut self) -> io::Result<()> {
        self.epoll.add(&self.queue, Token(Self::QUEUE_TOKEN), Ready::readable(), EpollOpt::level())?;

        while self.run.load(Ordering::Relaxed) {
            let size = self.epoll.wait(&mut self.events, Some(Duration::from_secs(10)))?;

            for i in 0..size {
                let event = self.events.get(i).unwrap();

                if event.token().0 == Self::QUEUE_TOKEN {
                    self.dispatch_queue()?;       
                } else {
                    self.dispatch_conn(event)?;
                }
            }
        }

        Ok(())
    }

    fn dispatch_queue(&mut self) -> io::Result<()> {
        if let Some(packet) = self.queue.pop() {
            match packet {
                Packet::NewConn{ stream, net_stream, options } => {
                    let entry1 = self.streams.vacant_entry();
                    let entry2 = self.nets.vacant_entry();

                    assert_eq!(entry1.key(), entry2.key());

                    let id = Self::START_TOKEN + entry1.key() * 2;
                    let id2 = Self::START_TOKEN + entry2.key() * 2 + 1;

                    self.epoll.add(&stream, Token(id), Ready::readable(), EpollOpt::level())?;
                    self.epoll.add(&net_stream, Token(id2), Ready::readable() | Ready::hup(), EpollOpt::edge())?;
                
                    entry1.insert(StreamConn::new(id, stream));

                    let mut conn = NetConn::new(id2, net_stream);

                    // handshake message
                    if let Some(options) = options {
                        let hand_message = msg!{
                            HANDSHAKE: options.method.as_str(),
                            ACCESS: options.access
                        };

                        conn.push_data(&self.epoll, hand_message)?;

                        let crypto = Crypto::new(&options.method, options.secret.as_bytes());
                        conn.crypto = Some(crypto);

                    } else {
                        let hand_message = msg!{
                            HANDSHAKE: ""
                        };

                        conn.push_data(&self.epoll, hand_message)?;
                    }

                    conn.role = Role::Conn;
                    conn.handshake = true;

                    entry2.insert(conn);
                }
                Packet::NewServ{stream, net_stream, access_fn} => {
                    let entry1 = self.streams.vacant_entry();
                    let entry2 = self.nets.vacant_entry();

                    assert_eq!(entry1.key(), entry2.key());

                    let id = Self::START_TOKEN + entry1.key() * 2;
                    let id2 = Self::START_TOKEN + entry2.key() * 2 + 1;

                    self.epoll.add(&stream, Token(id), Ready::readable(), EpollOpt::level())?;
                    self.epoll.add(&net_stream, Token(id2), Ready::readable() | Ready::hup(), EpollOpt::edge())?;
                
                    entry1.insert(StreamConn::new(id, stream));

                    let mut conn = NetConn::new(id2, net_stream);

                    conn.role = Role::Serv;
                    conn.access_fn = access_fn;

                    entry2.insert(conn);
                }
            }
        }

        Ok(())
    }

    fn dispatch_conn(&mut self, event: Event) -> io::Result<()> {
        let token = event.token().0 - Self::START_TOKEN;

        if token % 2 == 0 {
            self.dispatch_stream(token / 2)?;
        } else {
            self.dispatch_net_stream(token / 2, event.readiness())?;
        }

        Ok(())
    }

    fn dispatch_stream(&mut self, index: usize) -> io::Result<()> {
        let mut remove = false;

        if let Some(stream) = self.streams.get(index) {
            remove = stream.stream.is_close();

            if let Some(message) = stream.stream.recv() {
                self.push_data(index, message)?;
            }
        }

        if remove {
            self.remove_conn(index)?;
        }

        Ok(())
    }

    fn push_data(&mut self, index: usize, message: Message) -> io::Result<()> {
        if let Some(net_conn) = self.nets.get_mut(index) {
            net_conn.push_data(&self.epoll, message)?;
        }

        Ok(())
    }

    fn dispatch_net_stream(&mut self, index: usize, ready: Ready) -> io::Result<()> {
        let mut remove = ready.is_hup() || ready.is_error();

        if ready.is_readable() {
            if let Some(net_conn) = self.nets.get_mut(index) {
                let ret = net_conn.read(&self.streams[index]);
                remove = ret.is_err();
            }
        }

        if ready.is_writable() {
            if let Some(net_conn) = self.nets.get_mut(index) {
                remove = net_conn.write().is_err();

                if !remove {
                    self.epoll.modify(&net_conn.stream, Token(net_conn.id), net_conn.interest, EpollOpt::edge())?;
                }
            }
        }

        if remove {
            self.remove_conn(index)?;
        }

        Ok(())
    }

    fn remove_conn(&mut self, index: usize) -> io::Result<()> {
        let stream = self.streams.remove(index);
        self.epoll.delete(&stream.stream)?;

        let net = self.nets.remove(index);
        self.epoll.delete(&net.stream)?;

        Ok(())
    }
}

struct StreamConn {
    #[allow(dead_code)]
    id: usize,
    stream: Stream
}

impl StreamConn {
    fn new(id: usize, stream: Stream) -> StreamConn{
        StreamConn {
            id,
            stream
        }
    }
}

struct NetConn {
    id: usize,
    stream: TcpStream,
    interest: Ready,
    r_buffer: Vec<u8>,
    w_buffer: VecDeque<Vec<u8>>,
    handshake: bool,
    role: Role,
    access_fn: Option<AccessFn>,
    crypto: Option<Crypto>
}

#[derive(Debug)]
enum Role {
    Conn,
    Serv
}

impl NetConn {
    fn new(id: usize, stream: TcpStream) -> Self {
        Self {
            id,
            stream,
            interest: Ready::readable() | Ready::hup(),
            r_buffer: Vec::with_capacity(1024),
            w_buffer: VecDeque::new(),
            handshake: false,
            role: Role::Conn,
            access_fn: None,
            crypto: None
        }
    }

    fn read(&mut self, stream_conn: &StreamConn) -> io::Result<()> {
        if stream_conn.stream.is_full() {
            return Ok(())
        }

        loop {
            let ret = read_nonblock(&mut self.stream, &mut self.r_buffer);

            match ret {
                Ok(ret) => {
                    if let Some(mut bytes) = ret {
                        if self.handshake {
                            if let Some(crypto) = &mut self.crypto {
                                let _ = crypto.decrypt(&mut bytes).map_err(|err|
                                    io::Error::new(InvalidData, format!("{}", err)
                                ));
                            }

                            match Message::from_slice(&bytes) {
                                Ok(message) => {
                                    stream_conn.stream.send(message);
                                },
                                Err(err) => {
                                    return Err(io::Error::new(InvalidData, format!("Message::from_slice: {}", err)))
                                }
                            }
                        } else {
                            match &self.role {
                                Role::Conn => return Err(io::Error::new(InvalidData, "Role::Conn")),
                                Role::Serv => {
                                    let message =  match Message::from_slice(&bytes) {
                                        Ok(message) => {
                                            message
                                        },
                                        Err(err) => {
                                            return Err(io::Error::new(InvalidData, format!("Message::from_slice: {}", err)))
                                        }
                                    };

                                    let handshake = if let Ok(handshake) = message.get_str(HANDSHAKE) {
                                        handshake
                                    } else {
                                        return Err(io::Error::new(InvalidData, "message.get_str(HANDSHAKE)"))
                                    };

                                    if handshake == "" {
                                        if self.access_fn.is_none() {
                                            self.handshake = true;
                                        } else {
                                            return Err(io::Error::new(InvalidData, "!self.access_fn.is_none()"))
                                        }
                                    } else {
                                        if self.access_fn.is_none() {
                                            return Err(io::Error::new(InvalidData, "self.access_fn.is_none()"))
                                        };

                                        let method = if let Ok(method) = Method::from_str(handshake) {
                                            method
                                        } else {
                                            return Err(io::Error::new(InvalidData, "Method::from_str(handshake)"))
                                        };

                                        let access = if let Ok(access) = message.get_str(ACCESS) {
                                            access
                                        } else {
                                            return Err(io::Error::new(InvalidData, "message.get_str(ACCESS)"))
                                        };

                                        let secret = if let Some(secret) = self.access_fn.as_ref().unwrap()(access.to_string()) {
                                            secret
                                        } else {
                                            return Err(io::Error::new(PermissionDenied, "PermissionDenied"))
                                        };

                                        let crypto = Crypto::new(&method, secret.as_bytes());

                                        self.crypto = Some(crypto);

                                        self.handshake = true;
                                    }
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
        while let Some(front) = self.w_buffer.front_mut() {
            match self.stream.write(front) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
                    } else if size >= front.len() {
                        self.w_buffer.pop_front();
                    } else if size < front.len() {
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

        if self.w_buffer.is_empty() {
            self.interest.remove(Ready::writable());

            if self.w_buffer.capacity() > 64 {
                self.w_buffer.shrink_to_fit();
            }
        } else {
            self.interest.insert(Ready::writable());
        }

        Ok(())
    }

    fn push_data(&mut self, epoll: &Epoll, message: Message) -> io::Result<()> {
        let mut data = message.to_vec().unwrap();

        if let Some(crypto) = &mut self.crypto {
            let _ = crypto.encrypt(&mut data).map_err(|err|
                io::Error::new(InvalidData, format!("{}", err)
            ));
        }

        self.w_buffer.push_back(data);

        if !self.interest.contains(Ready::writable()) {
            self.interest.insert(Ready::writable());

            epoll.modify(&self.stream, Token(self.id), self.interest, EpollOpt::edge())?;
        }

        Ok(())
    }
}
