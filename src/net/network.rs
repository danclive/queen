use std::collections::VecDeque;
use std::str::FromStr;
use std::time::Duration;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};
use std::io::{
    self, Write,
    ErrorKind::{WouldBlock, BrokenPipe, InvalidData, PermissionDenied}
};

use queen_io::{
    epoll::{Epoll, Event, Events, Token, Ready, EpollOpt},
    queue::spsc::Queue,
    tcp::TcpStream
};

use nson::{Message, msg};

use slab::Slab;

use crate::Stream;
use crate::crypto::{Method, Crypto};
use crate::util::message::read_nonblock;
use crate::dict::*;
use crate::error::ErrorCode;

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

                    self.epoll.add(
                        &stream,
                        Token(id),
                        Ready::readable(),
                        EpollOpt::level()
                    )?;

                    self.epoll.add(
                        &net_stream,
                        Token(id2),
                        Ready::readable() | Ready::hup(),
                        EpollOpt::edge()
                    )?;
                
                    entry1.insert(StreamConn::new(id, stream));

                    let mut conn = NetConn::new(id2, net_stream);

                    // handshake message
                    if let Some(options) = options {
                        let hand_message = msg!{
                            CHAN: HANDSHAKE,
                            METHOD: options.method.as_str(),
                            ACCESS: options.access
                        };

                        conn.push_data(&self.epoll, hand_message)?;

                        let crypto = Crypto::new(&options.method, options.secret.as_bytes());
                        conn.crypto = Some(crypto);

                    } else {
                        let hand_message = msg!{
                            CHAN: HANDSHAKE
                        };

                        conn.push_data(&self.epoll, hand_message)?;
                    }

                    // conn.is_server = false;
                    // conn.handshake = false;

                    entry2.insert(conn);
                }
                Packet::NewServ{stream, net_stream, access_fn} => {
                    let entry1 = self.streams.vacant_entry();
                    let entry2 = self.nets.vacant_entry();

                    assert_eq!(entry1.key(), entry2.key());

                    let id = Self::START_TOKEN + entry1.key() * 2;
                    let id2 = Self::START_TOKEN + entry2.key() * 2 + 1;

                    self.epoll.add(
                        &stream,
                        Token(id),
                        Ready::readable(),
                        EpollOpt::level()
                    )?;

                    self.epoll.add(
                        &net_stream,
                        Token(id2),
                        Ready::readable() | Ready::hup(),
                        EpollOpt::edge()
                    )?;

                    entry1.insert(StreamConn::new(id, stream));

                    let mut conn = NetConn::new(id2, net_stream);

                    conn.is_server = true;
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
                let ret = net_conn.read(&self.epoll, &self.streams[index]);
                if ret.is_err() {
                    log::debug!("net_conn.read: {:?}", ret);
                    remove = true;
                }
            }
        }

        if ready.is_writable() {
            if let Some(net_conn) = self.nets.get_mut(index) {
                let ret = net_conn.write();
                if ret.is_err() {
                    log::debug!("net_conn.write: {:?}", ret);
                    remove = true;
                }

                if !remove {
                    self.epoll.modify(
                        &net_conn.stream,
                        Token(net_conn.id),
                        net_conn.interest,
                        EpollOpt::edge()
                    )?;
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
    is_server: bool,
    access_fn: Option<AccessFn>,
    crypto: Option<Crypto>
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
            is_server: false,
            access_fn: None,
            crypto: None
        }
    }

    fn read(&mut self, epoll: &Epoll, stream_conn: &StreamConn) -> io::Result<()> {
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
                            match Message::from_slice(&bytes) {
                                Ok(mut message) => {
                                    let chan = match message.get_str(CHAN) {
                                        Ok(chan) => chan,
                                        Err(_) => {
                                            return Err(io::Error::new(InvalidData, "message.get_str(CHAN)"))
                                        }
                                    };

                                    if chan != HANDSHAKE {
                                        return Err(io::Error::new(InvalidData, "chan != HANDSHAKE"))
                                    }

                                    if self.is_server {
                                        if let Ok(method) = message.get_str(METHOD) {
                                            let method = if let Ok(method) = Method::from_str(method) {
                                                method
                                            } else {
                                                return Err(io::Error::new(InvalidData, "Method::from_str(handshake)"))
                                            };

                                            let access = if let Ok(access) = message.get_str(ACCESS) {
                                                access
                                            } else {
                                                return Err(io::Error::new(InvalidData, "message.get_str(ACCESS)"))
                                            };

                                            let secret = if let Some(access_fn) = &self.access_fn {
                                                if let Some(secret) = access_fn(access.to_string()) {
                                                    secret
                                                } else {
                                                    return Err(io::Error::new(PermissionDenied, "PermissionDenied"))
                                                }
                                            } else {
                                                return Err(io::Error::new(PermissionDenied, "PermissionDenied"))
                                            };

                                            self.handshake = true;

                                            ErrorCode::OK.insert_message(&mut message);

                                            self.push_data(epoll, message)?;

                                            let crypto = Crypto::new(&method, secret.as_bytes());
                                            self.crypto = Some(crypto);
                                        } else {
                                            self.handshake = true;

                                            ErrorCode::OK.insert_message(&mut message);

                                            self.push_data(epoll, message)?;
                                        }
                                    } else {
                                        if message.get_i32(OK) == Ok(0) {
                                            self.handshake = true;
                                        } else {
                                            return Err(io::Error::new(InvalidData, "message.get_i32(OK) == Ok(0)"))
                                        }
                                    }
                                },
                                Err(err) => {
                                    return Err(io::Error::new(InvalidData, format!("Message::from_slice: {}", err)))
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

            epoll.modify(
                &self.stream,
                Token(self.id),
                self.interest,
                EpollOpt::edge()
            )?;
        }

        Ok(())
    }
}
