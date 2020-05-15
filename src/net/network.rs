use std::collections::VecDeque;
use std::str::FromStr;
use std::time::Duration;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};
use std::io::{
    Write,
    ErrorKind::{WouldBlock, Interrupted}
};

use queen_io::{
    epoll::{Epoll, Event, Events, Token, Ready, EpollOpt},
    queue::mpsc::Queue,
    tcp::TcpStream
};

use nson::{Message, msg};

use slab::Slab;

use crate::Stream;
use crate::crypto::{Method, Crypto};
use crate::util::message::read_nonblock;
use crate::dict::*;
use crate::error::{ErrorCode, Error, Result, RecvError};

use super::CryptoOptions;
use super::Codec;

pub type AccessFn = Arc<Box<dyn Fn(String) -> Option<String> + Send + Sync>>;

pub enum Packet {
    NewConn {
        stream: Stream<Message>,
        net_stream: TcpStream,
        options: Option<CryptoOptions>
    },
    NewServ {
        stream: Stream<Message>,
        net_stream: TcpStream,
        access_fn: Option<AccessFn>
    }
}

pub struct NetWork<C: Codec> {
    epoll: Epoll,
    events: Events,
    pub queue: Queue<Packet>,
    streams: Slab<Stream<Message>>,
    nets: Slab<NetConn<C>>,
    pub run: Arc<AtomicBool>
}

impl<C: Codec> NetWork<C> {
    const START_TOKEN: usize = 2;
    const QUEUE_TOKEN: usize = 0;

    pub fn new(queue: Queue<Packet>, run: Arc<AtomicBool>) -> Result<Self> {
        Ok(Self {
            epoll: Epoll::new()?,
            events: Events::with_capacity(1024),
            queue,
            streams: Slab::new(),
            nets: Slab::new(),
            run
        })
    }

    pub fn run(&mut self) -> Result<()> {
        self.epoll.add(&self.queue, Token(Self::QUEUE_TOKEN), Ready::readable(), EpollOpt::level())?;

        while self.run.load(Ordering::Relaxed) {
            let size = match self.epoll.wait(&mut self.events, Some(Duration::from_secs(10))) {
                Ok(size) => size,
                Err(err) => {
                    if err.kind() == Interrupted {
                        continue;
                    } else {
                        return Err(err.into())
                    }
                }
            };

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

    fn dispatch_queue(&mut self) -> Result<()> {
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

                    entry1.insert(stream);

                    let mut conn = NetConn::new(id2, net_stream);

                    // hand message
                    if let Some(options) = options {
                        let hand_message = msg!{
                            CHAN: HAND,
                            METHOD: options.method.as_str(),
                            ACCESS: options.access
                        };

                        conn.push_data(&self.epoll, hand_message)?;

                        let crypto = Crypto::new(&options.method, options.secret.as_bytes());
                        conn.crypto = Some(crypto);

                    } else {
                        let hand_message = msg!{
                            CHAN: HAND
                        };

                        conn.push_data(&self.epoll, hand_message)?;
                    }

                    // conn.is_server = false;
                    // conn.hand = false;

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
                        Ready::readable() | Ready::hup() | Ready::error(),
                        EpollOpt::edge()
                    )?;

                    entry1.insert(stream);

                    let mut conn = NetConn::new(id2, net_stream);

                    conn.is_server = true;
                    conn.access_fn = access_fn;

                    entry2.insert(conn);
                }
            }
        }

        Ok(())
    }

    fn dispatch_conn(&mut self, event: Event) -> Result<()> {
        let token = event.token().0 - Self::START_TOKEN;

        if token % 2 == 0 {
            self.dispatch_stream(token / 2)?;
        } else {
            self.dispatch_net_stream(token / 2, event.readiness())?;
        }

        Ok(())
    }

    fn dispatch_stream(&mut self, index: usize) -> Result<()> {
        let mut remove = false;

        if let Some(stream) = self.streams.get(index) {
            match stream.recv() {
                Ok(message) => {
                    if let Some(net_conn) = self.nets.get_mut(index) {
                        net_conn.push_data(&self.epoll, message)?;
                    }
                }
                Err(err) => {
                    if !matches!(err, RecvError::Empty) {
                        remove = true
                    }
                }
            }
        }

        if remove {
            self.remove_conn(index)?;
        }

        Ok(())
    }

    fn dispatch_net_stream(&mut self, index: usize, ready: Ready) -> Result<()> {
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
            }
        }

        if remove {
            self.remove_conn(index)?;
        }

        Ok(())
    }

    fn remove_conn(&mut self, index: usize) -> Result<()> {
        let stream = self.streams.remove(index);
        self.epoll.delete(&stream)?;

        let net = self.nets.remove(index);
        self.epoll.delete(&net.stream)?;

        Ok(())
    }
}

struct NetConn<C: Codec> {
    id: usize,
    stream: TcpStream,
    interest: Ready,
    r_buffer: Vec<u8>,
    w_buffer: VecDeque<Vec<u8>>,
    codec: C,
    hand: bool,
    is_server: bool,
    access_fn: Option<AccessFn>,
    crypto: Option<Crypto>
}

impl<C: Codec> NetConn<C> {
    fn new(id: usize, stream: TcpStream) -> Self {
        Self {
            id,
            stream,
            interest: Ready::readable() | Ready::hup() | Ready::error(),
            r_buffer: Vec::with_capacity(1024),
            w_buffer: VecDeque::new(),
            codec: C::new(),
            hand: false,
            is_server: false,
            access_fn: None,
            crypto: None
        }
    }

    fn read(&mut self, epoll: &Epoll, stream: &Stream<Message>) -> Result<()> {
        loop {
            let ret = read_nonblock(&mut self.stream, &mut self.r_buffer);

            match ret {
                Ok(ret) => {
                    if let Some(bytes) = ret {
                        if self.hand {
                            let message = self.codec.decode(&self.crypto, bytes)?;
                            let _ = stream.send(message);
                        } else {
                            let mut message = self.codec.decode(&None, bytes)?;

                            let chan = match message.get_str(CHAN) {
                                Ok(chan) => chan,
                                Err(_) => {
                                    return Err(Error::InvalidData("message.get_str(CHAN)".to_string()))
                                }
                            };

                            if chan != HAND {
                                return Err(Error::InvalidData("chan != HAND".to_string()))
                            }

                            if self.is_server {
                                if let Ok(method) = message.get_str(METHOD) {
                                    let method = if let Ok(method) = Method::from_str(method) {
                                        method
                                    } else {
                                        return Err(Error::InvalidData("Method::from_str(hand)".to_string()))
                                    };

                                    let access = if let Ok(access) = message.get_str(ACCESS) {
                                        access
                                    } else {
                                        return Err(Error::InvalidData("message.get_str(ACCESS)".to_string()))
                                    };

                                    let secret = if let Some(access_fn) = &self.access_fn {
                                        if let Some(secret) = access_fn(access.to_string()) {
                                            secret
                                        } else {
                                            return Err(Error::PermissionDenied("access_fn".to_string()))
                                        }
                                    } else {
                                        return Err(Error::PermissionDenied("access_fn".to_string()))
                                    };

                                    {
                                        let mut attr = stream.attr();
                                        attr.insert(ACCESS, access);
                                        attr.insert(SECRET, &secret);
                                    }

                                    self.hand = true;

                                    ErrorCode::OK.insert(&mut message);

                                    self.push_data(epoll, message)?;

                                    let crypto = Crypto::new(&method, secret.as_bytes());
                                    self.crypto = Some(crypto);
                                } else {
                                    if self.access_fn.is_some() {
                                        return Err(Error::PermissionDenied("access_fn".to_string()))
                                    }

                                    self.hand = true;

                                    ErrorCode::OK.insert(&mut message);

                                    self.push_data(epoll, message)?;
                                }
                            } else if message.get_i32(OK) == Ok(0) {
                                self.hand = true;
                            } else {
                                 return Err(Error::InvalidData("message.get_i32(OK) == Ok(0)".to_string()))
                            }
                        }
                    }
                }
                Err(err) => {
                    if err.kind() == WouldBlock {
                        break;
                    } else if err.kind() == Interrupted {
                        continue;
                    } else {
                        return Err(err.into())
                    }
                }
            }
        }

        Ok(())
    }

    fn write(&mut self) -> Result<()> {
        while let Some(front) = self.w_buffer.front_mut() {
            match self.stream.write(front) {
                Ok(size) => {
                    if size == 0 {
                        return Err(Error::BrokenPipe("NetConn.write".to_string()))
                    } else if size >= front.len() {
                        self.w_buffer.pop_front();
                    } else if size < front.len() {
                        // assert!(size > front.len());
                        *front = front[size..].to_vec();
                    }
                }
                Err(err) => {
                    if err.kind() == WouldBlock {
                        break;
                    } else if err.kind() == Interrupted {
                        continue;
                    } else {
                        return Err(err.into())
                    }
                }
            }
        }

        if self.w_buffer.is_empty() {
            self.interest.remove(Ready::writable());

            if self.w_buffer.capacity() > 64 {
                self.w_buffer.shrink_to_fit();
            }
        }

        Ok(())
    }

    fn push_data(&mut self, epoll: &Epoll, message: Message) -> Result<()> {
        let bytes = self.codec.encode(&self.crypto, message)?;

        self.w_buffer.push_back(bytes);

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
