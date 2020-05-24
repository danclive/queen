use std::collections::VecDeque;
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

use nson::Message;

use slab::Slab;

use crate::Wire;
use crate::crypto::Crypto;
use crate::util::message::read_nonblock;
use crate::error::{Error, Result, RecvError};

use super::Codec;

pub enum Packet<C: Codec> {
    NewConn {
        wire: Wire<Message>,
        stream: TcpStream,
        codec: C,
        crypto: Option<Crypto>
    }
}

pub struct NetWork<C: Codec> {
    epoll: Epoll,
    events: Events,
    pub queue: Queue<Packet<C>>,
    wires: Slab<Wire<Message>>,
    nets: Slab<NetConn<C>>,
    pub run: Arc<AtomicBool>
}

impl<C: Codec> NetWork<C> {
    const START_TOKEN: usize = 2;
    const QUEUE_TOKEN: usize = 0;

    pub fn new(queue: Queue<Packet<C>>, run: Arc<AtomicBool>) -> Result<Self> {
        Ok(Self {
            epoll: Epoll::new()?,
            events: Events::with_capacity(1024),
            queue,
            wires: Slab::new(),
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
                Packet::NewConn { wire, stream, codec, crypto } => {
                    let entry1 = self.wires.vacant_entry();
                    let entry2 = self.nets.vacant_entry();

                    assert_eq!(entry1.key(), entry2.key());

                    let id = Self::START_TOKEN + entry1.key() * 2;
                    let id2 = Self::START_TOKEN + entry2.key() * 2 + 1;

                    self.epoll.add(
                        &wire,
                        Token(id),
                        Ready::readable(),
                        EpollOpt::level()
                    )?;

                    self.epoll.add(
                        &stream,
                        Token(id2),
                        Ready::readable() | Ready::hup(),
                        EpollOpt::edge()
                    )?;

                    entry1.insert(wire);

                    let conn = NetConn::new(id2, stream, codec, crypto);

                    entry2.insert(conn);
                }
            }
        }

        Ok(())
    }

    fn dispatch_conn(&mut self, event: Event) -> Result<()> {
        let token = event.token().0 - Self::START_TOKEN;

        if token % 2 == 0 {
            self.dispatch_wire(token / 2)?;
        } else {
            self.dispatch_stream(token / 2, event.readiness())?;
        }

        Ok(())
    }

    fn dispatch_wire(&mut self, index: usize) -> Result<()> {
        let mut remove = false;

        if let Some(wire) = self.wires.get(index) {
            match wire.recv() {
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

    fn dispatch_stream(&mut self, index: usize, ready: Ready) -> Result<()> {
        let mut remove = ready.is_hup() || ready.is_error();

        if ready.is_readable() {
            if let Some(net_conn) = self.nets.get_mut(index) {
                let ret = net_conn.read(&self.wires[index]);
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
        let wire = self.wires.remove(index);
        self.epoll.delete(&wire)?;

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
    crypto: Option<Crypto>
}

impl<C: Codec> NetConn<C> {
    fn new(id: usize, stream: TcpStream, codec: C, crypto: Option<Crypto>) -> Self {
        Self {
            id,
            stream,
            interest: Ready::readable() | Ready::hup() | Ready::error(),
            r_buffer: Vec::with_capacity(1024),
            w_buffer: VecDeque::new(),
            codec,
            crypto
        }
    }

    fn read(&mut self, wire: &Wire<Message>) -> Result<()> {
        loop {
            let ret = read_nonblock(&mut self.stream, &mut self.r_buffer);

            match ret {
                Ok(ret) => {
                    if let Some(bytes) = ret {
                        let message = self.codec.decode(&self.crypto, bytes)?;
                        let _ = wire.send(message);
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
