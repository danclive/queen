use std::time::Instant;
use std::collections::VecDeque;
use std::time::Duration;
use std::io::{
    self,
    Write,
    Read,
    ErrorKind::{WouldBlock, Interrupted, InvalidData, BrokenPipe}
};
use std::mem;

use queen_io::{
    epoll::{Epoll, Event, Events, Token, Ready, EpollOpt},
    queue::mpsc::Queue,
    net::tcp::TcpStream,
    plus::slab::Slab
};
use queen_io::sys::timerfd::{TimerFd, TimerSpec, SetTimeFlags};

use nson::{Message, msg};

use crate::Wire;
use crate::crypto::Crypto;
use crate::error::{Error, Result, RecvError, Code};
use crate::dict::*;
use crate::timer::wheel::Wheel;
use crate::MAX_MESSAGE_LEN;

use super::Codec;
use super::KeepAlive;

pub enum Packet<C: Codec> {
    NewConn {
        wire: Wire<Message>,
        stream: TcpStream,
        codec: C,
        crypto: Option<Crypto>
    },
    Close
}

pub struct NetWork<C: Codec> {
    epoll: Epoll,
    events: Events,
    pub queue: Queue<Packet<C>>,
    wires: Slab<Wire<Message>>,
    nets: Slab<NetConn<C>>,
    keep_alive: KeepAlive,
    timer: TimerFd,
    time_id_counter: usize,
    wheel: Wheel<(usize, usize)>,
    instant: Instant,
}

impl<C: Codec> NetWork<C> {
    const QUEUE_TOKEN: usize = usize::MAX;
    const TIMER_TOKEN: usize = usize::MAX - 1;

    pub fn new(queue: Queue<Packet<C>>, keep_alive: KeepAlive) -> Result<Self> {
        Ok(Self {
            epoll: Epoll::new()?,
            events: Events::with_capacity(1024),
            queue,
            wires: Slab::new(),
            nets: Slab::new(),
            keep_alive,
            timer: TimerFd::new()?,
            time_id_counter: 0,
            wheel: Wheel::default(),
            instant: Instant::now()
        })
    }

    fn next_time_id(&mut self) -> usize {
        self.time_id_counter = self.time_id_counter.wrapping_add(1);
        self.time_id_counter
    }

    pub fn run(&mut self) -> Result<()> {
        self.epoll.add(&self.queue, Token(Self::QUEUE_TOKEN), Ready::readable(), EpollOpt::level())?;
        self.epoll.add(&self.timer, Token(Self::TIMER_TOKEN), Ready::readable(), EpollOpt::edge())?;

        let timerspec = TimerSpec {
            interval: Duration::new(1, 0),
            value: Duration::new(1, 0)
        };

        self.timer.settime(timerspec, SetTimeFlags::Default)?;

        loop {
            let size = match self.epoll.wait(&mut self.events, None) {
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

                match event.token().0 {
                    Self::QUEUE_TOKEN => {
                        if let Some(packet) = self.queue.pop() {
                            match packet {
                                Packet::NewConn { wire, stream, codec, crypto } => {
                                    let time_id = self.next_time_id();

                                    let entry1 = self.wires.vacant_entry();
                                    let entry2 = self.nets.vacant_entry();

                                    assert_eq!(entry1.key(), entry2.key());

                                    let token = entry1.key() * 2;
                                    let token2 = token + 1;

                                    self.epoll.add(
                                        &wire,
                                        Token(token),
                                        Ready::readable(),
                                        EpollOpt::edge()
                                    )?;

                                    self.epoll.add(
                                        &stream,
                                        Token(token2),
                                        Ready::readable() | Ready::hup(),
                                        EpollOpt::edge()
                                    )?;

                                    entry1.insert(wire);

                                    let conn = NetConn::new(token2, stream, codec, crypto, time_id, self.keep_alive.clone());
                                    // timer
                                    self.wheel.insert((token, time_id), conn.keep_alive.idle).expect("can't insert id into wheel");

                                    entry2.insert(conn);
                                }
                                Packet::Close => {
                                    return Ok(())
                                }
                            }
                        }
                    }
                    Self::TIMER_TOKEN => {
                        match self.timer.read() {
                            Ok(_) => (),
                            Err(err) => {
                                if err.kind() == WouldBlock {
                                    continue;
                                } else {
                                    return Err(err.into())
                                }
                            }
                        }

                        self.instant = Instant::now();

                        let list = self.wheel.tick();

                        for (token, time_id) in list {
                            let index = token / 2;
                            if let Some(net_conn) = self.nets.get_mut(index) {
                                if time_id == net_conn.time_id {
                                    if let Some((delay, detect)) = net_conn.keep_alive.tick(self.instant) {
                                        self.wheel.insert((net_conn.token, time_id), delay).expect("can't insert id into wheel");

                                        if detect {
                                            log::debug!("send keep alive message, addr: {:?}", net_conn.stream.peer_addr()?);
                                            let message = msg!{
                                                CHAN: KEEP_ALIVE
                                            };

                                            net_conn.push_data(message)?;
                                            net_conn.want_write(&self.epoll)?;
                                        }
                                    } else {
                                        self.remove_conn(index)?;
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        self.dispatch(event)?;
                    }
                }
            }
        }
    }

    fn dispatch(&mut self, event: Event) -> Result<()> {
        let token = event.token().0;

        if token % 2 == 0 {
            self.dispatch_wire(token / 2)?;
        } else {
            self.dispatch_stream(token / 2, event.readiness())?;
        }

        Ok(())
    }

    fn dispatch_wire(&mut self, index: usize) -> Result<()> {
        if let Some(_wire) = self.wires.get(index) {
            if let Some(net_conn) = self.nets.get_mut(index) {
                net_conn.want_write(&self.epoll)?;
            }
        }

        Ok(())
    }

    fn dispatch_stream(&mut self, index: usize, ready: Ready) -> Result<()> {
        let mut remove = ready.is_hup() || ready.is_error();

        if ready.is_readable() {
            if let Some(net_conn) = self.nets.get_mut(index) {
                let ret = net_conn.read(&self.epoll, &self.wires[index], self.instant);
                if ret.is_err() {
                    log::debug!("net_conn.read: {:?}", ret);
                    remove = true;
                }
            }
        }

        if ready.is_writable() {
            if let Some(wire) = self.wires.get(index) {
                if let Some(net_conn) = self.nets.get_mut(index) {

                    if !net_conn.w_buffer.is_empty() {
                        let ret = net_conn.write(&self.epoll);
                        if ret.is_err() {
                            log::debug!("net_conn.write: {:?}", ret);
                            remove = true;
                        }
                    } else {
                        match wire.recv() {
                            Ok(message) => {
                                net_conn.push_data(message)?;

                                let ret = net_conn.write(&self.epoll);
                                if ret.is_err() {
                                    log::debug!("net_conn.write: {:?}", ret);
                                    remove = true;
                                }
                            }
                            Err(err) => {
                                if matches!(err, RecvError::Empty) {
                                    if net_conn.w_buffer.is_empty() {
                                        net_conn.interest.remove(Ready::writable());
                                    }
                                } else {
                                    remove = true
                                }
                            }
                        }
                    }
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
    token: usize,
    stream: TcpStream,
    interest: Ready,
    r_buffer: (usize, Vec<u8>), // offset, buffer
    w_buffer: VecDeque<(usize, Vec<u8>)>, // offset, buffer
    codec: C,
    crypto: Option<Crypto>,
    time_id: usize,
    keep_alive: KeepAlive
}

impl<C: Codec> NetConn<C> {
    fn new(token: usize, stream: TcpStream, codec: C, crypto: Option<Crypto>, time_id: usize, mut keep_alive: KeepAlive) -> Self {
        keep_alive.reset(Instant::now());

        Self {
            token,
            stream,
            interest: Ready::readable() | Ready::hup() | Ready::error(),
            r_buffer: (0, Vec::new()),
            w_buffer: VecDeque::new(),
            codec,
            crypto,
            time_id,
            keep_alive
        }
    }

    fn read(&mut self, epoll: &Epoll, wire: &Wire<Message>, now: Instant) -> Result<()> {
        self.keep_alive.reset(now);

        loop {
            let ret = read(&mut self.stream, &mut self.r_buffer);

            match ret {
                Ok(ret) => {
                    if let Some(bytes) = ret {
                        let mut message = self.codec.decode(&self.crypto, bytes)?;

                        if message.get_str(CHAN) == Ok(KEEP_ALIVE) {
                            log::debug!("recv keep alive message, addr: {:?}", self.stream.peer_addr()?);

                            if Code::get(&message).is_none() {
                                Code::Ok.set(&mut message);

                                self.push_data(message)?;
                                self.want_write(epoll)?;
                            }

                            continue
                        }

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

    fn write(&mut self, epoll: &Epoll) -> Result<()> {
        while let Some((index, front)) = self.w_buffer.front_mut() {
            match self.stream.write(&front[*index..]) {
                Ok(size) => {
                    if size == 0 {
                        return Err(Error::BrokenPipe("NetConn.write".to_string()))
                    } else if size >= front.len() - *index {
                        self.w_buffer.pop_front();
                    } else {
                        *index += size;
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
            if self.w_buffer.capacity() > 64 {
                self.w_buffer.shrink_to_fit();
            }
        }

        if self.interest.contains(Ready::writable()) {
            epoll.modify(
                &self.stream,
                Token(self.token),
                self.interest,
                EpollOpt::edge()
            )?;
        }

        Ok(())
    }

    fn want_write(&mut self, epoll: &Epoll) -> Result<()> {
        if !self.interest.contains(Ready::writable()) {
            self.interest.insert(Ready::writable());

            epoll.modify(
                &self.stream,
                Token(self.token),
                self.interest,
                EpollOpt::edge()
            )?;
        }

        Ok(())
    }

    fn push_data(&mut self, message: Message) -> Result<()> {
        let bytes = self.codec.encode(&self.crypto, message)?;
        self.w_buffer.push_back((0, bytes));

        Ok(())
    }
}

fn read(stream: &mut TcpStream, buffer: &mut (usize, Vec<u8>)) -> io::Result<Option<Vec<u8>>> {
    if buffer.1.is_empty() {
        let mut len_bytes = [0u8; 4];
        let size = stream.peek(&mut len_bytes)?;

        if size == 0 {
            return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
        } else if size < 4 {
            let size = stream.read(&mut len_bytes[..size])?;
            if size == 0 {
                return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
            }

            buffer.0 = size;
            buffer.1 = len_bytes.to_vec();

            return Ok(None)
        }

        let len = u32::from_le_bytes(len_bytes) as usize;
        if len < 5 || len > MAX_MESSAGE_LEN {
            return Err(io::Error::new(InvalidData, format!("Invalid length of {}", len)))
        }

        // let mut buf = vec![0u8; len];
        let mut buf = Vec::with_capacity(len);
        unsafe { buf.set_len(len); }

        let size = stream.read(&mut buf)?;

        if size == 0 {
            return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
        } else if size < len {
            buffer.0 = size;
            buffer.1 = buf;

            return Ok(None)
        }

        // assert!(size == len);
        return Ok(Some(buf))
    } else {
        if buffer.0 < 4 {
            let size = stream.read(&mut buffer.1[buffer.0..])?;
            if size == 0 {
                return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
            }

            buffer.0 += size;

            if buffer.0 == 4 {
                let len = read_u32(&buffer.1, 0);
                if len < 5 || len > MAX_MESSAGE_LEN {
                    return Err(io::Error::new(InvalidData, format!("Invalid length of {}", len)))
                }

                buffer.0 = 4;
                buffer.1.reserve(len - 4);
                unsafe { buffer.1.set_len(len); }
            }
        } else {
            let size = stream.read(&mut buffer.1[buffer.0..])?;
            if size == 0 {
                return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
            }

            buffer.0 += size;

            if buffer.0 == buffer.1.len() {
                let mut vec = Vec::new();
                mem::swap(&mut buffer.1, &mut vec);
                buffer.0 = 0;

                return Ok(Some(vec))
            }
        }
    }

    return Ok(None)
}

fn read_u32(buf: &[u8], start: usize) -> usize {
    (
        u32::from(buf[start]) |
        u32::from(buf[start + 1]) << 8 |
        u32::from(buf[start + 2]) << 16 |
        u32::from(buf[start + 3]) << 24
    ) as usize
}
