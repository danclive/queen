use std::time::Instant;
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
    conns: Slab<Conn<C>>,
    keep_alive: KeepAlive,
    timer: TimerFd,
    timer_id_counter: usize,
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
            conns: Slab::new(),
            keep_alive,
            timer: TimerFd::new()?,
            timer_id_counter: 0,
            wheel: Wheel::default(),
            instant: Instant::now()
        })
    }

    fn next_timer_id(&mut self) -> usize {
        self.timer_id_counter = self.timer_id_counter.wrapping_add(1);
        self.timer_id_counter
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
                        if self.dispatch_queue()? {
                            return Ok(())
                        }
                    }
                    Self::TIMER_TOKEN => {
                        self.dispatch_timer()?;
                    }
                    _ => {
                        self.dispatch_io(event)?;
                    }
                }
            }
        }
    }

    fn dispatch_queue(&mut self) -> Result<bool> {
        if let Some(packet) = self.queue.pop() {
            match packet {
                Packet::NewConn { wire, stream, codec, crypto } => {
                    let time_id = self.next_timer_id();

                    let entry1 = self.wires.vacant_entry();
                    let entry2 = self.conns.vacant_entry();

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
                        Ready::readable() | Ready::writable() | Ready::hup(),
                        EpollOpt::edge()
                    )?;

                    let conn = Conn::new(
                        token2,
                        stream,
                        codec,
                        crypto,
                        time_id,
                        self.keep_alive.clone()
                    );

                    // timer
                    self.wheel.insert((token, time_id), conn.keep_alive.idle).expect("can't insert id into wheel");

                    entry1.insert(wire);
                    entry2.insert(conn);
                                }
                Packet::Close => {
                    return Ok(true)
                }
            }
        }

        Ok(false)
    }

    fn dispatch_timer(&mut self) -> Result<()> {
        match self.timer.read() {
            Ok(_) => (),
            Err(err) => {
                if err.kind() != WouldBlock {
                    return Ok(())
                } else {
                    return Err(err.into())
                }
            }
        }

        self.instant = Instant::now();

        let list = self.wheel.tick();

        for (token, timer_id) in list {
            let index = token / 2;

            if let Some(conn) = self.conns.get_mut(index) {
                if timer_id == conn.timer_id {
                    if let Some((delay, detect)) = conn.keep_alive.tick(self.instant) {
                        self.wheel.insert((conn.token, timer_id), delay).expect("can't insert id into wheel");

                        if detect && conn.w_buffer.is_empty() {
                            log::debug!("send keep alive message, addr: {:?}", conn.stream.peer_addr()?);
                            let message = msg!{
                                CHAN: KEEP_ALIVE
                            };

                            conn.push_message(message)?;
                            conn.write(&self.wires[index])?;
                        }
                    } else {
                        self.remove_conn(index)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn dispatch_io(&mut self, event: Event) -> Result<()> {
        let token = event.token().0;

        if token % 2 == 0 {
            self.dispatch_wire(token / 2)?;
        } else {
            self.dispatch_conn(token / 2, event.readiness())?;
        }

        Ok(())
    }

    fn dispatch_wire(&mut self, index: usize) -> Result<()> {
        let mut remove = false;

        if let Some(conn) = self.conns.get_mut(index) {
            if conn.writable {
                let ret = conn.write(&self.wires[index]);
                if ret.is_err() {
                    log::debug!("conn.read: {:?}", ret);
                    remove = true;
                }
            }
        }

        if remove {
            self.remove_conn(index)?;
        }

        Ok(())
    }

    fn dispatch_conn(&mut self, index: usize, ready: Ready) -> Result<()> {
        let mut remove = ready.is_hup() || ready.is_error();

        if ready.is_readable() {
            if let Some(conn) = self.conns.get_mut(index) {
                conn.keep_alive.reset(self.instant);

                let ret = conn.read(&self.wires[index]);
                if ret.is_err() {
                    log::debug!("conn.read: {:?}", ret);
                    remove = true;
                }
            }
        }

        if ready.is_writable() {
            if let Some(conn) = self.conns.get_mut(index) {
                let ret = conn.write(&self.wires[index]);
                if ret.is_err() {
                    log::debug!("conn.read: {:?}", ret);
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

        let net = self.conns.remove(index);
        self.epoll.delete(&net.stream)?;

        Ok(())
    }
}

struct Conn<C: Codec> {
    token: usize,
    stream: TcpStream,
    writable: bool,
    r_buffer: Buffer,
    w_buffer: Buffer,
    codec: C,
    crypto: Option<Crypto>,
    timer_id: usize,
    keep_alive: KeepAlive
}

impl<C: Codec> Conn<C> {
    fn new(token: usize, stream: TcpStream, codec: C, crypto: Option<Crypto>, timer_id: usize, mut keep_alive: KeepAlive) -> Self {
        keep_alive.reset(Instant::now());

        Self {
            token,
            stream,
            writable: true,
            r_buffer: Buffer::new(),
            w_buffer: Buffer::new(),
            codec,
            crypto,
            timer_id,
            keep_alive
        }
    }

    fn read(&mut self, wire: &Wire<Message>) -> Result<()> {
        loop {
            let ret = read(&mut self.stream, &mut self.r_buffer);

            match ret {
                Ok(ret) => {
                    if let Some(bytes) = ret {
                        let mut message = self.codec.decode(&self.crypto, bytes)?;

                        if message.get_str(CHAN) == Ok(KEEP_ALIVE) {
                            log::debug!("recv keep alive message, addr: {:?}", self.stream.peer_addr()?);

                            if Code::get(&message).is_none() {
                                if self.w_buffer.is_empty() {
                                    Code::Ok.set(&mut message);

                                    self.push_message(message)?;
                                    self.write(wire)?;
                                }
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

    fn write(&mut self, wire: &Wire<Message>) -> Result<()> {
        loop {
            if !self.w_buffer.is_empty() {
                match self.stream.write(&self.w_buffer.buf[self.w_buffer.pos..]) {
                    Ok(size) => {
                        if size == 0 {
                            return Err(Error::BrokenPipe("stream.write".to_string()))
                        } else if size >= self.w_buffer.buf.len() - self.w_buffer.pos {
                            self.w_buffer.reset();
                        } else {
                            self.w_buffer.pos += size;
                        }

                        self.writable = true;
                    }
                    Err(err) => {
                        if err.kind() == WouldBlock {
                            self.writable = false;
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
                match wire.recv() {
                    Ok(message) => {
                        let bytes = self.codec.encode(&self.crypto, message)?;

                        match self.stream.write(&bytes) {
                            Ok(size) => {
                                if size == 0 {
                                    return Err(Error::BrokenPipe("stream.write".to_string()))
                                } else if size < bytes.len() {
                                    self.w_buffer.buf.extend(&bytes[..size]);
                                }

                                self.writable = true;
                            }
                            Err(err) => {
                                if err.kind() == WouldBlock {
                                    self.w_buffer.buf.extend(bytes);
                                    self.writable = false;
                                    break;
                                } else if err.kind() == Interrupted {
                                    self.w_buffer.buf.extend(bytes);
                                    continue;
                                } else {
                                    return Err(err.into())
                                }
                            }
                        }
                    },
                    Err(err) => {
                        if matches!(err, RecvError::Empty) {
                            break;
                        } else {
                            return Err(err.into())
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn push_message(&mut self, message: Message) -> Result<()> {
        let bytes = self.codec.encode(&self.crypto, message)?;
        self.w_buffer.buf.extend(bytes);

        Ok(())
    }
}

struct Buffer {
    buf: Vec<u8>,
    pos: usize
}

impl Buffer {
    fn new() -> Self {
        Buffer{
            buf: Vec::new(),
            pos: 0
        }
    }

    fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn reset(&mut self) {
        self.buf = Vec::new();
        self.pos = 0;
    }
}

fn read(stream: &mut TcpStream, buffer: &mut Buffer) -> io::Result<Option<Vec<u8>>> {
    if buffer.buf.is_empty() {
        let mut len_bytes = [0u8; 4];
        let size = stream.peek(&mut len_bytes)?;

        if size == 0 {
            return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
        } else if size < 4 {
            let size = stream.read(&mut len_bytes[..size])?;
            if size == 0 {
                return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
            }

            buffer.pos = size;
            buffer.buf = len_bytes.to_vec();

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
            buffer.pos = size;
            buffer.buf = buf;

            return Ok(None)
        }

        // assert!(size == len);
        return Ok(Some(buf))
    } else {
        if buffer.pos < 4 {
            let size = stream.read(&mut buffer.buf[buffer.pos..])?;
            if size == 0 {
                return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
            }

            buffer.pos += size;

            if buffer.pos == 4 {
                let len = read_u32(&buffer.buf, 0);
                if len < 5 || len > MAX_MESSAGE_LEN {
                    return Err(io::Error::new(InvalidData, format!("Invalid length of {}", len)))
                }

                buffer.pos = 4;
                buffer.buf.reserve(len - 4);
                unsafe { buffer.buf.set_len(len); }
            }
        } else {
            let size = stream.read(&mut buffer.buf[buffer.pos..])?;
            if size == 0 {
                return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
            }

            buffer.pos += size;

            if buffer.pos == buffer.buf.len() {
                let mut vec = Vec::new();
                mem::swap(&mut buffer.buf, &mut vec);
                buffer.pos = 0;

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
