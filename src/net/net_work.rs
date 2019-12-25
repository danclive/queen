use std::collections::{VecDeque};
use std::io::{self, Write, Read, ErrorKind::{WouldBlock, BrokenPipe, InvalidData}};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use queen_io::epoll::{Epoll, Event, Events, Token, Ready, EpollOpt};
use queen_io::queue::spsc::Queue;

use nson::Message;

use slab::Slab;

use crate::Stream;
use crate::crypto::{Method, Aead};
use crate::net::{NetStream};
use crate::util::message::slice_msg;

pub enum Packet {
    NewConn(Stream, NetStream, Option<(Method, String)>)
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

    pub fn new(queue: Queue<Packet>, run: Arc<AtomicBool>) -> io::Result<NetWork> {
        Ok(NetWork {
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
            let size = self.epoll.wait(&mut self.events, None)?;

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
                Packet::NewConn(stream, net_stream, crypto) => {
                    let entry1 = self.streams.vacant_entry();
                    let entry2 = self.nets.vacant_entry();

                    assert_eq!(entry1.key(), entry2.key());

                    let id = Self::START_TOKEN + entry1.key() * 2;
                    let id2 = Self::START_TOKEN + entry2.key() * 2 + 1;

                    self.epoll.add(&stream, Token(id), Ready::readable(), EpollOpt::level())?;
                    self.epoll.add(&net_stream, Token(id2), Ready::readable() | Ready::hup(), EpollOpt::edge())?;
                
                    entry1.insert(StreamConn::new(id, stream));

                    let mut aead = None;

                    if let Some((method, key)) = crypto {
                        aead = Some(Aead::new(&method, key.as_bytes()));
                    }

                    entry2.insert(NetConn::new(id2, net_stream, aead));
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
            self.remove_conn(index);
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
                remove = net_conn.read(&self.streams[index]).is_err();
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
            self.remove_conn(index);
        }

        Ok(())
    }

    fn remove_conn(&mut self, index: usize) {
        self.streams.remove(index);
        self.nets.remove(index);
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
    stream: NetStream,
    interest: Ready,
    r_buffer: Vec<u8>,
    w_buffer: VecDeque<Vec<u8>>,
    crypto: Option<Crypto>
}

struct Crypto {
    aead: Aead,
    r_nonce: [u8; Aead::NONCE_LEN],
    w_nonce: [u8; Aead::NONCE_LEN]
}

impl NetConn {
    fn new(id: usize, stream: NetStream, aead: Option<Aead>) -> NetConn {
        NetConn {
            id,
            stream,
            interest: Ready::readable() | Ready::hup(),
            r_buffer: Vec::new(),
            w_buffer: VecDeque::new(),
            crypto: aead.and_then(|aead| {
                Some(Crypto {
                    aead,
                    r_nonce: Aead::init_nonce(),
                    w_nonce: Aead::init_nonce()
                })
            })
        }
    }

    fn read(&mut self, stream_conn: &StreamConn) -> io::Result<()> {
        if stream_conn.stream.is_full() {
            return Ok(())
        }

        loop {
            let mut buf = [0; 4 * 1024];

            match self.stream.read(&mut buf) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
                    } else {
                        let vec = slice_msg(&mut self.r_buffer, &buf[..size])?;

                        for mut data in vec {
                            if let Some(crypto) = &mut self.crypto {
                                Aead::increase_nonce(&mut crypto.r_nonce);

                                if crypto.aead.decrypt(crypto.r_nonce, &mut data).is_err() {
                                    return Err(io::Error::new(InvalidData, "InvalidData"))
                                }
                            }

                            match Message::from_slice(&data) {
                                Ok(message) => {
                                    stream_conn.stream.send(message);
                                },
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
            Aead::increase_nonce(&mut crypto.w_nonce);

            crypto.aead.encrypt(crypto.w_nonce, &mut data).expect("encrypt error");
        }

        self.w_buffer.push_back(data);

        if !self.interest.contains(Ready::writable()) {
            self.interest.insert(Ready::writable());

            epoll.modify(&self.stream, Token(self.id), self.interest, EpollOpt::edge())?;
        }

        Ok(())
    }
}
