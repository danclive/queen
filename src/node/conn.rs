use std::collections::{VecDeque, HashMap};
use std::io::{self, Read, Write, ErrorKind::{WouldBlock, BrokenPipe, InvalidData}};
use std::usize;

use queen_io::epoll::{Epoll, Token, Ready, EpollOpt, Evented};

use nson::Message;
use nson::message_id::MessageId;

use crate::crypto::Aead;
use crate::net::{Addr, Stream};
use crate::util::slice_msg;

#[derive(Debug)]
pub struct Connection {
    pub id: usize,
    pub addr: Addr,
    stream: Stream,
    pub interest: Ready,
    read_buffer: Vec<u8>,
    write_buffer: VecDeque<Vec<u8>>,
    pub auth: bool,
    pub chans: HashMap<String, Vec<String>>, // HashMap<Chan, Vec<Label>>
    pub port_id: Option<MessageId>,
    aead: Option<Aead>
}

impl Connection {
    pub fn new(id: usize, addr: Addr, stream: Stream, aead: Option<Aead>) -> Connection {
        Connection {
            id,
            addr,
            stream,
            interest: Ready::readable() | Ready::hup(),
            read_buffer: Vec::new(),
            write_buffer: VecDeque::new(),
            auth: false,
            chans: HashMap::new(),
            port_id: None,
            aead
        }
    }

    pub fn epoll_add(&self, epoll: &Epoll) -> io::Result<()>{
        epoll.add(
            &self.stream,
            Token(self.id),
            self.interest,
            EpollOpt::edge()
        )
    }

    pub fn epoll_modify(&self, epoll: &Epoll) -> io::Result<()>{
        epoll.modify(
            &self.stream,
            Token(self.id),
            self.interest,
            EpollOpt::edge()
        )
    }

    pub fn epoll_delete(&self, epoll: &Epoll) -> io::Result<()> {
        epoll.delete(&self.stream)
    }

    pub fn read(&mut self, read_buffer: &mut VecDeque<Message>) -> io::Result<()> {
        loop {
            let mut buf = [0; 4 * 1024];

            match self.stream.read(&mut buf) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
                    } else {
                        let vec = slice_msg(&mut self.read_buffer, &buf[..size])?;

                        for mut data in vec {
                            if let Some(aead) = &mut self.aead {
                                if aead.decrypt(&mut data).is_err() {
                                    return Err(io::Error::new(InvalidData, "InvalidData"))
                                }
                            }

                            match Message::from_slice(&data) {
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

    pub fn write(&mut self) -> io::Result<()> {
        while let Some(front) = self.write_buffer.front_mut() {
            match self.stream.write(front) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
                    } else if size >= front.len() {
                        self.write_buffer.pop_front();
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

        if self.write_buffer.is_empty() {
            self.interest.remove(Ready::writable());
        } else {
            self.interest.insert(Ready::writable());
        }

        Ok(())
    }

    pub fn push_data(&mut self, mut data: Vec<u8>) {
        if let Some(aead) = &mut self.aead {
            aead.encrypt(&mut data).expect("encrypt error");
        }

        self.write_buffer.push_back(data);
        self.interest.insert(Ready::writable());
    }
}

impl Evented for Stream {
    fn add(&self, epoll: &Epoll, token: Token, interest: Ready, opts: EpollOpt) -> io::Result<()> {
        match self {
            Stream::Tcp(tcp) => epoll.add(tcp, token, interest, opts),
            Stream::Uds(unix) => epoll.add(unix, token, interest, opts)
        }
    }

    fn modify(&self, epoll: &Epoll, token: Token, interest: Ready, opts: EpollOpt) -> io::Result<()> {
        match self {
            Stream::Tcp(tcp) => epoll.modify(tcp, token, interest, opts),
            Stream::Uds(unix) => epoll.modify(unix, token, interest, opts)
        }
    }

    fn delete(&self, epoll: &Epoll) -> io::Result<()> {
        match self {
            Stream::Tcp(tcp) => epoll.delete(tcp),
            Stream::Uds(unix) => epoll.delete(unix)
        }
    }
}
