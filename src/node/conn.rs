use std::collections::{VecDeque, HashSet};
use std::io::{self, Read, Write, ErrorKind::{WouldBlock, BrokenPipe, InvalidData}};
use std::usize;
use std::rc::Rc;

use queen_io::{Poll, Token, Ready, PollOpt, Evented};

use nson::Message;

use crate::net::{Addr, Stream};
use crate::util::{slice_msg, sign, verify};

#[derive(Debug)]
pub struct Connection {
    pub id: usize,
    pub addr: Addr,
    pub stream: Stream,
    pub interest: Ready,
    read_buffer: Vec<u8>,
    write_buffer: VecDeque<Vec<u8>>,
    pub auth: bool,
    pub chans: HashSet<String>,
    hmac_key: Option<Rc<String>>
}

impl Connection {
    pub fn new(id: usize, addr: Addr, stream: Stream, hmac_key: Option<Rc<String>>) -> Connection {
        Connection {
            id,
            addr,
            stream,
            interest: Ready::readable() | Ready::hup(),
            read_buffer: Vec::new(),
            write_buffer: VecDeque::new(),
            auth: false,
            chans: HashSet::new(),
            hmac_key
        }
    }

    pub fn register(&self, poll: &Poll) -> io::Result<()>{
        poll.register(
            &self.stream,
            Token(self.id),
            self.interest,
            PollOpt::edge()
        )
    }

    pub fn reregister(&self, poll: &Poll) -> io::Result<()>{
        poll.reregister(
            &self.stream,
            Token(self.id),
            self.interest,
            PollOpt::edge()
        )
    }

    pub fn deregister(&self, poll: &Poll) -> io::Result<()> {
        poll.deregister(&self.stream)
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

                        for data in vec {
                            if let Some(key) = &self.hmac_key {
                                if !verify(key.as_bytes(), &data) {
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
        if let Some(key) = &self.hmac_key {
            data = sign(key.as_bytes(), data);
        }

        self.write_buffer.push_back(data);
        self.interest.insert(Ready::writable());
    }
}

impl Evented for Stream {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        match self {
            Stream::Tcp(tcp) => poll.register(tcp, token, interest, opts),
            Stream::Unix(unix) => poll.register(unix, token, interest, opts)
        }
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        match self {
            Stream::Tcp(tcp) => poll.reregister(tcp, token, interest, opts),
            Stream::Unix(unix) => poll.reregister(unix, token, interest, opts)
        }
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        match self {
            Stream::Tcp(tcp) => poll.deregister(tcp),
            Stream::Unix(unix) => poll.deregister(unix)
        }
    }
}
