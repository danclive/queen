use std::io;
use std::io::{Read, Write};
use std::io::ErrorKind::WouldBlock;
use std::mem;

use queen_io::tcp::TcpStream;
use queen_io::Poll;
use queen_io::Token;
use queen_io::Ready;
use queen_io::PollOpt;
use queen_io::Evented;

use protocol::Message;
use util::bufstream::Stream;
use util::both_queue::BothQueue;

pub struct Connection {
    socket: TcpStream,
    token: Token,
    interest: Ready,
    stream: Stream
}

impl Connection {
    pub fn new(socket: TcpStream, token: Token) -> io::Result<Connection> {
        let conn = Connection {
            socket: socket,
            token: token,
            interest: Ready::readable() | Ready::hup(),
            stream: Stream::new()
        };

        Ok(conn)
    }

    pub fn reader(&mut self, poll: &Poll, msg_queue: &BothQueue<(usize, Message)>) -> io::Result<()> {
        self.interest.remove(Ready::readable());

        loop {
            let mut buf = [0; 1024];

            match self.socket.read(&mut buf) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "ConnectionAborted"))
                    } else {
                        self.stream.reader.write(&buf[0..size])?;
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

        loop {
            if self.stream.reader.len() < mem::size_of::<u32>() {
                return Ok(())
            }

            let message_length = {
                let mut buf = [0; 4];
                self.stream.reader.seek(&mut buf)?;

                (
                    ((buf[0] as u32) << 0) |
                    ((buf[1] as u32) << 8) |
                    ((buf[2] as u32) << 16) |
                    ((buf[3] as u32) << 24)
                ) as usize
            };

            if self.stream.reader.len() < message_length {
                break;
            }

            let message = Message::read(&mut self.stream.reader)?;

            msg_queue.tx.push((self.token.into(), message))?
        }

        if self.stream.reader.len() < 16 * 1024 * 1024 {
            self.interest.insert(Ready::readable());
        }

        if self.stream.writer.len() > 0 {
            self.interest.insert(Ready::writable());
        }

        self.reregister_insterest(poll);

        Ok(())
    }

    pub fn writer(&mut self, poll: &Poll) -> io::Result<()>{
        self.interest.remove(Ready::writable());

        loop {
            let mut buf = [0; 1024];

            let pos = self.stream.writer.seek(&mut buf)?;

            match self.socket.write(&buf[0..pos]) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "ConnectionAborted"))
                    } else {
                        self.stream.writer.set_position(size);

                        if self.stream.writer.is_empty() {
                            break;
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

        if self.stream.reader.len() < 16 * 1024 * 1024 {
            self.interest.insert(Ready::readable());
        }

        if self.stream.writer.len() > 0 {
            self.interest.insert(Ready::writable());
        }

        self.reregister_insterest(poll);

        Ok(())
    }

    pub fn register_insterest(&self, poll: &Poll) {
        self.register(
            poll,
            self.token,
            self.interest,
            PollOpt::edge() | PollOpt::oneshot()
        ).unwrap();
    }

    pub fn reregister_insterest(&mut self, poll: &Poll) {
        self.reregister(
            poll,
            self.token,
            self.interest,
            PollOpt::edge() | PollOpt::oneshot()
        ).unwrap();
    }
/*
    pub fn get_message(&mut self, msg_queue: &BothQueue<(usize, Message)>) -> io::Result<()> {
        loop {
            if self.stream.reader.len() < mem::size_of::<u32>() {
                return Ok(())
            }

            let message_length = {
                let mut buf = [0; 4];
                self.stream.reader.seek(&mut buf)?;

                (
                    ((buf[0] as u32) << 0) |
                    ((buf[1] as u32) << 8) |
                    ((buf[2] as u32) << 16) |
                    ((buf[3] as u32) << 24)
                ) as usize
            };

            if self.stream.reader.len() < message_length {
                return Ok(())
            }

            let message = Message::read(&mut self.stream.reader)?;

            msg_queue.tx.push((self.token.into(), message))?
        }
    }
*/
    pub fn set_message(&mut self, poll: &Poll, message: Message) -> io::Result<()> {
        message.write(&mut self.stream.writer)?;

        if self.stream.reader.len() < 16 * 1024 * 1024 {
            self.interest.insert(Ready::readable());
        }

        if self.stream.writer.len() > 0 {
            self.interest.insert(Ready::writable());
        }

        self.reregister_insterest(poll);

        Ok(())
    }
}

impl Evented for Connection {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt)
         -> io::Result<()>
    {
        self.socket.register(poll, token, interest, opts)
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt)
         -> io::Result<()>
    {
        self.socket.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.socket.deregister(poll)
    }
}
