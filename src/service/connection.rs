use std::io;
use std::io::{Read, Write};
use std::io::ErrorKind::WouldBlock;
use std::io::Cursor;
use std::mem;

use queen_io::tcp::TcpStream;
use queen_io::Poll;
use queen_io::Token;
use queen_io::Ready;
use queen_io::PollOpt;
use queen_io::Evented;
use queen_io::channel::Sender;

use byteorder::{LittleEndian, ReadBytesExt};

use wire_protocol::message::Message;
use wire_protocol::header::Header;

use super::bufstream::Stream;
use super::service::ServiceEvent;

pub struct Connection {
    socket: TcpStream,
    token: Token,
    interest: Ready,
    stream: Stream,
}

impl Connection {
    pub fn new(socket: TcpStream, token: Token) -> io::Result<Connection> {
        let conn = Connection {
            socket: socket,
            token: token,
            interest: Ready::readable() | Ready::hup(),
            stream: Stream::new(),
        };

        Ok(conn)
    }

    pub fn reader(&mut self, poll: &Poll, tx_out: &Sender<ServiceEvent>) -> io::Result<()> {
        self.interest.remove(Ready::readable());

        loop {
            let mut buf = [0; 8 * 1024];

            match self.socket.read(&mut buf) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "ConnectionAborted"))
                    } else {
                        self.stream.reader.extend_from_slice(&buf[0..size]);
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

            if self.stream.reader.len() > 32 * 1024 * 1024 {
                break;
            }
        }

        self.send_message(tx_out)?;

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
            match self.socket.write(&self.stream.writer) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "ConnectionAborted"))
                    } else {
                        if size < self.stream.writer.len() {
                            self.stream.writer = self.stream.writer.split_off(size);
                        } else {
                            self.stream.writer.clear();
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

    pub fn send_message(&mut self, tx_out: &Sender<ServiceEvent>) -> io::Result<()> {
        loop {

            if self.stream.reader.len() < mem::size_of::<Header>() {
                return Ok(())
            }

            let message_length = {
                Cursor::new(&self.stream.reader).read_i32::<LittleEndian>()? as usize
            };

            if self.stream.reader.len() < message_length {
                return Ok(())
            }

            let (message, position) = {

                let mut cursor = Cursor::new(&self.stream.reader);

                let message = match Message::read(&mut cursor)? {
                    Some(message) => message,
                    None => return Ok(()),
                };

                (message, cursor.position())
            };

            self.stream.reader = self.stream.reader.split_off(position as usize);

            let _ = tx_out.send(ServiceEvent::Message(self.token.into(), message));
        }
    }

    pub fn recv_message(&mut self, poll: &Poll, message: Message) -> io::Result<()> {
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
