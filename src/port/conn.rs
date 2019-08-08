use std::collections::{VecDeque};
use std::io::{self, Read, Write, ErrorKind::{WouldBlock, BrokenPipe, InvalidData}};
use std::os::unix::io::AsRawFd;

use nson::Message;

use crate::net::Stream;
use crate::util::slice_msg;

pub struct Connection {
    pub stream: Stream,
    pub read_buffer: Vec<u8>,
    pub write_buffer: VecDeque<Vec<u8>>,
}

impl Connection {
    pub fn new(stream: Stream) -> Connection {
        Connection {
            stream,
            read_buffer: Vec::new(),
            write_buffer: VecDeque::new(),
        }
    }

    pub fn fd(&self) -> i32 {
        self.stream.as_raw_fd()
    }

    pub fn read(&mut self, read_buffer: &mut VecDeque<Message>) -> io::Result<()> {
        loop {
            let mut buf = [0; 4 * 1024];

            match self.stream.read(&mut buf) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
                    } else {
                        let messages = slice_msg(&mut self.read_buffer, &buf[..size])?;

                        for message in messages {
                            match Message::from_slice(&message) {
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
                    } else if size == front.len() {
                        self.write_buffer.pop_front();
                    } else if size < front.len() {
                        // size < front.len()
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

        Ok(())
    }
}
