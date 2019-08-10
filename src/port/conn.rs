use std::collections::VecDeque;
use std::io::{self, Read, Write, ErrorKind::{WouldBlock, BrokenPipe, InvalidData}};
use std::os::unix::io::AsRawFd;

use nson::Message;

use crate::net::Stream;
use crate::util::{slice_msg, sign, verify};

pub struct Connection {
    stream: Stream,
    read_buffer: Vec<u8>,
    write_buffer: VecDeque<Vec<u8>>
}

impl Connection {
    pub fn new(stream: Stream) -> Connection {
        Connection {
            stream,
            read_buffer: Vec::new(),
            write_buffer: VecDeque::new()
        }
    }

    pub fn fd(&self) -> i32 {
        self.stream.as_raw_fd()
    }

    pub fn read(&mut self, read_buffer: &mut VecDeque<Message>, hmac_key: &Option<String>) -> io::Result<()> {
        loop {
            let mut buf = [0; 4 * 1024];

            match self.stream.read(&mut buf) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
                    } else {
                        let vec = slice_msg(&mut self.read_buffer, &buf[..size])?;

                        for data in vec {
                            if let Some(key) = hmac_key {
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

    pub fn push_data(&mut self, mut data: Vec<u8>, hmac_key: &Option<String>) {

        if let Some(key) = hmac_key {
            data = sign(key.as_bytes(), data);
        }

        self.write_buffer.push_back(data);
    }

    pub fn want_write(&self) -> bool {
        !self.write_buffer.is_empty()
    }
}
