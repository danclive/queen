use std::collections::VecDeque;
use std::io::{self, Read, Write, ErrorKind::{WouldBlock, BrokenPipe, InvalidData}};
use std::os::unix::io::AsRawFd;

use nson::Message;

use crate::crypto::Aead;
use crate::net::Stream;
use crate::util::slice_msg;

pub struct Connection {
    stream: Stream,
    read_buffer: Vec<u8>,
    write_buffer: VecDeque<Vec<u8>>,
    aead: Option<Aead>
}

impl Connection {
    pub fn new(stream: Stream, aead: Option<Aead>) -> Connection {
        Connection {
            stream,
            read_buffer: Vec::new(),
            write_buffer: VecDeque::new(),
            aead
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

    pub fn push_data(&mut self, mut data: Vec<u8>) {
        if let Some(aead) = &mut self.aead {
            aead.encrypt(&mut data).expect("encrypt error");
        }

        self.write_buffer.push_back(data);
    }

    pub fn want_write(&self) -> bool {
        !self.write_buffer.is_empty()
    }
}
