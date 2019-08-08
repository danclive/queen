use std::collections::{VecDeque, HashSet};
use std::io::{self, Read, Write, ErrorKind::{WouldBlock, BrokenPipe, InvalidData}};
use std::os::unix::io::AsRawFd;
use std::time::Duration;
use std::thread::{self, sleep};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use queen_io::tcp::TcpStream;
use queen_io::unix::UnixStream;
use queen_io::queue::mpsc::Queue;

use nson::{Message, msg};

use crate::poll::{poll, Ready, Events};
use crate::net::{Addr, Stream};
use crate::util::slice_msg;

use super::conn::Connection;

pub struct Hub {
    id: AtomicUsize,
    queen: Queue<Packet>
}

struct HubInner {
    addr: Addr,
    auth_msg: Message,
    conn: Option<(i32, Connection)>,
    state: State,
    read_buffer: VecDeque<Message>,
    queen: Queue<Packet>,
    run: bool
}

#[derive(Debug, Eq, PartialEq)]
enum State {
    UnAuth,
    Authing,
    Authed
}

impl Hub {
    pub fn connect(addr: Addr, auth_msg: Message) -> io::Result<Hub> {
        let queen = Queue::new()?;

        let mut inner = HubInner {
            addr,
            auth_msg,
            conn: None,
            state: State::UnAuth,
            read_buffer: VecDeque::new(),
            queen: queen.clone(),
            run: true
        };

        thread::spawn(move || {
            inner.run()
        });

        Ok(Hub {
            id: AtomicUsize::new(0),
            queen
        })
    }

    pub fn channel(_chan: &str) -> Channel {
        unimplemented!()
    }

    pub fn recv(_chan: &str) -> Recv { // iter
        unimplemented!()
    }

    pub fn send(_chan: &str, _msg: Message) {

    }
}

impl HubInner {
    fn run(&mut self) -> io::Result<()> {
        while self.run {
            self.run_once()?;
        }

        Ok(())
    }

    pub fn run_once(&mut self) -> io::Result<()> {
        if self.conn.is_none() {
            let conn = match self.addr.connect() {
                Ok(conn) => conn,
                Err(err) => {
                    println!("link: {:?} err: {}", self.addr, err);
                
                    sleep(Duration::from_secs(1));

                    return Ok(())
                }
            };

            let fd = conn.fd();

            self.conn = Some((fd, conn));
        }

        if self.state == State::UnAuth {
            let mut msg = msg!{
                "_chan": "_auth",
            };

            msg.extend(self.auth_msg.clone());

            self.conn.as_mut().unwrap().1.write_buffer.push_back(msg.to_vec().unwrap());

            self.state = State::Authing;
        }

        let mut events = Events::new();

        // conn
        let (fd, conn) = self.conn.as_ref().unwrap();

        let mut interest = Ready::readable() | Ready::hup();

        if !conn.write_buffer.is_empty() {
            interest.insert(Ready::writable());
        }

        events.put(*fd, interest);

        // queen
        events.put(self.queen.as_raw_fd(), Ready::readable());

        if poll(&mut events, Some(Duration::from_secs(1)))? > 0 {
            for event in &events {
                if self.conn.as_ref().map(|(id, _)| { *id == event.fd() }).unwrap() {
                    let readiness = event.readiness();

                    if readiness.is_hup() || readiness.is_error() {
                        self.conn = None;
                        self.state = State::UnAuth;

                        break;
                    }

                    if readiness.is_readable() {
                        if let Some((_, conn)) = &mut self.conn {
                            if conn.read(&mut self.read_buffer).is_err() {
                                self.conn = None;
                                self.state = State::UnAuth;
                            }
                        }

                        if !self.read_buffer.is_empty() {
                            self.handle_message_from_conn()?;
                        }
                    }

                    if readiness.is_writable() {
                        if let Some((_, conn)) = &mut self.conn {
                            if conn.write().is_err() {
                                self.conn = None;
                                self.state = State::UnAuth;
                            }
                        }
                    }
                } else if event.fd() == self.queen.as_raw_fd() {
                    self.handle_message_from_queue()?;
                }
            }
        }

        Ok(())
    }

    fn handle_message_from_conn(&mut self) -> io::Result<()> {
        while let Some(message) = self.read_buffer.pop_front() {
            if let Ok(chan) = message.get_str("_chan") {
                
            }
        }

        Ok(())
    }

    fn handle_message_from_queue(&mut self) -> io::Result<()> {
        Ok(())
    }
}

enum Packet {
    Send(Message),
    Attatch(usize, String, Sender<Message>), // id, chan, sender
    Detatch(usize)
}

pub struct Recv {
    id: usize,
    recv: Receiver<Message>
}

pub struct Channel {

}

impl Channel {
    pub fn tx() {

    }

    pub fn rx() {

    }
}
