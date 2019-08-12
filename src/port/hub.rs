use std::collections::{VecDeque, HashMap};
use std::io::{self, ErrorKind::{PermissionDenied}};
use std::os::unix::io::AsRawFd;
use std::time::Duration;
use std::thread::{self, sleep};
use std::sync::mpsc::{self, channel, Sender, Receiver};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use queen_io::queue::mpsc::Queue;
use queen_io::poll::{poll, Ready, Events};

use nson::{Message, msg};

use crate::net::Addr;

use super::conn::Connection;

#[derive(Clone)]
pub struct Hub {
    id: Arc<AtomicUsize>,
    queen: Queue<Packet>
}

struct HubInner {
    addr: Addr,
    auth_msg: Message,
    conn: Option<(i32, Connection)>,
    state: State,
    read_buffer: VecDeque<Message>,
    queen: Queue<Packet>,
    chans: HashMap<String, Vec<(usize, Sender<Message>)>>,
    tx_index: HashMap<usize, String>,
    hmac_key: Option<String>,
    run: bool
}

#[derive(Debug, Eq, PartialEq)]
enum State {
    UnAuth,
    Authing,
    Authed
}

impl Hub {
    pub fn connect(addr: Addr, auth_msg: Message, hmac_key: Option<String>) -> io::Result<Hub> {
        let queen = Queue::new()?;

        let mut inner = HubInner {
            addr,
            auth_msg,
            conn: None,
            state: State::UnAuth,
            read_buffer: VecDeque::new(),
            queen: queen.clone(),
            chans: HashMap::new(),
            tx_index: HashMap::new(),
            hmac_key,
            run: true
        };

        thread::spawn(move || {
            inner.run()
        });

        Ok(Hub {
            id: Arc::new(AtomicUsize::new(0)),
            queen
        })
    }

    pub fn channel(_chan: &str) -> Channel {
        unimplemented!()
    }

    pub fn recv(&self, chan: &str) -> Recv { // iter
        let (tx, rx) = channel();

        let id = self.id.fetch_add(1, Ordering::SeqCst);

        self.queen.push(Packet::Attatch(id, chan.to_string(), tx));

        Recv {
            hub: self.clone(),
            id,
            recv: rx
        }
    }

    pub fn send(&self, chan: &str, mut msg: Message) {
        msg.insert("_chan", chan);

        loop {
            if self.queen.pending() < 64 {
                self.queen.push(Packet::Send(msg));
                return
            }

            thread::sleep(Duration::from_millis(10));
        }
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

            self.conn.as_mut().unwrap()
                .1.push_data(msg.to_vec().unwrap(), &self.hmac_key);

            self.state = State::Authing;
        }

        let mut events = Events::new();

        // conn
        let (fd, conn) = self.conn.as_ref().unwrap();

        let mut interest = Ready::readable() | Ready::hup();

        if conn.want_write() {
            interest.insert(Ready::writable());
        }

        events.put(*fd, interest);

        // queen
        events.put(self.queen.as_raw_fd(), Ready::readable());

        if poll(&mut events, Some(Duration::from_secs(1)))? > 0 {
            for event in &events {
                if self.conn.as_ref().map(|(id, _)| { *id == event.fd() }).unwrap_or(false) {
                    let readiness = event.readiness();

                    if readiness.is_hup() || readiness.is_error() {
                        self.conn = None;
                        self.state = State::UnAuth;

                        break;
                    }

                    if readiness.is_readable() {
                        if let Some((_, conn)) = &mut self.conn {
                            if conn.read(&mut self.read_buffer, &self.hmac_key).is_err() {
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
                if chan.starts_with('_') {
                    match chan {
                        "_auth" => {
                            if let Ok(ok) = message.get_i32("ok") {
                                if ok == 0 {
                                    self.state = State::Authed;

                                    for (chan, _) in &self.chans {
                                        let msg = msg!{
                                            "_chan": "_atta",
                                            "_valu": chan
                                        };

                                        self.conn.as_mut().unwrap()
                                            .1.push_data(msg.to_vec().unwrap(), &self.hmac_key);
                                    }
                                } else {
                                    return Err(io::Error::new(PermissionDenied, "PermissionDenied"))
                                }
                            }
                        }
                        "_atta" => {
                            if let Ok(ok) = message.get_i32("ok") {
                                if ok != 0 {
                                    println!("_atta: {:?}", message);
                                }
                            }
                        }
                        _ => ()
                    }
                } else {
                    self.relay_message(message)?;
                }
            }
        }

        Ok(())
    }

    fn handle_message_from_queue(&mut self) -> io::Result<()> {
        if let Some(packet) = self.queen.pop() {
            match packet {
                Packet::Send(msg) => {
                    if self.state == State::Authed {
                        self.conn.as_mut().unwrap()
                            .1.push_data(msg.to_vec().unwrap(), &self.hmac_key);
                    }

                    self.relay_message(msg)?;
                },
                Packet::Attatch(id, chan, tx) => {
                    let ids = self.chans.entry(chan.clone()).or_insert_with(|| vec![]);

                    if ids.is_empty() {
                        let msg = msg!{
                            "_chan": "_atta",
                            "_valu": chan
                        };

                        self.conn.as_mut().unwrap()
                            .1.push_data(msg.to_vec().unwrap(), &self.hmac_key);
                    }

                    ids.push((id, tx));
                },
                Packet::Detatch(id) => {
                    if let Some(chan) = self.tx_index.remove(&id) {
                        if let Some(ids) = self.chans.get_mut(&chan) {
                            if let Some(pos) = ids.iter().position(|(x, _)| x == &id) {
                                ids.remove(pos);
                            }

                            if ids.is_empty() {
                                let msg = msg!{
                                    "_chan": "_deta",
                                    "_valu": chan
                                };

                                self.conn.as_mut().unwrap()
                                    .1.push_data(msg.to_vec().unwrap(), &self.hmac_key);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn relay_message(&mut self, message: Message) -> io::Result<()> {
        if let Ok(chan) = message.get_str("_chan") {
            if let Some(ids) = self.chans.get(chan) {
                for (_, tx) in ids {
                    let _ = tx.send(message.clone());
                }
            }
        }

        Ok(())
    }
}

enum Packet {
    Send(Message),
    Attatch(usize, String, Sender<Message>), // id, chan, sender
    Detatch(usize)
}

pub struct Recv {
    hub: Hub,
    id: usize,
    recv: Receiver<Message>
}

impl Iterator for Recv {
    type Item = Result<Message, mpsc::RecvError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Some(self.recv.recv())
        loop {
            let ret = self.recv.recv();

            if ret.is_err() {
                thread::yield_now();
                continue;
            }

            return Some(ret)
        }
    }
}

impl Drop for Recv {
    fn drop(&mut self) {
        self.hub.queen.push(Packet::Detatch(self.id));
    }
}

pub struct Channel {

}

impl Channel {
    pub fn tx() {

    }

    pub fn rx() {

    }
}
