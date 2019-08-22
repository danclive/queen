use std::collections::{VecDeque, HashMap};
use std::io::{self, ErrorKind::{PermissionDenied}};
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Duration;
use std::thread::{self, sleep};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use queen_io::poll::{poll, Ready, Events};
use queen_io::queue::mpsc::Queue;
use queen_io::queue::spsc::Queue as SpscQueue;

use nson::{Message, msg};

use crate::net::Addr;
use crate::crypto::{Method, Aead};

use super::conn::Connection;

#[derive(Clone)]
pub struct Hub {
    id: Arc<AtomicUsize>,
    queue: Queue<Packet>
}

#[derive(Debug, Clone)]
pub struct HubConfig {
    pub addr: Addr,
    pub auth_msg: Message,
    pub aead_key: Option<String>,
    pub aead_method: Method
}

impl HubConfig {
    pub fn new(addr: Addr, auth_msg: Message, aead_key: Option<String>) -> HubConfig {
        HubConfig {
            addr,
            auth_msg,
            aead_key,
            aead_method: Method::default()
        }
    }

    pub fn set_aead_key(&mut self, key: &str) {
        self.aead_key = Some(key.to_string())
    }

    pub fn set_aead_method(&mut self, method: Method) {
        self.aead_method = method;
    }
}

#[derive(Debug, Eq, PartialEq)]
enum State {
    UnAuth,
    Authing,
    Authed
}

impl Hub {
    pub fn connect(config: HubConfig) -> io::Result<Hub> {
        let queue = Queue::new()?;

        let mut inner = HubInner {
            addr: config.addr,
            auth_msg: config.auth_msg,
            conn: None,
            state: State::UnAuth,
            read_buffer: VecDeque::new(),
            queue: queue.clone(),
            chans: HashMap::new(),
            tx_index: HashMap::new(),
            aead_key: config.aead_key,
            aead_method: config.aead_method,
            run: true
        };

        thread::spawn(move || {
            inner.run()
        });

        Ok(Hub {
            id: Arc::new(AtomicUsize::new(0)),
            queue
        })
    }

    pub fn recv(&self, chan: &str) -> Recv { // iter
        let (tx, rx) = channel();

        let id = self.id.fetch_add(1, Ordering::SeqCst);

        self.queue.push(Packet::AttatchBlock(id, chan.to_string(), tx));

        Recv {
            hub: self.clone(),
            id,
            recv: rx
        }
    }

    pub fn async_recv(&self, chan: &str) -> io::Result<AsyncRecv> {
        let spsc_queue = SpscQueue::with_cache(128)?;

        let id = self.id.fetch_add(1, Ordering::SeqCst);

        self.queue.push(Packet::AttatchAsync(id, chan.to_string(), spsc_queue.clone()));

        Ok(AsyncRecv {
            hub: self.clone(),
            id,
            recv: spsc_queue
        })
    }

    pub fn send(&self, chan: &str, mut msg: Message) {
        msg.insert("_chan", chan);

        loop {
            if self.queue.pending() < 1024 {
                self.queue.push(Packet::Send(msg));
                return
            }

            thread::sleep(Duration::from_millis(10));
        }
    }
}

enum Packet {
    Send(Message),
    AttatchBlock(usize, String, Sender<Message>), // id, chan, sender
    AttatchAsync(usize, String, SpscQueue<Message>),
    Detatch(usize)
}

pub struct Recv {
    hub: Hub,
    id: usize,
    recv: Receiver<Message>
}

impl Iterator for Recv {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        self.recv.recv().ok()
    }
}

impl Drop for Recv {
    fn drop(&mut self) {
        self.hub.queue.push(Packet::Detatch(self.id));
    }
}

pub struct AsyncRecv {
    hub: Hub,
    id: usize,
    recv: SpscQueue<Message>
}

impl AsyncRecv {
    pub fn recv(&self) -> Option<Message> {
        self.recv.pop()
    }
}

impl AsRawFd for AsyncRecv {
    fn as_raw_fd(&self) -> RawFd {
        self.recv.as_raw_fd()
    }
}

impl Drop for AsyncRecv {
    fn drop(&mut self) {
        self.hub.queue.push(Packet::Detatch(self.id));
    }
}

struct HubInner {
    addr: Addr,
    auth_msg: Message,
    conn: Option<(i32, Connection)>,
    state: State,
    read_buffer: VecDeque<Message>,
    queue: Queue<Packet>,
    chans: HashMap<String, Vec<(usize, SenderType)>>,
    tx_index: HashMap<usize, String>,
    aead_key: Option<String>,
    aead_method: Method,
    run: bool
}

enum SenderType {
    Block(Sender<Message>),
    Async(SpscQueue<Message>)
}

impl HubInner {
    fn run(&mut self) -> io::Result<()> {
        while self.run {
            self.run_once()?;
        }

        Ok(())
    }

    fn run_once(&mut self) -> io::Result<()> {
        if self.conn.is_none() {
            let stream = match self.addr.connect() {
                Ok(conn) => conn,
                Err(err) => {
                    println!("link: {:?} err: {}", self.addr, err);
                
                    sleep(Duration::from_secs(1));

                    return Ok(())
                }
            };

            let aead = self.aead_key.as_ref().map(|key| Aead::new(&self.aead_method, key.as_bytes()));

            let conn = Connection::new(stream, aead);

            let fd = conn.fd();

            self.conn = Some((fd, conn));
        }

        if self.state == State::UnAuth {
            let mut msg = msg!{
                "_chan": "_auth",
            };

            msg.extend(self.auth_msg.clone());

            self.conn.as_mut().unwrap()
                .1.push_data(msg.to_vec().unwrap());

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

        // queue
        events.put(self.queue.as_raw_fd(), Ready::readable());

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
                } else if event.fd() == self.queue.as_raw_fd() {
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
                                            .1.push_data(msg.to_vec().unwrap());
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
        if let Some(packet) = self.queue.pop() {
            match packet {
                Packet::Send(msg) => {
                    if self.state == State::Authed {
                        self.conn.as_mut().unwrap()
                            .1.push_data(msg.to_vec().unwrap());
                    }

                    self.relay_message(msg)?;
                }
                Packet::AttatchBlock(id, chan, tx) => {
                    let ids = self.chans.entry(chan.clone()).or_insert_with(|| vec![]);

                    if ids.is_empty() {
                        let msg = msg!{
                            "_chan": "_atta",
                            "_valu": chan
                        };

                        self.conn.as_mut().unwrap()
                            .1.push_data(msg.to_vec().unwrap());
                    }

                    ids.push((id, SenderType::Block(tx)));
                }
                Packet::AttatchAsync(id, chan, queue) => {
                    let ids = self.chans.entry(chan.clone()).or_insert_with(|| vec![]);

                    if ids.is_empty() {
                        let msg = msg!{
                            "_chan": "_atta",
                            "_valu": chan
                        };

                        self.conn.as_mut().unwrap()
                            .1.push_data(msg.to_vec().unwrap());
                    }

                    ids.push((id, SenderType::Async(queue)));
                }
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
                                    .1.push_data(msg.to_vec().unwrap());
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
                    match tx {
                        SenderType::Block(tx) => {
                            let _ = tx.send(message.clone());
                        }
                        SenderType::Async(queue) => {
                            queue.push(message.clone());
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
