#![allow(unused_imports)]
#![allow(dead_code)]
use std::sync::Arc;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;
use std::thread::{self, sleep};
use std::io;
use std::io::ErrorKind::PermissionDenied;
use std::os::unix::io::AsRawFd;

use nson::{Message, msg};
use nson::message_id::MessageId;

use queen_io::poll::{poll, Ready, Events};
use queen_io::queue::mpsc::Queue;
use queen_io::plus::lru_cache::LruCache;
use queen_io::plus::block_queue::BlockQueue;

use crate::net::{Addr, Listen};
use crate::oneshot::{oneshot, Sender};
use crate::crypto::{Aead, Method};
use crate::dict::*;

use super::{Hub, HubConfig};
use super::conn::Connection;

pub struct Point {
    queue: Queue<Packet>,
}

pub struct PointConfig {
    pub addr: Addr,
    pub auth_msg: Message,
    pub aead_key: Option<String>,
    pub aead_method: Method,
    pub port_id: MessageId,
    pub worker_num: usize
}

impl PointConfig {
    pub fn new(addr: Addr, auth_msg: Message, aead_key: Option<String>) -> PointConfig {
        PointConfig {
            addr,
            auth_msg,
            aead_key,
            aead_method: Method::default(),
            port_id: MessageId::new(),
            worker_num: 4
        }
    }

    pub fn set_aead_key(&mut self, key: &str) {
        self.aead_key = Some(key.to_string())
    }

    pub fn set_aead_method(&mut self, method: Method) {
        self.aead_method = method;
    }

    pub fn set_worker_num(&mut self, num: usize) {
        self.worker_num = num;
    }
}

impl Point {
    pub fn connect(config: PointConfig) -> io::Result<Point> {
        let queue = Queue::new()?;

        let mut inner = InnerPoint {
            addr: config.addr,
            auth_msg: config.auth_msg,
            queue: queue.clone(),
            handles: HashMap::new(),
            chans: HashMap::new(),
            conn: None,
            state: State::UnAuth,
            read_buffer: VecDeque::new(),
            un_call: HashMap::new(),
            calling: HashMap::new(),
            worker_queue: BlockQueue::with_capacity(128),
            aead_key: config.aead_key,
            aead_method: config.aead_method,
            port_id: config.port_id,
            run: true
        };

        for i in 0..config.worker_num {
            let worker = Worker {
                worker_queue: inner.worker_queue.clone(),
                queue: queue.clone()
            };

            worker.run(format!("worker: {:?}", i));
        }

        thread::spawn(move || {
            inner.run()
        });

        Ok(Point {
            queue
        })
    }

    pub fn call(
        &self,
        module: &str,
        method: &str,
        mut request: Message,
        timeout: Option<Duration>
    ) -> Result<Message, ()> {
        if request.get_message_id(ID).is_err() {
            request.insert(ID, MessageId::new());
        }

        let (tx, rx) = oneshot::<Message>();

        let packet = Packet::Call(
            module.to_string(),
            method.to_string(),
            request,
            tx
        );

        self.queue.push(packet);

        if let Some(timeout) = timeout {
            rx.wait_timeout(timeout).ok_or(())
        } else {
            rx.wait().ok_or(())
        }
    }

    pub fn add(
        &self,
        module: &str,
        method: &str,
        handle: impl Fn(Message) -> Message + Sync + Send + 'static
    ) {
        let packet = Packet::Add(
            module.to_string(),
            method.to_string(),
            Box::new(handle)
        );

        self.queue.push(packet);
    }
}

type Handle = dyn Fn(Message) -> Message + Sync + Send + 'static;

enum Packet {
    Call(String, String, Message, Sender<Message>),
    Add(String, String, Box<Handle>),
    Res(Message)
}

struct InnerPoint {
    addr: Addr,
    auth_msg: Message,
    queue: Queue<Packet>,
    handles: HashMap<String, (String, Arc<HandleBox>)>, // HashMap<res_key, (req_key, handle)>
    chans: HashMap<String, bool>,
    conn: Option<(i32, Connection)>,
    state: State,
    read_buffer: VecDeque<Message>,
    un_call: HashMap<MessageId, (String, Message, Sender<Message>)>, // HashMap<MessageId, (req_key, _)>
    calling: HashMap<MessageId, (String, Sender<Message>)>, // HashMap<MessageId, (req_key, _)>
    worker_queue: BlockQueue<(String, MessageId, MessageId, Message, Arc<HandleBox>)>, // (req_key, from_id, message_id, handle)
    aead_key: Option<String>,
    aead_method: Method,
    port_id: MessageId,
    run: bool,
}

#[derive(Debug, Eq, PartialEq)]
enum State {
    UnAuth,
    Authing,
    Authed
}

struct HandleBox {
    handle: Box<Handle>
}

impl HandleBox {
    fn new(handle: Box<Handle>) -> HandleBox {
        HandleBox {
            handle
        }
    }

    fn call(&self, message: Message) -> Message {
        (self.handle)(message)
    }
}

struct Service {
    id: i32
}

impl InnerPoint {
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
                CHAN: AUTH,
                PORT_ID: self.port_id.clone()
            };

            msg.extend(self.auth_msg.clone());

            self.conn.as_mut().unwrap()
                .1.push_data(msg.to_vec().unwrap());

            self.state = State::Authing;
        }

        if !self.un_call.is_empty() && self.state == State::Authed {
            let mut temp = Vec::new();

            for (key, (req_key, _, _)) in &self.un_call {
                if let Some(ok) = self.chans.get(req_key) {
                    if *ok {
                        temp.push(key.clone());
                    }
                }
            }

            for key in temp {
                if let Some((req_key, message, tx)) = self.un_call.remove(&key) {
                    self.calling.insert(key, (req_key, tx));

                    self.conn.as_mut().unwrap()
                        .1.push_data(message.to_vec().unwrap());
                }
            }
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
                if event.fd() == self.queue.as_raw_fd() {
                    self.handle_message_from_queue()?;
                } else if self.conn.as_ref().map(|(id, _)| { *id == event.fd() }).unwrap_or(false) {
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
                }
            }
        }

        Ok(())
    }

    fn handle_message_from_queue(&mut self) -> io::Result<()> {
        if let Some(packet) = self.queue.pop() {
            match packet {
                Packet::Call(module, method, mut message, tx) => {
                    let req_key = format!("RPC/{}.{}/REQ", module, method);
                    let res_key = format!("RPC/{}.{}/RES", module, method);

                    message.insert(CHAN, res_key);
                    message.insert(SHARE, true);

                    let id = message.get_message_id(ID).expect("Must provid message id");

                    if let Some(ok) = self.chans.get(&req_key) {
                        if *ok {
                            self.calling.insert(id.clone(), (req_key, tx));

                            self.conn.as_mut().unwrap()
                                .1.push_data(message.to_vec().unwrap());
                        } else {
                            self.un_call.insert(id.clone(), (req_key, message, tx));
                        }
                    } else {
                        if self.state == State::Authed {
                            let msg = msg!{
                                CHAN: ATTACH,
                                VALUE: &req_key
                            };

                            self.conn.as_mut().unwrap()
                                .1.push_data(msg.to_vec().unwrap());
                        }

                        self.un_call.insert(id.clone(), (req_key.clone(), message, tx));

                        self.chans.insert(req_key, false);
                    }
                }
                Packet::Add(module, method, handle) => {
                    let req_key = format!("RPC/{}.{}/REQ", module, method);
                    let res_key = format!("RPC/{}.{}/RES", module, method);
                    self.handles.insert(res_key.clone(), (req_key, Arc::new(HandleBox::new(handle))));

                    if !self.chans.contains_key(&res_key) {
                        if self.state == State::Authed {
                            let msg = msg!{
                                CHAN: ATTACH,
                                VALUE: &res_key
                            };

                            self.conn.as_mut().unwrap()
                                .1.push_data(msg.to_vec().unwrap());
                        }

                        self.chans.insert(res_key, false);
                    }
                }
                Packet::Res(message) => {
                    if self.state == State::Authed {
                        self.conn.as_mut().unwrap()
                            .1.push_data(message.to_vec().unwrap());
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_message_from_conn(&mut self) -> io::Result<()> {
        while let Some(message) = self.read_buffer.pop_front() {
            if let Ok(chan) = message.get_str(CHAN) {
                if chan.starts_with('_') {
                    match chan {
                        AUTH => {
                            if let Ok(ok) = message.get_i32(OK) {
                                if ok == 0 {
                                    self.state = State::Authed;

                                    for (chan, _) in &self.chans {
                                        let msg = msg!{
                                            CHAN: ATTACH,
                                            VALUE: chan
                                        };

                                        self.conn.as_mut().unwrap()
                                            .1.push_data(msg.to_vec().unwrap());
                                    }

                                } else {
                                    return Err(io::Error::new(PermissionDenied, "PermissionDenied"))
                                }
                            }
                        }
                        ATTACH => {
                            if let Ok(ok) = message.get_i32(OK) {
                                if ok == 0 {
                                    if let Ok(value) = message.get_str(VALUE) {
                                        self.chans.insert(value.to_string(), true);
                                    }
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

    fn relay_message(&mut self, message: Message) -> io::Result<()> {
        if let Ok(chan) = message.get_str(CHAN) {
            if chan.ends_with("/RES") {
                if let Ok(from_id) = message.get_message_id(FROM) {
                    if let Ok(id) = message.get_message_id(ID) {
                        if let Some((req_key, handle_box)) = self.handles.get(chan) {
                            self.worker_queue.push((
                                req_key.to_string(),
                                from_id.clone(),
                                id.clone(),
                                message,
                                handle_box.clone()
                            ));
                        }
                    }
                }
            } else if chan.ends_with("/REQ") {
                if let Ok(id) = message.get_message_id(ID) {
                    if let Some((req_key, tx)) = self.calling.remove(id) {
                        if chan == req_key {
                            tx.send(message);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

struct Worker {
    worker_queue: BlockQueue<(String, MessageId, MessageId, Message, Arc<HandleBox>)>, // (req_key, from_id, message_id, handle, message)
    queue: Queue<Packet>,
}

impl Worker {
    fn run(self, name: String) {
        thread::Builder::new().name(name).spawn(|| {
            let worker = self;

            loop {
                let (req_key, from_id, message_id, message, handle) = worker.worker_queue.pop();

                let mut res_message = handle.call(message);

                res_message.insert(CHAN, req_key);
                res_message.insert(ID, message_id);
                res_message.insert(FROM, from_id);

                worker.queue.push(Packet::Res(res_message));

            }
        }).unwrap();
    }
}
