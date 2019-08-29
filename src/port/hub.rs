use std::collections::{VecDeque, HashMap, HashSet};
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

use nson::{Message, msg, message_id::MessageId};

use crate::net::Addr;
use crate::crypto::{Method, Aead};
use crate::dict::*;

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
    pub aead_method: Method,
    pub port_id: MessageId
}

impl HubConfig {
    pub fn new(addr: Addr, auth_msg: Message, aead_key: Option<String>) -> HubConfig {
        HubConfig {
            addr,
            auth_msg,
            aead_key,
            aead_method: Method::default(),
            port_id: MessageId::new()
        }
    }

    pub fn set_aead_key(&mut self, key: &str) {
        self.aead_key = Some(key.to_string())
    }

    pub fn set_aead_method(&mut self, method: Method) {
        self.aead_method = method;
    }
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
            recvs: HashMap::new(),
            tx_index: HashMap::new(),
            un_send: VecDeque::new(),
            aead_key: config.aead_key,
            aead_method: config.aead_method,
            port_id: config.port_id,
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

    pub fn recv(&self, chan: &str, lables: Option<Vec<String>>) -> Recv { // iter
        let (tx, rx) = channel();

        let id = self.id.fetch_add(1, Ordering::SeqCst);

        self.queue.push(Packet::AttatchBlock(id, chan.to_string(), lables, tx));

        Recv {
            hub: self.clone(),
            id,
            recv: rx
        }
    }

    pub fn async_recv(&self, chan: &str, lables: Option<Vec<String>>) -> io::Result<AsyncRecv> {
        let spsc_queue = SpscQueue::with_cache(128)?;

        let id = self.id.fetch_add(1, Ordering::SeqCst);

        self.queue.push(Packet::AttatchAsync(id, chan.to_string(), lables, spsc_queue.clone()));

        Ok(AsyncRecv {
            hub: self.clone(),
            id,
            recv: spsc_queue
        })
    }

    pub fn send(&self, chan: &str, mut msg: Message, lable: Option<Vec<String>>) {
        msg.insert(CHAN, chan);

        if let Some(lable) = lable {
            msg.insert(LABEL, lable);
        }

        if msg.get_message_id(ID).is_err() {
            msg.insert(ID, MessageId::new());
        }

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
    AttatchBlock(usize, String, Option<Vec<String>>, Sender<Message>), // id, chan, lables, sender
    AttatchAsync(usize, String, Option<Vec<String>>, SpscQueue<Message>),
    Detatch(usize)
}

pub struct Recv {
    pub hub: Hub,
    pub id: usize,
    pub recv: Receiver<Message>
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
    pub hub: Hub,
    pub id: usize,
    pub recv: SpscQueue<Message>
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
    chans: HashMap<String, HashSet<String>>, // HashMap<Chan, HashSet<Label>>
    recvs: HashMap<String, Vec<(usize, SenderType, Option<Vec<String>>)>>, // HashMap<Chan, Vec<id, tx, Vec<Lable>>>
    tx_index: HashMap<usize, String>,
    un_send: VecDeque<Message>,
    aead_key: Option<String>,
    aead_method: Method,
    port_id: MessageId,
    run: bool
}

#[derive(Debug, Eq, PartialEq)]
enum State {
    UnAuth,
    Authing,
    Authed
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
                CHAN: AUTH,
                PORT_ID: self.port_id.clone()
            };

            msg.extend(self.auth_msg.clone());

            self.conn.as_mut().unwrap()
                .1.push_data(msg.to_vec().unwrap());

            self.state = State::Authing;
        }

        if !self.un_send.is_empty() && self.state == State::Authed {
            while let Some(msg) = self.un_send.pop_front() {
                self.conn.as_mut().unwrap()
                    .1.push_data(msg.to_vec().unwrap());
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
                Packet::Send(msg) => {
                    if self.state == State::Authed {
                        self.conn.as_mut().unwrap()
                            .1.push_data(msg.to_vec().unwrap());
                    } else {
                        self.un_send.push_back(msg.clone());
                    }

                    // self.relay_message(msg)?;
                }
                Packet::AttatchBlock(id, chan, labels, tx) => {
                    let ids = self.recvs.entry(chan.clone()).or_insert_with(|| vec![]);

                    let mut labels_set = HashSet::new();

                    if let Some(labels) = &labels {
                        for label in labels {
                            labels_set.insert(label.to_string());
                        }
                    }

                    if ids.is_empty() {
                        if self.state == State::Authed {
                            let labels: Vec<String> = labels_set.iter().map(|s| s.to_string()).collect();

                            let msg = msg!{
                                CHAN: ATTACH,
                                VALUE: &chan,
                                LABEL: labels
                            };

                            self.conn.as_mut().unwrap()
                                .1.push_data(msg.to_vec().unwrap());
                        }

                        self.chans.insert(chan.to_string(), labels_set);
                    } else {
                        let old_set = self.chans.get_mut(&chan).unwrap();

                        let labels: Vec<String> = labels_set.iter().filter(|l| !old_set.contains(*l)).map(|s| s.to_string()).collect();

                        if !labels.is_empty() {
                            if self.state == State::Authed {

                                let msg = msg!{
                                    CHAN: ATTACH,
                                    VALUE: &chan,
                                    LABEL: labels
                                };

                                self.conn.as_mut().unwrap()
                                    .1.push_data(msg.to_vec().unwrap());
                            }

                            for label in labels_set {
                                old_set.insert(label);
                            }
                        }
                    }

                    ids.push((id, SenderType::Block(tx), labels));
                    self.tx_index.insert(id, chan);
                }
                Packet::AttatchAsync(id, chan, labels, queue) => {
                    let ids = self.recvs.entry(chan.clone()).or_insert_with(|| vec![]);

                    let mut labels_set = HashSet::new();

                    if let Some(labels) = &labels {
                        for label in labels {
                            labels_set.insert(label.to_string());
                        }
                    }

                    if ids.is_empty() {
                        if self.state == State::Authed {
                            let labels: Vec<String> = labels_set.iter().map(|s| s.to_string()).collect();

                            let msg = msg!{
                                CHAN: ATTACH,
                                VALUE: &chan,
                                LABEL: labels
                            };

                            self.conn.as_mut().unwrap()
                                .1.push_data(msg.to_vec().unwrap());
                        }

                        self.chans.insert(chan.to_string(), labels_set);
                    } else {
                        let old_set = self.chans.get_mut(&chan).unwrap();

                        let labels: Vec<String> = labels_set.iter().filter(|l| !old_set.contains(*l)).map(|s| s.to_string()).collect();

                        if !labels.is_empty() {
                            if self.state == State::Authed {

                                let msg = msg!{
                                    CHAN: ATTACH,
                                    VALUE: &chan,
                                    LABEL: labels
                                };

                                self.conn.as_mut().unwrap()
                                    .1.push_data(msg.to_vec().unwrap());
                            }

                            for label in labels_set {
                                old_set.insert(label);
                            }
                        }
                    }

                    ids.push((id, SenderType::Async(queue), labels));
                    self.tx_index.insert(id, chan);
                }
                Packet::Detatch(id) => {
                    if let Some(chan) = self.tx_index.remove(&id) {
                        let mut remove_chan = false;

                        if let Some(ids) = self.recvs.get_mut(&chan) {
                            if let Some(pos) = ids.iter().position(|(x, _, _)| x == &id) {
                                ids.remove(pos);
                            }

                            if ids.is_empty() {
                                remove_chan = true;
                            } else {
                                let old_set = self.chans.get_mut(&chan).unwrap();

                                let new_set: HashSet<String> = old_set.iter().filter(|l| {
                                    for (_, _, labels) in ids.iter() {
                                        if let Some(labels) = labels {
                                            if labels.contains(l) {
                                                return true;
                                            }
                                        }
                                    }

                                    false
                                }).map(|s| s.to_string()).collect();

                                if self.state == State::Authed {
                                    let change: Vec<String> = old_set.iter().filter(|l| {
                                        !new_set.contains(&**l)
                                    }).map(|s| s.to_string()).collect();

                                    if !change.is_empty() {
                                        let msg = msg!{
                                            CHAN: DETACH,
                                            VALUE: &chan,
                                            LABEL: change
                                        };

                                        self.conn.as_mut().unwrap()
                                            .1.push_data(msg.to_vec().unwrap());
                                    }
                                }

                                *old_set = new_set;
                            }
                        }

                        if remove_chan {
                            self.recvs.remove(&chan);
                            self.chans.remove(&chan);

                            if self.state == State::Authed {
                                let msg = msg!{
                                    CHAN: DETACH,
                                    VALUE: chan
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

    fn handle_message_from_conn(&mut self) -> io::Result<()> {
        while let Some(message) = self.read_buffer.pop_front() {
            if let Ok(chan) = message.get_str(CHAN) {
                if chan.starts_with('_') {
                    match chan {
                        AUTH => {
                            if let Ok(ok) = message.get_i32(OK) {
                                if ok == 0 {
                                    self.state = State::Authed;

                                    for (chan, set) in &self.chans {
                                        let labels: Vec<String> = set.iter().map(|s| s.to_string()).collect();

                                        let msg = msg!{
                                            CHAN: ATTACH,
                                            VALUE: chan,
                                            LABEL: labels
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

    fn relay_message(&mut self, message: Message) -> io::Result<()> {
        if let Ok(chan) = message.get_str(CHAN) {
            if let Some(ids) = self.recvs.get(chan) {
                let mut labels = vec![];

                if let Some(label) = message.get(LABEL) {
                    if let Some(label) = label.as_str() {
                        labels.push(label.to_string());
                    } else if let Some(label) = label.as_array() {
                        label.iter().for_each(|v| {
                            if let Some(v) = v.as_str() {
                                labels.push(v.to_string());
                            }
                        });
                    }
                }

                if labels.is_empty() {
                    for (_, tx, _) in ids {
                        match tx {
                            SenderType::Block(tx) => {
                                let _ = tx.send(message.clone());
                            }
                            SenderType::Async(queue) => {
                                queue.push(message.clone());
                            }
                        }
                    }
                } else {
                    for (_, tx, recv_labels) in ids {
                        if let Some(recv_labels) = recv_labels {
                            if recv_labels.iter().any(|l| labels.contains(l)) {
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
                }
            }
        }

        Ok(())
    }
}

#[test]
fn attach_detach() {
    // init
    let mut hub_inner = HubInner {
        addr: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
        auth_msg: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
        conn: None,
        state: State::UnAuth,
        read_buffer: VecDeque::new(),
        queue: Queue::new().unwrap(),
        chans: HashMap::new(),
        recvs: HashMap::new(),
        tx_index: HashMap::new(),
        un_send: VecDeque::new(),
        aead_key: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
        aead_method: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
        port_id: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
        run: true
    };

    // attach 1
    let (tx, _rx) = channel();
    hub_inner.queue.push(Packet::AttatchBlock(1, "chan1".to_string(), None, tx));

    hub_inner.handle_message_from_queue().unwrap();

    assert_eq!(hub_inner.chans.len(), 1);
    assert_eq!(hub_inner.recvs.len(), 1);
    assert_eq!(hub_inner.chans.get("chan1").unwrap().len(), 0);
    assert_eq!(hub_inner.recvs.get("chan1").unwrap().len(), 1);
    assert_eq!(hub_inner.tx_index.len(), 1);

    // attach 2
    let (tx, _rx) = channel();
    hub_inner.queue.push(Packet::AttatchBlock(2, "chan1".to_string(), None, tx));

    hub_inner.handle_message_from_queue().unwrap();

    assert_eq!(hub_inner.chans.len(), 1);
    assert_eq!(hub_inner.recvs.len(), 1);
    assert_eq!(hub_inner.chans.get("chan1").unwrap().len(), 0);
    assert_eq!(hub_inner.recvs.get("chan1").unwrap().len(), 2);
    assert_eq!(hub_inner.tx_index.len(), 2);

    // attach 3
    let (tx, _rx) = channel();
    hub_inner.queue.push(Packet::AttatchBlock(3, "chan2".to_string(), None, tx));

    hub_inner.handle_message_from_queue().unwrap();

    assert_eq!(hub_inner.chans.len(), 2);
    assert_eq!(hub_inner.recvs.len(), 2);
    assert_eq!(hub_inner.chans.get("chan2").unwrap().len(), 0);
    assert_eq!(hub_inner.recvs.get("chan2").unwrap().len(), 1);
    assert_eq!(hub_inner.tx_index.len(), 3);

    // detach 1
    hub_inner.queue.push(Packet::Detatch(1));

    hub_inner.handle_message_from_queue().unwrap();

    assert_eq!(hub_inner.chans.len(), 2);
    assert_eq!(hub_inner.recvs.len(), 2);
    assert_eq!(hub_inner.chans.get("chan1").unwrap().len(), 0);
    assert_eq!(hub_inner.recvs.get("chan1").unwrap().len(), 1);
    assert_eq!(hub_inner.tx_index.len(), 2);

    // detach 2
    hub_inner.queue.push(Packet::Detatch(2));

    hub_inner.handle_message_from_queue().unwrap();

    assert_eq!(hub_inner.chans.len(), 1);
    assert_eq!(hub_inner.recvs.len(), 1);
    assert_eq!(hub_inner.chans.get("chan1").is_none(), true);
    assert_eq!(hub_inner.recvs.get("chan1").is_none(), true);
    assert_eq!(hub_inner.tx_index.len(), 1);
}

#[test]
fn attach_detach_with_labels() {
    // init
    let mut hub_inner = HubInner {
        addr: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
        auth_msg: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
        conn: None,
        state: State::UnAuth,
        read_buffer: VecDeque::new(),
        queue: Queue::new().unwrap(),
        chans: HashMap::new(),
        recvs: HashMap::new(),
        tx_index: HashMap::new(),
        un_send: VecDeque::new(),
        aead_key: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
        aead_method: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
        port_id: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
        run: true
    };

    // attach 1
    let (tx, _rx) = channel();
    hub_inner.queue.push(Packet::AttatchBlock(1, "chan1".to_string(), Some(vec!["label1".to_string()]), tx));

    hub_inner.handle_message_from_queue().unwrap();

    assert_eq!(hub_inner.chans.len(), 1);
    assert_eq!(hub_inner.recvs.len(), 1);
    assert_eq!(hub_inner.chans.get("chan1").unwrap().len(), 1);
    assert_eq!(hub_inner.recvs.get("chan1").unwrap().len(), 1);
    assert_eq!(hub_inner.tx_index.len(), 1);

    // attach 2
    let (tx, _rx) = channel();
    hub_inner.queue.push(Packet::AttatchBlock(2, "chan1".to_string(), Some(vec!["label2".to_string()]), tx));

    hub_inner.handle_message_from_queue().unwrap();

    assert_eq!(hub_inner.chans.len(), 1);
    assert_eq!(hub_inner.recvs.len(), 1);
    assert_eq!(hub_inner.chans.get("chan1").unwrap().len(), 2);
    assert_eq!(hub_inner.recvs.get("chan1").unwrap().len(), 2);
    assert_eq!(hub_inner.tx_index.len(), 2);

    // attach 3
    let (tx, _rx) = channel();
    hub_inner.queue.push(Packet::AttatchBlock(3, "chan1".to_string(), Some(vec!["label1".to_string()]), tx));

    hub_inner.handle_message_from_queue().unwrap();

    assert_eq!(hub_inner.chans.len(), 1);
    assert_eq!(hub_inner.recvs.len(), 1);
    assert_eq!(hub_inner.chans.get("chan1").unwrap().len(), 2);
    assert_eq!(hub_inner.recvs.get("chan1").unwrap().len(), 3);
    assert_eq!(hub_inner.tx_index.len(), 3);

    // attach 4
    let (tx, _rx) = channel();
    hub_inner.queue.push(Packet::AttatchBlock(4, "chan2".to_string(), Some(vec!["label3".to_string()]), tx));

    hub_inner.handle_message_from_queue().unwrap();

    assert_eq!(hub_inner.chans.len(), 2);
    assert_eq!(hub_inner.recvs.len(), 2);
    assert_eq!(hub_inner.chans.get("chan2").unwrap().len(), 1);
    assert_eq!(hub_inner.recvs.get("chan2").unwrap().len(), 1);
    assert_eq!(hub_inner.tx_index.len(), 4);

    // detach 1
    hub_inner.queue.push(Packet::Detatch(2));

    hub_inner.handle_message_from_queue().unwrap();

    assert_eq!(hub_inner.chans.len(), 2);
    assert_eq!(hub_inner.recvs.len(), 2);
    assert_eq!(hub_inner.chans.get("chan1").unwrap().len(), 1);
    assert_eq!(hub_inner.recvs.get("chan1").unwrap().len(), 2);
    assert_eq!(hub_inner.tx_index.len(), 3);

    // detach 2
    hub_inner.queue.push(Packet::Detatch(3));

    hub_inner.handle_message_from_queue().unwrap();

    assert_eq!(hub_inner.chans.len(), 2);
    assert_eq!(hub_inner.recvs.len(), 2);
    assert_eq!(hub_inner.chans.get("chan1").unwrap().len(), 1);
    assert_eq!(hub_inner.recvs.get("chan1").unwrap().len(), 1);
    assert_eq!(hub_inner.tx_index.len(), 2);

    // detach 3
    hub_inner.queue.push(Packet::Detatch(1));

    hub_inner.handle_message_from_queue().unwrap();

    assert_eq!(hub_inner.chans.len(), 1);
    assert_eq!(hub_inner.recvs.len(), 1);
    assert_eq!(hub_inner.chans.get("chan1"), None);
    assert!(hub_inner.recvs.get("chan1").is_none());
    assert_eq!(hub_inner.tx_index.len(), 1);

    // detach 4
    hub_inner.queue.push(Packet::Detatch(4));

    hub_inner.handle_message_from_queue().unwrap();

    assert_eq!(hub_inner.chans.len(), 0);
    assert_eq!(hub_inner.recvs.len(), 0);
    assert_eq!(hub_inner.chans.get("chan1"), None);
    assert!(hub_inner.recvs.get("chan1").is_none());
    assert_eq!(hub_inner.tx_index.len(), 0);
}
