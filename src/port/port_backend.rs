use std::time::Duration;
use std::io::{self, ErrorKind::PermissionDenied};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::sync::mpsc::{Sender};
use std::collections::{HashMap, HashSet};

use queen_io::epoll::{Epoll, Events, Token, Ready, EpollOpt};
use queen_io::queue::spsc::Queue;
use queen_io::queue::mpsc;
use queen_io::plus::block_queue::BlockQueue;

use nson::{Message, msg};
use nson::message_id::MessageId;

use crate::Stream;
use crate::net::{NetWork, Packet as NetPacket};
use crate::util::oneshot::Sender as OneshotSender;
use crate::dict::*;

use super::Connector;

type Handle = dyn Fn(Message) -> Message + Sync + Send + 'static;

pub(crate) enum Packet {
    Send(Message),
    AttachBlock(usize, String, Option<Vec<String>>, Sender<Message>), // id, chan, labels, sender
    AttachAsync(usize, String, Option<Vec<String>>, Queue<Message>),
    Detach(usize),
    // sync
    Call(MessageId, String, Message, OneshotSender<Message>), // id, method, message, sender
    UnCall(MessageId),
    Add(usize, String, Box<Handle>, Option<Vec<String>>), // id, method, hendle, labels
    Remove(usize),
    Response(Message)
}

pub(crate) type WorkerQueue = BlockQueue<Option<(MessageId, MessageId, Message, Arc<Box<Handle>>)>>;

pub(crate) struct PortBackend {
    id: MessageId,
    connector: Connector,
    auth_msg: Message,
    queue: mpsc::Queue<Packet>,
    epoll: Epoll,
    events: Events,
    session: Session,
    // sync
    calling: HashMap<MessageId, OneshotSender<Message>>,
    handles: HashMap<usize, Arc<Box<Handle>>>,
    worker_queue: WorkerQueue,
    run: Arc<AtomicBool>
}

impl PortBackend {
    const QUEUE_TOKEN: usize = 0;
    const STREAM_TOKEN: usize = 1;

    pub(crate) fn new(
        id: MessageId,
        connector: Connector,
        auth_msg: Message,
        queue: mpsc::Queue<Packet>,
        worker_queue: WorkerQueue,
        run: Arc<AtomicBool>
    ) -> io::Result<PortBackend> {
        Ok(PortBackend {
            id,
            connector,
            auth_msg,
            queue,
            epoll: Epoll::new()?,
            events: Events::with_capacity(64),
            session: Session::new(),
            calling: HashMap::new(),
            handles: HashMap::new(),
            worker_queue,
            run
        })
    }

    pub(crate) fn run(&mut self) -> io::Result<()> {
        self.epoll.add(&self.queue, Token(Self::QUEUE_TOKEN), Ready::readable(), EpollOpt::level())?;

        while self.run.load(Ordering::Relaxed) {
            let size = self.epoll.wait(&mut self.events, Some(<Duration>::from_secs(1)))?;

            if !self.is_connect()? {
                thread::sleep(Duration::from_secs(1));
                continue;
            }

            if !self.is_link() {
                thread::sleep(Duration::from_secs(1));
                continue;
            }

            self.auth()?;
        
            for i in 0..size {
                let event = self.events.get(i).unwrap();

                match event.token().0 {
                    Self::QUEUE_TOKEN => self.dispatch_queue()?,
                    Self::STREAM_TOKEN => self.dispatch_stream()?,
                    _ => ()
                }
            }
        }

        Ok(())
    }

    fn is_connect(&mut self) -> io::Result<bool> {
        if self.session.state == State::UnConnect {
            match &self.connector {
                Connector::Net(addr, crypto) => {
                    if self.session.net_work.is_none() {
                        let queue: Queue<NetPacket> = Queue::with_cache(16)?;
                        let queue2 = queue.clone();
                        let mut net_work = NetWork::new(queue, self.run.clone())?;

                        thread::Builder::new().name("port_net".to_string()).spawn(move || {
                            net_work.run().unwrap()
                        }).unwrap();

                        self.session.net_work = Some(queue2);
                    }

                    match addr.connect() {
                        Ok(net_stream) => {
                            let (stream1, stream2) = Stream::pipe(64, msg!{})?;

                            self.session.net_work.as_ref().unwrap().push(NetPacket::NewConn(stream1, net_stream, crypto.clone()));
                            self.session.stream = Some(stream2);
                            self.session.state = State::UnAuth;

                        },
                        Err(_err) => {
                            // println!("{:?}", err);
                            return Ok(false)
                        }
                    }
                    

                },
                Connector::Queen(queen, attr) => {
                    let stream = queen.connect(attr.clone(), None)?;

                    self.session.stream = Some(stream);
                    self.session.state = State::UnAuth;
                }
            }
        }

        Ok(true)
    }

    fn is_link(&mut self) -> bool { 
        match &self.session.stream {
            Some(stream) => {
                if stream.is_close() {
                    self.session.state = State::UnConnect;
                    self.session.stream = None;
                    self.session.net_work = None;

                    false
                } else {
                    true
                }
            }
            None => false
        }
    }

    fn auth(&mut self) -> io::Result<()> {
        if self.session.state == State::UnAuth { 
            let mut message = msg!{
                CHAN: AUTH,
                PORT_ID: self.id.clone()
            };

            message.extend(self.auth_msg.clone());

            let stream = self.session.stream.as_ref().unwrap();
            stream.send(message);

            self.epoll.add(stream, Token(Self::STREAM_TOKEN), Ready::readable(), EpollOpt::level())?;

            self.session.state = State::Authing;
        }

        Ok(())
    }

    fn attach(&mut self, chan: &str, labels_set: &mut HashSet<String>) {
        let must_attach = !self.session.chans.contains_key(chan);

        // chan
        let set = self.session.chans.entry(chan.to_string()).or_insert_with(HashSet::new);

        if must_attach {
            // attach
            let mut message = msg!{
                CHAN: ATTACH,
                VALUE: chan
            };

            if !labels_set.is_empty() {
                let labels: Vec<String> = labels_set.iter().map(|s| s.to_string()).collect();
                message.insert(LABEL, labels);
            }

            let stream = self.session.stream.as_ref().unwrap();
            stream.send(message);
        } else {
            if !labels_set.is_empty() {
                let diff_labels = &*labels_set - set;

                if !diff_labels.is_empty() {
                    let mut message = msg!{
                        CHAN: ATTACH,
                        VALUE: chan
                    };

                    let labels: Vec<String> = diff_labels.iter().map(|s| s.to_string()).collect();
                    message.insert(LABEL, labels);

                    let stream = self.session.stream.as_ref().unwrap();
                    stream.send(message);
                }
            }
        }

        set.extend(labels_set.clone());
    }

    fn detach(&mut self, id: usize, chan: String, labels: HashSet<String>) {
        let mut remove_chan = false;

        if let Some(ids) = self.session.chans2.get_mut(&chan) {
            ids.remove(&id);

            if ids.is_empty() {
                remove_chan = true;
            } else if chan != REPLY && chan != UNKNOWN {
                let mut labels_set = HashSet::new();

                for (_, value) in &self.session.recvs {
                    labels_set.extend(value.2.clone());
                }

                for (_, value) in &self.session.recvs2 {
                    labels_set.extend(value.1.clone());
                }

                let diff_labels = &labels - &labels_set;

                if !diff_labels.is_empty() {

                    let change: Vec<String> = diff_labels.iter().map(|s| s.to_string()).collect();

                    let message = msg!{
                        CHAN: DETACH,
                        VALUE: &chan,
                        LABEL: change
                    };

                    let stream = self.session.stream.as_ref().unwrap();
                    stream.send(message);
                    // chans 1
                    self.session.chans.insert(chan.to_string(), labels_set);
                }
            }
        }

        if remove_chan {
            self.session.chans.remove(&chan);
            self.session.chans2.remove(&chan);

            if chan != REPLY && chan != UNKNOWN {
                let message = msg!{
                    CHAN: DETACH,
                    VALUE: chan
                };

                let stream = self.session.stream.as_ref().unwrap();
                stream.send(message);
            }
        }
    }

    fn dispatch_queue(&mut self) -> io::Result<()> {
        if self.session.state == State::Authed {
            if let Some(packet) = self.queue.pop() {
                match packet {
                    Packet::Send(message) => {
                        let stream = self.session.stream.as_ref().unwrap();
                        stream.send(message);
                    }
                    Packet::AttachBlock(id, chan, labels, tx) => {
                        let mut labels_set = HashSet::new();

                        if chan != REPLY && chan != UNKNOWN {
                            if let Some(labels) = labels {
                                for label in labels {
                                    labels_set.insert(label.to_string());
                                }
                            }

                            self.attach(&chan, &mut labels_set);
                        }

                        // chan2
                        let set = self.session.chans2.entry(chan.clone()).or_insert_with(HashSet::new);
                        set.insert(id);

                        // recvs
                        self.session.recvs.insert(id, (chan, SenderType::Block(tx), labels_set));
                    }
                    Packet::AttachAsync(id, chan, labels, tx) => {
                        let mut labels_set = HashSet::new();

                        if chan != REPLY && chan != UNKNOWN {
                            if let Some(labels) = labels {
                                for label in labels {
                                    labels_set.insert(label.to_string());
                                }
                            }

                            self.attach(&chan, &mut labels_set);
                        }

                        // chans 2
                        let set = self.session.chans2.entry(chan.clone()).or_insert_with(HashSet::new);
                        set.insert(id);

                        // recvs
                        self.session.recvs.insert(id, (chan, SenderType::Async(tx), labels_set));
                    }
                    Packet::Detach(id) => {
                        if let Some(recv) = self.session.recvs.remove(&id) {
                            self.detach(id, recv.0, recv.2);
                        }
                    },
                    Packet::Call(id, method, mut message, tx) => {
                        let req_chan = format!("RPC/REQ/{}", method);
                        message.insert(CHAN, req_chan);
                        message.insert(SHARE, true);

                        let stream = self.session.stream.as_ref().unwrap();
                        stream.send(message);

                        self.calling.insert(id, tx);
                    },
                    Packet::UnCall(id) => {
                        self.calling.remove(&id);
                    },
                    Packet::Add(id, method, handle, labels) => {
                        let req_chan = format!("RPC/REQ/{}", method);

                        let mut labels_set = HashSet::new();

                        if let Some(labels) = labels {
                            for label in labels {
                                labels_set.insert(label.to_string());
                            }
                        }

                        self.attach(&req_chan, &mut labels_set);

                        // chans 2
                        let set = self.session.chans2.entry(req_chan.clone()).or_insert_with(HashSet::new);
                        set.insert(id);

                        self.session.recvs2.insert(id, (req_chan, labels_set));

                        self.handles.insert(id, Arc::new(handle));
                    },
                    Packet::Remove(id) => {
                        if let Some(recv) = self.session.recvs2.remove(&id) {
                            self.detach(id, recv.0, recv.1);
                        }
                    },
                    Packet::Response(message) => {
                        let stream = self.session.stream.as_ref().unwrap();
                        stream.send(message);
                    }
                }
            }
        } else {
            thread::sleep(Duration::from_millis(10));
        }

        Ok(())
    }

    fn dispatch_stream(&mut self) -> io::Result<()> {
        if let Some(stream) = &self.session.stream {
            if let Some(message) = stream.recv() {
                if let Ok(chan) = message.get_str(CHAN) {
                    if chan.starts_with('_') {
                        match chan {
                            AUTH => {
                                if let Ok(ok) = message.get_i32(OK) {
                                    if ok == 0 {
                                        self.session.state = State::Authed;

                                        for (chan, set) in &self.session.chans {
                                            let labels: Vec<String> = set.iter().map(|s| s.to_string()).collect();

                                            let message = msg!{
                                                CHAN: ATTACH,
                                                VALUE: chan,
                                                LABEL: labels
                                            };

                                            let stream = self.session.stream.as_ref().unwrap();
                                            stream.send(message);
                                        }
                                    } else {
                                        return Err(io::Error::new(PermissionDenied, "PermissionDenied"))
                                    }
                                } else {
                                    return Err(io::Error::new(PermissionDenied, "PermissionDenied"))
                                }
                            }
                            ATTACH => {
                                // println!("{:?}", message);
                            }
                            _ => ()
                        }
                    } else {
                        let chan = chan.to_string();
                        self.handle_message(&chan, message);
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_message(&mut self, chan: &str, message: Message) {
        if let Ok(_ok) = message.get_i32(OK) {
            if let Some(ids) = self.session.chans2.get(REPLY) {
                for id in ids {
                    if let Some((_, tx, _)) = self.session.recvs.get(id) {
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

            return
        }

        if chan == RPC_RECV {
            if let Ok(request_id) = message.get_message_id(REQUEST_ID) {
                if let Some(tx) = self.calling.remove(request_id) {
                    if tx.is_needed() {
                        tx.send(message.clone());
                    }
                }
            }
        }

        if let Some(ids) = self.session.chans2.get(chan) {
            let mut labels = HashSet::new();

            if let Some(label) = message.get(LABEL) {
                if let Some(label) = label.as_str() {
                    labels.insert(label.to_string());
                } else if let Some(label_array) = label.as_array() {
                    for v in label_array {
                        if let Some(v) = v.as_str() {
                            labels.insert(v.to_string());
                        }
                    }
                }
            }

            for id in ids {
                if let Some((_, tx, label2)) = self.session.recvs.get(id) {
                    if !labels.is_empty() {
                        if (&labels & label2).is_empty() {
                            continue;
                        }
                    }

                    match tx {
                        SenderType::Block(tx) => {
                            let _ = tx.send(message.clone());
                        }
                        SenderType::Async(queue) => {
                            queue.push(message.clone());
                        }
                    }
                }

                if let Some((_, label2)) = self.session.recvs2.get(id) {
                    if !labels.is_empty() {
                        if (&labels & label2).is_empty() {
                            continue;
                        }
                    }

                    if let Ok(from_id) = message.get_message_id(FROM) {
                        if let Ok(request_id) = message.get_message_id(REQUEST_ID) {
                            if let Some(handle) = self.handles.get(id) {
                                self.worker_queue.push(Some((from_id.clone(), request_id.clone(), message.clone(), handle.clone())))
                            }       
                        }
                    }
                }
            }
        }

        if let Some(ids) = self.session.chans2.get(UNKNOWN) {
            for id in ids {
                if let Some((_, tx, _)) = self.session.recvs.get(id) {
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

impl Drop for PortBackend {
    fn drop(&mut self) {
        self.run.store(false, Ordering::Relaxed);
    }
}

struct Session {
    state: State,
    stream: Option<Stream>,
    net_work: Option<Queue<NetPacket>>,
    chans: HashMap<String, HashSet<String>>, // HashMap<Chan, HashSet<Label>>
    chans2: HashMap<String, HashSet<usize>>, // HashMap<Chan, id>
    recvs: HashMap<usize, (String, SenderType, HashSet<String>)>, // HashMap<id, (Chan, tx, Labels)>
    recvs2: HashMap<usize, (String, HashSet<String>)> // HashMap<id, (Chan, Labels)>
}

enum SenderType {
    Block(Sender<Message>),
    Async(Queue<Message>)
}

#[derive(Debug, Eq, PartialEq, PartialOrd)]
enum State {
    UnConnect,
    UnAuth,
    Authing,
    Authed
}

impl Session {
    fn new() -> Session {
        Session {
            state: State::UnConnect,
            stream: None,
            net_work: None,
            chans: HashMap::new(),
            chans2: HashMap::new(),
            recvs: HashMap::new(),
            recvs2: HashMap::new(),
        }
    }
}
