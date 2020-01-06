use std::time::Duration;
use std::io::{self, ErrorKind::{PermissionDenied, InvalidData}};
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
use crate::error::{Result, ErrorCode};

use super::Connector;

type Handle = dyn Fn(Message) -> Message + Sync + Send + 'static;

pub(crate) enum Packet {
    Send {
        message: Message,
        ack_tx: OneshotSender<Result<()>>
    },
    AttachBlock {
        id: u32,
        chan: String,
        labels: Option<Vec<String>>,
        tx: Sender<Message>,
        ack_tx: OneshotSender<Result<()>>
    },
    AttachAsync {
        id: u32, 
        chan: String,
        labels: Option<Vec<String>>,
        tx: Queue<Message>,
        ack_tx: OneshotSender<Result<()>>
    },
    Detach {
        id: u32
    },
    Call {
        id: MessageId,
        method: String,
        message: Message,
        ack_tx: OneshotSender<Result<Message>>
    },
    UnCall {
        id: MessageId
    },
    Add {
        id: u32,
        method: String,
        handle: Box<Handle>,
        labels: Option<Vec<String>>,
        ack_tx: OneshotSender<Result<()>>
    },
    Remove {
        id: u32
    },
    Response {
        message: Message
    }
}

pub(crate) type WorkerQueue = BlockQueue<Option<WorkerQueueMessage>>;

#[derive(Clone)]
pub(crate) struct WorkerQueueMessage {
    pub(crate) from_id: MessageId,
    pub(crate) req_id: MessageId,
    pub(crate) req_message: Message,
    pub(crate) handle: Arc<Box<Handle>>
}

pub(crate) struct PortBackend {
    id: MessageId,
    connector: Connector,
    auth_message: Message,
    queue: mpsc::Queue<Packet>,
    epoll: Epoll,
    events: Events,
    session: Session,
    attaching: HashMap<u32, OneshotSender<Result<()>>>,
    sending: HashMap<MessageId, OneshotSender<Result<()>>>,
    calling: HashMap<MessageId, OneshotSender<Result<Message>>>,
    handles: HashMap<u32, Arc<Box<Handle>>>,
    works: usize,
    worker_queue: WorkerQueue,
    run: Arc<AtomicBool>
}

impl PortBackend {
    const QUEUE_TOKEN: usize = 0;
    const STREAM_TOKEN: usize = 1;

    pub(crate) fn new(
        id: MessageId,
        connector: Connector,
        auth_message: Message,
        queue: mpsc::Queue<Packet>,
        works: usize,
        worker_queue: WorkerQueue,
        run: Arc<AtomicBool>
    ) -> io::Result<PortBackend> {
        Ok(PortBackend {
            id,
            connector,
            auth_message,
            queue,
            epoll: Epoll::new()?,
            events: Events::with_capacity(64),
            session: Session::new(),
            attaching: HashMap::new(),
            sending: HashMap::new(),
            calling: HashMap::new(),
            handles: HashMap::new(),
            works,
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
                            net_work.run()
                        }).unwrap();

                        self.session.net_work = Some(queue2);
                    }

                    match addr.connect() {
                        Ok(net_stream) => {
                            let (stream1, stream2) = Stream::pipe(64, msg!{})?;

                            self.session.net_work.as_ref().unwrap()
                                .push(NetPacket::NewConn{
                                    stream: stream1,
                                    net_stream,
                                    options: crypto.clone()
                                });
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

            message.extend(self.auth_message.clone());

            let stream = self.session.stream.as_ref().unwrap();
            stream.send(message);

            self.epoll.add(stream, Token(Self::STREAM_TOKEN), Ready::readable(), EpollOpt::level())?;

            self.session.state = State::Authing;
        }

        Ok(())
    }

    fn attach(&mut self, id: u32, chan: &str, labels_set: &mut HashSet<String>, attaching_tx: OneshotSender<Result<()>>) {
        let must_attach = !self.session.chans.contains_key(chan);

        // chan
        let set = self.session.chans.entry(chan.to_string()).or_insert_with(HashSet::new);

        if must_attach {
            // attach
            let mut message = msg!{
                CHAN: ATTACH,
                ATTACH_ID: id,
                VALUE: chan
            };

            if !labels_set.is_empty() {
                let labels: Vec<String> = labels_set.iter().map(|s| s.to_string()).collect();
                message.insert(LABEL, labels);
            }

            let stream = self.session.stream.as_ref().unwrap();
            stream.send(message);

            // attaching
            self.attaching.insert(id, attaching_tx);
        } else if !labels_set.is_empty() {
            let diff_labels = &*labels_set - set;

            if !diff_labels.is_empty() {
                let mut message = msg!{
                    CHAN: ATTACH,
                    ATTACH_ID: id,
                    VALUE: chan
                };

                let labels: Vec<String> = diff_labels.iter().map(|s| s.to_string()).collect();
                message.insert(LABEL, labels);

                let stream = self.session.stream.as_ref().unwrap();
                stream.send(message);

                // attaching
                self.attaching.insert(id, attaching_tx);
            } else {
                attaching_tx.send(Ok(()));
            }
        } else {
            attaching_tx.send(Ok(()));
        }

        set.extend(labels_set.clone());
    }

    fn detach(&mut self, id: u32, chan: String, labels: HashSet<String>) {
        let mut remove_chan = false;

        if let Some(ids) = self.session.chans2.get_mut(&chan) {
            ids.remove(&id);

            if ids.is_empty() {
                remove_chan = true;
            } else if chan != UNKNOWN {
                let mut labels_set = HashSet::new();

                for value in self.session.recvs.values() {
                    labels_set.extend(value.2.clone());
                }

                for value in self.session.recvs2.values() {
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

            if chan != UNKNOWN {
                let message = msg!{
                    CHAN: DETACH,
                    VALUE: chan
                };

                let stream = self.session.stream.as_ref().unwrap();
                stream.send(message);
            }
        }
    }

    fn detach_no_send(&mut self, id: u32, chan: String, labels: HashSet<String>) {
        let mut remove_chan = false;

        if let Some(ids) = self.session.chans2.get_mut(&chan) {
            ids.remove(&id);

            if ids.is_empty() {
                remove_chan = true;
            } else {
                let mut labels_set = HashSet::new();

                for value in self.session.recvs.values() {
                    labels_set.extend(value.2.clone());
                }

                for value in self.session.recvs2.values() {
                    labels_set.extend(value.1.clone());
                }

                let diff_labels = &labels - &labels_set;

                if !diff_labels.is_empty() {
                    // chans 1
                    self.session.chans.insert(chan.to_string(), labels_set);
                }
            }
        }

        if remove_chan {
            self.session.chans.remove(&chan);
            self.session.chans2.remove(&chan);
        }
    }

    fn dispatch_queue(&mut self) -> io::Result<()> {
        if self.session.state == State::Authed {
            if let Some(packet) = self.queue.pop() {
                match packet {
                    Packet::Send { message, ack_tx } => {
                        let message_id = message.get_message_id(ID).expect("InvalidData");

                        self.sending.insert(message_id.to_owned(), ack_tx);

                        let stream = self.session.stream.as_ref().unwrap();
                        stream.send(message);
                    }
                    Packet::AttachBlock { id, chan, labels, tx, ack_tx } => {
                        let mut labels_set = HashSet::new();

                        if chan == UNKNOWN {
                            ack_tx.send(Ok(()));
                        } else {
                            if let Some(labels) = labels {
                                for label in labels {
                                    labels_set.insert(label.to_string());
                                }
                            }

                            self.attach(id, &chan, &mut labels_set, ack_tx);
                        }

                        // chan2
                        let set = self.session.chans2.entry(chan.clone()).or_insert_with(HashSet::new);
                        set.insert(id);

                        // recvs
                        self.session.recvs.insert(id, (chan, SenderType::Block(tx), labels_set));
                    }
                    Packet::AttachAsync { id, chan, labels, tx, ack_tx } => {
                        let mut labels_set = HashSet::new();

                        if chan == UNKNOWN {
                            ack_tx.send(Ok(()));
                        } else {
                            if let Some(labels) = labels {
                                for label in labels {
                                    labels_set.insert(label.to_string());
                                }
                            }

                            self.attach(id, &chan, &mut labels_set, ack_tx);
                        }

                        // chans 2
                        let set = self.session.chans2.entry(chan.clone()).or_insert_with(HashSet::new);
                        set.insert(id);

                        // recvs
                        self.session.recvs.insert(id, (chan, SenderType::Async(tx), labels_set));
                    }
                    Packet::Detach { id } => {
                        if let Some(recv) = self.session.recvs.remove(&id) {
                            self.detach(id, recv.0, recv.2);
                        }
                    },
                    Packet::Call { id, method, mut message, ack_tx } => {
                        let req_chan = format!("RPC/REQ/{}", method);
                        message.insert(CHAN, req_chan);
                        message.insert(SHARE, true);

                        let stream = self.session.stream.as_ref().unwrap();
                        stream.send(message);

                        self.calling.insert(id, ack_tx);
                    },
                    Packet::UnCall { id } => {
                        self.calling.remove(&id);
                    },
                    Packet::Add { id, method, handle, labels, ack_tx } => {
                        let req_chan = format!("RPC/REQ/{}", method);

                        let mut labels_set = HashSet::new();

                        if let Some(labels) = labels {
                            for label in labels {
                                labels_set.insert(label.to_string());
                            }
                        }

                        self.attach(id, &req_chan, &mut labels_set, ack_tx);

                        // chans 2
                        let set = self.session.chans2.entry(req_chan.clone()).or_insert_with(HashSet::new);
                        set.insert(id);

                        self.session.recvs2.insert(id, (req_chan, labels_set));

                        self.handles.insert(id, Arc::new(handle));
                    },
                    Packet::Remove { id } => {
                        if let Some(recv) = self.session.recvs2.remove(&id) {
                            self.detach(id, recv.0, recv.1);
                        }
                    },
                    Packet::Response { message } => {
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
                                    return Err(io::Error::new(InvalidData, "InvalidData"))
                                }
                            }
                            ATTACH => {
                                let ok = if let Ok(ok) = message.get_i32(OK) {
                                    ok
                                } else {
                                    return Err(io::Error::new(InvalidData, "InvalidData"))
                                };

                                let attach_id = if let Ok(attach_id) = message.get_u32(ATTACH_ID) {
                                    attach_id
                                } else {
                                    return Err(io::Error::new(InvalidData, "InvalidData"))
                                };

                                if let Some(attaching_tx) = self.attaching.remove(&attach_id) {
                                    if ok == 0 {
                                        attaching_tx.send(Ok(()));
                                    } else {
                                        attaching_tx.send(Err(ErrorCode::from_i32(ok).into()));

                                        if let Some(recv) = self.session.recvs.remove(&attach_id) {
                                            self.detach_no_send(attach_id, recv.0, recv.2);
                                        }

                                        if let Some(recv) = self.session.recvs2.remove(&attach_id) {
                                            self.detach_no_send(attach_id, recv.0, recv.1);
                                        }
                                    }
                                }
                            }
                            _ => ()
                        }
                    } else {
                        let chan = chan.to_string();
                        self.handle_message(&chan, message)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_message(&mut self, chan: &str, message: Message) -> io::Result<()> {
        if let Ok(ok) = message.get_i32(OK) {
            if let Ok(message_id) = message.get_message_id(ID) {
                if let Some(sending_tx) = self.sending.remove(message_id) {
                    if ok == 0 {
                        sending_tx.send(Ok(()));
                    } else {
                        sending_tx.send(Err(ErrorCode::from_i32(ok).into()));
                    }
                }
            } else if let Ok(request_id) = message.get_message_id(REQUEST_ID) {
                if let Some(sending_tx) = self.calling.remove(request_id) {
                    sending_tx.send(Err(ErrorCode::from_i32(ok).into()));
                }
            } else {
                return Err(io::Error::new(InvalidData, "InvalidData"));
            }

            return Ok(())
        }

        if chan == RPC_RECV {
            if let Ok(request_id) = message.get_message_id(REQUEST_ID) {
                if let Some(tx) = self.calling.remove(request_id) {
                    if tx.is_needed() {
                        tx.send(Ok(message));
                    }
                }
            }

            return Ok(())
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
                    if !labels.is_empty() && (&labels & label2).is_empty() {
                        continue;
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
                    if !labels.is_empty() && (&labels & label2).is_empty() {
                        continue;
                    }

                    if let Ok(from_id) = message.get_message_id(FROM) {
                        if let Ok(request_id) = message.get_message_id(REQUEST_ID) {
                            if let Some(handle) = self.handles.get(id) {
                                self.worker_queue.push(Some(WorkerQueueMessage {
                                    from_id: from_id.clone(),
                                    req_id: request_id.clone(),
                                    req_message: message.clone(),
                                    handle: handle.clone()
                                }))
                            }       
                        }
                    }
                }
            }

            return Ok(())
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

        Ok(())
    }
}

impl Drop for PortBackend {
    fn drop(&mut self) {
        self.run.store(false, Ordering::Relaxed);

        loop {
            if self.queue.pop().is_none() {
                break;
            }
        }

        for _ in 0..self.works {
            self.worker_queue.push(None);
        }
    }
}

struct Session {
    state: State,
    stream: Option<Stream>,
    net_work: Option<Queue<NetPacket>>,
    chans: HashMap<String, HashSet<String>>, // HashMap<Chan, HashSet<Label>>
    chans2: HashMap<String, HashSet<u32>>, // HashMap<Chan, id>
    recvs: HashMap<u32, (String, SenderType, HashSet<String>)>, // HashMap<id, (Chan, tx, Labels)>
    recvs2: HashMap<u32, (String, HashSet<String>)> // HashMap<id, (Chan, Labels)>
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
