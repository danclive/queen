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
    AttachBlock(usize, String, Option<Vec<String>>, Sender<Message>), // id, chan, lables, sender
    AttachAsync(usize, String, Option<Vec<String>>, Queue<Message>),
    Detach(usize, String),
    // sync
    Call(MessageId, String, Message, OneshotSender<Message>),
    UnCall(MessageId),
    Add(String, Box<Handle>, Option<Vec<String>>),
    Remove(String),
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
    handles: HashMap<String, Arc<Box<Handle>>>,
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

    fn dispatch_queue(&mut self) -> io::Result<()> {
        if self.session.state == State::Authed {
            if let Some(packet) = self.queue.pop() {
                match packet {
                    Packet::Send(message) => {
                        let stream = self.session.stream.as_ref().unwrap();
                        stream.send(message);
                    }
                    Packet::AttachBlock(id, chan, labels, tx) => {
                        let ids = self.session.recvs.entry(chan.clone()).or_insert_with(|| vec![]);

                        let mut labels_set = HashSet::new();

                        if chan != REPLY {
                            if let Some(labels) = &labels {
                                for label in labels {
                                    labels_set.insert(label.to_string());
                                }
                            }

                            if ids.is_empty() {
                                let labels: Vec<String> = labels_set.iter().map(|s| s.to_string()).collect();

                                let message = msg!{
                                    CHAN: ATTACH,
                                    VALUE: &chan,
                                    LABEL: labels
                                };

                                let stream = self.session.stream.as_ref().unwrap();
                                stream.send(message);

                                self.session.chans.insert(chan.to_string(), labels_set);
                            } else {
                                let old_set = self.session.chans.get_mut(&chan).unwrap();

                                let labels: Vec<String> = labels_set.iter().filter(|l| !old_set.contains(*l)).map(|s| s.to_string()).collect();
                            
                                if !labels.is_empty() {
                                    let message = msg!{
                                        CHAN: ATTACH,
                                        VALUE: &chan,
                                        LABEL: labels
                                    };

                                    let stream = self.session.stream.as_ref().unwrap();
                                    stream.send(message);
                                }
                            }
                        }

                        ids.push((id, SenderType::Block(tx), labels));
                    }
                    Packet::AttachAsync(id, chan, labels, tx) => {
                        let ids = self.session.recvs.entry(chan.clone()).or_insert_with(|| vec![]);

                        let mut labels_set = HashSet::new();

                        if chan != REPLY {
                            if let Some(labels) = &labels {
                                for label in labels {
                                    labels_set.insert(label.to_string());
                                }
                            }

                            if ids.is_empty() {
                                let labels: Vec<String> = labels_set.iter().map(|s| s.to_string()).collect();

                                let message = msg!{
                                    CHAN: ATTACH,
                                    VALUE: &chan,
                                    LABEL: labels
                                };

                                let stream = self.session.stream.as_ref().unwrap();
                                stream.send(message);

                                self.session.chans.insert(chan.to_string(), labels_set);
                            } else {
                                let old_set = self.session.chans.get_mut(&chan).unwrap();

                                let labels: Vec<String> = labels_set.iter().filter(|l| !old_set.contains(*l)).map(|s| s.to_string()).collect();
                            
                                if !labels.is_empty() {
                                    let message = msg!{
                                        CHAN: ATTACH,
                                        VALUE: &chan,
                                        LABEL: labels
                                    };

                                    let stream = self.session.stream.as_ref().unwrap();
                                    stream.send(message);
                                }
                            }
                        }

                        ids.push((id, SenderType::Async(tx), labels));
                    }
                    Packet::Detach(id, chan) => {
                        let mut remove_chan = false;

                        if let Some(ids) = self.session.recvs.get_mut(&chan) {
                            if let Some(pos) = ids.iter().position(|(x, _, _)| x == &id) {
                                ids.remove(pos);
                            }

                            if ids.is_empty() {
                                remove_chan = true;
                            } else if chan != REPLY {
                                let old_set = self.session.chans.get_mut(&chan).unwrap();
                                    
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

                                let change: Vec<String> = old_set.iter().filter(|l| {
                                    !new_set.contains(&**l)
                                }).map(|s| s.to_string()).collect();

                                if !change.is_empty() {
                                    let message = msg!{
                                        CHAN: DETACH,
                                        VALUE: &chan,
                                        LABEL: change
                                    };

                                    let stream = self.session.stream.as_ref().unwrap();
                                    stream.send(message);
                                }
                            }
                        }

                        if remove_chan {
                            self.session.recvs.remove(&chan);
                            self.session.chans.remove(&chan);
                            
                            if chan != REPLY {
                                let message = msg!{
                                    CHAN: DETACH,
                                    VALUE: chan
                                };

                                let stream = self.session.stream.as_ref().unwrap();
                                stream.send(message);
                            }
                        }
                    },
                    Packet::Call(id, method, mut message, tx) => {
                        let req_chan = format!("RPC/REQ/{}", method);
                        message.insert(CHAN, req_chan);

                        let stream = self.session.stream.as_ref().unwrap();
                        stream.send(message);

                        self.calling.insert(id, tx);
                    },
                    Packet::UnCall(id) => {
                        self.calling.remove(&id);
                    },
                    Packet::Add(method, handle, labels) => {
                        let req_chan = format!("RPC/REQ/{}", method);

                        let mut msg = msg!{
                            CHAN: ATTACH,
                            VALUE: &req_chan
                        };

                        let mut labels_set = HashSet::new();

                        if let Some(labels) = labels {
                            for label in &labels {
                                labels_set.insert(label.to_string());
                            }

                            msg.insert(LABEL, labels);
                        }

                        let stream = self.session.stream.as_ref().unwrap();
                        stream.send(msg);

                        self.session.chans.insert(req_chan.to_string(), labels_set);
                        self.handles.insert(req_chan, Arc::new(handle));
                    },
                    Packet::Remove(method) => {
                        let req_chan = format!("RPC/REQ/{}", method);

                        let msg = msg!{
                            CHAN: DETACH,
                            VALUE: &req_chan
                        };

                        let stream = self.session.stream.as_ref().unwrap();
                        stream.send(msg);

                        self.handles.remove(&req_chan);
                        self.session.chans.remove(&req_chan);
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
                        self.handle_message(&chan, &message);
                        self.handle_message2(&chan, message);
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_message(&mut self, chan: &str, message: &Message) {
        if let Ok(_ok) = message.get_i32(OK) {
            if let Some(ids) = self.session.recvs.get(REPLY) {
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
            }

            return
        }

        if let Some(ids) = self.session.recvs.get(chan) {
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

    fn handle_message2(&mut self, chan: &str, message: Message) {
        if let Ok(_ok) = message.get_i32(OK) {
            return
        }

        if chan == RPC_RECV {
            let request_id = match message.get_message_id(REQUEST_ID) {
                Ok(id) => id,
                Err(_) => return
            };

            if let Some(tx) = self.calling.remove(request_id) {
                if tx.is_needed() {
                    tx.send(message);
                }
            }
        } else {
            let req_chan = match message.get_str(CHAN) {
                Ok(chan) => chan,
                Err(_) => return
            };

            let from_id = match message.get_message_id(FROM) {
                Ok(id) => id,
                Err(_) => return
            };

            let request_id = match message.get_message_id(REQUEST_ID) {
                Ok(id) => id,
                Err(_) => return
            };

            if let Some(handle) = self.handles.get(req_chan) {
                self.worker_queue.push(Some((from_id.clone(), request_id.clone(), message, handle.clone())))
            }
        }
    }
}

impl Drop for PortBackend {
    fn drop(&mut self) {
        self.run.store(false, Ordering::Relaxed);
    }
}

type RecvsMap = HashMap<String, Vec<(usize, SenderType, Option<Vec<String>>)>>;

struct Session {
    state: State,
    stream: Option<Stream>,
    net_work: Option<Queue<NetPacket>>,
    chans: HashMap<String, HashSet<String>>, // HashMap<Chan, HashSet<Label>>
    recvs: RecvsMap, // HashMap<Chan, Vec<id, tx, Vec<Lable>>>
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
            recvs: HashMap::new()
        }
    }
}
