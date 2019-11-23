use std::time::Duration;
use std::io::{self, ErrorKind::PermissionDenied};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::collections::{HashMap, HashSet};

use queen_io::epoll::{Epoll, Events, Token, Ready, EpollOpt};
use queen_io::queue::spsc::Queue;
use queen_io::plus::block_queue::BlockQueue;
use queen_io::queue::mpsc;

use nson::{Message, msg};
use nson::message_id::MessageId;

use crate::Stream;
use crate::net::{NetWork, Packet as NetPacket};
use crate::dict::*;
use crate::Connector;
use crate::util::oneshot::Sender;

type Handle = dyn Fn(Message) -> Message + Sync + Send + 'static;

pub(crate) enum Packet {
    Call(MessageId, String, Message, Sender<Message>),
    UnCall(MessageId),
    Add(String, Box<Handle>, Option<Vec<String>>),
    Remove(String),
    Response(Message)
}

pub(crate) type WorkerQueue = BlockQueue<Option<(MessageId, MessageId, Message, Arc<Box<Handle>>)>>;

pub(crate) struct RpcBackend {
    id: MessageId,
    connector: Connector,
    auth_msg: Message,
    queue: mpsc::Queue<Packet>,
    epoll: Epoll,
    events: Events,
    session: Session,
    worker_queue: WorkerQueue,
    calling: HashMap<MessageId, Sender<Message>>,
    handles: HashMap<String, Arc<Box<Handle>>>,
    run: Arc<AtomicBool>
}

impl RpcBackend {
    const QUEUE_TOKEN: usize = 0;
    const STREAM_TOKEN: usize = 1;

    pub(crate) fn new(
        id: MessageId,
        connector: Connector,
        auth_msg: Message,
        queue: mpsc::Queue<Packet>,
        worker_queue: WorkerQueue,
        run: Arc<AtomicBool>
    ) -> io::Result<RpcBackend> {
        Ok(RpcBackend {
            id,
            connector,
            auth_msg,
            queue,
            epoll: Epoll::new()?,
            events: Events::with_capacity(64),
            session: Session::new(),
            worker_queue,
            calling: HashMap::new(),
            handles: HashMap::new(),
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
                    Packet::Call(id, method, mut message, tx) => {
                        let req_chan = format!("RPC/REQ/{}", method);
                        message.insert(CHAN, req_chan);

                        let stream = self.session.stream.as_ref().unwrap();
                        stream.send(message);

                        self.calling.insert(id, tx);
                    }
                    Packet::UnCall(id) => {
                        self.calling.remove(&id);
                    }
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
                    }
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
                    }
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

                                        let stream = self.session.stream.as_ref().unwrap();

                                        for (chan, set) in &self.session.chans {
                                            let labels: Vec<String> = set.iter().map(|s| s.to_string()).collect();

                                            let message = msg!{
                                                CHAN: ATTACH,
                                                VALUE: chan,
                                                LABEL: labels
                                            };

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
                        self.handle_message(chan.to_string(), message);
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_message(&mut self, chan: String, message: Message) {
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

impl Drop for RpcBackend {
    fn drop(&mut self) {
        self.run.store(false, Ordering::Relaxed);
    }
}

struct Session {
    state: State,
    stream: Option<Stream>,
    net_work: Option<Queue<NetPacket>>,
    chans: HashMap<String, HashSet<String>> // HashMap<Chan, HashSet<Label>>
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
            chans: HashMap::new()
        }
    }
}
