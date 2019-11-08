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

use nson::{Message, msg};
use nson::message_id::MessageId;

use crate::{Stream};
use crate::net::{NetWork, Packet as NetPacket};
use crate::dict::*;

use super::Connector;

pub(crate) enum Packet {
    Send(Message),
    AttatchBlock(usize, String, Option<Vec<String>>, Sender<Message>), // id, chan, lables, sender
    AttatchAsync(usize, String, Option<Vec<String>>, Queue<Message>),
    Detatch(usize)
}

pub(crate) struct PortBackend {
    id: MessageId,
    connector: Connector,
    auth_msg: Message,
    queue: mpsc::Queue<Packet>,
    epoll: Epoll,
    events: Events,
    session: Session,
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
            run
        })
    }

    pub(crate) fn run(&mut self) -> io::Result<()> {
        self.epoll.add(&self.queue, Token(Self::QUEUE_TOKEN), Ready::readable(), EpollOpt::level())?;

        while self.run.load(Ordering::Relaxed) {
            let size = self.epoll.wait(&mut self.events, Some(<Duration>::from_secs(1)))?;

            if !self.is_connect()? {
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
                        Err(err) => {
                            println!("{:?}", err);
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
        if self.session.stream.as_ref().unwrap().is_close() {
            self.session.stream = None;
            self.session.net_work = None;
            return false
        }

        true
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
                    Packet::AttatchBlock(id, chan, labels, tx) => {
                        let ids = self.session.recvs.entry(chan.clone()).or_insert_with(|| vec![]);

                        let mut labels_set = HashSet::new();

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

                        ids.push((id, SenderType::Block(tx), labels));
                        self.session.recvs_index.insert(id, chan);
                    }
                    Packet::AttatchAsync(id, chan, labels, tx) => {
                        let ids = self.session.recvs.entry(chan.clone()).or_insert_with(|| vec![]);

                        let mut labels_set = HashSet::new();

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

                        ids.push((id, SenderType::Async(tx), labels));
                        self.session.recvs_index.insert(id, chan);
                    }
                    Packet::Detatch(id) => {
                        if let Some(chan) = self.session.recvs_index.remove(&id) {
                            let mut remove_chan = false;

                            if let Some(ids) = self.session.recvs.get_mut(&chan) {
                                if let Some(pos) = ids.iter().position(|(x, _, _)| x == &id) {
                                    ids.remove(pos);
                                }

                                if ids.is_empty() {
                                    remove_chan = true;
                                } else {
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
                            
                                let message = msg!{
                                    CHAN: DETACH,
                                    VALUE: chan
                                };

                                let stream = self.session.stream.as_ref().unwrap();
                                stream.send(message);
                            }
                        }
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
                    if chan.starts_with("_") {
                        match chan {
                            AUTH => {
                                if let Ok(ok) = message.get_i32(OK) {
                                    if ok == 0 {
                                        self.session.state = State::Authed;
                                    } else {
                                        return Err(io::Error::new(PermissionDenied, "PermissionDenied"))
                                    }
                                } else {
                                    return Err(io::Error::new(PermissionDenied, "PermissionDenied"))
                                }
                            }
                            ATTACH => {
                                println!("{:?}", message);
                            }
                            _ => ()
                        }
                    } else {
                        self.handle_message(chan.to_string(), message)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_message(&mut self, chan: String, message: Message) -> io::Result<()> {
        if let Ok(_ok) = message.get_i32(OK) {
            return Ok(())
        }

        if let Some(ids) = self.session.recvs.get(&chan) {
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

        Ok(())
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
    recvs: HashMap<String, Vec<(usize, SenderType, Option<Vec<String>>)>>, // HashMap<Chan, Vec<id, tx, Vec<Lable>>>
    recvs_index: HashMap<usize, String>,
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
            recvs: HashMap::new(),
            recvs_index: HashMap::new()
        }
    }
}
