extern crate byteorder;
extern crate queen_io;
extern crate queen_core;
#[macro_use]
extern crate queen_log;

use std::io;
use std::thread;
use std::sync::mpsc::TryRecvError;
use std::io::ErrorKind::{WouldBlock, ConnectionAborted};
use std::collections::HashMap;

use queen_io::*;
use queen_io::channel::{self, Receiver, Sender};

use queen_core::{Service, ServiceMessage, Command};
use queen_core::wire_protocol::{Message, OpCode};

// use queen_log::Level;
// use queen_log::NopLogger;
// use queen_log::init;

const RECV: Token = Token(1);
const TIME: Token = Token(2);

struct Queen {
    poll: Poll,
    events: Events,
    send: Sender<ServiceMessage>,
    recv: Receiver<ServiceMessage>,
    sessions: Session
}

#[derive(Debug)]
struct Session {
    send: Sender<ServiceMessage>,
    session_1: HashMap<String, usize>,
    session_2: HashMap<usize, String>,
    subscribes: Vec<(String, usize)>,
    node: Node,
    cache: HashMap<String, usize>
}

#[derive(Debug)]
struct Node {
    name: String,
    nodes: Vec<Node>,
    services: Vec<String>,
    subscribes: Vec<String>
}

impl Node {
    fn new(name: &str) -> Node {
        Node {
            name: name.to_owned(),
            nodes: Vec::new(),
            services: Vec::new(),
            subscribes: Vec::new()
        }
    }
}

impl Session {
    fn new(name: &str, send: Sender<ServiceMessage>) -> Session {
        Session {
            send: send,
            session_1: HashMap::new(),
            session_2: HashMap::new(),
            subscribes: Vec::new(),
            node: Node::new(name),
            cache: HashMap::new()
        }
    }

    fn handle(&mut self, id: usize, message: Message) -> io::Result<()> {

        //INFO!("opcode: {:?}", message.opcode);

        match message.opcode {
            OpCode::CONNECT => {
                //INFO!("opcode::connect, id: {:?}, message: {:?}", id, message);

                let origin = message.origin.clone();

                if self.has_name(&origin) {

                    let mut error_message = message;
                    error_message.opcode = OpCode::ERROR;

                    let _ = self.send.send(ServiceMessage::Message(id, error_message));
                    let _ = self.send.send(ServiceMessage::Command(Command::CloseConn { id: id}));

                } else {

                    self.add(id, origin.clone());

                    let mut connack_message = message;
                    connack_message.opcode = OpCode::CONNACK;
                    connack_message.target = origin;
                    connack_message.origin = self.node.name.clone();

                    //INFO!("opcode::connack, id: {:?}, message: {:?}", id, connack_message);
                    let _ = self.send.send(ServiceMessage::Message(id, connack_message));
                }
            }
            OpCode::PING => {
                //INFO!("opcode::ping, id: {:?}, message: {:?}", id, message);
                let mut pong_message = message;
                pong_message.opcode = OpCode::PONG;
                // todo: ping other server?

                //INFO!("opcode::pong, id: {:?}, message: {:?}", id, pong_message);
                let _ = self.send.send(ServiceMessage::Message(id, pong_message));
            }
            OpCode::REQUEST | OpCode::RESPONSE => {
                let target = message.target.clone();

                if let Some(id) = self.session_1.get(&target) {
                    //INFO!("redirect message to id: {:?}, name: {:?}", id, target);
                    let _ = self.send.send(ServiceMessage::Message(*id, message));

                    return Ok(())
                } else if let Some(id) = self.cache.get(&target) {
                    //INFO!("redirect message to id: {:?}, name: {:?}", id, target);
                    let _ = self.send.send(ServiceMessage::Message(*id, message));
                    // Todo: node
                    return Ok(())
                }

                //INFO!("target({:?}) not found", target);
                let mut error_message = message;
                error_message.opcode = OpCode::ERROR;
                let _ = self.send.send(ServiceMessage::Message(id, error_message));
            }
            OpCode::SUBSCRIBE => {
                //INFO!("opcode::subscribe, id: {:?}, message: {:?}", id, message);

                let target = message.target.clone();
                self.subscribes.push((target, id)); // unique? filter?
                // Todo: sync

                let mut suback_message = message;
                suback_message.opcode = OpCode::SUBACK;
                let _ = self.send.send(ServiceMessage::Message(id, suback_message));
            }
            OpCode::UNSUBSCRIBE => {
                //INFO!("opcode::unsubscribe, id: {:?}, message: {:?}", id, message);

                let target = message.target.clone();

                self.subscribes = self.subscribes.clone().into_iter().filter(|&(ref name, i)| i == id && name == &target).collect();

                let mut unsubscribe_message = message;
                unsubscribe_message.opcode = OpCode::UNSUBSCRIBE;
                let _ = self.send.send(ServiceMessage::Message(id, unsubscribe_message));
            }
            OpCode::PUBLISH => {
                //INFO!("opcode::publish, id: {:?}, message:: {:?}", id, message);

                let target = message.target.clone();

                for &(ref name, id) in self.subscribes.iter() {
                    if name == &target {
                        let mut publish_message = message.clone();

                        publish_message.target.clear();
                        publish_message.origin = target.clone();

                        let _ = self.send.send(ServiceMessage::Message(id, publish_message));
                    }
                }

                let mut puback_message = message.clone();
                puback_message.opcode = OpCode::PUBACK;
                puback_message.content_type = 0;
                puback_message.body = vec![];
                let _ = self.send.send(ServiceMessage::Message(id, puback_message));
            }
            _ => {
                let mut error_message = message;
                error_message.opcode = OpCode::ERROR;

                let _ = self.send.send(ServiceMessage::Message(id, error_message));
                let _ = self.send.send(ServiceMessage::Command(Command::CloseConn { id: id}));
            }
        }

        Ok(())
    }

    fn has_name(&self, name: &str) -> bool {
        if self.session_1.contains_key(name) {
            return true
        }

        // Todo: node?

        false
    }

    fn add(&mut self, id: usize, name: String) {
        // Todo: sync
        self.session_1.insert(name.clone(), id);
        self.session_2.insert(id, name.clone());
        self.node.services.push(name);
    }

    fn remove(&mut self, id: usize) {
        INFO!("remove session: {:?}", id);
        // Todo: sync
        // Todo: remove node
        if let Some(name) = self.session_2.remove(&id) {
            self.session_1.remove(&name);
            self.node.services = self.node.services.clone().into_iter().filter(|n| n == &name).collect();
        }

        self.subscribes = self.subscribes.clone().into_iter().filter(|&(_, i)| i == id).collect();
    }
}

// content type
// 0 => empty
// 1 => binary
// 2 => nson
// 3 => text
// 4 => json

impl Queen {
    fn new(name: &str) -> io::Result<Queen> {
        let (mut service, send, recv) = Service::new()?;

        let queen = Queen {
            poll: Poll::new()?,
            events: Events::with_capacity(128),
            send: send.clone(),
            recv: recv,
            //listen: None,
            //linkto: Vec::new(),
            sessions: Session::new(name, send)
        };

        queen.poll.register(&queen.recv, RECV, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;

        thread::Builder::new().name("service".to_owned()).spawn(move || {
            service.run()
        }).unwrap();

        Ok(queen)
    }

    fn recv_message(&mut self) -> io::Result<()> {
        loop {
            let msg = match self.recv.try_recv() {
                Ok(msg) => msg,
                Err(err) => {
                    if let TryRecvError::Empty = err {
                        break;
                    }

                    return Err(io::Error::new(ConnectionAborted, err).into())
                }
            };

            match msg {
                ServiceMessage::Message(id, message) => {
                    self.message_handle(id, message)?;
                }
                ServiceMessage::Command(command) => {
                    self.command_handle(command)?;
                }
            }
        }

        self.poll.reregister(&self.recv, RECV, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;

        Ok(())
    }

    fn message_handle(&mut self, id: usize, message: Message) -> io::Result<()> {
        //INFO!("message handle, id: {:?}, message: {:?}", id, message);
        self.sessions.handle(id, message)
    }

    fn command_handle(&mut self, command: Command) -> io::Result<()> {
        INFO!("command handle: {:?}", command);
        match command {
            Command::Listen {..} => {

            }
            Command::LinkTo {..} => {

            }
            Command::Shoutdown => {

            }
            Command::CloseConn {id} => {
                self.sessions.remove(id);
            }
            Command::ListenReply {id, ..} => {
                if id == 0 {

                } else {

                }
            }
            Command::LinkToReply {..} => {

            }
        }

        Ok(())
    }

    fn run(&mut self, listen: Option<String>, linkto: Option<Vec<String>>) -> io::Result<()> {
        INFO!("start");

        if listen.is_some() {
            INFO!("listen: {:?}", listen);
        }

        if linkto.is_some() {
            INFO!("Linkto: {:?}", linkto);
        }

        if let Some(addr) = listen {
            let listen_command = ServiceMessage::Command(
                Command::Listen {
                    id: 0,
                    addr: addr
                }
            );

            self.send.send(listen_command).unwrap();
        }

        if let Some(addrs) = linkto {
            for addr in addrs.into_iter() {
                let linkto_command = ServiceMessage::Command(
                    Command::LinkTo {
                        id: 0,
                        addr: addr
                    }
                );

                self.send.send(linkto_command).unwrap();
            }
        }

        loop {

            let size = self.poll.poll(&mut self.events, None)?;

            for i in 0..size {
                let event = self.events.get(i).unwrap();

                match event.token() {
                    RECV => {
                        self.recv_message()?;
                    }
                    TIME => {

                    }
                    _ => ()
                }
            }

            //break;
        }

        INFO!("shutdown");

        Ok(())
    }
}

fn main() {
    queen_log::init(queen_log::Level::Trace, &queen_log::DefaultLogger);

    let mut queen = Queen::new("aaa").unwrap();

    queen.run(Some("127.0.0.1:50000".to_owned()), None).unwrap();
}
