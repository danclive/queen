extern crate byteorder;
extern crate queen_io;
extern crate queen_core;
#[macro_use]
extern crate queen_log;

use std::io;
use std::thread;
use std::sync::mpsc::TryRecvError;
use std::io::ErrorKind::ConnectionAborted;
use std::collections::HashMap;

use queen_io::*;
use queen_io::channel::{Receiver, Sender};

use queen_core::{Service, ServiceMessage, Command};
use queen_core::wire_protocol::{Message, OpCode};

const RECV: Token = Token(1);
const TIME: Token = Token(2);

#[allow(dead_code)]
struct Queen {
    poll: Poll,
    events: Events,
    send: Sender<ServiceMessage>,
    recv: Receiver<ServiceMessage>,
    sessions: Session
}

#[derive(Debug)]
struct Session {
    name: String,
    send: Sender<ServiceMessage>,
    session_1: HashMap<String, usize>,
    session_2: HashMap<usize, String>,
    subscribes: HashMap<String, Vec<usize>>
}

impl Session {
    fn new(name: &str, send: Sender<ServiceMessage>) -> Session {
        Session {
            name: name.to_owned(),
            send: send,
            session_1: HashMap::new(),
            session_2: HashMap::new(),
            subscribes: HashMap::new()
        }
    }

    fn handle(&mut self, id: usize, message: Message) -> io::Result<()> {
        match message.opcode {
            OpCode::CONNECT => {
                if message.content_type == 0 {
                    
                }
            }
            _ => ()
        }

        Ok(())
    }
    /*
    fn handle(&mut self, id: usize, message: Message) -> io::Result<()> {
        match message.opcode {
            OpCode::CONNECT => {
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
                    connack_message.origin = self.name.clone();

                    let _ = self.send.send(ServiceMessage::Message(id, connack_message));
                }
            }
            OpCode::PING => {
                let origin = message.origin.clone();

                let mut pong_message = message;
                pong_message.opcode = OpCode::PONG;
                pong_message.target = origin;
                pong_message.origin = self.name.clone();

                let _ = self.send.send(ServiceMessage::Message(id, pong_message));
            }
            OpCode::REQUEST | OpCode::RESPONSE => {
                let target = message.target.clone();

                if let Some(id) = self.session_1.get(&target) {
                    let _ = self.send.send(ServiceMessage::Message(*id, message));

                    return Ok(())
                }

                let mut error_message = message;
                error_message.opcode = OpCode::ERROR;
                let _ = self.send.send(ServiceMessage::Message(id, error_message));
            }
            OpCode::SUBSCRIBE => {
                let target = message.target.clone();

                if self.subscribes.contains_key(&target) {
                    if let Some(mut targets) = self.subscribes.get_mut(&target) {
                        if !targets.contains(&id) {
                            targets.push(id);
                        }
                    }
                } else {
                    self.subscribes.insert(target, vec![id]);
                }

                let mut suback_message = message;
                suback_message.opcode = OpCode::SUBACK;
                let _ = self.send.send(ServiceMessage::Message(id, suback_message));
            }
            OpCode::UNSUBSCRIBE => {
                let target = message.target.clone();

                if self.subscribes.contains_key(&target) {
                    if let Some(mut targets) = self.subscribes.get_mut(&target) {
                        if let Some(pos) = targets.iter().position(|x| *x == id) {
                            targets.remove(pos);
                        }
                    }
                }

                let mut unsubscribe_message = message;
                unsubscribe_message.opcode = OpCode::UNSUBACK;
                let _ = self.send.send(ServiceMessage::Message(id, unsubscribe_message));
            }
            OpCode::PUBLISH => {
                let target = message.target.clone();

                if let Some(targets) = self.subscribes.get(&target) {
                    for target_id in targets {
                        let mut publish_message = message.clone();

                        publish_message.target.clear();
                        publish_message.origin = target.clone();

                        let _ = self.send.send(ServiceMessage::Message(*target_id, publish_message));
                    }
                }

                let mut puback_message = message.clone();
                puback_message.opcode = OpCode::PUBACK;
                puback_message.content_type = 0;
                puback_message.body = vec![];
                let _ = self.send.send(ServiceMessage::Message(id, puback_message));
            }
            OpCode::PUBACK => {
                //unimplemented!()
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
    */

    fn has_name(&self, name: &str) -> bool {
        if self.session_1.contains_key(name) {
            return true
        }

        false
    }

    fn add(&mut self, id: usize, name: String) {
        self.session_1.insert(name.clone(), id);
        self.session_2.insert(id, name.clone());
    }

    fn remove(&mut self, id: usize) {
        INFO!("remove session: {:?}", id);

        if let Some(name) = self.session_2.remove(&id) {
            self.session_1.remove(&name);
        }

        for (_topic, targets) in self.subscribes.iter_mut() {
            if let Some(pos) = targets.iter().position(|x| *x == id) {
                targets.remove(pos);
            }
        }
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
            Command::Shoutdown => {

            }
            Command::CloseConn {id} => {
                self.sessions.remove(id);
            }
            _ => ()
        }

        Ok(())
    }

    #[allow(dead_code)]
    fn listen(&self, addr: &str) -> io::Result<()> {
        let listen_command = ServiceMessage::Command(
            Command::Listen {
                id: 0,
                addr: addr.to_owned()
            }
        );

        self.send.send(listen_command).unwrap();

        Ok(())
    }

    #[allow(dead_code)]
    fn link_to(&mut self, addr: &str) -> io::Result<()> {
        let connent_command = ServiceMessage::Command(
            Command::Connent {
                id: 0,
                addr: addr.to_owned()
            }
        );

        self.send.send(connent_command).unwrap();

        Ok(())
    }

    fn run(&mut self) -> io::Result<()> {
        INFO!("start");

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
        }
    }
}

fn main() {
    queen_log::init(queen_log::Level::Trace, &queen_log::DefaultLogger);

    let mut queen = Queen::new("queen").unwrap();

    queen.run().unwrap();
}
