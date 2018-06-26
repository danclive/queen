use std::collections::{HashMap, HashSet};
use std::io;
use std::error::Error;

use serde_json;
use nson;

use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};

use service::Command;
use protocol::{Message, OpCode, ContentType};
use util::both_queue::BothQueue;
use commom::{Connect, ConnectReply};

#[derive(Debug)]
pub struct Session {
    msg_queue: BothQueue<(usize, Message)>,
    cmd_queue: BothQueue<Command>,
    clients: HashSet<usize>,
    cache0: HashMap<String, Vec<usize>>,
    cache1: HashMap<usize, Vec<String>>,
    cache2: HashMap<String, Vec<usize>>,
    thread_rng: ThreadRng
}

impl Session {
    pub fn new(msg_queue: BothQueue<(usize, Message)>, cmd_queue: BothQueue<Command>) -> Session {
        Session {
            msg_queue: msg_queue,
            cmd_queue: cmd_queue,
            clients: HashSet::new(),
            cache0: HashMap::new(),
            cache1: HashMap::new(),
            cache2: HashMap::new(),
            thread_rng: thread_rng()
        }
    }

    pub fn check_password(&mut self, username: &str, password: &str) -> bool {
        if username == "danc" && password == "password" {
            return true
        }

        false
    }

    pub fn handle(&mut self, id: usize, message: Message) -> io::Result<()> {
        //println!("session:handle: id: {:?}, message: {:?}", id, message);
        match message.opcode {
            OpCode::CONNECT => {
                self.handle_connect(id, message)?;
            }
            OpCode::PING => {
                self.handle_ping(id, message)?;
            }
            OpCode::REQUEST => {
                self.handle_request(id, message)?;
            }
            OpCode::RESPONSE => {
                self.handle_response(id, message)?;
            }
            OpCode::SUBSCRIBE => {
                self.handle_subscribe(id, message)?;
            }
            OpCode::UNSUBSCRIBE => {
                self.handle_unsubscribe(id, message)?;
            }
            OpCode::PUBLISH => {
                self.handle_publish(id, message)?;
            }
            OpCode::ERROR => {
                self.handle_error(id, message)?;
            }
            _ => ()
        }

        Ok(())
    }

    pub fn handle_connect(&mut self, id: usize, message: Message) -> io::Result<()> {
        if message.body.is_empty() {
            return Ok(())
        }

        let connect =  match message.content_type {
            t if t == ContentType::NSON.bits() => {
                match nson::decode::from_slice::<Connect>(&message.body) {
                    Ok(connect_object) => connect_object,
                    Err(err) => {

                        let connect_reply = ConnectReply {
                            id: 0,
                            message: err.description().to_owned()
                        };

                        let mut return_message = Message::new();
                        return_message.message_id = message.message_id;
                        return_message.opcode = OpCode::CONNACK;
                        return_message.content_type = message.content_type;
                        return_message.body = nson::encode::to_vec(&connect_reply).unwrap();

                        self.msg_queue.rx.push((id, return_message)).unwrap();

                        return Ok(())
                    }
                }
            }
            t if t == ContentType::JSON.bits() => {
                match serde_json::from_slice::<Connect>(&message.body) {
                    Ok(connect_json) => connect_json,
                    Err(err) => {

                        let connect_reply = ConnectReply {
                            id: 0,
                            message: err.description().to_owned()
                        };

                        let mut return_message = Message::new();
                        return_message.message_id = message.message_id;
                        return_message.opcode = OpCode::CONNACK;
                        return_message.content_type = message.content_type;
                        return_message.body = serde_json::to_vec(&connect_reply).unwrap();

                        self.msg_queue.rx.push((id, return_message)).unwrap();

                        return Ok(())
                    }
                }
            }
            t if t == ContentType::TEXT.bits() => {
                // username:value&password:value&method:value1,value2,value3
                match Connect::from_str(String::from_utf8_lossy(&message.body).to_owned().to_string()) {
                    Some(context_text) => context_text,
                    None => {

                        let mut return_message = Message::new();
                        return_message.message_id = message.message_id;
                        return_message.opcode = OpCode::CONNACK;
                        return_message.content_type = message.content_type;
                        return_message.body = "id:0&message:Data structure error".as_bytes().to_vec();

                        self.msg_queue.rx.push((id, return_message)).unwrap();

                        return Ok(())
                    }
                }
            }
            _ => {
                return Ok(())
            }
        };

        if self.check_password(&connect.username, &connect.password) {
            if connect.methods.is_empty() {
                self.insert_client(id, None);
            } else {
                self.insert_client(id, Some(connect.methods))
            }

            let mut return_message = Message::new();
            return_message.message_id = message.message_id;
            return_message.opcode = OpCode::CONNACK;

            self.msg_queue.rx.push((id, return_message)).unwrap();

            return Ok(())
        }

        let mut return_message = Message::new();
        return_message.message_id = message.message_id;
        return_message.opcode = OpCode::ERROR;
        return_message.content_type = ContentType::TEXT.bits();
        return_message.body = "Password authentication failed".as_bytes().to_vec();

        self.msg_queue.rx.push((id, return_message)).unwrap();

        Ok(())
    }

    pub fn handle_ping(&mut self, id: usize, message: Message) -> io::Result<()> {
        if self.check_client(id, message.message_id)? {
            let mut return_message = Message::new();
            return_message.message_id = message.message_id;
            return_message.opcode = OpCode::PONG;

            self.msg_queue.rx.push((id, return_message))?;
        }

        return Ok(())
    }

    pub fn handle_request(&mut self, id: usize, message: Message) -> io::Result<()> {
        if self.check_client(id, message.message_id)? {
            let topic = message.topic.to_owned();
            let mut socket_id: usize = 0;

            if let Some(sockets) = self.cache0.get(&topic) {
                if sockets.len() == 1 {
                    socket_id = *sockets.iter().next().unwrap();
                } else {
                    let sockets_id: Vec<usize> = sockets.iter().map(|s| *s ).collect();

                    if let Some(id) = self.thread_rng.choose(&sockets_id) {
                        socket_id = *id;
                    } else {
                        socket_id = sockets_id[0];
                    }
                }
            }

            let _ = socket_id;

            let mut message = message;
            message.origin = id as u32;

            self.msg_queue.rx.push((socket_id, message))?;
        }

        return Ok(())
    }

    pub fn handle_response(&mut self, id: usize, message: Message) -> io::Result<()> {
        if self.check_client(id, message.message_id)? {
            let socket_id = message.origin as usize;

            self.msg_queue.rx.push((socket_id, message))?;
        }

        return Ok(())
    }

    pub fn handle_subscribe(&mut self, id: usize, message: Message) -> io::Result<()> {
        if self.check_client(id, message.message_id)? {
            if self.cache2.contains_key(&message.topic) {
                if let Some(client_ids) = self.cache2.get_mut(&message.topic) {
                    if !client_ids.contains(&id) {
                        client_ids.push(id);
                    }
                }
            } else {
                self.cache2.insert(message.topic, vec![id]);
            }
        }

        let mut return_message = Message::new();
        return_message.message_id = message.message_id;
        return_message.opcode = OpCode::SUBACK;

        self.msg_queue.rx.push((id, return_message))?;

        return Ok(())
    }

    pub fn handle_unsubscribe(&mut self, id: usize, message: Message) -> io::Result<()> {
        if self.check_client(id, message.message_id)? {
            let mut is_empty = false;

            if let Some(client_ids) = self.cache2.get_mut(&message.topic) {
                if let Some(pos) = client_ids.iter().position(|x| *x == id) {
                    client_ids.remove(pos);
                }

                if client_ids.len() == 0 {
                    is_empty = true;
                }
            }

            if is_empty {
                self.cache2.remove(&message.topic);
            }
        }

        let mut return_message = Message::new();
        return_message.message_id = message.message_id;
        return_message.opcode = OpCode::UNSUBACK;

        self.msg_queue.rx.push((id, return_message))?;

        return Ok(())
    }

    pub fn handle_publish(&mut self, id: usize, message: Message) -> io::Result<()> {
        if self.check_client(id, message.message_id)? {
            if let Some(client_ids) = self.cache2.get(&message.topic) {
                for client_id in client_ids {
                    self.msg_queue.rx.push((*client_id, message.clone()))?;
                }
            }
        }

        let mut return_message = Message::new();
        return_message.message_id = message.message_id;
        return_message.opcode = OpCode::PUBACK;

        self.msg_queue.rx.push((id, return_message))?;

        return Ok(())
    }

    pub fn handle_error(&mut self, id: usize, message: Message) -> io::Result<()> {
        if self.check_client(id, message.message_id)? {
            if message.origin != 0 {
                self.msg_queue.rx.push((message.origin as usize, message))?;
            }
        }

        Ok(())
    }

    fn insert_client(&mut self, socket_id: usize, methods: Option<Vec<String>>) {
        self.clients.insert(socket_id);

        if let Some(methods) = methods {

            for method in methods.iter() {
                if self.cache0.contains_key(method) {
                    if let Some(client_ids) = self.cache0.get_mut(method) {
                        if !client_ids.contains(&socket_id) {
                            client_ids.push(socket_id)
                        }
                    }
                } else {
                    self.cache0.insert(method.to_owned(), vec![socket_id]);
                }
            }

            if self.cache1.contains_key(&socket_id) {
                if let Some(methods2) = self.cache1.get_mut(&socket_id) {
                    for method in methods.iter() {
                        if !methods2.contains(method) {
                            methods2.push(method.to_owned());
                        }
                    }
                }
            } else {
                self.cache1.insert(socket_id, methods);
            }
        }
    }

    pub fn remove_client(&mut self, socket_id: usize) {
        //println!("session:remove_client: {:?}", socket_id);
        self.clients.remove(&socket_id);
        self.cache1.remove(&socket_id);

        let mut temps = Vec::new();

        for (key, item) in self.cache0.iter_mut() {
            if let Some(pos) = item.iter().position(|x| *x == socket_id) {
                item.remove(pos);
            }

            if item.len() == 0 {
                temps.push(key.to_owned());
            }
        }

        for temp in temps {
            self.cache0.remove(&temp);
        }

        let mut temps = Vec::new();

        for (topic, client_ids) in self.cache2.iter_mut() {
            if let Some(pos) = client_ids.iter().position(|x| *x == socket_id) {
                client_ids.remove(pos);
            }

            if client_ids.len() == 0 {
                temps.push(topic.to_owned());
            }
        }

        for temp in temps {
            self.cache2.remove(&temp);
        }
    }

    pub fn check_client(&mut self, socket_id: usize, message_id: u32) -> io::Result<bool> {
        let has = self.clients.contains(&socket_id);

        if !has {
            let mut return_message = Message::new();
            return_message.message_id = message_id;
            return_message.opcode = OpCode::ERROR;
            return_message.content_type = ContentType::TEXT.bits();
            return_message.body = "Permission is not allowed!".as_bytes().to_vec();

            self.msg_queue.rx.push((socket_id, return_message))?;
        }

        Ok(has)
    }
}
