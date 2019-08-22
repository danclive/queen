use std::collections::{VecDeque, HashMap, HashSet};
use std::io::{self, ErrorKind::WouldBlock};
use std::usize;
use std::net::ToSocketAddrs;

use queen_io::epoll::{Epoll, Events, Token, Ready, EpollOpt, Event};

use nson::{Message, msg};
use nson::message_id::MessageId;

use slab::Slab;

use rand::{self, thread_rng,rngs::ThreadRng};
use rand::seq::SliceRandom;

use crate::net::Addr;
use crate::error::ErrorCode;
use crate::crypto::{Method, Aead};

use conn::Connection;
use net::Listen;
pub use callback::Callback;

mod conn;
mod net;
mod callback;

pub struct Node<T> {
    node_id: String,
    epoll: Epoll,
    events: Events,
    listens: HashMap<usize, Listen>,
    token: usize,
    conns: Slab<Connection>,
    read_buffer: VecDeque<Message>,
    callback: Callback<T>,
    chans: HashMap<String, HashSet<usize>>,
    ports: HashMap<String, usize>,
    rand: ThreadRng,
    user_data: T,
    aead_key: Option<String>,
    aead_method: Method,
    run: bool
}

#[derive(Default)]
pub struct NodeConfig {
    pub node_id: String,
    pub addrs: Vec<Addr>,
    pub aead_key: Option<String>,
    pub aead_method: Method
}

impl NodeConfig {
    pub fn new() -> NodeConfig {
        NodeConfig {
            addrs: Vec::new(),
            aead_key: None,
            aead_method: Method::default(),
            node_id: MessageId::new().to_string()
        }
    }

    pub fn add_tcp<A: ToSocketAddrs>(&mut self, addr: A) -> io::Result<()> {
        let addr = Addr::tcp(addr)?;
        self.addrs.push(addr);
        Ok(())
    }

    pub fn add_uds(&mut self, path: String) {
        self.addrs.push(Addr::Uds(path))
    }

    pub fn set_aead_key(&mut self, key: &str) {
        self.aead_key = Some(key.to_string())
    }

    pub fn set_aead_method(&mut self, method: Method) {
        self.aead_method = method;
    }
}

impl<T> Node<T> {
    pub fn bind(config: NodeConfig, user_data: T) -> io::Result<Node<T>> {
        if config.addrs.is_empty() {
            panic!("{:?}", "config.addrs must >= 1");
        }

        let mut listens = HashMap::new();
        let mut token = usize::MAX - 1;

        for addr in config.addrs {
            let listen = addr.bind()?;
            listens.insert(token, listen);
            token -= 1;
        }

        let node = Node {
            node_id: config.node_id,
            epoll: Epoll::new()?,
            events: Events::with_capacity(1024),
            listens,
            token,
            // timer: Timer::new()?,
            conns: Slab::new(),
            read_buffer: VecDeque::new(),
            callback: Callback::new(),
            chans: HashMap::new(),
            ports: HashMap::new(),
            rand: thread_rng(),
            user_data,
            aead_key: config.aead_key,
            aead_method: config.aead_method,
            run: true
        };

        Ok(node)
    }

    pub fn set_callback(&mut self, callback: Callback<T>) {
        self.callback = callback;
    }

    pub fn run(&mut self) -> io::Result<()> {
        for (id, listen) in &self.listens {
            listen.add(&self.epoll, Token(*id), Ready::readable(), EpollOpt::edge())?;
        }

        while self.run {
            self.run_once()?;
        }

        Ok(())
    }

    #[inline]
    fn run_once(&mut self) -> io::Result<()> {
        let size = self.epoll.wait(&mut self.events, None)?;

        for i in 0..size {
            let event = self.events.get(i).unwrap();
            self.dispatch(event)?;
        }

        Ok(())
    }

    fn dispatch(&mut self, event: Event) -> io::Result<()> {
        match event.token() {
            // Self::TIMER => self.dispatch_timer()?,
            token if token.0 >= self.token => self.dispatch_listen(token.0)?,
            token => self.dispatch_conn(token, event)?
        }

        Ok(())
    }

    fn dispatch_listen(&mut self, id: usize) -> io::Result<()> {
        if let Some(listener) = self.listens.get(&id) {
            loop {
                let (socket, addr) = match listener.accept() {
                    Ok((socket, addr)) => (socket, addr),
                    Err(err) => {
                        if let WouldBlock = err.kind() {
                            break;
                        } else {
                            return Err(err)
                        }
                    }
                };

                let aead = self.aead_key.as_ref().map(|key| Aead::new(&self.aead_method, key.as_bytes()));

                let entry = self.conns.vacant_entry();

                let success = if let Some(accept_fn) = &self.callback.accept_fn {
                    accept_fn(entry.key(), &addr, &mut self.user_data)
                } else {
                    true
                };

                if success {

                    let conn = Connection::new(entry.key(), addr, socket, aead);
                    conn.add(&self.epoll)?;

                    entry.insert(conn);
                }
            }
        }

        Ok(())
    }

    fn dispatch_conn(&mut self, token: Token, event: Event) -> io::Result<()> {
        let readiness = event.readiness();
        
        let mut remove = readiness.is_hup() || readiness.is_error();

        if readiness.is_readable() {
            if let Some(conn) = self.conns.get_mut(token.0) {
                if conn.read(&mut self.read_buffer).is_err() {
                    remove = true;
                }

                let addr = conn.addr.clone();

                if !self.read_buffer.is_empty() {
                    self.handle_message_from_conn(token.0, addr)?;
                }
            }
        }

        if readiness.is_writable() {
            if let Some(conn) = self.conns.get_mut(token.0) {
                if conn.write().is_err() {
                    remove = true;
                } else {
                    conn.modify(&self.epoll)?;
                }
            }
        }

        if remove {
            self.remove_conn(token.0)?;
        }

        Ok(())
    }

    fn remove_conn(&mut self, id: usize) -> io::Result<()> {
        if self.conns.contains(id) {
            let conn = self.conns.remove(id);
            conn.delete(&self.epoll)?;

            for (chan, _) in conn.chans {
                if let Some(ids) = self.chans.get_mut(&chan) {
                    ids.remove(&id);

                    if ids.is_empty() {
                        self.chans.remove(&chan);
                    }
                }
            }

            if let Some(port_id) = conn.port_id {
                self.ports.remove(&port_id);
            }

            if let Some(remove_fn) = &self.callback.remove_fn {
                remove_fn(id, &conn.addr, &mut self.user_data);
            }
        }

        Ok(())
    }

    fn handle_message_from_conn(&mut self, id: usize, addr: Addr) -> io::Result<()> {
        while let Some(mut message) = self.read_buffer.pop_front() {
            if !self.can_recv(id, &addr, &mut message)? {
                return Ok(())
            }

            let chan = match message.get_str("_chan") {
                Ok(chan) => chan,
                Err(_) => {
                    ErrorCode::CannotGetChanField.insert_message(&mut message);

                    self.push_data_to_conn(id, message.to_vec().unwrap())?;
                    continue;
                }
            };

            if chan.starts_with('_') {
                match chan {
                    "_auth" => self.node_auth(id, &addr, message)?,
                    "_atta" => self.node_attach(id, &addr, message)?,
                    "_deta" => self.node_detach(id, &addr, message)?,
                    "_ping" => self.node_ping(id, message)?,
                    // "_quer" => self.node_query(id, message)?,
                    _ => {
                        ErrorCode::UnsupportedChan.insert_message(&mut message);

                        self.push_data_to_conn(id, message.to_vec().unwrap())?;
                    }
                }
            } else {
                self.relay_message(id, &addr, chan.to_string().to_string(), message)?;
            }
        }

        Ok(())
    }

    fn push_data_to_conn(&mut self, id: usize, data: Vec<u8>) -> io::Result<()> {
        if let Some(conn) = self.conns.get_mut(id) {
            conn.push_data(data);
            conn.modify(&self.epoll)?;
        }

        Ok(())
    }

    fn node_auth(&mut self, id: usize, addr: &Addr,mut message: Message) -> io::Result<()> {
        if !self.can_auth(id, addr, &mut message)? {
            return Ok(())
        }

        if let Some(conn) = self.conns.get_mut(id) {
            if let Ok(port_id) = message.get_str("_ptid") {
                if self.ports.contains_key(port_id) {

                    ErrorCode::DuplicatePortId.insert_message(&mut message);

                    conn.push_data(message.to_vec().unwrap());
                    conn.modify(&self.epoll)?;

                    return Ok(())
                }

                self.ports.insert(port_id.to_string(), id);

                conn.port_id = Some(port_id.to_string());
            }

            conn.auth = true;

            message.insert("_noid", &self.node_id);

            ErrorCode::OK.insert_message(&mut message);

            conn.push_data(message.to_vec().unwrap());
            conn.modify(&self.epoll)?;
        }

        Ok(())
    }

    fn node_attach(&mut self, id: usize, addr: &Addr, mut message: Message) -> io::Result<()> {
        if !self.check_auth(id, &mut message)? {
            return Ok(())
        }

        if let Ok(chan) = message.get_str("_valu").map(ToOwned::to_owned) {
            if !self.can_attach(id, addr, &mut message)? {
                return Ok(())
            }

            let labels = if let Ok(label) = message.get_str("_labe") {
                vec![label.to_string()]
            } else if let Ok(labels) = message.get_array("_labe") {
                let mut vec = vec![];

                labels.iter().for_each(|v| {
                    if let Some(label) = v.as_str() {
                        vec.push(label.to_string());
                    }
                });

                vec
            } else {
                vec![]
            };

            self.session_attach(id, chan, labels)?;

            ErrorCode::OK.insert_message(&mut message);
        } else {
            ErrorCode::CannotGetValueField.insert_message(&mut message);
        }

        self.push_data_to_conn(id, message.to_vec().unwrap())?;

        Ok(())
    }

    fn node_detach(&mut self, id: usize, addr: &Addr, mut message: Message) -> io::Result<()> {
        if !self.check_auth(id, &mut message)? {
            return Ok(())
        }

        if let Ok(chan) = message.get_str("_valu").map(ToOwned::to_owned) {
            if let Some(detach_fn) = &self.callback.detach_fn {
                detach_fn(id, addr, &mut message, &mut self.user_data);
            }

            self.session_detach(id, chan)?;

            ErrorCode::OK.insert_message(&mut message);
        } else {
            ErrorCode::CannotGetValueField.insert_message(&mut message);
        }

        self.push_data_to_conn(id, message.to_vec().unwrap())?;

        Ok(())
    }

    fn node_ping(&mut self, id: usize, mut message: Message) -> io::Result<()> {
        ErrorCode::OK.insert_message(&mut message);

        self.push_data_to_conn(id, message.to_vec().unwrap())?;

        Ok(())
    }

    fn session_attach(&mut self, id: usize, chan: String, labels: Vec<String>) -> io::Result<()> {
        let ids = self.chans.entry(chan.to_owned()).or_insert_with(HashSet::new);

        ids.insert(id);

        if let Some(conn) = self.conns.get_mut(id) {
            conn.chans.insert(chan, labels);
        }

        Ok(())
    }

    fn session_detach(&mut self, id: usize, chan: String) -> io::Result<()> {
        if let Some(conn) = self.conns.get_mut(id) {
            conn.chans.remove(&chan);

            if let Some(ids) = self.chans.get_mut(&chan) {
                ids.remove(&id);

                if ids.is_empty() {
                    self.chans.remove(&chan);
                }
            }
        }

        Ok(())
    }

    pub fn relay_message(&mut self, id: usize, addr: &Addr, chan: String, mut message: Message) -> io::Result<()> {
        if !self.check_auth(id, &mut message)? {
            return Ok(())
        }

        if !self.can_emit(id, &addr, &mut message)? {
            return Ok(())
        }

        if let Ok(to) = message.get_str("_to") {
            if !self.ports.contains_key(to) {
                ErrorCode::TargetPortIdNotExist.insert_message(&mut message);

                self.push_data_to_conn(id, message.to_vec().unwrap())?;

                return Ok(())
            }
        }

        if let Some(conn) = self.conns.get(id) {
            if let Some(port_id) = &conn.port_id {
                message.insert("_from", port_id);
            }
        }

        let label = message.get_str("_labe").map(|s| s.to_string()).ok();

        let mut no_consumers = true;

        if let Ok(to) = message.get_str("_to") {
            no_consumers = false;

            if let Some(conn_id) = self.ports.get(to) {
                if let Some(conn) = self.conns.get_mut(*conn_id) {
                    // check can send
                    let success = if let Some(send_fn) = &self.callback.send_fn {
                        send_fn(conn.id, &conn.addr, &mut message, &mut self.user_data)
                    } else {
                        true
                    };

                    if success {
                        conn.push_data(message.to_vec().unwrap());
                        conn.modify(&self.epoll)?;
                    }
                }
            }
        } else if message.get_bool("_shar").ok().unwrap_or(false) {
            let mut array: Vec<usize> = Vec::new();

            if let Some(ids) = self.chans.get(&chan) {
                for conn_id in ids {
                    if *conn_id == id {
                        continue;
                    }

                    if let Some(conn) = self.conns.get_mut(*conn_id) {
                        if let Some(label) = &label {
                            if let Some(labels) = conn.chans.get(&chan) {
                                if !labels.contains(label) {
                                    continue;
                                }
                            } else {
                                    panic!("{:?}", "It's not going to go here!");
                            }
                        }

                        array.push(*conn_id);
                    }
                }
            }

            if !array.is_empty() {
                no_consumers = false;

                if array.len() == 1 {
                    if let Some(conn) = self.conns.get_mut(array[0]) {
                            // check can send
                        let success = if let Some(send_fn) = &self.callback.send_fn {
                            send_fn(conn.id, &conn.addr, &mut message, &mut self.user_data)
                        } else {
                            true
                        };

                        if success {
                            conn.push_data(message.to_vec().unwrap());
                            conn.modify(&self.epoll)?;
                        }
                    }
                } else if let Some(id) = array.choose(&mut self.rand) {
                    if let Some(conn) = self.conns.get_mut(*id) {
                        // check can send
                        let success = if let Some(send_fn) = &self.callback.send_fn {
                            send_fn(conn.id, &conn.addr, &mut message, &mut self.user_data)
                        } else {
                            true
                        };

                        if success {
                            conn.push_data(message.to_vec().unwrap());
                            conn.modify(&self.epoll)?;
                        }
                    }
                }
            }

        } else if let Some(ids) = self.chans.get(&chan) {
            for conn_id in ids {
                if *conn_id == id {
                    continue;
                }

                if let Some(conn) = self.conns.get_mut(*conn_id) {
                    if let Some(label) = &label {
                        if let Some(labels) = conn.chans.get(&chan) {
                            if !labels.contains(label) {
                                continue;
                            }
                        } else {
                            panic!("{:?}", "It's not going to go here!");
                        }
                    }

                    no_consumers = false;

                    // check can send
                    let success = if let Some(send_fn) = &self.callback.send_fn {
                        send_fn(conn.id, &conn.addr, &mut message, &mut self.user_data)
                    } else {
                        true
                    };

                    if success {
                        conn.push_data(message.to_vec().unwrap());
                        conn.modify(&self.epoll)?;
                    }
                }
            }
        }

        if no_consumers {
            ErrorCode::NoConsumers.insert_message(&mut message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;

            return Ok(())
        }

        if let Some(ack) = message.get("_ack") {
            let mut reply_msg = msg!{
                "_ack": ack.clone()
            };

            ErrorCode::OK.insert_message(&mut reply_msg);

            self.push_data_to_conn(id, reply_msg.to_vec().unwrap())?;

            message.remove("_ack");
        }

        Ok(())
    }

    fn check_auth(&mut self, id: usize, message: &mut Message) -> io::Result<bool> {
        if let Some(conn) = self.conns.get_mut(id) {
            if conn.auth {
                return Ok(true)
            }

            ErrorCode::Unauthorized.insert_message(message);

            conn.push_data(message.to_vec().unwrap());
            conn.modify(&self.epoll)?;
        }

        Ok(false)
    }

    fn can_recv(&mut self, id: usize, addr: &Addr, message: &mut Message) -> io::Result<bool> {
        let success = if let Some(recv_fn) = &self.callback.recv_fn {
            recv_fn(id, addr, message, &mut self.user_data)
        } else {
            true
        };

        if !success {
            ErrorCode::RefuseReceiveMessage.insert_message(message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

        Ok(success)
    }

    fn can_auth(&mut self, id: usize, addr: &Addr, message: &mut Message) -> io::Result<bool> {
        let success = if let Some(auth_fn) = &self.callback.auth_fn {
            auth_fn(id, addr, message, &mut self.user_data)
        } else {
            true
        };

        if !success {
            ErrorCode::AuthenticationFailed.insert_message(message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

        Ok(success)
    }

    fn can_attach(&mut self, id: usize, addr: &Addr, message: &mut Message) -> io::Result<bool> {
         let success = if let Some(attach_fn) = &self.callback.attach_fn {
            attach_fn(id, addr, message, &mut self.user_data)
        } else {
            true
        };

        if !success {
            ErrorCode::Unauthorized.insert_message(message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

        Ok(success)
    }

    fn can_emit(&mut self, id: usize, addr: &Addr, message: &mut Message) -> io::Result<bool> {
        let success = if let Some(emit_fn) = &self.callback.emit_fn {
            emit_fn(id, addr, message, &mut self.user_data)
        } else {
            true
        };

        if !success {
            ErrorCode::Unauthorized.insert_message(message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

        Ok(success)
    }
}
