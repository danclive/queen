use std::collections::HashMap;
use std::thread;
use std::io::{self, Read, Write, ErrorKind::WouldBlock};
use std::net::TcpStream;
use std::collections::VecDeque;
use std::os::unix::io::AsRawFd;
use std::cell::Cell;
use std::i32;
use std::error::Error;

use nson::msg;
use queen_io::plus::block_queue::BlockQueue;
use queen_io::plus::mpms_queue::Queue;

use crate::Message;
use crate::Queen;
use crate::poll::{poll, Ready, Events, Event};
use crate::util::split_message;

pub struct Control {
    queue_i: Queue<(Message)>,
    queue_o: BlockQueue<(String, Message)>,
    conn: Option<Connection>,
    events: Events,
    cache: VecDeque<Vec<u8>>,
    attach_events: HashMap<String, usize>,
    event_id: Cell<i32>,
    handshake: bool,
    run: bool
}

impl Control {
    pub fn new(queen: &Queen) -> io::Result<Control> {
        let control = Control {
            queue_i: queen.inner.queue_o.clone(),
            queue_o: queen.inner.queue_i.clone(),
            conn: None,
            events: Events::with_capacity(2),
            cache: VecDeque::with_capacity(1024),
            attach_events: HashMap::new(),
            event_id: Cell::new(0),
            handshake: false,
            run: true
        };

        Ok(control)
    }

    pub fn run(self) {
        thread::Builder::new().name("control".into()).spawn(move || {
            let mut control = self;

            while control.run {
                control.run_once().unwrap();
            }
        }).unwrap();
    }

    fn run_once(&mut self) -> io::Result<()> {

        self.events.clear();

        let queen_i_fd = self.queue_i.as_raw_fd();
        self.events.put(queen_i_fd, Ready::readable());

        let mut conn_fd = -1;

        if let Some(ref mut conn) = self.conn {
            if !self.cache.is_empty() {
                conn.interest.insert(Ready::writable());
            }

            conn_fd = conn.socket.as_raw_fd();
            self.events.put(conn_fd, conn.interest);
        }
        
        if poll(&mut self.events, None)? > 0 {
            for i in 0..self.events.len() {
                let event = self.events.get(i).unwrap();
                match event.fd() {
                    fd if fd == queen_i_fd => {
                        self.dispatch_queue_i()?;
                    }
                    fd if fd == conn_fd => {
                        self.dispatch_conn(event)?;
                    }
                    _ => ()
                }

            }
        }

        Ok(())
    }

    fn dispatch_queue_i(&mut self) -> io::Result<()> {
        loop {
            let mut message = match self.queue_i.pop() {
                Some(message) => message,
                None => {
                    break;
                }
            };

            let event = message.get_str("event")
                .expect("Can't get event!")
                .to_string();
        
            if let Err(_) = message.get_i32("event_id") {
                message.insert("event_id", self.get_event_id());
            }

            if event.starts_with("pub:") {
                if let Some(_) = self.attach_events.get(&event) {
                    self.queue_o.push((event, message.clone()));
                }

                self.push_back(message.to_vec().unwrap());

            } else if event.starts_with("sys:") {
                match event.as_str() {
                    "sys:hand" => {
                        self.push_back(message.to_vec().unwrap());
                    }
                    "sys:handed" => {
                        self.push_back(message.to_vec().unwrap());
                    }
                    "sys:attach" => {
                        let event = message.get_str("v").expect("Can't get v at attach");
                        if self.attach(event) && event.starts_with("pub:") && self.handshake {
                            self.push_back(message.to_vec().unwrap());
                        }
                    }
                    "sys:detach" => {
                        let event = message.get_str("v").expect("Can't get v at attach");
                        if self.detach(event) && event.starts_with("pub:") && self.handshake {
                            self.push_back(message.to_vec().unwrap());
                        }
                    }
                    "sys:link" => {
                        // msg!{
                        //  "protocol": "tcp", // unix
                        //  "addr": "127.0.0.1:6666",
                        //  "path": "/path/to/the/socket"
                        // };
                        let protocol = match message.get_str("protocol") {
                            Ok(protocol) => protocol,
                            Err(_) => {
                                message.insert("ok", false);
                                message.insert("error", "Message format error: can't not get protocol!");

                                self.queue_o.push((event, message));

                                continue;
                            }
                        };

                        if protocol == "tcp" {
                            let addr = match message.get_str("addr") {
                                Ok(addr) => addr,
                                Err(_) => {
                                    message.insert("ok", false);
                                    message.insert("error", "Message format error: can't not get addr!");

                                    self.queue_o.push((event, message));

                                    continue;
                                }
                            };

                            match TcpStream::connect(addr) {
                                Ok(socket) => {
                                    socket.set_nodelay(true)?;
                                    socket.set_nonblocking(true)?;
                                    let conn = Connection::new(socket)?;

                                    self.conn = Some(conn);

                                    message.insert("ok", true);

                                    self.queue_o.push((event, message));

                                    continue;
                                }
                                Err(err) => {
                                    message.insert("ok", false);
                                    message.insert("error", err.description());

                                    self.queue_o.push((event, message));

                                    continue;
                                }
                            }
                        }

                        message.insert("ok", false);
                        message.insert("error", "unimplemented");

                        self.queue_o.push((event, message));
                    }
                    "sys:unlink" => {
                        if self.conn.is_some() {
                            let conn = self.conn.take().unwrap();

                            message.insert("ok", true);
                            message.insert("protocol", "tcp");
                            message.insert("addr", conn.addr);

                            self.handshake = false;
                        } else {
                            message.insert("ok", false);
                            message.insert("error", "already unlink!");
                        }

                        self.queue_o.push((event, message));
                    }
                    "sys:sync" => {

                    }
                    "sys:shoutdown" => {
                        self.run = false;
                    }
                    _ => ()
                }
            }
        }

        Ok(())
    }

    #[inline]
    fn push_back(&mut self, data: Vec<u8>) {
        self.cache.push_back(data);
    }

    fn dispatch_conn(&mut self, event: Event) -> io::Result<()> {
        let readiness = event.readiness();

        if readiness.is_hup() || readiness.is_error() {
            self.remove_link();
            return Ok(())
        }

        let mut close = false;

        if readiness.is_readable() {
            if let Some(conn) = &mut self.conn {
                if let Ok(msgs) = conn.reader() {
                    self.decode_messages(msgs);
                } else {
                    close = true;
                }
            }
        }

        if readiness.is_writable() {
            if let Some(conn) = &mut self.conn {
                if conn.writer(&mut self.cache).is_err() {
                    close = true;
                }
            }
        }

        if close {
            self.remove_link();
        }

        Ok(())
    }

    fn decode_messages(&mut self, msgs: Vec<Vec<u8>>) {
        for msg in msgs {
            let message = match Message::from_slice(&msg) {
                Ok(m) => m,
                Err(_) => {
                    return
                }
            };

            let event = match message.get_str("event") {
                Ok(event) => event.to_owned(),
                Err(_) => {
                    return
                }
            };

            if let Some(_) = self.attach_events.get("sys:recv") {
                self.queue_o.push(("sys:recv".to_string(), message.clone()));
            }

            macro_rules! queue_o_push {
                () => (
                    if let Some(_) = self.attach_events.get(&event) {
                        self.queue_o.push((event, message));
                    }
                )
            }

            if event.starts_with("sys:") {
                match event.as_str() {
                    "sys:hand" => {
                        if let Ok(ok) = message.get_bool("ok") {
                            if ok {
                                self.handshake = true;

                                for (k, _) in &self.attach_events {
                                    if k.starts_with("pub:") {
                                        let message = msg!{
                                            "event": "sys:attach", "v": k
                                        };

                                        self.cache.push_back(message.to_vec().unwrap());
                                    }
                                }
                            }
                        }

                        queue_o_push!();
                    }
                    "sys:handed" => {
                        queue_o_push!();
                    }
                    "sys:sync" => {

                    }
                    "sys:attach" => {
                        queue_o_push!();
                    }
                    "sys:detach" => {
                        queue_o_push!();
                    }
                    _ => ()
                }
            } else if event.starts_with("pub:") {
                if let Ok(_) = message.get_bool("ok") {
                    if let Some(_) = self.attach_events.get("sys:reply") {
                        self.queue_o.push(("sys:reply".to_string(), message));
                    }
                } else {
                    self.queue_o.push((event, message));
                }
            }
        }
    }

    fn get_event_id(&self) -> i32 {
        let mut id = self.event_id.get() + 1;
        if id >= i32::MAX {
            id = 1;
        }

        self.event_id.replace(id)
    }

    fn attach(&mut self, event: &str) -> bool {
        let mut sync = false;

        let count = self.attach_events.entry(event.to_string()).or_insert(0);
        if count == &mut 0 {
            sync = true;
        }

        *count += 1;

        sync
    }

    fn detach(&mut self, event: &str) -> bool {
        let mut sync = false;

        if let Some(count) = self.attach_events.get_mut(event) {
            *count -= 1;

            if count <= &mut 0 {
                self.attach_events.remove(event);
                sync = true;
            }
        }

        sync
    }

    fn remove_link(&mut self) {
        let mut message = msg!{
            "event": "sys:remove",
            "protocol": "tcp",
        };

        if let Some(ref conn) = self.conn {
            message.insert("addr", conn.addr.clone());
        }

        self.handshake = false;
        self.conn = None;

        self.queue_o.push(("sys:remove".to_string(), message));
    }
}

pub struct Connection {
    socket: TcpStream,
    addr: String,
    buffer: Vec<u8>,
    interest: Ready
}

impl Connection {
    fn new(socket: TcpStream) -> io::Result<Connection> {
        Ok(Connection {
            addr: socket.peer_addr()?.to_string(),
            socket,
            buffer: Vec::with_capacity(4 * 1024),
            interest: Ready::readable() | Ready::hup()
        })
    }

    fn reader(&mut self) -> io::Result<Vec<Vec<u8>>> {
        let mut messages = Vec::new();

        loop {
            let mut buf = [0; 4 * 1024];
            match self.socket.read(&mut buf) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "ConnectionAborted"))
                    } else {
                        let mut m = split_message(&mut self.buffer, &buf[..size]);
                        messages.append(&mut m);
                    }
                }
                Err(err) => {
                    if let WouldBlock = err.kind() {
                        break;
                    } else {
                        return Err(err)
                    }
                }
            }
        }

        Ok(messages)
    }

    fn writer(&mut self, cache: &mut VecDeque<Vec<u8>>) -> io::Result<()> {
        loop {
            if let Some(front) = cache.front_mut() {
                match self.socket.write(front) {
                    Ok(size) => {
                        if size == 0 {
                            return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "ConnectionAborted"))
                        } else {
                            if size == front.len() {
                                cache.pop_front();
                            } else {
                                // size < front.len()
                                assert!(size > front.len());
                                *front = front[size..].to_vec();
                            }
                        }
                    }
                    Err(err) => {
                        if let WouldBlock = err.kind() {
                            break;
                        } else {
                            return Err(err)
                        }
                    }
                }

            } else {
                break;
            }
        }

        if cache.is_empty() {
            self.interest.remove(Ready::writable());
        } else {
            self.interest.insert(Ready::writable());
        }

        Ok(())
    }
}