use std::collections::HashMap;
use std::cell::Cell;
use std::io::{self, ErrorKind::WouldBlock};
use std::i32;
use std::error::Error;
use std::collections::VecDeque;
use std::io::{Write, Read};
use std::thread;
use std::os::unix::io::{AsRawFd, RawFd};

use queen_io::queue::spsc::Queue;
use queen_io::{Poll, Events, Token, Ready, PollOpt, Event, Evented};
use queen_io::tcp::TcpListener;
use queen_io::tcp::TcpStream;

use nson::Value;
use nson::msg;

use crate::Message;
use crate::util::split_message;

pub struct Network {
    rx: Queue<Message>,
    tx: Queue<Message>
}

impl Network {
    pub fn new() -> io::Result<Network> {
        is_send::<Network>();
        is_sync::<Network>();

        let service = Service::new()?;

        let rx = service.rx.clone();
        let tx = service.tx.clone();


        thread::Builder::new().name("service".into()).spawn(move || {
            let mut service = service;
            let _ = service.run();
        }).unwrap();

        let network = Network {
            rx,
            tx
        };

        Ok(network)
    }

    pub fn recv(&self) -> Option<Message> {
        self.tx.pop()
    }

    pub fn send(&self, message: Message) {
        self.rx.push(message);
    }

    pub fn event_fd(&self) -> RawFd {
        self.tx.as_raw_fd()
    }
}

impl Evented for Network {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        self.tx.register(poll, token, interest, opts)
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        self.tx.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.tx.deregister(poll)
    }
}

struct Service {
    poll: Poll,
    events: Events,
    rx: Queue<Message>,
    tx: Queue<Message>,
    listens: HashMap<i32, TcpListener>,
    conns: HashMap<i32, Connection>,
    next_listen_id: Cell<i32>,
    next_conn_id: Cell<i32>,
    run: bool
}

impl Service {
    const RX: Token = Token(1);

    fn new() -> io::Result<Service> {
        let service = Service {
            poll: Poll::new()?,
            events: Events::with_capacity(1024),
            rx: Queue::with_cache(4 * 1000)?,
            tx: Queue::with_cache(4 * 1000)?,
            listens: HashMap::with_capacity(100),
            conns: HashMap::with_capacity(1024),
            next_listen_id: Cell::new(100), // 100 ~ 1000
            next_conn_id: Cell::new(1001), // 1001 ~
            run: true,
        };

        Ok(service)
    }

    fn run(&mut self) -> io::Result<()> {
        self.poll.register(&self.rx, Self::RX, Ready::readable(), PollOpt::edge())?;

        while self.run {
            self.run_once()?;
        }

        Ok(())
    }

    #[inline]
    fn run_once(&mut self) -> io::Result<()> {
        let size = self.poll.wait(&mut self.events, None)?;

        for i in 0..size {
            let event = self.events.get(i).unwrap();
            self.dispatch(event)?;
        }

        Ok(())
    }

    #[inline]
    fn dispatch(&mut self, event: Event) -> io::Result<()> {
        match event.token() {
            Self::RX => self.dispatch_rx()?,
            // listen
            token if token.0 >= 100 && token.0 < 1000 => {
                if let Some(listen) = self.listens.get(&(token.0 as i32)) {
                    loop {
                        let (socket, addr) = match listen.accept() {
                            Ok(socket) => socket,
                            Err(err) => {
                                if let WouldBlock = err.kind() {
                                    break;
                                } else {
                                    return Err(err)
                                }
                            }
                        };

                        socket.set_nodelay(true)?;

                        let id = self.next_conn_id();

                        let conn = Connection::new(id, socket, false)?;

                        self.poll.register(&conn.socket, Token(id as usize), Ready::readable() | Ready::hup(), PollOpt::edge())?;
                        self.conns.insert(id, conn);

                        let msg = msg!{
                            "event": "net:accept",
                            "proto": "tcp",
                            "id": id,
                            "addr": addr.to_string()
                        };

                        self.tx.push(msg);

                    }

                    self.poll.reregister(listen, token, Ready::readable(), PollOpt::edge())?;
                }
            }
            // socket
            token => {
                let readiness = event.readiness();

                if readiness.is_hup() || readiness.is_error() {
                    self.remove_conn(token)?;
                    return Ok(())
                }

                if readiness.is_readable() {
                    if let Some(conn) = self.conns.get_mut(&(token.0 as i32)) {
                        if conn.reader(&self.poll, &self.tx).is_err() {
                            self.remove_conn(token)?;
                        }
                    }
                }

                if readiness.is_writable() {
                    if let Some(conn) = self.conns.get_mut(&(token.0 as i32)) {
                        if conn.writer(&self.poll).is_err() {
                            self.remove_conn(token)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn dispatch_rx(&mut self) -> io::Result<()> {
        while let Some(message) = self.rx.pop() {
            if let Ok(event) = message.get_str("event") {
                match event {
                    "net:listen" => {
                        if let Ok(proto) = message.get_str("proto") {
                            match proto {
                                "tcp" => {
                                    if let Ok(addr) = message.get_str("addr") {
                                        match TcpListener::bind(addr) {
                                            Ok(listen) => {
                                                let id = self.next_listen_id();

                                                let mut message = message;
                                                message.insert("ok", true);
                                                message.insert("id", id);
                                                message.insert("addr", listen.local_addr()?.to_string());

                                                self.poll.register(&listen, Token(id as usize), Ready::readable(), PollOpt::edge())?;

                                                self.listens.insert(id, listen);

                                                self.tx.push(message);
                                            }
                                            Err(err) => {
                                                let mut message = message;
                                                message.insert("ok", false);
                                                message.insert("error", err.description());

                                                self.tx.push(message);
                                            }
                                        }
                                    } else {
                                        let mut message = message;
                                        message.insert("ok", false);
                                        message.insert("error", "Can't get addr!");

                                        self.tx.push(message);
                                    }
                                }
                                _ => {
                                    let mut message = message;
                                    message.insert("ok", false);
                                    message.insert("error", "Proto unsupport!");

                                    self.tx.push(message);
                                }
                            }
                        } else {
                            let mut message = message;
                            message.insert("ok", false);
                            message.insert("error", "Can't get proto!");

                            self.tx.push(message);
                        }
                    }
                    "net:unlisten" => {
                        if let Ok(id) = message.get_i32("id") {
                            let mut message = message;

                            if let Some(listen) = self.listens.remove(&id) {
                                message.insert("ok", true);
                                message.insert("addr", listen.local_addr()?.to_string());
                            } else {
                                message.insert("ok", false);
                                message.insert("error", "Can't find listen!");
                            }

                            self.tx.push(message);
                        } else {
                            let mut message = message;
                            message.insert("ok", false);
                            message.insert("error", "Can't get id!");

                            self.tx.push(message);
                        }
                    }
                    "net:link" => {
                        if let Ok(proto) = message.get_str("proto") {
                            match proto {
                                "tcp" => {
                                    if let Ok(addr) = message.get_str("addr") {
                                        match TcpStream::connect(addr) {
                                            Ok(socket) => {
                                                socket.set_nodelay(true)?;

                                                let id = self.next_conn_id();
                                                let conn = Connection::new(id, socket, true)?;

                                                self.poll.register(&conn.socket, Token(id as usize), Ready::readable() | Ready::hup(), PollOpt::edge() | PollOpt::oneshot())?;
                                                self.conns.insert(id, conn);

                                                let mut message = message;
                                                message.insert("ok", true);
                                                message.insert("conn_id", id);

                                                self.tx.push(message);

                                            }
                                            Err(err) => {
                                                let mut message = message;
                                                message.insert("ok", false);
                                                message.insert("error", err.description());

                                                self.tx.push(message);
                                            }
                                        }
                                    } else {
                                        let mut message = message;
                                        message.insert("ok", false);
                                        message.insert("error", "Can't get addr!");

                                        self.tx.push(message);
                                    }
                                }
                                _ => {
                                    let mut message = message;
                                    message.insert("ok", false);
                                    message.insert("error", "Proto unsupport!");

                                    self.tx.push(message);
                                }
                            }
                        } else {
                            let mut message = message;
                            message.insert("ok", false);
                            message.insert("error", "Can't get proto!");

                            self.tx.push(message);
                        }
                    }
                    "net:unlink" => {
                        if let Ok(id) = message.get_i32("id") {
                            let mut message = message;

                            if let Some(conn) = self.conns.remove(&id) {
                                self.poll.deregister(&conn.socket)?;

                                message.insert("ok", true);
                                message.insert("proto", "tcp");
                                message.insert("addr", conn.addr);
                            } else {
                                message.insert("ok", false);
                                message.insert("error", "Can't find listen!");
                            }

                            self.tx.push(message);
                        } else {
                            let mut message = message;
                            message.insert("ok", false);
                            message.insert("error", "Can't get id!");

                            self.tx.push(message);
                        }
                    }
                    "net:send" => {
                        if let Ok(conns) = message.get_array("conns") {
                            if let Ok(data) = message.get_binary("data") {
                                if data.len() < 4 {
                                    let mut message = message;
                                    message.insert("ok", false);
                                    message.insert("error", "Data is too short!");

                                    self.tx.push(message);
                                } else {
                                    for conn in conns {
                                        if let Value::I32(id) = conn {
                                            if let Some(conn) = self.conns.get_mut(id) {
                                                conn.write_buffer.push_back(data.clone());
                                                conn.interest.insert(Ready::writable());
                                                self.poll.reregister(&conn.socket, Token(conn.id as usize), conn.interest, PollOpt::edge())?;
                                            }
                                        }
                                    }

                                    let mut message = message;
                                    message.insert("ok", true);

                                    self.tx.push(message);
                                }
                            } else {
                                let mut message = message;
                                message.insert("ok", false);
                                message.insert("error", "Can't get data!");

                                self.tx.push(message);
                            }
                        } else {
                            let mut message = message;
                            message.insert("ok", false);
                            message.insert("error", "Can't get conns!");

                            self.tx.push(message);
                        }
                    }
                    _ => ()
                }
            }
        }

        Ok(())
    }

    fn next_listen_id(&self) -> i32 {
        let mut id = self.next_listen_id.get() + 1;
        if id >= 1000 {
            id = 100;
        }

        self.next_listen_id.replace(id)
    }

    fn next_conn_id(&self) -> i32 {
        let mut id = self.next_conn_id.get() + 1;
        if id == i32::MAX {
            id = 1001;
        }

        self.next_conn_id.replace(id)
    }

    #[inline]
    fn remove_conn(&mut self, token: Token) -> io::Result<()> {
        if let Some(conn) = self.conns.remove(&(token.0 as i32)) {
            self.poll.deregister(&conn.socket)?;

            let msg = msg!{
                "event": "net:remove",
                "proto": "tcp",
                "id": conn.id,
                "addr": conn.addr,
                "mine": conn.mine
            };

            self.tx.push(msg);
        }

        Ok(())
    }
}

pub struct Connection {
    id: i32,
    socket: TcpStream,
    addr: String,
    mine: bool,
    write_buffer: VecDeque<Vec<u8>>,
    read_buffer: Vec<u8>,
    interest: Ready
}

impl Connection {
    fn new(id: i32, socket: TcpStream, mine: bool) -> io::Result<Connection> {
        Ok(Connection {
            id,
            addr: socket.peer_addr()?.to_string(),
            socket,
            mine,
            write_buffer: VecDeque::with_capacity(1024),
            read_buffer: Vec::with_capacity(4 * 1024),
            interest: Ready::readable() | Ready::hup()
        })
    }

    fn reader(&mut self, poll: &Poll, tx: &Queue<Message>) -> io::Result<()> {
        loop {
            let mut buf = [0; 4 * 1024];

            match self.socket.read(&mut buf) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "ConnectionAborted"))
                    } else {
                        let messages = split_message(&mut self.read_buffer, &buf[..size]);
                        for message in messages {

                            let msg = msg!{
                                "event": "net:recv",
                                "id": self.id,
                                "data": message
                            };

                            tx.push(msg);
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
        }

        poll.reregister(&self.socket, Token(self.id as usize), self.interest, PollOpt::edge())?;

        Ok(())
    }

    fn writer(&mut self, poll: &Poll) -> io::Result<()> {
        while let Some(front) = self.write_buffer.front_mut() {
            match self.socket.write(front) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "ConnectionAborted"))
                    } else if size == front.len() {
                        self.write_buffer.pop_front();
                    } else {
                        // size < front.len()
                        assert!(size > front.len());
                        *front = front[size..].to_vec();
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

        if self.write_buffer.is_empty() {
            self.interest.remove(Ready::writable());
        } else {
            self.interest.insert(Ready::writable());
        }

        poll.reregister(&self.socket, Token(self.id as usize), self.interest, PollOpt::edge() | PollOpt::oneshot())?;

        Ok(())
    }
}

fn is_send<T: Send>() {}
fn is_sync<T: Sync>() {}
