use std::collections::HashMap;
use std::cell::Cell;
use std::io::{self, ErrorKind::WouldBlock};
use std::i32;
use std::error::Error;
use std::collections::VecDeque;
use std::io::{Write, Read};
use std::thread;
use std::os::unix::io::{AsRawFd, RawFd};

use queen_io::{Poll, Events, Token, Ready, PollOpt, Event, Evented};
use queen_io::plus::mpms_queue::Queue;
use queen_io::tcp::TcpListener;
use queen_io::tcp::TcpStream;
use nson::Value;
use nson::msg;

use crate::Message;
use crate::util::split_message;

#[derive(Clone)]
pub struct Service {
    queue_i: Queue<Message>,
    queue_o: Queue<Message>
}

impl Service {
    pub fn new() -> io::Result<Service> {
        let inner = InnerService::new()?;

        let queue_i = inner.queue_i.clone();
        let queue_o = inner.queue_o.clone();

        thread::Builder::new().name("service".into()).spawn(move || {
            let mut inner = inner;
            let _ = inner.run();
        }).unwrap();

        let service = Service {
            queue_i,
            queue_o
        };

        Ok(service)
    }

    pub fn recv(&self) -> Option<Message> {
        self.queue_o.pop()
    }

    pub fn send(&self, message: Message) -> Result<(), Message> {
        self.queue_i.push(message)
    }

    pub fn fd(&self) -> RawFd {
        self.queue_o.as_raw_fd()
    }
}

impl Evented for Service {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        self.queue_o.register(poll, token, interest, opts)
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        self.queue_o.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.queue_o.deregister(poll)
    }
}

struct InnerService {
    poll: Poll,
    events: Events,
    queue_i: Queue<Message>,
    queue_o: Queue<Message>,
    listens: HashMap<i32, TcpListener>,
    conns: HashMap<i32, Connection>,
    next_listen_id: Cell<i32>,
    next_conn_id: Cell<i32>,
    run: bool
}

//const QUEUE_I: Token = Token(1);
//const EVENT_EMMITER_O: Token = Token(2);

impl InnerService {
    const QUEUE_I: Token = Token(1);

    fn new() -> io::Result<InnerService> {
        let service = InnerService {
            poll: Poll::new()?,
            events: Events::with_capacity(1024),
            queue_i: Queue::with_capacity(8 * 1000)?,
            queue_o: Queue::with_capacity(8 * 1000)?,
            listens: HashMap::with_capacity(100),
            conns: HashMap::with_capacity(1024),
            next_listen_id: Cell::new(100), // 100 ~ 199
            next_conn_id: Cell::new(200), // 200 ~ +
            run: true,
        };

        Ok(service)
    }

    fn run(&mut self) -> io::Result<()> {
        self.poll.register(&self.queue_i, Self::QUEUE_I, Ready::readable(), PollOpt::edge())?;

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
            Self::QUEUE_I => {
                dispatch_queue_i(self)?;
            }
            token if token.0 >= 100 && token.0 < 200 => {
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
                            "event": "sys:accept",
                            "protocol": "tcp",
                            "conn_id": id,
                            "addr": addr.to_string()
                        };

                        self.queue_o.push(msg).unwrap();

                    }

                    self.poll.reregister(listen, token, Ready::readable(), PollOpt::edge())?;
                }
            }
            token => { // >= 200
                let readiness = event.readiness();

                if readiness.is_hup() || readiness.is_error() {
                    self.remove_conn(token)?;
                    return Ok(())
                }

                let mut close = false;

                if readiness.is_readable() {
                    if let Some(conn) = self.conns.get_mut(&(token.0 as i32)) {
                        if conn.reader(&self.poll, &self.queue_o).is_err() {
                            close = true;
                        }
                    }
                }

                if readiness.is_writable() {
                    if let Some(conn) = self.conns.get_mut(&(token.0 as i32)) {
                        if conn.writer(&self.poll).is_err() {
                            close = true;
                        }
                    }
                }

                if close {
                    self.remove_conn(token)?;
                }
            }
        }

        Ok(())
    }

    fn next_listen_id(&self) -> i32 {
        let mut id = self.next_listen_id.get() + 1;
        if id >= i32::MAX {
            id = 200;
        }

        self.next_listen_id.replace(id)
    }

    fn next_conn_id(&self) -> i32 {
        let mut id = self.next_conn_id.get() + 1;
        if id >= i32::MAX {
            id = 200;
        }

        self.next_conn_id.replace(id)
    }

    #[inline]
    fn remove_conn(&mut self, token: Token) -> io::Result<()> {
        if let Some(conn) = self.conns.remove(&(token.0 as i32)) {
            self.poll.deregister(&conn.socket)?;
            // Todo, break data

            let recycle: Vec<Vec<u8>> = conn.queue_i.into();

            let msg = msg!{
                "event": "sys:remove",
                "protocol": "tcp",
                "conn_id": conn.id,
                "addr": conn.addr,
                "my": conn.my,
                "recycle": recycle
            };

            self.queue_o.push(msg).unwrap();
        }

        Ok(())
    }
}

fn dispatch_queue_i(service: &mut InnerService) -> io::Result<()> {
    loop {
        let message = match service.queue_i.pop() {
            Some(message) => message,
            None => {
                break;
            }
        };

        let event = match message.get_str("event") {
            Ok(event) => event,
            Err(_) => {
                continue;
            }
        };

        match event {
            "sys:listen" => {
                // msg!{
                //  "protocol": "tcp", // unix
                //  "addr": "127.0.0.1:6666",
                //  "path": "/path/to/the/socket"
                // };
                let protocol = match message.get_str("protocol") {
                    Ok(protocol) => protocol,
                    Err(_) => {
                        let mut message = message;
                        message.insert("ok", false);
                        message.insert("error", "Message format error: can't not get protocol!");

                        service.queue_o.push(message).unwrap();

                        continue;
                    }
                };

                if protocol == "tcp" {
                    let addr = match message.get_str("addr") {
                        Ok(addr) => addr,
                        Err(_) => {
                            let mut message = message;
                            message.insert("ok", false);
                            message.insert("error", "Message format error: can't not get addr!");

                            service.queue_o.push(message).unwrap();

                            continue;
                        }
                    };

                    match TcpListener::bind(addr) {
                        Ok(listen) => {
                            let id = service.next_listen_id();
                            let mut message = message;
                            message.insert("ok", true);
                            message.insert("listen_id", id);
                            message.insert("addr", listen.local_addr()?.to_string());

                            service.poll.register(&listen, Token(id as usize), Ready::readable(), PollOpt::edge())?;
                            service.listens.insert(id, listen);

                            service.queue_o.push(message).unwrap();

                            continue;
                        },
                        Err(err) => {
                            let mut message = message;
                            message.insert("ok", false);
                            message.insert("error", err.description());

                            service.queue_o.push(message).unwrap();

                            continue;
                        }
                    }
                }

                if protocol == "unix" {
                    let mut message = message;
                    message.insert("ok", false);
                    message.insert("error", "unimplemented");

                    service.queue_o.push(message).unwrap();
                }
            },
            "sys:unlisten" => {
                let listen_id = match message.get_i32("listen_id") {
                    Ok(listen_id) => listen_id,
                    Err(_) => {
                        let mut message = message;
                        message.insert("ok", false);
                        message.insert("error", "Message format error: can't not get listen_id!");

                        service.queue_o.push(message).unwrap();

                        continue;
                    }
                };

                if let Some(listen) = service.listens.remove(&listen_id) {

                    let msg = msg!{
                        "event": "sys:unlisten",
                        "ok": true,
                        "protocol": "tcp",
                        "listen_id": listen_id,
                        "addr": listen.local_addr()?.to_string(),
                    };

                    service.queue_o.push(msg).unwrap();
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
                        let mut message = message;
                        message.insert("ok", false);
                        message.insert("error", "Message format error: can't not get protocol!");

                        service.queue_o.push(message).unwrap();

                        continue;
                    }
                };

                if protocol == "tcp" {
                    let addr = match message.get_str("addr") {
                        Ok(addr) => addr,
                        Err(_) => {
                            let mut message = message;
                            message.insert("ok", false);
                            message.insert("error", "Message format error: can't not get addr!");

                            service.queue_o.push(message).unwrap();

                            continue;
                        }
                    };

                    match TcpStream::connect(addr) {
                        Ok(socket) => {
                            socket.set_nodelay(true)?;

                            let id = service.next_conn_id();
                            let conn = Connection::new(id, socket, true)?;

                            service.poll.register(&conn.socket, Token(id as usize), Ready::readable() | Ready::hup(), PollOpt::edge() | PollOpt::oneshot())?;
                            service.conns.insert(id, conn);

                            let mut message = message;
                            message.insert("ok", true);
                            message.insert("conn_id", id);

                            service.queue_o.push(message).unwrap();
                        }
                        Err(err) => {
                            let mut message = message;
                            message.insert("ok", false);
                            message.insert("error", err.description());

                            service.queue_o.push(message).unwrap();
                        }
                    }
                }
            },
            "sys:unlink" => {
                let conn_id = match message.get_i32("conn_id") {
                    Ok(conn_id) => conn_id,
                    Err(_) => {
                        let mut message = message;
                        message.insert("ok", false);
                        message.insert("error", "Message format error: can't not get conn_id!");

                        service.queue_o.push(message).unwrap();

                        continue;
                    }
                };

                if let Some(conn) = service.conns.remove(&conn_id) {
                    service.poll.deregister(&conn.socket)?;

                    let recycle: Vec<Vec<u8>> = conn.queue_i.into();

                    let msg = msg!{
                        "event": "sys:unlink",
                        "ok": true,
                        "protocol": "tcp",
                        "conn_id": conn.id,
                        "addr": conn.addr,
                        "my": conn.my,
                        "recycle": recycle
                    };

                    service.queue_o.push(msg).unwrap();
                }
            }
            "sys:send" => {
                // msg!{
                //  "conns": [1i32, 2, 3],
                //  "data": vec![5u8, 0, 0, 0, 1]
                // };
                let conns = match message.get_array("conns") {
                    Ok(conns) => conns,
                    Err(_) => {
                        let mut message = message;
                        message.insert("ok", false);
                        message.insert("error", "Message format error: can't not get conns!");

                        service.queue_o.push(message).unwrap();

                        continue;
                    }
                };

                let data = match message.get_binary("data") {
                    Ok(data) => {
                        // assert type
                        if data.len() < 4 {
                            let mut message = message;
                            message.insert("ok", false);
                            message.insert("error", "Message format error: the data is too short!");

                            service.queue_o.push(message).unwrap();

                            continue;
                        }

                        data
                    }
                    Err(_) => {
                        let mut message = message;
                        message.insert("ok", false);
                        message.insert("error", "Message format error: can't not get data!");

                        service.queue_o.push(message).unwrap();

                        continue;
                    }
                };

                for conn in conns {
                    if let Value::I32(conn_id) = conn {
                        if let Some(conn) = service.conns.get_mut(conn_id) {
                            conn.queue_i.push_back(data.clone());
                            conn.interest.insert(Ready::writable());
                            service.poll.reregister(&conn.socket, Token(conn.id as usize), conn.interest, PollOpt::edge())?;
                        }
                    }
                }

                let mut message = message;
                message.insert("ok", true);
                service.queue_o.push(message).unwrap();
            },
            "sys:shoutdown" => {
                service.run = false;
            }
            _ => unimplemented!()
        }
    }

    service.poll.reregister(&service.queue_i, InnerService::QUEUE_I, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;

    Ok(())
}

pub struct Connection {
    id: i32,
    socket: TcpStream,
    addr: String,
    my: bool,
    queue_i: VecDeque<Vec<u8>>,
    buffer: Vec<u8>,
    interest: Ready
}

impl Connection {
    fn new(id: i32, socket: TcpStream, my: bool) -> io::Result<Connection> {
        Ok(Connection {
            id,
            addr: socket.peer_addr()?.to_string(),
            socket,
            my,
            queue_i: VecDeque::with_capacity(1024),
            buffer: Vec::with_capacity(4 * 1024),
            interest: Ready::readable() | Ready::hup()
        })
    }

    fn reader(&mut self, poll: &Poll, queue_o: &Queue<Message>) -> io::Result<()> {
        loop {
            let mut buf = [0; 4 * 1024];

            match self.socket.read(&mut buf) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "ConnectionAborted"))
                    } else {
                        let messages = split_message(&mut self.buffer, &buf[..size]);
                        for message in messages {

                            let msg = msg!{
                                "event": "sys:recv",
                                "conn_id": self.id,
                                "data": message
                            };

                            queue_o.push(msg).unwrap();
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
        loop {
            if let Some(front) = self.queue_i.front_mut() {
                match self.socket.write(front) {
                    Ok(size) => {
                        if size == 0 {
                            return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "ConnectionAborted"))
                        } else {
                            if size == front.len() {
                                self.queue_i.pop_front();
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

        if self.queue_i.is_empty() {
            self.interest.remove(Ready::writable());
        } else {
            self.interest.insert(Ready::writable());
        }

        poll.reregister(&self.socket, Token(self.id as usize), self.interest, PollOpt::edge() | PollOpt::oneshot())?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use nson::msg;
    use crate::service::split_message;

    #[test]
    fn test_small_buf() {
        let mut buffer: Vec<u8> = Vec::new();
        let mut data: Vec<u8> = Vec::new();
        data.extend_from_slice(&[16, 0, 0]);
        split_message(&mut buffer, &data);

        assert!(buffer == &[16, 0, 0]);
    }

    #[test]
    fn test_read() {
        let mut data: Vec<u8> = Vec::new();

        let message1 = msg!{"aa": "bb"};
        message1.encode(&mut data).unwrap();
        let message2 = msg!{"cc": "bb"};
        message2.encode(&mut data).unwrap();
        data.extend_from_slice(&[16, 0, 0, 0, 6]);

        let mut buffer: Vec<u8> = Vec::new();
        let m = split_message(&mut buffer, &data);
        assert!(m.len() == 2);
        assert!(buffer == &[16, 0, 0, 0, 6]);
    }

    #[test]
    fn test_read2() {
        let mut data: Vec<u8> = Vec::new();

        let message1 = msg!{"aa": "bb"};
        message1.encode(&mut data).unwrap();
        let message2 = msg!{"cc": "bb"};
        message2.encode(&mut data).unwrap();

        data.extend_from_slice(&[16, 0, 0, 0, 6]);

        let mut buffer: Vec<u8> = Vec::new();
        split_message(&mut buffer, &data);

        let data = [99, 99, 0, 3, 0, 0, 0, 98, 98, 0];

        let m = split_message(&mut buffer, &data);
        assert!(m.len() == 0);
        assert!(buffer == &[16, 0, 0, 0, 6, 99, 99, 0, 3, 0, 0, 0, 98, 98, 0]);
    }

    #[test]
    fn test_read3() {
        let mut data: Vec<u8> = Vec::new();

        let message1 = msg!{"aa": "bb"};
        message1.encode(&mut data).unwrap();
        let message2 = msg!{"cc": "bb"};
        message2.encode(&mut data).unwrap();

        data.extend_from_slice(&[16, 0, 0, 0, 6]);

        let mut buffer: Vec<u8> = Vec::new();
        split_message(&mut buffer, &data);

        let data = [99, 99, 0, 3, 0, 0, 0, 98, 98, 0, 0, 5, 0, 0, 0];

        let m = split_message(&mut buffer, &data);
        assert!(m.len() == 1);
        assert!(buffer == &[5, 0, 0, 0]);
    }

    #[test]
    fn test_read4() {
        let mut data: Vec<u8> = Vec::new();

        let message1 = msg!{"aa": "bb"};
        message1.encode(&mut data).unwrap();
        let message2 = msg!{"cc": "bb"};
        message2.encode(&mut data).unwrap();

        data.extend_from_slice(&[16, 0, 0, 0, 6]);

        let mut buffer: Vec<u8> = Vec::new();
        split_message(&mut buffer, &data);

        let data = [99, 99, 0, 3, 0, 0, 0, 98, 98, 0, 0, 5, 0, 0, 0, 1];

        let m = split_message(&mut buffer, &data);
        assert!(m.len() == 2);
    }
}
