use std::rc::Rc;
use std::collections::{VecDeque, HashMap};
use std::io::{self, Read, Write, ErrorKind::WouldBlock};
use std::usize;
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::collections::BinaryHeap;
use std::cmp::Ordering;

use queen_io::sys::timerfd::{TimerFd, TimerSpec};
use queen_io::tcp::{TcpListener, TcpStream};
use queen_io::unix::{UnixListener, UnixStream};
use queen_io::{Poll, Events, Token, Ready, PollOpt, Event, Evented};

use nson::{Message, msg};

use slab::Slab;

use rand;
use rand::seq::SliceRandom;

use crate::util::split_message;

pub struct Node {
    poll: Poll,
    events: Events,
    tcp_listen: Option<TcpListener>,
    unix_listen: Option<UnixListener>,
    timer: Timer<Message>,
    conns: Slab<Connection>,
    read_buffer: VecDeque<Message>,
    callback: Callback,
    channels: HashMap<String, Vec<usize>>,
    rand: rand::rngs::ThreadRng,
    run: bool
}

#[derive(Debug, Clone)]
pub enum Addr {
    Tcp(std::net::SocketAddr),
    Unix(std::os::unix::net::SocketAddr)
}

pub struct Callback {
    pub accept_fn: Option<Rc<dyn Fn(usize, Addr) -> bool>>,
    pub remove_fn: Option<Rc<dyn Fn(usize, Addr)>>,
    pub recv_fn: Option<Rc<dyn Fn(usize, &mut Message) -> bool>>,
    pub auth_fn: Option<Rc<dyn Fn(usize, &mut Message) -> bool>>,
    pub attach_fn: Option<Rc<dyn Fn(usize, &mut Message) -> bool>>,
    pub detach_fn: Option<Rc<dyn Fn(usize, &mut Message)>>,
    pub emit_fn: Option<Rc<dyn Fn(usize, &mut Message) -> bool>>
}

impl Node {
    const TCP_LISTEN: Token = Token(usize::MAX - 1);
    const UNIX_LISTEN: Token = Token(usize::MAX - 2);
    const TIMER: Token = Token(usize::MAX - 3);

    pub fn new(addr: Option<&str>, path: Option<&str>) -> io::Result<Node> {
        let tcp_listen = if let Some(addr) = addr {
            Some(TcpListener::bind(addr)?)
        } else {
            None
        };

        let unix_listen = if let Some(path) = path {
            Some(UnixListener::bind(path)?)
        } else {
            None
        };

        let node = Node {
            poll: Poll::new()?,
            events: Events::with_capacity(1024),
            tcp_listen,
            unix_listen,
            timer: Timer::new()?,
            conns: Slab::new(),
            read_buffer: VecDeque::new(),
            callback: Callback {
                accept_fn: None,
                remove_fn: None,
                recv_fn: None,
                auth_fn: None,
                attach_fn: None,
                detach_fn: None,
                emit_fn: None
            },
            channels: HashMap::new(),
            rand: rand::thread_rng(),
            run: true
        };

        Ok(node)
    }

    pub fn run(&mut self) -> io::Result<()> {
        if let Some(tcp) = &self.tcp_listen {
            self.poll.register(
                tcp,
                Self::TCP_LISTEN,
                Ready::readable(),
                PollOpt::edge()
            )?;
        };

        if let Some(unix) = &self.unix_listen {
            self.poll.register(
                unix,
                Self::UNIX_LISTEN,
                Ready::readable(),
                PollOpt::edge()
            )?;
        };

        self.poll.register(
            &self.timer.as_raw_fd(),
            Self::TIMER,
            Ready::readable(),
            PollOpt::edge()
        )?;

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

    fn dispatch(&mut self, event: Event) -> io::Result<()> {
        match event.token() {
            Self::TCP_LISTEN => self.dispatch_tcp_listen()?,
            Self::UNIX_LISTEN => self.dispatch_unix_listen()?,
            Self::TIMER => self.dispatch_timer()?,
            token => self.dispatch_conn(token, event)?
        }

        Ok(())
    }

    fn dispatch_tcp_listen(&mut self) -> io::Result<()> {
        if let Some(tcp) = &self.tcp_listen {
            loop {
                let (socket, addr) = match tcp.accept() {
                    Ok((socket, addr)) => (socket, addr),
                    Err(err) => {
                        if let WouldBlock = err.kind() {
                            break;
                        } else {
                            return Err(err)
                        }
                    }
                };

                socket.set_nodelay(true)?;

                let entry = self.conns.vacant_entry();

                let success = if let Some(accept_fn) = self.callback.accept_fn.clone() {
                    accept_fn(entry.key(), Addr::Tcp(addr))
                } else {
                    true
                };

                if success {
                    let conn = Connection::new(entry.key(), Addr::Tcp(addr), Stream::Tcp(socket))?;
                    conn.register(&self.poll)?;

                    entry.insert(conn);
                }
            }
        }

        Ok(())
    }

    fn dispatch_unix_listen(&mut self) -> io::Result<()> {
        if let Some(unix) = &self.unix_listen {
            loop {
                let (socket, addr) = match unix.accept() {
                    Ok((socket, addr)) => (socket, addr),
                    Err(err) => {
                        if let WouldBlock = err.kind() {
                            break;
                        } else {
                            return Err(err)
                        }
                    }
                };

                //socket.set_nodelay(true)?;

                let entry = self.conns.vacant_entry();

                let success = if let Some(accept_fn) = self.callback.accept_fn.clone() {
                    accept_fn(entry.key(), Addr::Unix(addr.clone()))
                } else {
                    true
                };

                if success {
                    let conn = Connection::new(entry.key(), Addr::Unix(addr), Stream::Unix(socket))?;
                    conn.register(&self.poll)?;

                    entry.insert(conn);
                }
            }
        }

        Ok(())
    }

    fn dispatch_timer(&mut self) -> io::Result<()> {
        self.timer.done()?;

        if let Some(task) = self.timer.pop() {
            self.relay_message(task.data)?;
        }

        self.timer.refresh()?;

        Ok(())
    }

    fn dispatch_conn(&mut self, token: Token, event: Event) -> io::Result<()> {
        let readiness = event.readiness();
        if readiness.is_hup() || readiness.is_error() {
            self.remove_conn(token.0)?;
        }

        if readiness.is_readable() {
            if let Some(conn) = self.conns.get_mut(token.0) {
                if conn.read(&mut self.read_buffer).is_err() {
                    self.remove_conn(token.0)?;
                } else {
                    conn.reregister(&self.poll)?;
                }

                if !self.read_buffer.is_empty() {
                    self.handle_message_from_conn(token.0)?;
                }
            }
        }

        if readiness.is_writable() {
            if let Some(conn) = self.conns.get_mut(token.0) {
                if conn.write().is_err() {
                    self.remove_conn(token.0)?;
                } else {
                    conn.reregister(&self.poll)?;
                }
            }
        }

        Ok(())
    }

    fn remove_conn(&mut self, id: usize) -> io::Result<()> {
        if self.conns.contains(id) {
            let conn = self.conns.remove(id);
            conn.deregister(&self.poll)?;
        }

        Ok(())
    }

    fn handle_message_from_conn(&mut self, id: usize) -> io::Result<()> {
        while let Some(mut message) = self.read_buffer.pop_front() {

            let success = if let Some(recv_fn) = self.callback.recv_fn.clone() {
                recv_fn(id, &mut message)
            } else {
                true
            };

            if !success {
                message.insert("ok", false);
                message.insert("error", "Refuse to receive message!");
                self.push_data_to_conn(id, message.to_vec().unwrap())?;
            } else {
                let event = match message.get_str("event") {
                    Ok(message) => message,
                    Err(_) => {
                        message.insert("ok", false);
                        message.insert("error", "Can not get event!");
                        self.push_data_to_conn(id, message.to_vec().unwrap())?;
                        continue;
                    }
                };

                if event.starts_with("node::") {
                    match event {
                        "node::auth" => self.node_auth(id, message)?,
                        "node::attach" => self.node_attach(id, message)?,
                        "node::detach" => self.node_detach(id, message)?,
                        "node::deltime" => self.node_deltime(id, message)?,
                        _ => {
                            message.insert("ok", false);
                            message.insert("error", "Event unsupport!");

                            self.push_data_to_conn(id, message.to_vec().unwrap())?;
                        }
                    }
                } else {
                    let access = self.conns.get(id).map(|conn| conn.auth == true).unwrap_or_default();

                    if !access {
                        message.insert("ok", false);
                        message.insert("error", "No permission!");

                        self.push_data_to_conn(id, message.to_vec().unwrap())?;

                        return Ok(())
                    }

                    let success = if let Some(emit_fn) = self.callback.emit_fn.clone() {
                        emit_fn(id, &mut message)
                    } else {
                        true
                    };

                    if !success {
                        message.insert("ok", false);
                        message.insert("error", "Not auth!");
                        self.push_data_to_conn(id, message.to_vec().unwrap())?;
                    } else {
                        if let Some(message_id) = message.get("_id") {
                            let reply_msg = msg!{
                                "_id": message_id.clone(),
                                "ok": true
                            };

                            self.push_data_to_conn(id, reply_msg.to_vec().unwrap())?;
                        }

                        if let Ok(time) = message.get_u32("_time") {
                            if time > 0 {
                                let mut timeid = None;

                                if let Ok(tid) = message.get_str("_timeid") {
                                    timeid = Some(tid.to_owned());
                                }

                                let task = Task {
                                    data: message,
                                    time: Duration::from_millis(u64::from(time)),
                                    id: timeid
                                };

                                self.timer.push(task)?;

                                continue;
                            }
                        }

                        self.relay_message(message)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn push_data_to_conn(&mut self, id: usize, data: Vec<u8>) -> io::Result<()> {
        if let Some(conn) = self.conns.get_mut(id) {
            conn.push_data(data);
            conn.reregister(&self.poll)?;
        }

        Ok(())
    }

    fn node_auth(&mut self, id: usize, mut message: Message) -> io::Result<()> {
        let success = if let Some(auth_fn) = self.callback.auth_fn.clone() {
            auth_fn(id, &mut message)
        } else {
            true
        };

        if !success {
            message.insert("ok", false);
            message.insert("error", "Authentication failed!");

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        } else {
            if let Some(conn) = self.conns.get_mut(id) {
                conn.auth = true;

                message.insert("ok", true);

                self.push_data_to_conn(id, message.to_vec().unwrap())?;
            }
        }

        Ok(())
    }

    fn node_attach(&mut self, id: usize, mut message: Message) -> io::Result<()> {
        if let Ok(event) = message.get_str("value").map(ToOwned::to_owned) {
            let access = self.conns.get(id).map(|conn| conn.auth == true).unwrap_or_default();

            if !access {
                message.insert("ok", false);
                message.insert("error", "No permission!");

                self.push_data_to_conn(id, message.to_vec().unwrap())?;
            } else {
                let success = if let Some(attach_fn) = self.callback.attach_fn.clone() {
                    attach_fn(id, &mut message)
                } else {
                    true
                };

                if !success {
                    message.insert("ok", false);
                    message.insert("error", "Not auth!");
                    self.push_data_to_conn(id, message.to_vec().unwrap())?;
                } else {

                    self.session_attach(id, event);

                    message.insert("ok", true);
                    self.push_data_to_conn(id, message.to_vec().unwrap())?;
                }
            }
        } else {
            message.insert("ok", false);
            message.insert("error", "Can not get value from message!");

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

        Ok(())
    }

    fn node_detach(&mut self, id: usize, mut message: Message) -> io::Result<()> {
        if let Ok(event) = message.get_str("value").map(ToOwned::to_owned) {
            if let Some(detach_fn) = self.callback.detach_fn.clone() {
                detach_fn(id, &mut message);
            }

            self.session_detach(id, event)
        } else {
            message.insert("ok", false);
            message.insert("error", "Can not get value from message!");

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

        Ok(())
    }

    fn node_deltime(&mut self, id: usize, mut message: Message) -> io::Result<()> {
        if let Ok(timeid) = message.get_str("_timeid") {
            self.timer.remove(timeid.to_owned());
            self.timer.refresh()?;

            message.insert("ok", true);
        } else {
            message.insert("ok", false);
            message.insert("error", "Can not get _timeid!");
        }

        self.push_data_to_conn(id, message.to_vec().unwrap())?;

        Ok(())
    }

    fn session_attach(&mut self, id: usize, event: String) {
        let channel = self.channels.entry(event.to_owned()).or_insert_with(Vec::new); 
        if !channel.contains(&id) {
            channel.push(id);
        }

        if let Some(conn) = self.conns.get_mut(id) {
            let count = conn.events.entry(event).or_insert(0);
            *count += 1;
        }
    }

    fn session_detach(&mut self, id: usize, event: String) {
        if let Some(conn) = self.conns.get_mut(id) {
            if let Some(count) = conn.events.get_mut(&event) {
                *count -= 1;

                if *count == 0 {
                    conn.events.remove(&event);

                    if let Some(channel) = self.channels.get_mut(&event) {
                        if let Some(pos) = channel.iter().position(|x| *x == id) {
                            channel.remove(pos);
                        }

                        if channel.is_empty() {
                            self.channels.remove(&event);
                        }
                    }
                }
            }
        }
    }

    fn relay_message(&mut self, message: Message) -> io::Result<()> {
        if let Ok(event) = message.get_str("event") {
            if let Ok(share) = message.get_bool("_share") {
                if share {

                    let mut array: Vec<usize> = Vec::new();

                    if let Some(channel) = self.channels.get(event) {
                        for id in channel {
                            if let Some(conn) = self.conns.get_mut(*id) {
                                if conn.auth {
                                    array.push(*id);
                                }
                            }
                        }
                    }

                    if array.is_empty() {
                        return Ok(())
                    }

                    if array.len() == 1 {
                        if let Some(conn) = self.conns.get_mut(array[0]) {
                            conn.push_data(message.to_vec().unwrap());
                            conn.reregister(&self.poll)?;
                        }
                    } else {
                        if let Some(id) = array.choose(&mut self.rand) {
                            if let Some(conn) = self.conns.get_mut(*id) {
                                conn.push_data(message.to_vec().unwrap());
                                conn.reregister(&self.poll)?;
                            }
                        }
                    }

                    return Ok(())
                }
            }

            if let Some(channel) = self.channels.get(event) {
                for id in channel {
                    if let Some(conn) = self.conns.get_mut(*id) {
                        if conn.auth {
                            conn.push_data(message.to_vec().unwrap());
                            conn.reregister(&self.poll)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Connection {
    id: usize,
    addr: Addr,
    stream: Stream,
    interest: Ready,
    read_buffer: Vec<u8>,
    write_buffer: VecDeque<Vec<u8>>,
    // session
    auth: bool,
    events: HashMap<String, usize>
}

#[derive(Debug)]
enum Stream {
    Tcp(TcpStream),
    Unix(UnixStream)
}

impl Connection {
    fn new(id: usize, addr: Addr, stream: Stream) -> io::Result<Connection> {
        let conn = Connection {
            id,
            addr,
            stream,
            interest: Ready::readable() | Ready::hup(),
            read_buffer: Vec::new(),
            write_buffer: VecDeque::new(),
            auth: false,
            events: HashMap::new()
        };

        Ok(conn)
    }

    fn register(&self, poll: &Poll) -> io::Result<()>{
        poll.register(
            &self.stream,
            Token(self.id),
            self.interest,
            PollOpt::edge()
        )
    }

    fn reregister(&self, poll: &Poll) -> io::Result<()>{
        poll.reregister(
            &self.stream,
            Token(self.id),
            self.interest,
            PollOpt::edge()
        )
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        poll.deregister(&self.stream)
    }

    fn read(&mut self, read_buffer: &mut VecDeque<Message>) -> io::Result<()> {
        loop {
            let mut buf = [0; 4 * 1024];

            match self.stream.read(&mut buf) {
                Ok(size) => {
                    if size == 0 {
                        return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "ConnectionAborted"))
                    } else {
                        let messages = split_message(&mut self.read_buffer, &buf[..size]);
                    
                        for message in  messages {
                            match Message::from_slice(&message) {
                                Ok(message) => read_buffer.push_back(message),
                                Err(err) => {
                                    let error = msg!{
                                        "ok": false,
                                        "error": err.to_string()
                                    };

                                    let _ = error.encode(&mut self.stream);
                                }
                            }
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

        Ok(())
    }

    fn write(&mut self) -> io::Result<()> {
        while let Some(front) = self.write_buffer.front_mut() {
            match self.stream.write(front) {
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

        Ok(())
    }

    fn push_data(&mut self, data: Vec<u8>) {
        self.write_buffer.push_back(data.clone());
        self.interest.insert(Ready::writable());
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(tcp) => tcp.read(buf),
            Stream::Unix(unix) => unix.read(buf)
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(tcp) => tcp.write(buf),
            Stream::Unix(unix) => unix.write(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Tcp(tcp) => tcp.flush(),
            Stream::Unix(unix) => unix.flush()
        }
    }
}

impl Evented for Stream {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        match self {
            Stream::Tcp(tcp) => poll.register(tcp, token, interest, opts),
            Stream::Unix(unix) => poll.register(unix, token, interest, opts)
        }
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        match self {
            Stream::Tcp(tcp) => poll.reregister(tcp, token, interest, opts),
            Stream::Unix(unix) => poll.reregister(unix, token, interest, opts)
        }
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        match self {
            Stream::Tcp(tcp) => poll.deregister(tcp),
            Stream::Unix(unix) => poll.deregister(unix)
        }
    }
}

#[derive(Clone, Debug)]
pub struct Task<T> {
    pub data: T,
    pub time: Duration,
    pub id: Option<String>
}

impl<T> PartialOrd for Task<T> {
    fn partial_cmp(&self, other: &Task<T>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Task<T> {
    fn cmp(&self, other: &Task<T>) -> Ordering {
        match self.time.cmp(&other.time) {
            Ordering::Equal => Ordering::Equal,
            Ordering::Greater => Ordering::Less,
            Ordering::Less => Ordering::Greater
        }
    }
}

impl<T> PartialEq for Task<T> {
    fn eq(&self, other: &Task<T>) -> bool {
        self.time == other.time
    }
}

impl<T> Eq for Task<T> {}

pub struct Timer<T> {
    tasks: BinaryHeap<Task<T>>,
    timerfd: TimerFd
}

impl<T: Clone> Timer<T> {
    pub fn new() -> io::Result<Timer<T>> {
        Ok(Timer {
            tasks: BinaryHeap::new(),
            timerfd: TimerFd::new()?
        })
    }

    pub fn peek(&self) -> Option<&Task<T>> {
        self.tasks.peek()
    }

    pub fn push(&mut self, task: Task<T>) -> io::Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

        let mut task = task;
        task.time += now;

        if let Some(peek_task) = self.peek() {
            if task > *peek_task {
                self.settime(true, TimerSpec {
                    interval: Duration::new(0, 0),
                    value: task.time
                })?;
            }
        } else {
            self.settime(true, TimerSpec {
                interval: Duration::new(0, 0),
                value: task.time
            })?;
        }

        self.tasks.push(task);

        Ok(())
    }

    #[inline]
    pub fn pop(&mut self) -> Option<Task<T>> {
        self.tasks.pop()
    }

    #[inline]
    pub fn settime(&self, abstime: bool, value: TimerSpec) -> io::Result<TimerSpec>{
        self.timerfd.settime(abstime, value)
    }

    #[inline]
    pub fn gettime(&self) -> io::Result<TimerSpec> {
        self.timerfd.gettime()
    }

    #[inline]
    pub fn done(&self) -> io::Result<u64> {
        self.timerfd.read()
    }

    pub fn remove(&mut self, id: String) {
        let id = Some(id);

        let mut tasks_vec: Vec<Task<T>> = Vec::from(self.tasks.clone());

        if let Some(pos) = tasks_vec.iter().position(|x| x.id == id) {
            tasks_vec.remove(pos);
        }

        self.tasks = tasks_vec.into();
    }

    pub fn refresh(&mut self) -> io::Result<()> {
        if let Some(task) = self.peek() {
            self.settime(true, TimerSpec {
                interval: Duration::new(0, 0),
                value: task.time
            })?;
        }

        Ok(())
    }
}

impl<T> AsRawFd for Timer<T> {
    fn as_raw_fd(&self) -> RawFd {
        self.timerfd.as_raw_fd()
    }
}
