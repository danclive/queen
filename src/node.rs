use std::collections::{VecDeque, HashMap, HashSet};
use std::io::{self, Read, Write, ErrorKind::{WouldBlock, BrokenPipe}};
use std::usize;
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use std::net::SocketAddr as TcpSocketAddr;
use std::os::unix::net::SocketAddr as UnixSocketAddr;

use queen_io::sys::timerfd::{TimerFd, TimerSpec};
use queen_io::tcp::{TcpListener, TcpStream};
use queen_io::unix::{UnixListener, UnixStream};
use queen_io::{Poll, Events, Token, Ready, PollOpt, Event, Evented};

use nson::{Message, msg};

use slab::Slab;

use rand::{self, thread_rng,rngs::ThreadRng};
use rand::seq::SliceRandom;

use crate::util::slice_msg;
use crate::error::ErrorCode;

pub struct Node {
    poll: Poll,
    events: Events,
    tcp_listen: Option<TcpListener>,
    unix_listen: Option<UnixListener>,
    timer: Timer<(Option<usize>, Message)>,
    conns: Slab<Connection>,
    read_buffer: VecDeque<Message>,
    callback: Callback,
    chans: HashMap<String, HashSet<usize>>,
    bridges: HashSet<usize>,
    rand: ThreadRng,
    run: bool
}

#[derive(Debug, Clone)]
pub enum Addr {
    Tcp(TcpSocketAddr),
    Unix(UnixSocketAddr)
}

#[derive(Default)]
pub struct Callback {
    pub accept_fn: Option<Box<dyn Fn(usize, Addr) -> bool>>,
    pub remove_fn: Option<Box<dyn Fn(usize, Addr)>>,
    pub recv_fn: Option<Box<dyn Fn(usize, &mut Message) -> bool>>,
    pub auth_fn: Option<Box<dyn Fn(usize, &mut Message) -> bool>>,
    pub attach_fn: Option<Box<dyn Fn(usize, &mut Message) -> bool>>,
    pub detach_fn: Option<Box<dyn Fn(usize, &mut Message)>>,
    pub emit_fn: Option<Box<dyn Fn(usize, &mut Message) -> bool>>
}

impl Node {
    const TCP_LISTEN: Token = Token(usize::MAX - 1);
    const UNIX_LISTEN: Token = Token(usize::MAX - 2);
    const TIMER: Token = Token(usize::MAX - 3);

    pub fn bind(addr: Option<&str>, path: Option<&str>) -> io::Result<Node> {
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
            chans: HashMap::new(),
            bridges: HashSet::new(),
            rand: thread_rng(),
            run: true
        };

        Ok(node)
    }

    pub fn set_callback(&mut self, callback: Callback) {
        self.callback = callback;
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

                let success = if let Some(accept_fn) = &self.callback.accept_fn {
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

                let entry = self.conns.vacant_entry();

                let success = if let Some(accept_fn) = &self.callback.accept_fn {
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
            self.relay_message(task.data.0, task.data.1)?;
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

            for chan in conn.chans {
                if let Some(ids) = self.chans.get_mut(&chan) {
                    ids.remove(&id);

                    if ids.is_empty() {
                        self.chans.remove(&chan);

                        self.to_bridge_detach(&chan)?;
                    }
                }
            }

            if let Some(remove_fn) = &self.callback.remove_fn {
                remove_fn(id, conn.addr);
            }
        }

        Ok(())
    }

    fn handle_message_from_conn(&mut self, id: usize) -> io::Result<()> {
        while let Some(mut message) = self.read_buffer.pop_front() {

            if !self.can_recv(id, &mut message)? {
                return Ok(())
            }

            let chan = match message.get_str("_chan") {
                Ok(chan) => chan,
                Err(_) => {

                    ErrorCode::CannotGetChanField.to_message(&mut message);

                    self.push_data_to_conn(id, message.to_vec().unwrap())?;
                    continue;
                }
            };

            if chan.starts_with("_") {
                match chan {
                    "_auth" => self.node_auth(id, message)?,
                    "_atta" => self.node_attach(id, message)?,
                    "_deta" => self.node_detach(id, message)?,
                    "_delt" => self.node_deltime(id, message)?,
                    "_ping" => self.node_ping(id, message)?,
                    // "_quer" => self.node_query(id, message)?,
                    _ => {

                        ErrorCode::UnsupportedChan.to_message(&mut message);

                        self.push_data_to_conn(id, message.to_vec().unwrap())?;
                    }
                }
            } else {
                let mut has_chan = false;

                if self.chans.contains_key(chan) {
                    has_chan = true;
                }

                if !self.check_auth(id, &mut message)? {
                    return Ok(())
                }

                if !self.can_emit(id, &mut message)? {
                    return Ok(())
                }

                if !has_chan {

                    ErrorCode::NoConsumers.to_message(&mut message);

                    self.push_data_to_conn(id, message.to_vec().unwrap())?;

                    return Ok(())
                }

                if let Some(message_id) = message.get("_id") {
                    let mut reply_msg = msg!{
                        "_id": message_id.clone()
                    };

                    ErrorCode::OK.to_message(&mut reply_msg);

                    self.push_data_to_conn(id, reply_msg.to_vec().unwrap())?;
                }

                let back = match message.get_bool("_back") {
                    Ok(back) => back,
                    Err(_) => false
                };

                if let Ok(time) = message.get_u32("_time") {
                    if time > 0 {
                        let mut timeid = None;

                        if let Ok(tid) = message.get_str("_tmid") {
                            timeid = Some(tid.to_owned());
                        }

                        let data = if back {
                            (None, message)
                        } else {
                            (Some(id), message)
                        };

                        let task = Task {
                            data,
                            time: Duration::from_millis(u64::from(time)),
                            id: timeid
                        };

                        self.timer.push(task)?;

                        continue;
                    }
                }

                if back {
                    self.relay_message(None, message)?;
                } else {
                    self.relay_message(Some(id), message)?;
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
        if !self.can_auth(id, &mut message)? {
            return Ok(())
        }

        let mut is_bridge = false;

        if let Some(conn) = self.conns.get_mut(id) {
            if let Ok(bridge) = message.get_bool("_brge") {
                if bridge {
                    conn.bridge = true;

                    self.bridges.insert(id);

                    is_bridge = true;
                }
            }

            conn.auth = true;

            ErrorCode::OK.to_message(&mut message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

        if is_bridge {
            if let Some(conn) = self.conns.get_mut(id) {
                for (chan, _) in &self.chans {
                    let msg = msg!{
                        "_chan": "_brge_atta",
                        "_valu": chan,
                    };

                    conn.push_data(msg.to_vec().unwrap());
                }

                conn.reregister(&self.poll)?;
            }
        }

        Ok(())
    }

    fn node_attach(&mut self, id: usize, mut message: Message) -> io::Result<()> {
        if !self.check_auth(id, &mut message)? {
            return Ok(())
        }

        if let Ok(chan) = message.get_str("_valu").map(ToOwned::to_owned) {
            if !self.can_attach(id, &mut message)? {
                return Ok(())
            }

            self.session_attach(id, chan)?;

            ErrorCode::OK.to_message(&mut message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;

        } else {

            ErrorCode::CannotGetValueField.to_message(&mut message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

        Ok(())
    }

    fn node_detach(&mut self, id: usize, mut message: Message) -> io::Result<()> {
        if !self.check_auth(id, &mut message)? {
            return Ok(())
        }

        if let Ok(chan) = message.get_str("_valu").map(ToOwned::to_owned) {
            if let Some(detach_fn) = &self.callback.detach_fn {
                detach_fn(id, &mut message);
            }

            self.session_detach(id, chan)?;

            ErrorCode::OK.to_message(&mut message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        } else {

            ErrorCode::CannotGetValueField.to_message(&mut message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

        Ok(())
    }

    fn node_deltime(&mut self, id: usize, mut message: Message) -> io::Result<()> {
        if !self.check_auth(id, &mut message)? {
            return Ok(())
        }

        if let Ok(timeid) = message.get_str("_tmid") {
            let has = self.timer.remove(timeid.to_owned());

            if has {
                self.timer.refresh()?;

                ErrorCode::OK.to_message(&mut message);
            } else {
                ErrorCode::TimeidNotExist.to_message(&mut message);
            }
        } else {
            ErrorCode::CannotGetTimeidField.to_message(&mut message);
        }

        self.push_data_to_conn(id, message.to_vec().unwrap())?;

        Ok(())
    }

    fn node_ping(&mut self, id: usize, mut message: Message) -> io::Result<()> {
        ErrorCode::OK.to_message(&mut message);

        self.push_data_to_conn(id, message.to_vec().unwrap())?;

        Ok(())
    }

    fn session_attach(&mut self, id: usize, chan: String) -> io::Result<()> {
        let ids = self.chans.entry(chan.to_owned()).or_insert_with(HashSet::new);

        let mut to_bridge = false;
        if ids.is_empty() {
            to_bridge = true;
        }

        ids.insert(id);

        if to_bridge {
            self.to_bridge_attach(&chan)?;
        }

        if let Some(conn) = self.conns.get_mut(id) {
            conn.chans.insert(chan);
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

                    self.to_bridge_detach(&chan)?;
                }
            }
        }

        Ok(())
    }

    fn relay_message(&mut self, self_id: Option<usize>, message: Message) -> io::Result<()> {
        if let Ok(chan) = message.get_str("_chan") {
            if let Ok(share) = message.get_bool("_shar") {
                if share {

                    let mut array: Vec<usize> = Vec::new();

                    if let Some(ids) = self.chans.get(chan) {
                        for id in ids {

                            if let Some(self_id) = self_id {
                                if self_id == *id {
                                    continue;
                                }
                            } 

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
                            self.push_data_to_conn(*id, message.to_vec().unwrap())?;
                        }
                    }

                    return Ok(())
                }
            }

            if let Some(ids) = self.chans.get(chan) {
                for id in ids {
                    if let Some(self_id) = self_id {
                        if self_id == *id {
                            continue;
                        }
                    }

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

    fn check_auth(&mut self, id: usize, message: &mut Message) -> io::Result<bool> {
        let access = self.conns.get(id).map(|conn| conn.auth == true).unwrap_or_default();

        if !access {
            ErrorCode::Unauthorized.to_message(message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

        Ok(access)
    }

    fn can_recv(&mut self, id: usize, message: &mut Message) -> io::Result<bool> {
        let success = if let Some(recv_fn) = &self.callback.recv_fn {
            recv_fn(id, message)
        } else {
            true
        };

        if !success {
            ErrorCode::RefuseReceiveMessage.to_message(message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

        Ok(success)
    }

    fn can_auth(&mut self, id: usize, message: &mut Message) -> io::Result<bool> {
        let success = if let Some(auth_fn) = &self.callback.auth_fn {
            auth_fn(id, message)
        } else {
            true
        };

        if !success {

            ErrorCode::AuthenticationFailed.to_message(message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

        Ok(success)
    }

    fn can_attach(&mut self, id: usize, message: &mut Message) -> io::Result<bool> {
         let success = if let Some(attach_fn) = &self.callback.attach_fn {
            attach_fn(id, message)
        } else {
            true
        };

        if !success {
            ErrorCode::Unauthorized.to_message(message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

        Ok(success)
    }

    fn can_emit(&mut self, id: usize, message: &mut Message) -> io::Result<bool> {
        let success = if let Some(emit_fn) = &self.callback.emit_fn {
            emit_fn(id, message)
        } else {
            true
        };

        if !success {
            ErrorCode::Unauthorized.to_message(message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

        Ok(success)
    }

    fn to_bridge_attach(&mut self, chan: &str) -> io::Result<()> {
        let msg = msg!{
            "_chan": "_brge_atta",
            "_valu": chan
        };

        for id in &self.bridges {
            if let Some(conn) = self.conns.get_mut(*id) {
                conn.push_data(msg.to_vec().unwrap());
                conn.reregister(&self.poll)?;
            }
        }

        Ok(())
    }

    fn to_bridge_detach(&mut self, chan: &str) -> io::Result<()>{
        let msg = msg!{
            "_chan": "_brge_deta",
            "_valu": chan,
        };

        for id in &self.bridges {
            if let Some(conn) = self.conns.get_mut(*id) {
                conn.push_data(msg.to_vec().unwrap());
                conn.reregister(&self.poll)?;
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
    auth: bool,
    bridge: bool,
    chans: HashSet<String>
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
            bridge: false,
            chans: HashSet::new()
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
                        return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
                    } else {
                        let messages = slice_msg(&mut self.read_buffer, &buf[..size])?;

                        for message in messages {
                            match Message::from_slice(&message) {
                                Ok(message) => read_buffer.push_back(message),
                                Err(_err) => {
                    
                                    let mut error = msg!{};

                                    #[cfg(debug_assertions)]
                                    error.insert("error_info", _err.to_string());

                                    ErrorCode::UnsupportedFormat.to_message(&mut error);

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
                        return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
                    } else if size == front.len() {
                        self.write_buffer.pop_front();
                    } else if size < front.len() {
                        // size < front.len()
                        // assert!(size > front.len());
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

    pub fn remove(&mut self, id: String) -> bool {
        let id = Some(id);

        let mut tasks_vec: Vec<Task<T>> = Vec::from(self.tasks.clone());

        let mut has = false;

        if let Some(pos) = tasks_vec.iter().position(|x| x.id == id) {
            tasks_vec.remove(pos);
            has  = true;
        }

        self.tasks = tasks_vec.into();

        has
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

impl Callback {
    pub fn accept<F>(&mut self, f: F) where F: Fn(usize, Addr) -> bool + 'static {
        self.accept_fn = Some(Box::new(f))
    }

    pub fn remove<F>(&mut self, f: F) where F: Fn(usize, Addr) + 'static {
        self.remove_fn = Some(Box::new(f))
    }

    pub fn recv<F>(&mut self, f: F) where F: Fn(usize, &mut Message) -> bool + 'static {
        self.recv_fn = Some(Box::new(f))
    }

    pub fn auth<F>(&mut self, f: F) where F: Fn(usize, &mut Message) -> bool + 'static {
        self.auth_fn = Some(Box::new(f))
    }

    pub fn attach<F>(&mut self, f: F) where F: Fn(usize, &mut Message) -> bool + 'static {
        self.attach_fn = Some(Box::new(f))
    }

    pub fn detach<F>(&mut self, f: F) where F: Fn(usize, &mut Message) + 'static {
        self.detach_fn = Some(Box::new(f))
    }

    pub fn emit<F>(&mut self, f: F) where F: Fn(usize, &mut Message) -> bool + 'static {
        self.emit_fn = Some(Box::new(f))
    }
}
