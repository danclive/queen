use std::collections::{VecDeque, HashMap, HashSet};
use std::io::{self, Read, Write, ErrorKind::{WouldBlock, BrokenPipe}};
use std::usize;
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use std::net::ToSocketAddrs;

use libc;

use queen_io::sys::timerfd::{TimerFd, TimerSpec};
use queen_io::tcp::TcpListener;
use queen_io::unix::UnixListener;
use queen_io::{Poll, Events, Token, Ready, PollOpt, Event, Evented};

use nson::{Message, msg};

use slab::Slab;

use rand::{self, thread_rng,rngs::ThreadRng};
use rand::seq::SliceRandom;

use crate::net::{Addr, Stream};
use crate::util::slice_msg;
use crate::error::ErrorCode;

const KEEP_ALIVE: i32 = 1; // 开启keepalive属性
const KEEP_IDLE: i32 = 60; // 如该连接在60秒内没有任何数据往来,则进行探测 
const KEEP_INTERVAL: i32 = 5; // 探测时发包的时间间隔为5秒
const KEEP_COUNT: i32 = 3; // 探测尝试的次数.如果第1次探测包就收到响应了,则后2次的不再发.

pub struct Node<T> {
    poll: Poll,
    events: Events,
    listens: HashMap<usize, Listen>,
    token: usize,
    timer: Timer<(Option<usize>, Message)>,
    conns: Slab<Connection>,
    read_buffer: VecDeque<Message>,
    callback: Callback<T>,
    chans: HashMap<String, HashSet<usize>>,
    rand: ThreadRng,
    user_data: T,
    run: bool
}

pub struct Callback<T> {
    pub accept_fn: Option<AcceptFn<T>>,
    pub remove_fn: Option<RemoveFn<T>>,
    pub recv_fn: Option<RecvFn<T>>,
    pub send_fn: Option<SendFn<T>>,
    pub auth_fn: Option<AuthFn<T>>,
    pub attach_fn: Option<AttachFn<T>>,
    pub detach_fn: Option<DetachFn<T>>,
    pub emit_fn: Option<EmitFn<T>>
}

type AcceptFn<T> = Box<dyn Fn(usize, &Addr, &mut T) -> bool>;
type RemoveFn<T> = Box<dyn Fn(usize, &Addr, &mut T)>;
type RecvFn<T> = Box<dyn Fn(usize, &Addr, &mut Message, &mut T) -> bool>;
type SendFn<T> = Box<dyn Fn(usize, &Addr, &mut Message, &mut T) -> bool>;
type AuthFn<T> = Box<dyn Fn(usize, &Addr, &mut Message, &mut T) -> bool>;
type AttachFn<T> = Box<dyn Fn(usize, &Addr, &mut Message, &mut T) -> bool>;
type DetachFn<T> = Box<dyn Fn(usize, &Addr, &mut Message, &mut T)>;
type EmitFn<T> = Box<dyn Fn(usize, &Addr, &mut Message, &mut T) -> bool>;

#[derive(Default)]
pub struct NodeConfig {
    pub addrs: Vec<Addr>
}

impl NodeConfig {
    pub fn new() -> NodeConfig {
        NodeConfig {
            addrs: Vec::new()
        }
    }

    pub fn tcp<A: ToSocketAddrs>(&mut self, addr: A) -> io::Result<()> {
        let addr = Addr::tcp(addr)?;
        self.addrs.push(addr);
        Ok(())
    }

    pub fn uds(&mut self, path: String) {
        self.addrs.push(Addr::Uds(path))
    }
}

impl<T> Node<T> {
    const TIMER: Token = Token(usize::MAX);

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
            poll: Poll::new()?,
            events: Events::with_capacity(1024),
            listens,
            token,
            timer: Timer::new()?,
            conns: Slab::new(),
            read_buffer: VecDeque::new(),
            callback: Callback::new(),
            chans: HashMap::new(),
            rand: thread_rng(),
            user_data,
            run: true
        };

        Ok(node)
    }

    pub fn set_callback(&mut self, callback: Callback<T>) {
        self.callback = callback;
    }

    pub fn run(&mut self) -> io::Result<()> {
        for (id, listen) in &self.listens {
            listen.register(&self.poll, Token(*id), Ready::readable(), PollOpt::edge())?;
        }

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
            Self::TIMER => self.dispatch_timer()?,
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

                let entry = self.conns.vacant_entry();

                let success = if let Some(accept_fn) = &self.callback.accept_fn {
                    accept_fn(entry.key(), &addr, &mut self.user_data)
                } else {
                    true
                };

                if success {
                    let conn = Connection::new(entry.key(), addr, socket);
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
        
        let mut remove = readiness.is_hup() || readiness.is_error();

        if readiness.is_readable() {
            if let Some(conn) = self.conns.get_mut(token.0) {
                if conn.read(&mut self.read_buffer).is_err() {
                    remove = true;
                } else {
                    conn.reregister(&self.poll)?;
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
                    conn.reregister(&self.poll)?;
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
            conn.deregister(&self.poll)?;

            for chan in conn.chans {
                if let Some(ids) = self.chans.get_mut(&chan) {
                    ids.remove(&id);

                    if ids.is_empty() {
                        self.chans.remove(&chan);
                    }
                }
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

                    ErrorCode::CannotGetChanField.to_message(&mut message);

                    self.push_data_to_conn(id, message.to_vec().unwrap())?;
                    continue;
                }
            };

            if chan.starts_with('_') {
                match chan {
                    "_auth" => self.node_auth(id, &addr, message)?,
                    "_atta" => self.node_attach(id, &addr, message)?,
                    "_deta" => self.node_detach(id, &addr, message)?,
                    "_delt" => self.node_deltime(id, message)?,
                    "_ping" => self.node_ping(id, message)?,
                    // "_quer" => self.node_query(id, message)?,
                    _ => {

                        ErrorCode::UnsupportedChan.to_message(&mut message);

                        self.push_data_to_conn(id, message.to_vec().unwrap())?;
                    }
                }
            } else {
                let has_chan = self.chans.contains_key(chan);

                if !self.check_auth(id, &mut message)? {
                    return Ok(())
                }

                if !self.can_emit(id, &addr, &mut message)? {
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
                    Ok(back) => {
                        message.remove("_back");
                        back
                    }
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

    fn node_auth(&mut self, id: usize, addr: &Addr,mut message: Message) -> io::Result<()> {
        if !self.can_auth(id, addr, &mut message)? {
            return Ok(())
        }

        if let Some(conn) = self.conns.get_mut(id) {
            conn.auth = true;

            ErrorCode::OK.to_message(&mut message);

            conn.push_data(message.to_vec().unwrap());

            conn.reregister(&self.poll)?;
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

            self.session_attach(id, chan)?;

            ErrorCode::OK.to_message(&mut message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;

        } else {
            ErrorCode::CannotGetValueField.to_message(&mut message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

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

        ids.insert(id);

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
                }
            }
        }

        Ok(())
    }

    fn relay_message(&mut self, self_id: Option<usize>, mut message: Message) -> io::Result<()> {
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
                            // check can send
                            let success = if let Some(send_fn) = &self.callback.send_fn {
                                send_fn(conn.id, &conn.addr, &mut message, &mut self.user_data)
                            } else {
                                true
                            };

                            if success {
                                conn.push_data(message.to_vec().unwrap());
                                conn.reregister(&self.poll)?;
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
                                conn.reregister(&self.poll)?;
                            }
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
        if let Some(conn) = self.conns.get_mut(id) {
            if conn.auth {
                return Ok(true)
            }

            ErrorCode::Unauthorized.to_message(message);

            conn.push_data(message.to_vec().unwrap());
            conn.reregister(&self.poll)?;
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
            ErrorCode::RefuseReceiveMessage.to_message(message);

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

            ErrorCode::AuthenticationFailed.to_message(message);

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
            ErrorCode::Unauthorized.to_message(message);

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
            ErrorCode::Unauthorized.to_message(message);

            self.push_data_to_conn(id, message.to_vec().unwrap())?;
        }

        Ok(success)
    }
}

impl Addr {
    fn bind(&self) -> io::Result<Listen> {
        match self {
            Addr::Tcp(addr) => Ok(Listen::Tcp(TcpListener::bind(addr)?)),
            Addr::Uds(addr) => Ok(Listen::Unix(UnixListener::bind(addr)?))
        }
    }
}

#[derive(Debug)]
enum Listen {
    Tcp(TcpListener),
    Unix(UnixListener)
}

impl Listen {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        match self {
            Listen::Tcp(tcp) => poll.register(tcp, token, interest, opts),
            Listen::Unix(unix) => poll.register(unix, token, interest, opts)
        }
    }

    fn accept(&self) -> io::Result<(Stream, Addr)> {
        match self {
            Listen::Tcp(tcp) => {
                tcp.accept().and_then(|(socket, addr)| {
                    socket.set_nodelay(true)?;

                    let fd = socket.as_raw_fd();

                    unsafe {
                        libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, &KEEP_ALIVE as *const i32 as *const _, 4);
                        libc::setsockopt(fd, libc::SOL_TCP, libc::TCP_KEEPIDLE, &KEEP_IDLE as *const i32 as *const _, 4);
                        libc::setsockopt(fd, libc::SOL_TCP, libc::TCP_KEEPINTVL, &KEEP_INTERVAL as *const i32 as *const _, 4);
                        libc::setsockopt(fd, libc::SOL_TCP, libc::TCP_KEEPCNT, &KEEP_COUNT as *const i32 as *const _, 4);
                    }

                    Ok((Stream::Tcp(socket), Addr::Tcp(addr))) 
                })
            }
            Listen::Unix(unix) => {
                unix.accept().and_then(|(socket, addr)| {
                   
                    let addr = addr.as_pathname().map(|p| p.display().to_string()).unwrap_or_else(|| "unnamed".to_string());

                    Ok((Stream::Unix(socket), Addr::Uds(addr)))
                })
            }
        }
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

impl Connection {
    fn new(id: usize, addr: Addr, stream: Stream) -> Connection {
        Connection {
            id,
            addr,
            stream,
            interest: Ready::readable() | Ready::hup(),
            read_buffer: Vec::new(),
            write_buffer: VecDeque::new(),
            auth: false,
            bridge: false,
            chans: HashSet::new()
        }
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

impl<T> Callback<T> {
    pub fn new() -> Callback<T> {
        Callback {
            accept_fn: None,
            remove_fn: None,
            recv_fn: None,
            send_fn: None,
            auth_fn: None,
            attach_fn: None,
            detach_fn: None,
            emit_fn: None
        }
    }

    pub fn accept<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut T) -> bool + 'static {
        self.accept_fn = Some(Box::new(f))
    }

    pub fn remove<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut T) + 'static {
        self.remove_fn = Some(Box::new(f))
    }

    pub fn recv<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut Message, &mut T) -> bool + 'static {
        self.recv_fn = Some(Box::new(f))
    }

    pub fn send<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut Message, &mut T) -> bool + 'static {
        self.send_fn = Some(Box::new(f))
    }

    pub fn auth<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut Message, &mut T) -> bool + 'static {
        self.auth_fn = Some(Box::new(f))
    }

    pub fn attach<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut Message, &mut T) -> bool + 'static {
        self.attach_fn = Some(Box::new(f))
    }

    pub fn detach<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut Message, &mut T) + 'static {
        self.detach_fn = Some(Box::new(f))
    }

    pub fn emit<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut Message, &mut T) -> bool + 'static {
        self.emit_fn = Some(Box::new(f))
    }
}

impl<T> Default for Callback<T> {
    fn default() -> Callback<T> {
        Callback::new()
    }
}
