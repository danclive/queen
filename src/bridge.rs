use std::collections::{VecDeque, HashMap, HashSet};

// use std::net::ToSocketAddrs;
// use std::path::Path;
use std::io::{self, Read, Write, ErrorKind::{WouldBlock, BrokenPipe, InvalidData}};
use std::os::unix::io::AsRawFd;

use queen_io::tcp::TcpStream;
use queen_io::unix::UnixStream;

use nson::{Message, msg};

use crate::poll::{poll, Ready, Events};
use crate::util::slice_msg;

pub struct Bridge {
    read_buffer: VecDeque<Message>,
    run: bool,
}

struct Session {
    fd: i32,
    conn: Option<Connection>,
    state: SessionState
}

#[derive(Debug, Eq, PartialEq)]
enum SessionState {
    UnAuth,
    Authing,
    Authed
}

impl Bridge {

}
// white list
/*
pub struct Bridge {
    conns: HashMap<i32, Connection>,
    sessions: Vec<Session>,
    read_buffer: VecDeque<Message>,
    run: bool,

    conn_a: Option<(i32, Connection)>,
    conn_b: Option<(i32, Connection)>
}

#[derive(Clone)]
pub struct ConnConfig {
    pub addr: ConnConfigAddr,
    pub auth_msg: Message 
}

#[derive(Clone)]
pub enum ConnConfigAddr {
    Tcp(String),
    Unix(String)
}

struct Session {
    config: ConnConfig,
    conn_id: Option<i32>,
    state: SessionState,
    chans: HashSet<String>
}

#[derive(Debug, Eq, PartialEq)]
enum SessionState {
    UnAuth,
    Authing,
    Authed
}

impl Bridge {
    pub fn link(configs: &[ConnConfig]) -> io::Result<Bridge> {
        let mut bridge = Bridge {
            conns: HashMap::new(),
            conn_a: None,
            conn_b: None,
            sessions: Vec::new(),
            read_buffer: VecDeque::new(),
            run: true
        };

        if configs.len() != 2 {
            panic!("{:?}", "configs must equal 2");
        }

        for conn_config in configs.iter() {
            match conn_config.addr {
                ConnConfigAddr::Tcp(ref addr) => {
                    let socket = TcpStream::connect(addr)?;
                    let fd = socket.as_raw_fd();

                    let conn = Connection::new(Stream::Tcp(socket))?;

                    bridge.conn_a = Some((fd, conn));

                    // bridge.conns.insert(fd, conn);

                    // bridge.sessions.push(Session {
                    //     config: conn_config.clone(),
                    //     conn_id: Some(fd),
                    //     state: SessionState::UnAuth,
                    //     chans: HashSet::new(),
                    // });
                },
                ConnConfigAddr::Unix(ref addr) => {
                    let socket = UnixStream::connect(addr)?;
                    let fd = socket.as_raw_fd();

                    let conn = Connection::new(Stream::Unix(socket))?;

                    bridge.conn_b = Some((fd, conn));

                    // bridge.conns.insert(fd, conn);

                    // bridge.sessions.push(Session {
                    //     config: conn_config.clone(),
                    //     conn_id: Some(fd),
                    //     state: SessionState::UnAuth,
                    //     chans: HashSet::new(),
                    // });
                }
            }
        }

        Ok(bridge)
    }

    pub fn run(&mut self) -> io::Result<()> {
        while self.run {
            self.run_once()?;
        }

        Ok(())
    }

    fn run_once(&mut self) -> io::Result<()> {
        {
            for session in &mut self.sessions {
                if let Some(id) = session.conn_id {
                    match session.state {
                        SessionState::UnAuth => {

                            let mut msg = msg!{
                                "_chan": "_auth",
                                "_brge": true
                            };

                            msg.extend(session.config.auth_msg.clone());

                            println!("{:?}", msg);

                            self.conns.get_mut(&id).unwrap()
                                .write_buffer.push_back(msg.to_vec().unwrap());

                            session.state = SessionState::Authing;

                            println!("{:?}", session.state);
                        },
                        _ => ()
                    }
                } else {
                    // todo
                }
            }
        }

        let mut events = Events::new();

        for (id, conn) in &self.conns {
            let mut interest = Ready::readable() | Ready::hup();

            if conn.write_buffer.len() > 0 {
                interest.insert(Ready::writable());
            }

            events.put(*id, interest);
        }

        if poll(&mut events, None)? > 0 {
            for event in &events {
                let readiness = event.readiness();

                if readiness.is_hup() || readiness.is_error() {
                    self.remove_conn(event.fd())?;

                    continue;
                }

                if readiness.is_readable() {
                    if let Some(conn) = self.conns.get_mut(&event.fd()) {
                        if conn.read(&mut self.read_buffer).is_err() {
                            self.remove_conn(event.fd())?;
                        }

                        if !self.read_buffer.is_empty() {
                            self.handle_message_from_conn(event.fd())?;
                        }
                    }
                }

                if readiness.is_writable() {
                    if let Some(conn) = self.conns.get_mut(&event.fd()) {
                        if conn.write().is_err() {
                            self.remove_conn(event.fd())?;
                        }
                    }
                }
            }
        }
 
        Ok(())
    }

    fn remove_conn(&mut self, id: i32) -> io:: Result<()> {
        for session in &mut self.sessions {
            if session.conn_id == Some(id) {
                session.conn_id = None;
                session.state = SessionState::UnAuth;
            }
        }

        self.conns.remove(&id);

        Ok(())
    }

    fn handle_message_from_conn(&mut self, id: i32) -> io::Result<()> {
        while let Some(message) = self.read_buffer.pop_front() {
            println!("{:?}", message);
            if let Ok(chan) = message.get_str("_chan") {
                if chan.starts_with("_") {
                    match chan {
                        "_auth" => {
                            if let Ok(ok) = message.get_i32("ok") {
                                if ok == 0 {
                                    let mut chans: HashSet<String> = HashSet::new();

                                    for session in &mut self.sessions {
                                        if session.conn_id == None {
                                            continue;
                                        } else if session.conn_id == Some(id) {
                                            session.state = SessionState::Authed;
                                        } else {
                                            chans.extend(session.chans.clone());
                                        }
                                    }

                                    let conn = self.conns.get_mut(&id).unwrap();

                                    for chan in chans {
                                        let msg = msg!{
                                            "_chan": "_atta",
                                            "_valu": chan
                                        };

                                        conn.write_buffer.push_back(msg.to_vec().unwrap());
                                    }
                                } else {
                                    // error
                                    panic!("{:?}", "Auth failed!");
                                }
                            }
                        },
                        "_brge_atta" => {
                            if let Ok(value) = message.get_str("_valu") {
                                let mut already_atta = false;

                                for session in &mut self.sessions {
                                    if session.conn_id == Some(id) {
                                        session.chans.insert(value.to_string());
                                    } else {
                                        if session.chans.contains(value) {
                                            already_atta = true;
                                        }
                                    }
                                }

                                if !already_atta {
                                    for (key, conn) in &mut self.conns {
                                        if *key == id {
                                            continue;
                                        }

                                        let msg = msg!{
                                            "_chan": "_atta",
                                            "_valu": value
                                        };

                                        conn.write_buffer.push_back(msg.to_vec().unwrap());
                                    }
                                }
                            }
                        },
                        "_brge_deta" => {
                            if let Ok(value) = message.get_str("_valu") {
                                let mut should_deta = true;

                                for session in &mut self.sessions {
                                    if session.conn_id == Some(id) {
                                        session.chans.insert(value.to_string());
                                    } else {
                                        if session.chans.contains(value) {
                                            should_deta = false;
                                        }
                                    }
                                }

                                if should_deta {
                                    for (key, conn) in &mut self.conns {
                                        if *key == id {
                                            continue;
                                        }

                                        let msg = msg!{
                                            "_chan": "_deta",
                                            "_valu": value
                                        };

                                        conn.write_buffer.push_back(msg.to_vec().unwrap());
                                    }
                                }
                            }
                        },
                        "_atta" => (println!("{:?}", "_atta")),
                        "_deta" => (println!("{:?}", "_deta")),
                        _ => ()
                    }
                } else {
                    for session in &mut self.sessions {
                        if session.conn_id == Some(id) {
                            continue;
                        }

                        if session.state != SessionState::Authed {
                            continue;
                        }

                        if !session.chans.contains(chan) {
                            continue;
                        }

                        if let Some(conn) = self.conns.get_mut(&id) {
                            conn.write_buffer.push_back(message.to_vec().unwrap());
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
*/
#[derive(Debug)]
enum Stream {
    Tcp(TcpStream),
    Unix(UnixStream)
}

struct Connection {
    stream: Stream,
    // interest: Ready,
    read_buffer: Vec<u8>,
    write_buffer: VecDeque<Vec<u8>>,
}

impl Connection {
    fn new(stream: Stream) -> io::Result<Connection> {
        let conn = Connection {
            stream,
            // interest: Ready::readable() | Ready::hup(),
            read_buffer: Vec::new(),
            write_buffer: VecDeque::new(),
        };

        Ok(conn)
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
                                    return Err(io::Error::new(InvalidData, "InvalidData"))
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

        Ok(())
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
