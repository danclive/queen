use std::time::Duration;
use std::collections::{HashMap, HashSet};
use std::io::{self, ErrorKind::{ConnectionAborted}};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use queen_io::epoll::{Epoll, Events, Token, Ready, EpollOpt};
use queen_io::queue::mpsc;

use nson::{Message, Value, Array, msg};
use nson::message_id::MessageId;

use slab::Slab;

use rand::{self, thread_rng, rngs::ThreadRng};
use rand::seq::SliceRandom;

use crate::stream::Stream;
use crate::util::oneshot;
use crate::dict::*;
use crate::error::ErrorCode;

pub use callback::Callback;

mod callback;

#[derive(Clone)]
pub struct Queen {
    queue: mpsc::Queue<Packet>,
    run: Arc<AtomicBool>
}

#[derive(Debug)]
struct A;

impl Queen {
    pub fn new<T: Send + 'static>(
        id: MessageId,
        data: T,
        callback: Option<Callback<T>>
    ) -> io::Result<Queen> {
        let queue = mpsc::Queue::new()?;
        let run = Arc::new(AtomicBool::new(true));

        let queen = Queen {
            queue: queue.clone(),
            run: run.clone()
        };

        thread::Builder::new().name("relay".to_string()).spawn(move || {
            let mut inner = QueenInner::new(id, queue, data, callback.unwrap_or_default(), run)?;

            inner.run()
        }).unwrap();

        Ok(queen)
    }

    pub fn stop(&mut self) {
        self.run.store(false, Ordering::Relaxed);
    }

    pub fn is_run(&self) -> bool {
        self.run.load(Ordering::Relaxed)
    }

    pub fn connect(&self, attr: Message, timeout: Option<Duration>) -> io::Result<Stream> {
        let (stream1, stream2) = Stream::pipe(64, attr)?;

        let (tx, rx) = oneshot::oneshot::<bool>();

        let packet = Packet::NewConn(stream1, tx);

        self.queue.push(packet);

        if let Some(timeout) = timeout {
            let ret = rx.wait_timeout(timeout)?;

            if !ret.unwrap_or_default() {
                return Err(io::Error::new(ConnectionAborted, "ConnectionAborted"))
            }
        } else {
            let ret = rx.wait();

            if !ret.unwrap_or_default() {
                return Err(io::Error::new(ConnectionAborted, "ConnectionAborted"))
            }
        }

        Ok(stream2)
    }
}

impl Drop for Queen {
    fn drop(&mut self) {
        if Arc::strong_count(&self.run) == 1 {
            self.run.store(false, Ordering::Relaxed);
        }
    }
}

struct QueenInner<T> {
    id: MessageId,
    epoll: Epoll,
    events: Events,
    queue: mpsc::Queue<Packet>,
    sessions: Sessions,
    rand: ThreadRng,
    data: T,
    callback: Callback<T>,
    run: Arc<AtomicBool>
}

enum Packet {
    NewConn(Stream, oneshot::Sender<bool>)
}

impl<T> QueenInner<T> {
    const QUEUE_TOKEN: Token = Token(usize::max_value());

    fn new(
        id: MessageId,
        queue: mpsc::Queue<Packet>,
        data: T,
        callback: Callback<T>,
        run: Arc<AtomicBool>
    ) -> io::Result<QueenInner<T>> {
        Ok(QueenInner {
            id,
            epoll: Epoll::new()?,
            events: Events::with_capacity(1024),
            queue,
            sessions: Sessions::new(),
            rand: thread_rng(),
            callback,
            data,
            run
        })
    }

    fn run(&mut self) -> io::Result<()> {
        self.epoll.add(&self.queue, Self::QUEUE_TOKEN, Ready::readable(), EpollOpt::level())?;

        while self.run.load(Ordering::Relaxed) {
            let size = self.epoll.wait(&mut self.events, None)?;

            for i in 0..size {
                let event = self.events.get(i).unwrap();

                if event.token() == Self::QUEUE_TOKEN {
                    self.dispatch_queue()?;
                } else {
                    self.dispatch_conn(event.token().0)?;
                }
            }
        }

        Ok(())
    }

    fn dispatch_queue(&mut self) -> io::Result<()> {
        if let Some(packet) = self.queue.pop() {
            match packet {
                Packet::NewConn(stream, sender) => {
                    let entry = self.sessions.conns.vacant_entry();

                    let session = Session::new(entry.key(), stream);

                    let success = if let Some(accept_fn) = &self.callback.accept_fn {
                       accept_fn(&session, &self.data)
                    } else {
                       true
                    };

                    if success {
                        self.epoll.add(&session.stream, Token(entry.key()), Ready::readable(), EpollOpt::level())?;
                        
                        entry.insert(session);

                        sender.send(true);
                    } else {
                        // stream.close();
                        sender.send(false);
                    }
                }
            }
        }

        Ok(())
    }

    fn dispatch_conn(&mut self, token: usize) -> io::Result<()> {
        if let Some(conn) = self.sessions.conns.get(token) {
            if let Some(message) = conn.stream.recv() {
                if message.is_empty() && conn.stream.is_close() {
                    self.remove_conn(token)?;
                } else {
                    self.handle_message(token, message)?;
                }
            }
        }

        Ok(())
    }

    fn remove_conn(&mut self, token: usize) -> io::Result<()> {
        if self.sessions.conns.contains(token) {
            let conn = self.sessions.conns.remove(token);
            // conn.stream.close();
            self.epoll.delete(&conn.stream)?;

            for chan in conn.chans.keys() {
                if let Some(ids) = self.sessions.chans.get_mut(chan) {
                    ids.remove(&token);

                    if ids.is_empty() {
                        self.sessions.chans.remove(chan);
                    }
                }
            }

            if let Some(remove_fn) = &self.callback.remove_fn {
                remove_fn(&conn, &self.data);
            }

            // port event
            // {
            //     CHAN: PORT_BREAK,
            //     PORT_ID: $port_id
            // }
            let mut event_msg = msg!{
                CHAN: PORT_BREAK
            };

            if let Some(port_id) = conn.id {
                self.sessions.ports.remove(&port_id);
                event_msg.insert(PORT_ID, port_id);
            }

            self.relay_super_message(token, PORT_BREAK, event_msg);
        }

        Ok(())
    }

    fn handle_message(&mut self, token: usize, mut message: Message) -> io::Result<()> {
        let success = if let Some(recv_fn) = &self.callback.recv_fn {
            recv_fn(&self.sessions.conns[token], &mut message, &self.data)
        } else {
            true
        };

        if !success {
            ErrorCode::RefuseReceiveMessage.insert_message(&mut message);
            
            self.sessions.conns[token].stream.send(message);
        
            return Ok(())
        }

        let chan = match message.get_str(CHAN) {
            Ok(chan) => chan,
            Err(_) => {
                ErrorCode::CannotGetChanField.insert_message(&mut message);

                self.sessions.conns[token].stream.send(message);

                return Ok(())
            }
        };

        if chan.starts_with('_') {
            match chan {
                AUTH => self.auth(token, message),
                ATTACH => self.attach(token, message),
                DETACH => self.detach(token, message),
                PING => self.ping(token, message),
                QUERY => self.query(token, message),
                CUSTOM => self.custom(token, message),
                PORT_KILL => self.kill(token, message)?,
                _ => {
                    ErrorCode::UnsupportedChan.insert_message(&mut message);

                    self.sessions.conns[token].stream.send(message);
                }
            }
        } else {
            self.relay_message(token, chan.to_string(), message);
        }

        Ok(())
    }

    fn auth(&mut self, token: usize, mut message: Message) {
        let success = if let Some(auth_fn) = &self.callback.auth_fn {
            auth_fn(&self.sessions.conns[token], &mut message, &self.data)
        } else {
            true
        };

        if !success {
            ErrorCode::AuthenticationFailed.insert_message(&mut message);

            self.sessions.conns[token].stream.send(message);

            return
        }

        let mut conn = &mut self.sessions.conns[token];

        if let Some(s) = message.get(SUPER) {
            if let Some(s) = s.as_bool() {
                conn.supe = s;
            } else {
                ErrorCode::InvalidSuperFieldType.insert_message(&mut message);

                self.sessions.conns[token].stream.send(message);

                return
            }
        }

        if let Some(port_id) = message.get(PORT_ID) {
            if let Some(port_id) = port_id.as_message_id() {
                if let Some(other_token) = self.sessions.ports.get(port_id) {
                        if *other_token != token {
                            ErrorCode::DuplicatePortId.insert_message(&mut message);

                            self.sessions.conns[token].stream.send(message);

                            return
                        }
                    }

                    if let Some(port_id) = &conn.id {
                        self.sessions.ports.remove(port_id);
                    }

                    self.sessions.ports.insert(port_id.clone(), token);

                    conn.id = Some(port_id.clone());
            } else {
                ErrorCode::InvalidPortIdFieldType.insert_message(&mut message);

                self.sessions.conns[token].stream.send(message);

                return
            }
        } else if let Some(port_id) = &conn.id {
            message.insert(PORT_ID, port_id.clone());
        } else {
            let port_id = MessageId::new();

            self.sessions.ports.insert(port_id.clone(), token);

            conn.id = Some(port_id.clone());

            message.insert(PORT_ID, port_id);
        }

        conn.auth = true;

        message.insert(NODE_ID, self.id.clone());

        ErrorCode::OK.insert_message(&mut message);
        
        // port event
        // {
        //     CHAN: PORT_READY,
        //     SUPER: $conn.supe,
        //     PORT_ID: $port_id
        // }
        let mut event_msg = msg!{
            CHAN: PORT_READY,
            SUPER: conn.supe
        };

        if let Some(port_id) = &conn.id {
            event_msg.insert(PORT_ID, port_id.clone());
        }

        self.sessions.conns[token].stream.send(message);

        self.relay_super_message(token, PORT_READY, event_msg);
    }

    fn attach(&mut self, token: usize, mut message: Message) {
        // check auth
        if !self.sessions.conns[token].auth {
            ErrorCode::Unauthorized.insert_message(&mut message);

            self.sessions.conns[token].stream.send(message);

            return
        }

        if let Ok(chan) = message.get_str(VALUE).map(ToOwned::to_owned) {
            // check super
            match chan.as_str() {
                PORT_READY | PORT_BREAK | PORT_ATTACH | PORT_DETACH | PORT_SEND | PORT_RECV => {

                    if !self.sessions.conns[token].supe {
                        ErrorCode::Unauthorized.insert_message(&mut message);

                        self.sessions.conns[token].stream.send(message);

                        return
                    }

                }
                _ => ()
            }

            // can attach
            let success = if let Some(attach_fn) = &self.callback.attach_fn {
                attach_fn(&self.sessions.conns[token], &mut message, &self.data)
            } else {
                true
            };

            if !success {
                ErrorCode::Unauthorized.insert_message(&mut message);

                self.sessions.conns[token].stream.send(message);

                return
            }

            // label
            let mut labels = HashSet::new();

            if let Some(label) = message.get(LABEL) {
                if let Some(label) = label.as_str() {
                    labels.insert(label.to_string());
                } else if let Some(label_array) = label.as_array() {
                    for v in label_array {
                        if let Some(v) = v.as_str() {
                            labels.insert(v.to_string());
                        } else {
                            ErrorCode::InvalidLabelFieldType.insert_message(&mut message);

                            self.sessions.conns[token].stream.send(message);

                            return
                        }
                    }
                } else {
                    ErrorCode::InvalidLabelFieldType.insert_message(&mut message);

                    self.sessions.conns[token].stream.send(message);

                    return
                }
            }

            // port event
            // {
            //     CHAN: PORT_ATTACH,
            //     VALUE: $chan,
            //     LABEL: $label, // string or array
            //     PORT_ID: $port_id
            // }
            let mut event_msg = msg!{
                CHAN: PORT_ATTACH
            };

            event_msg.insert(VALUE, &chan);

            if let Some(label) = message.get(LABEL) {
                event_msg.insert(LABEL, label.clone());
            }

            // session_attach
            let ids = self.sessions.chans.entry(chan.to_owned()).or_insert_with(HashSet::new);
            ids.insert(token);

            {

                let conn = self.sessions.conns.get_mut(token).unwrap();
                let set = conn.chans.entry(chan).or_insert_with(HashSet::new);
                set.extend(labels);

                if let Some(port_id) = &conn.id {
                    event_msg.insert(PORT_ID, port_id.clone());
                }
            }

            self.relay_super_message(token, PORT_ATTACH, event_msg);

            ErrorCode::OK.insert_message(&mut message);
        } else {
            ErrorCode::CannotGetValueField.insert_message(&mut message);
        }

        self.sessions.conns[token].stream.send(message);
    }

    fn detach(&mut self, token: usize, mut message: Message) {
        // check auth
        if !self.sessions.conns[token].auth {
            ErrorCode::Unauthorized.insert_message(&mut message);

            self.sessions.conns[token].stream.send(message);

            return
        }

        if let Ok(chan) = message.get_str(VALUE).map(ToOwned::to_owned) {
            if let Some(detach_fn) = &self.callback.detach_fn {
                detach_fn(&self.sessions.conns[token], &mut message, &self.data);
            }

            // label
            let mut labels = HashSet::new();

            if let Some(label) = message.get(LABEL) {
                if let Some(label) = label.as_str() {
                    labels.insert(label.to_string());
                } else if let Some(label_array) = label.as_array() {
                    for v in label_array {
                        if let Some(v) = v.as_str() {
                            labels.insert(v.to_string());
                        } else {
                            ErrorCode::InvalidLabelFieldType.insert_message(&mut message);

                            self.sessions.conns[token].stream.send(message);

                            return
                        }
                    }
                } else {
                    ErrorCode::InvalidLabelFieldType.insert_message(&mut message);

                    self.sessions.conns[token].stream.send(message);

                    return
                }
            }

            // port event
            // {
            //     CHAN: PORT_DETACH,
            //     VALUE: $chan,
            //     LABEL: $label, // string or array
            //     PORT_ID: $port_id
            // }
            let mut event_msg = msg!{
                CHAN: PORT_DETACH,
                VALUE: &chan
            };

            if let Some(label) = message.get(LABEL) {
                event_msg.insert(LABEL, label.clone());
            }

            // session_detach
            {
                let conn = self.sessions.conns.get_mut(token).unwrap();

                if labels.is_empty() {
                    conn.chans.remove(&chan);

                    if let Some(ids) = self.sessions.chans.get_mut(&chan) {
                        ids.remove(&token);

                        if ids.is_empty() {
                            self.sessions.chans.remove(&chan);
                        }
                    }
                } else if let Some(set) = conn.chans.get_mut(&chan) {
                    *set = set.iter().filter(|label| !labels.contains(*label)).map(|s| s.to_string()).collect();
                }

                if let Some(port_id) = &conn.id {
                    event_msg.insert(PORT_ID, port_id.clone());
                }
            }

            self.relay_super_message(token, PORT_DETACH, event_msg);
        
            ErrorCode::OK.insert_message(&mut message);
        } else {
            ErrorCode::CannotGetValueField.insert_message(&mut message);
        }

        self.sessions.conns[token].stream.send(message);
    }

    fn ping(&self, token: usize, mut message: Message) {
        ErrorCode::OK.insert_message(&mut message);

        self.sessions.conns[token].stream.send(message);
    }

    fn query(&self, token: usize, mut message: Message) {
        {
            let conn = &self.sessions.conns[token];

            if !conn.auth || !conn.supe {
                ErrorCode::Unauthorized.insert_message(&mut message);

                conn.stream.send(message);

                return
            }
        }

        for (key, value) in message.clone() {
            if value == Value::String(QUERY_PORT_NUM.to_string()) {
                message.insert(key, self.sessions.ports.len() as u32);
            } else if value == Value::String(QUERY_CHAN_NUM.to_string()) {
                message.insert(key, self.sessions.chans.len() as u32);
            } else if value == Value::String(QUERY_PORTS.to_string()) {
                let mut array = Array::new();

                for (_, conn) in self.sessions.conns.iter() {
                    let mut chans = Message::new();

                    for (chan, labels) in &conn.chans {
                        let labels: Vec<&String> = labels.iter().collect();

                        chans.insert(chan, labels);
                    }

                    let port_id: Value = if let Some(id) = &conn.id {
                        id.clone().into()
                    } else {
                        Value::Null
                    };

                    array.push(msg!{
                        AUTH: conn.auth,
                        SUPER: conn.supe,
                        CHANS: chans,
                        PORT_ID: port_id,
                        ATTR: conn.stream.attr.clone()
                    });
                }

                message.insert(key, array);
            } else if value == Value::String(QUERY_PORT.to_string()) {
                if let Ok(port_id) = message.get_message_id(PORT_ID) {
                    if let Some(id) = self.sessions.ports.get(port_id) {
                        if let Some(conn) = self.sessions.conns.get(*id) {
                            let mut chans = Message::new();

                            for (chan, labels) in &conn.chans {
                                let labels: Vec<&String> = labels.iter().collect();

                                chans.insert(chan, labels);
                            }

                            let port_id: Value = if let Some(id) = &conn.id {
                                id.clone().into()
                            } else {
                                Value::Null
                            };

                            let port = msg!{
                                AUTH: conn.auth,
                                SUPER: conn.supe,
                                CHANS: chans,
                                PORT_ID: port_id,
                                ATTR: conn.stream.attr.clone()
                            };

                            message.insert(key, port);
                            message.remove(PORT_ID);
                        } else {
                            unreachable!()
                        }
                    } else {
                        ErrorCode::NotFound.insert_message(&mut message);

                        self.sessions.conns[token].stream.send(message);

                        return
                    }
                } else {
                    ErrorCode::InvalidPortIdFieldType.insert_message(&mut message);

                    self.sessions.conns[token].stream.send(message);

                    return
                }
            }
        }

        ErrorCode::OK.insert_message(&mut message);

        self.sessions.conns[token].stream.send(message);
    }

    fn custom(&self, token: usize, mut message: Message) {
        {
            let conn = &self.sessions.conns[token];

            if !conn.auth {
                ErrorCode::Unauthorized.insert_message(&mut message);

                conn.stream.send(message);

                return
            }
        }

        if let Some(custom_fn) = &self.callback.custom_fn {
            custom_fn(&self.sessions, token, &mut message, &self.data);
        }
    }

    fn kill(&mut self, token: usize, mut message: Message) -> io::Result<()> {
        {
            let conn = &self.sessions.conns[token];

            if !conn.auth || !conn.supe {
                ErrorCode::Unauthorized.insert_message(&mut message);

                conn.stream.send(message);

                return Ok(())
            }
        }

        let success = if let Some(kill_fn) = &self.callback.kill_fn {
            kill_fn(&self.sessions.conns[token], &mut message, &self.data)
        } else {
            true
        };

        if !success {
            ErrorCode::Unauthorized.insert_message(&mut message);

            self.sessions.conns[token].stream.send(message);

            return Ok(())
        }

        let mut remove_id = None;

        if let Some(port_id) = message.get(PORT_ID) {
            if let Some(port_id) = port_id.as_message_id() {
                if let Some(other_id) = self.sessions.ports.get(port_id).cloned() {
                    remove_id = Some(other_id);
                }
            } else {
                ErrorCode::InvalidPortIdFieldType.insert_message(&mut message);

                self.sessions.conns[token].stream.send(message);

                return Ok(())
            }
        }

        ErrorCode::OK.insert_message(&mut message);

        self.sessions.conns[token].stream.send(message);

        if let Some(remove_id) = remove_id {
            self.remove_conn(remove_id)?;
        }

        Ok(())
    }

    fn relay_message(&mut self, token: usize, chan: String, mut message: Message) {
        // check auth
        if !self.sessions.conns[token].auth {
            ErrorCode::Unauthorized.insert_message(&mut message);

            self.sessions.conns[token].stream.send(message);

            return
        }

        let success = if let Some(emit_fn) = &self.callback.emit_fn {
            emit_fn(&self.sessions.conns[token], &mut message, &self.data)
        } else {
            true
        };

        if !success {
            ErrorCode::Unauthorized.insert_message(&mut message);

            self.sessions.conns[token].stream.send(message);

            return
        }

        // build reply message
        let reply_msg = if let Some(ack) = message.get(ACK) {
            let mut reply_msg = msg!{
                CHAN: &chan,
                ACK: ack.clone()
            };

            if let Ok(message_id) = message.get_message_id(ID) {
                reply_msg.insert(ID, message_id);
            }

            ErrorCode::OK.insert_message(&mut reply_msg);

            message.remove(ACK);

            Some(reply_msg)
        } else {
            None
        };

        // to
        let mut to_ids = vec![];

        if let Some(to) = message.get(TO) {
            if let Some(to_id) = to.as_message_id() {
                if !self.sessions.ports.contains_key(to_id) {
                    ErrorCode::TargetPortIdNotExist.insert_message(&mut message);

                    self.sessions.conns[token].stream.send(message);

                    return
                }

                to_ids.push(to_id.clone());
            } else if let Some(to_array) = to.as_array() {
                for to in to_array {
                    if let Some(to_id) = to.as_message_id() {
                        if !self.sessions.ports.contains_key(to_id) {
                            ErrorCode::TargetPortIdNotExist.insert_message(&mut message);

                            self.sessions.conns[token].stream.send(message);

                            return
                        }

                        to_ids.push(to_id.clone());
                    } else {
                        ErrorCode::InvalidToFieldType.insert_message(&mut message);

                        self.sessions.conns[token].stream.send(message);

                        return
                    }
                }
            } else {
                ErrorCode::InvalidToFieldType.insert_message(&mut message);

                self.sessions.conns[token].stream.send(message);

                return
            }
        }

        if let Some(port_id) = &self.sessions.conns[token].id {
            message.insert(FROM, port_id.clone());
        }

        // labels
        let mut labels = HashSet::new();

        if let Some(label) = message.get(LABEL) {
            if let Some(label) = label.as_str() {
                labels.insert(label.to_string());
            } else if let Some(label_array) = label.as_array() {
                for v in label_array {
                    if let Some(v) = v.as_str() {
                        labels.insert(v.to_string());
                    } else {
                        ErrorCode::InvalidLabelFieldType.insert_message(&mut message);

                        self.sessions.conns[token].stream.send(message);

                        return
                    }
                }
            } else {
                ErrorCode::InvalidLabelFieldType.insert_message(&mut message);

                self.sessions.conns[token].stream.send(message);

                return
            }
        }

        macro_rules! send {
            ($self: ident, $conn: ident, $message: ident) => {
                let success = if let Some(send_fn) = &$self.callback.send_fn {
                    send_fn(&$conn, &mut $message, &$self.data)
                } else {
                    true
                };

                if success {
                    $conn.stream.send($message.clone());

                    // port event
                    // {
                    //     CHAN: PORT_RECV,
                    //     VALUE: $message
                    // }
                    let mut event_msg = msg!{
                        CHAN: PORT_RECV,
                        VALUE: $message.clone()
                    };

                    if let Some(port_id) = &$conn.id {
                        event_msg.insert(TO, port_id);
                    }

                    let id = $conn.token;

                    $self.relay_super_message(id, PORT_RECV, event_msg);
                }
            };
        }

        let mut no_consumers = true;

        if !to_ids.is_empty() {
            no_consumers = false;

            for to in &to_ids {
                if let Some(conn_id) = self.sessions.ports.get(to) {
                    if let Some(conn) = self.sessions.conns.get(*conn_id) {
                        send!(self, conn, message);
                    }
                }
            }
        } else if message.get_bool(SHARE).ok().unwrap_or(false) {
            let mut array: Vec<usize> = Vec::new();

            if let Some(ids) = self.sessions.chans.get(&chan) {
                for conn_id in ids {
                    if let Some(conn) = self.sessions.conns.get(*conn_id) {
                        // filter labels
                        if !labels.is_empty() {
                            let conn_labels = conn.chans.get(&chan).expect("It shouldn't be executed here!");

                            // if !conn_labels.iter().any(|l| labels.contains(l)) {
                            //     continue
                            // }
                            if (conn_labels & &labels).is_empty() {
                                continue;
                            }
                        }

                        array.push(*conn_id);
                    }
                }
            }

            if !array.is_empty() {
                no_consumers = false;

                if array.len() == 1 {
                    if let Some(conn) = self.sessions.conns.get(array[0]) {
                        send!(self, conn, message);
                    }
                } else if let Some(id) = array.choose(&mut self.rand) {
                    if let Some(conn) = self.sessions.conns.get(*id) {
                        send!(self, conn, message);
                    }
                }
            }

        } else if let Some(ids) = self.sessions.chans.get(&chan) {
            for conn_id in ids {
                if let Some(conn) = self.sessions.conns.get(*conn_id) {
                    // filter labels
                    if !labels.is_empty() {
                        let conn_labels = conn.chans.get(&chan).expect("It shouldn't be executed here!");

                        if !conn_labels.iter().any(|l| labels.contains(l)) {
                            continue
                        }
                    }

                    no_consumers = false;

                    send!(self, conn, message);
                }
            }
        }

        if no_consumers {
            ErrorCode::NoConsumers.insert_message(&mut message);

            self.sessions.conns[token].stream.send(message);

            return
        }

        // port event
        // {
        //     CHAN: PORT_SEND,
        //     VALUE: $message
        // }
        let event_msg = msg!{
            CHAN: PORT_SEND,
            VALUE: message
        };

        self.relay_super_message(token, PORT_SEND, event_msg);

        // send reply message
        if let Some(reply_msg) = reply_msg {
            self.sessions.conns[token].stream.send(reply_msg);
        }
    }

    fn relay_super_message(&self, token: usize, chan: &str, message: Message) {
        if let Some(tokens) = self.sessions.chans.get(chan) {
            for other_token in tokens {
                if token == *other_token {
                    continue;
                }

                if let Some(conn) = self.sessions.conns.get(*other_token) {
                    let mut message = message.clone();

                    let success = if let Some(send_fn) = &self.callback.send_fn {
                        send_fn(&conn, &mut message, &self.data)
                    } else {
                        true
                    };

                    if success {
                        conn.stream.send(message);
                    }
                }
            }
        }
    }
}

#[derive(Default)]
pub struct Sessions {
    pub conns: Slab<Session>,
    pub chans: HashMap<String, HashSet<usize>>,
    pub ports: HashMap<MessageId, usize>,
}

impl Sessions {
    pub fn new() -> Sessions {
        Sessions {
            conns: Slab::new(),
            chans: HashMap::new(),
            ports: HashMap::new()
        }
    }
}

pub struct Session {
    pub token: usize,
    pub auth: bool,
    pub supe: bool,
    pub chans: HashMap<String, HashSet<String>>,
    pub stream: Stream,
    pub id: Option<MessageId>
}

impl Session {
    pub fn new(token: usize, stream: Stream) -> Session {
        Session {
            token,
            auth: false,
            supe: false,
            chans: HashMap::new(),
            stream,
            id: None
        }
    }
}
