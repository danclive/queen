use std::time::Duration;
use std::collections::{HashMap, HashSet};
use std::thread;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};
use std::cell::Cell;
use std::io::ErrorKind::Interrupted;

use queen_io::{
    epoll::{Epoll, Events, Token, Ready, EpollOpt},
    queue::mpsc::Queue
};

use nson::{
    Message, Value, Array, msg,
    message_id::MessageId
};

use slab::Slab;

use rand::{SeedableRng, seq::SliceRandom, rngs::SmallRng};

use crate::stream::Stream;
use crate::dict::*;
use crate::error::{ErrorCode, Result, Error};

pub use callback::Callback;

mod callback;

#[derive(Clone)]
pub struct Queen {
    queue: Queue<Packet>,
    run: Arc<AtomicBool>
}

impl Queen {
    pub fn new<T: Send + 'static>(
        id: MessageId,
        data: T,
        callback: Option<Callback<T>>
    ) -> Result<Queen> {
        let queue = Queue::new()?;
        let run = Arc::new(AtomicBool::new(true));

        let queen = Queen {
            queue: queue.clone(),
            run: run.clone()
        };

        let mut inner = QueenInner::new(
            id,
            queue,
            data,
            callback.unwrap_or_default(),
            run
        )?;

        thread::Builder::new().name("queen".to_string()).spawn(move || {
            let ret = inner.run();
            if ret.is_err() {
                log::error!("relay thread exit: {:?}", ret);
            } else {
                log::trace!("relay thread exit");
            }

            inner.run.store(false, Ordering::Relaxed);
        }).unwrap();

        Ok(queen)
    }

    pub fn stop(&self) {
        self.run.store(false, Ordering::Relaxed);
    }

    pub fn is_run(&self) -> bool {
        self.run.load(Ordering::Relaxed)
    }

    pub fn connect(
        &self,
        attr: Message,
        capacity: Option<usize>,
        timeout: Option<Duration>
    ) -> Result<Stream<Message>> {
        let (stream1, stream2) = Stream::pipe(capacity.unwrap_or(64), attr)?;

        let packet = Packet::NewConn(stream1);

        self.queue.push(packet);

        let ret = stream2.wait(Some(timeout.unwrap_or(Duration::from_secs(60))));
        log::debug!("Queen::connect: {:?}", ret);

        if ret.is_err() {
            return Err(Error::ConnectionRefused("Queen::connect".to_string()))
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
    queue: Queue<Packet>,
    sessions: Sessions,
    rand: SmallRng,
    data: T,
    callback: Callback<T>,
    run: Arc<AtomicBool>
}

enum Packet {
    NewConn(Stream<Message>)
}

impl<T> QueenInner<T> {
    const QUEUE_TOKEN: Token = Token(usize::max_value());

    fn new(
        id: MessageId,
        queue: Queue<Packet>,
        data: T,
        callback: Callback<T>,
        run: Arc<AtomicBool>
    ) -> Result<QueenInner<T>> {
        Ok(QueenInner {
            id,
            epoll: Epoll::new()?,
            events: Events::with_capacity(1024),
            queue,
            sessions: Sessions::new(),
            rand: SmallRng::from_entropy(),
            callback,
            data,
            run
        })
    }

    fn run(&mut self) -> Result<()> {
        self.epoll.add(&self.queue, Self::QUEUE_TOKEN, Ready::readable(), EpollOpt::level())?;

        while self.run.load(Ordering::Relaxed) {
            let size = match self.epoll.wait(&mut self.events, Some(Duration::from_secs(10))) {
                Ok(size) => size,
                Err(err) => {
                    if err.kind() == Interrupted {
                        continue;
                    } else {
                        return Err(err.into())
                    }
                }
            };

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

    fn dispatch_queue(&mut self) -> Result<()> {
        if let Some(packet) = self.queue.pop() {
            match packet {
                Packet::NewConn(stream) => {
                    let entry = self.sessions.conns.vacant_entry();

                    let session = Session::new(entry.key(), stream);

                    let success = if let Some(accept_fn) = &self.callback.accept_fn {
                       accept_fn(&session, &self.data)
                    } else {
                       true
                    };

                    if success && matches!(session.stream.send(&mut Some(msg!{OK: 0i32})), Ok(_)) {
                        self.epoll.add(&session.stream, Token(entry.key()), Ready::readable(), EpollOpt::level())?;
                        entry.insert(session);
                    } else {
                        session.stream.close();
                    }
                }
            }
        }

        Ok(())
    }

    fn dispatch_conn(&mut self, token: usize) -> Result<()> {
        if let Some(conn) = self.sessions.conns.get(token) {
            match conn.recv() {
                Ok(message) => {
                    self.handle_message(token, message)?;
                }
                Err(err) => {
                    if !matches!(err, Error::Empty(_)) {
                        self.remove_conn(token)?;
                    }
                } 
            }
        }

        Ok(())
    }

    fn remove_conn(&mut self, token: usize) -> Result<()> {
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

            // 这里要记得移除 clinet_id，因为 stream 在一开始建立连接时就会默认分配一个
            // 认证成功时可以修改
            self.sessions.client_ids.remove(&conn.id);

            if let Some(remove_fn) = &self.callback.remove_fn {
                remove_fn(&conn, &self.data);
            }

            // 这里发一个事件，表示有 CLIENT 断开
            // 注意，只有在 CLIENT_READY 和 CLIENT_BREAK 这两个事件才会返回
            // CLIENT 的 LABEL 和 ATTR
            // client event
            // {
            //     CHAN: CLIENT_BREAK,
            //     CLIENT_ID: $client_id,
            //     LABEL: $label,
            //     ATTR: $attr
            // }
            let event_message = msg!{
                CHAN: CLIENT_BREAK,
                CLIENT_ID: conn.id,
                LABEL: conn.label.clone(),
                ATTR: conn.stream.attr().clone()
            };

            self.relay_super_message(token, CLIENT_BREAK, event_message);
        }

        Ok(())
    }

    fn handle_message(&mut self, token: usize, mut message: Message) -> Result<()> {
        let success = if let Some(recv_fn) = &self.callback.recv_fn {
            recv_fn(&self.sessions.conns[token], &mut message, &self.data)
        } else {
            true
        };

        if !success {
            ErrorCode::RefuseReceiveMessage.insert(&mut message);
            
            self.send_message(&self.sessions.conns[token], message);
        
            return Ok(())
        }

        let chan = match message.get_str(CHAN) {
            Ok(chan) => chan,
            Err(_) => {
                ErrorCode::CannotGetChanField.insert(&mut message);

                self.send_message(&self.sessions.conns[token], message);

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
                MINE => self.mine(token, message),
                CUSTOM => self.custom(token, message),
                CLIENT_KILL => self.kill(token, message)?,
                _ => {
                    ErrorCode::UnsupportedChan.insert(&mut message);

                    self.send_message(&self.sessions.conns[token], message);
                }
            }
        } else {
            self.relay_message(token, chan.to_string(), message);
        }

        Ok(())
    }

    fn send_message(&self, conn: &Session, mut message: Message) {
        let success = if let Some(send_fn) = &self.callback.send_fn {
            send_fn(&conn, &mut message, &self.data)
        } else {
            true
        };

        if success && !conn.stream.tx.is_full() {
            let _ = conn.send(&mut Some(message));
        }
    }

    fn auth(&mut self, token: usize, mut message: Message) {
        let success = if let Some(auth_fn) = &self.callback.auth_fn {
            // 这里应该验证自定义字段的合法性
            // 这里应该验证超级用户的合法性
            // 这里应该验证 CLIENT_ID 的合法性
            auth_fn(&self.sessions.conns[token], &mut message, &self.data)
        } else {
            true
        };

        if !success {
            ErrorCode::AuthenticationFailed.insert(&mut message);

            self.send_message(&self.sessions.conns[token], message);

            return
        }

        let mut conn = &mut self.sessions.conns[token];

        // 这两个临时变量是为了防止在认证失败时修改这两个属性值
        let mut tmp_supe = false;
        let mut tmp_label = None;

        // SUPER
        if let Some(s) = message.get(SUPER) {
            // SUPER 字段的值只能为 bool
            if let Some(s) = s.as_bool() {
                tmp_supe = s;
            } else {
                ErrorCode::InvalidSuperFieldType.insert(&mut message);

                self.send_message(&self.sessions.conns[token], message);

                return
            }
        }

        // LABEL
        if let Some(label) = message.get(LABEL) {
            // LABEL 字段的值只能为 Message
            if let Some(label) = label.as_message() {
                tmp_label = Some(label.clone());
            } else {
                ErrorCode::InvalidLabelFieldType.insert(&mut message);

                self.send_message(&self.sessions.conns[token], message);

                return
            }
        }

        // CLIENT_ID
        if let Some(client_id) = message.get(CLIENT_ID) {
            // CLIENT_ID 的类型必须为 MessageId
            if let Some(client_id) = client_id.as_message_id() {
                if let Some(other_token) = self.sessions.client_ids.get(client_id) {
                    // CLIENT ID 不能重复，且不能挤掉已存在的客户端
                    if *other_token != token {
                        ErrorCode::DuplicateClientId.insert(&mut message);

                        self.send_message(&self.sessions.conns[token], message);

                        return
                    }
                }

                // 认证时, 可以改变 CLIENT_ID
                self.sessions.client_ids.remove(client_id);
                // 认证成功时，设置
                // 记得在 CLIENT 掉线时移除
                self.sessions.client_ids.insert(client_id.clone(), token);
                conn.id = client_id.clone();

            } else {
                ErrorCode::InvalidClientIdFieldType.insert(&mut message);

                self.send_message(&self.sessions.conns[token], message);

                return
            }
        } else {
            // 认证时，如果没有提供 CLIENT_ID, 可返回上次认证成功的 CLIENT_ID
            message.insert(CLIENT_ID, conn.id.clone());
            // 认证成功时，设置
            // 记得在 CLIENT 掉线时移除
            self.sessions.client_ids.insert(conn.id.clone(), token);
        }

        conn.supe = tmp_supe;

        if let Some(label) = tmp_label {
            conn.label = label;
        } else {
            // 认证时，如果没有提供 LABEL, 可返回上次认证成功的 LABEL
            // 如果 LABEL 为空的话，就没必要返回了
            if !conn.label.is_empty() {
                message.insert(LABEL, conn.label.clone());
            }
        }

        conn.auth = true;

        message.insert(NODE_ID, self.id.clone());

        ErrorCode::OK.insert(&mut message);
        
        // 这里发一个事件，表示有 CLIENT 认证成功，准备好接收消息了
        // 注意，只有在 CLIENT_READY 和 CLIENT_BREAK 这两个事件才会返回
        // CLIENT 的 LABEL 和 ATTR
        // client event
        // {
        //     CHAN: CLIENT_READY,
        //     SUPER: $conn.supe,
        //     CLIENT_ID: $client_id,
        //     LABEL: $label,
        //     ATTR: $attr
        // }
        let event_message = msg!{
            CHAN: CLIENT_READY,
            SUPER: conn.supe,
            CLIENT_ID: conn.id.clone(),
            LABEL: conn.label.clone(),
            ATTR: conn.stream.attr().clone()
        };

        // 之所以在这里才发送，是因为上面使用了 conn 的几个字段
        self.send_message(&self.sessions.conns[token], message);

        self.relay_super_message(token, CLIENT_READY, event_message);
    }

    fn attach(&mut self, token: usize, mut message: Message) {
        // check auth
        if !self.sessions.conns[token].auth {
            ErrorCode::Unauthorized.insert(&mut message);

            self.send_message(&self.sessions.conns[token], message);

            return
        }

        if let Ok(chan) = message.get_str(VALUE).map(ToOwned::to_owned) {
            // check super
            match chan.as_str() {
                CLIENT_READY | CLIENT_BREAK | CLIENT_ATTACH | CLIENT_DETACH | CLIENT_SEND | CLIENT_RECV => {

                    if !self.sessions.conns[token].supe {
                        ErrorCode::Unauthorized.insert(&mut message);

                        self.send_message(&self.sessions.conns[token], message);

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
                ErrorCode::Unauthorized.insert(&mut message);

                self.send_message(&self.sessions.conns[token], message);

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
                            ErrorCode::InvalidLabelFieldType.insert(&mut message);

                            self.send_message(&self.sessions.conns[token], message);

                            return
                        }
                    }
                } else {
                    ErrorCode::InvalidLabelFieldType.insert(&mut message);

                    self.send_message(&self.sessions.conns[token], message);

                    return
                }
            }

            // client event
            // {
            //     CHAN: CLIENT_ATTACH,
            //     VALUE: $chan,
            //     LABEL: $label, // string or array
            //     client_id: $client_id
            // }
            let mut event_message = msg!{
                CHAN: CLIENT_ATTACH,
                VALUE: &chan,
                CLIENT_ID: self.sessions.conns[token].id.clone()
            };

            if let Some(label) = message.get(LABEL) {
                event_message.insert(LABEL, label.clone());
            }

            // session_attach
            let ids = self.sessions.chans.entry(chan.to_owned()).or_insert_with(HashSet::new);
            ids.insert(token);

            {
                let conn = &mut self.sessions.conns[token];
                let set = conn.chans.entry(chan).or_insert_with(HashSet::new);
                set.extend(labels);
            }

            self.relay_super_message(token, CLIENT_ATTACH, event_message);

            ErrorCode::OK.insert(&mut message);
        } else {
            ErrorCode::CannotGetValueField.insert(&mut message);
        }

        self.send_message(&self.sessions.conns[token], message);
    }

    fn detach(&mut self, token: usize, mut message: Message) {
        // check auth
        if !self.sessions.conns[token].auth {
            ErrorCode::Unauthorized.insert(&mut message);

            self.send_message(&self.sessions.conns[token], message);

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
                            ErrorCode::InvalidLabelFieldType.insert(&mut message);

                            self.send_message(&self.sessions.conns[token], message);

                            return
                        }
                    }
                } else {
                    ErrorCode::InvalidLabelFieldType.insert(&mut message);

                    self.send_message(&self.sessions.conns[token], message);

                    return
                }
            }

            // client event
            // {
            //     CHAN: CLIENT_DETACH,
            //     VALUE: $chan,
            //     LABEL: $label, // string or array
            //     client_id: $client_id
            // }
            let mut event_message = msg!{
                CHAN: CLIENT_DETACH,
                VALUE: &chan,
                CLIENT_ID: self.sessions.conns[token].id.clone()
            };

            if let Some(label) = message.get(LABEL) {
                event_message.insert(LABEL, label.clone());
            }

            // session_detach
            {
                let conn = &mut self.sessions.conns[token];

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
            }

            self.relay_super_message(token, CLIENT_DETACH, event_message);
        
            ErrorCode::OK.insert(&mut message);
        } else {
            ErrorCode::CannotGetValueField.insert(&mut message);
        }

        self.send_message(&self.sessions.conns[token], message);
    }

    fn ping(&self, token: usize, mut message: Message) {
        ErrorCode::OK.insert(&mut message);

        self.send_message(&self.sessions.conns[token], message);
    }

    fn query(&self, token: usize, mut message: Message) {
        {
            let conn = &self.sessions.conns[token];

            if !conn.auth || !conn.supe {
                ErrorCode::Unauthorized.insert(&mut message);

                self.send_message(conn, message);

                return
            }
        }

        for (key, value) in message.clone() {
            if value == Value::String(QUERY_CLIENT_NUM.to_string()) {
                message.insert(key, self.sessions.client_ids.len() as u32);
            } else if value == Value::String(QUERY_CHAN_NUM.to_string()) {
                message.insert(key, self.sessions.chans.len() as u32);
            } else if value == Value::String(QUERY_CLIENTS.to_string()) {
                let mut array = Array::new();

                for (_, conn) in self.sessions.conns.iter() {
                    let mut chans = Message::new();

                    for (chan, labels) in &conn.chans {
                        let labels: Vec<&String> = labels.iter().collect();

                        chans.insert(chan, labels);
                    }

                    let client = msg!{
                        AUTH: conn.auth,
                        SUPER: conn.supe,
                        CHANS: chans,
                        CLIENT_ID: conn.id.clone(),
                        LABEL: conn.label.clone(),
                        ATTR: conn.stream.attr().clone(),
                        SEND_MESSAGES: conn.send_messages.get() as u64,
                        RECV_MESSAGES: conn.recv_messages.get() as u64
                    };

                    array.push(client);
                }

                message.insert(key, array);
            } else if value == Value::String(QUERY_CLIENT.to_string()) {
                if let Ok(client_id) = message.get_message_id(CLIENT_ID) {
                    if let Some(id) = self.sessions.client_ids.get(client_id) {
                        if let Some(conn) = self.sessions.conns.get(*id) {
                            let mut chans = Message::new();

                            for (chan, labels) in &conn.chans {
                                let labels: Vec<&String> = labels.iter().collect();

                                chans.insert(chan, labels);
                            }

                            let client = msg!{
                                AUTH: conn.auth,
                                SUPER: conn.supe,
                                CHANS: chans,
                                CLIENT_ID: conn.id.clone(),
                                LABEL: conn.label.clone(),
                                ATTR: conn.stream.attr().clone(),
                                SEND_MESSAGES: conn.send_messages.get() as u64,
                                RECV_MESSAGES: conn.recv_messages.get() as u64
                            };

                            message.insert(key, client);
                            message.remove(CLIENT_ID);
                        }
                    } else {
                        ErrorCode::NotFound.insert(&mut message);

                        self.send_message(&self.sessions.conns[token], message);

                        return
                    }
                } else {
                    ErrorCode::InvalidClientIdFieldType.insert(&mut message);

                    self.send_message(&self.sessions.conns[token], message);

                    return
                }
            }
        }

        ErrorCode::OK.insert(&mut message);

        self.send_message(&self.sessions.conns[token], message);
    }

    fn mine(&self, token: usize, mut message: Message) {
        if let Some(conn) = self.sessions.conns.get(token) {
            let mut chans = Message::new();

            for (chan, labels) in &conn.chans {
                let labels: Vec<&String> = labels.iter().collect();

                chans.insert(chan, labels);
            }

            let client = msg!{
                AUTH: conn.auth,
                SUPER: conn.supe,
                CHANS: chans,
                CLIENT_ID: conn.id.clone(),
                LABEL: conn.label.clone(),
                ATTR: conn.stream.attr().clone(),
                SEND_MESSAGES: conn.send_messages.get() as u64,
                RECV_MESSAGES: conn.recv_messages.get() as u64
            };

            message.insert(VALUE, client);
        }

        ErrorCode::OK.insert(&mut message);

        self.send_message(&self.sessions.conns[token], message);
    } 

    fn custom(&self, token: usize, mut message: Message) {
        {
            let conn = &self.sessions.conns[token];

            if !conn.auth {
                ErrorCode::Unauthorized.insert(&mut message);

                self.send_message(conn, message);

                return
            }
        }

        if let Some(custom_fn) = &self.callback.custom_fn {
            custom_fn(&self.sessions, token, &mut message, &self.data);
        }
    }

    fn kill(&mut self, token: usize, mut message: Message) -> Result<()> {
        {
            let conn = &self.sessions.conns[token];

            if !conn.auth || !conn.supe {
                ErrorCode::Unauthorized.insert(&mut message);

                self.send_message(conn, message);

                return Ok(())
            }
        }

        let success = if let Some(kill_fn) = &self.callback.kill_fn {
            kill_fn(&self.sessions.conns[token], &mut message, &self.data)
        } else {
            true
        };

        if !success {
            ErrorCode::Unauthorized.insert(&mut message);

            self.send_message(&self.sessions.conns[token], message);

            return Ok(())
        }

        let remove_token;

        if let Some(client_id) = message.get(CLIENT_ID) {
            if let Some(client_id) = client_id.as_message_id() {
                if let Some(other_token) = self.sessions.client_ids.get(client_id).cloned() {
                    remove_token = Some(other_token);
                } else {
                    ErrorCode::TargetClientIdNotExist.insert(&mut message);

                    self.send_message(&self.sessions.conns[token], message);

                    return Ok(())
                }
            } else {
                ErrorCode::InvalidClientIdFieldType.insert(&mut message);

                self.send_message(&self.sessions.conns[token], message);

                return Ok(())
            }
        } else {
            ErrorCode::CannotGetClientIdField.insert(&mut message);

            self.send_message(&self.sessions.conns[token], message);

            return Ok(())
        }

        ErrorCode::OK.insert(&mut message);

        self.send_message(&self.sessions.conns[token], message);

        if let Some(remove_token) = remove_token {
            self.remove_conn(remove_token)?;
        }

        Ok(())
    }

    #[allow(clippy::cognitive_complexity)]
    fn relay_message(&mut self, token: usize, chan: String, mut message: Message) {
        // check auth
        if !self.sessions.conns[token].auth {
            ErrorCode::Unauthorized.insert(&mut message);

            self.send_message(&self.sessions.conns[token], message);

            return
        }

        let success = if let Some(emit_fn) = &self.callback.emit_fn {
            emit_fn(&self.sessions.conns[token], &mut message, &self.data)
        } else {
            true
        };

        if !success {
            ErrorCode::Unauthorized.insert(&mut message);

            self.send_message(&self.sessions.conns[token], message);

            return
        }

        // 如果消息中有 ACK 字段，就发送一条回复消息
        // ACK 可以是任意支持的类型
        // ACK 只是针对 CLIENT -> QUEEN，以便于某些情况下 CLIENT 控制自己的流速，
        // 在目前的实现中，如果 CLIENT 发送过快，填满缓冲区，此时 QUEEN 已经没有
        // 能力进行处理，会丢失溢出的消息。在 QUEEN -> CLIENT 时，如果缓冲区满了，也就
        // 意味着 CLIENT 的网络拥堵，或者 CLIENT 处理能力不足，会丢失溢出的消息，但
        // 不会断开连接
        let reply_message = if let Some(ack) = message.get(ACK) {
            let mut reply_message = msg!{
                CHAN: &chan,
                ACK: ack.clone()
            };

            if let Ok(message_id) = message.get_message_id(ID) {
                reply_message.insert(ID, message_id);
            }

            ErrorCode::OK.insert(&mut reply_message);

            message.remove(ACK);

            Some(reply_message)
        } else {
            None
        };

        // 两种模式下，自己都可以收到自己发送的消息，如果不想处理，可以利用 `FROM` 进行过滤，
        // 也就是此时 FROM == 自己的 CLIENT_ID 

        // P2P 的优先级比较高
        // 不管 CLIENT 是否 ATTACH，都可给其发送消息
        // 忽略 LABEL 和 SHARE
        // 自己可以收到自己发送的消息
        let mut to_ids = vec![];

        if let Some(to) = message.remove(TO) {
            if let Some(to_id) = to.as_message_id() {
                if !self.sessions.client_ids.contains_key(to_id) {
                    ErrorCode::TargetClientIdNotExist.insert(&mut message);
                    message.insert(CLIENT_ID, to_id);

                    self.send_message(&self.sessions.conns[token], message);

                    return
                }

                to_ids.push(to_id.clone());
            } else if let Some(to_array) = to.as_array() {
                let mut not_exist_ids = vec![];

                for to in to_array {
                    if let Some(to_id) = to.as_message_id() {
                        if self.sessions.client_ids.contains_key(to_id) {
                            to_ids.push(to_id.clone());
                        } else {
                            not_exist_ids.push(to_id.clone());
                        }
                    } else {
                        ErrorCode::InvalidToFieldType.insert(&mut message);

                        self.send_message(&self.sessions.conns[token], message);

                        return
                    }
                }

                if !not_exist_ids.is_empty() {
                    ErrorCode::TargetClientIdNotExist.insert(&mut message);
                    message.insert(CLIENT_ID, not_exist_ids);

                    self.send_message(&self.sessions.conns[token], message);

                    return
                }
            } else {
                ErrorCode::InvalidToFieldType.insert(&mut message);

                self.send_message(&self.sessions.conns[token], message);

                return
            }
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
                        ErrorCode::InvalidLabelFieldType.insert(&mut message);

                        self.send_message(&self.sessions.conns[token], message);

                        return
                    }
                }
            } else {
                ErrorCode::InvalidLabelFieldType.insert(&mut message);

                self.send_message(&self.sessions.conns[token], message);

                return
            }
        }

        message.insert(FROM, &self.sessions.conns[token].id.clone());

        macro_rules! send {
            ($self: ident, $conn: ident, $message: ident) => {
                let success = if let Some(push_fn) = &$self.callback.push_fn {
                    push_fn(&$conn, &mut $message, &$self.data)
                } else {
                    true
                };

                if success {
                    self.send_message($conn, $message.clone());

                    // client event
                    // {
                    //     CHAN: CLIENT_RECV,
                    //     VALUE: $message
                    // }
                    let event_message = msg!{
                        CHAN: CLIENT_RECV,
                        VALUE: $message.clone(),
                        TO: $conn.id.clone()
                    };

                    let id = $conn.token;

                    $self.relay_super_message(id, CLIENT_RECV, event_message);
                }
            };
        }

        let mut no_consumers = true;

        // 发送 P2P 消息
        if !to_ids.is_empty() {
            no_consumers = false;

            for to in &to_ids {
                if let Some(conn_id) = self.sessions.client_ids.get(to) {
                    if let Some(conn) = self.sessions.conns.get(*conn_id) {
                        send!(self, conn, message);
                    }
                }
            }
        // 如果存在 SHARE 字段，且值为 false，则只会随机给其中一个 CLIENT 发送消息
        // 会根据 LABEL 过滤
        } else if message.get_bool(SHARE).ok().unwrap_or(false) {
            let mut array: Vec<usize> = Vec::new();

            if let Some(ids) = self.sessions.chans.get(&chan) {
                // 这里没有进行过滤 `.filter(|id| **id != token )`
                // 也就是自己可以收到自己发送的消息
                for conn_id in ids.iter() {
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

        // 给每个 CLIENT 发送消息
        // 会根据 LABEL 过滤
        } else if let Some(ids) = self.sessions.chans.get(&chan) {
            // 这里没有进行过滤 `.filter(|id| **id != token )`
            // 也就是自己可以收到自己发送的消息
            for conn_id in ids.iter() {
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
            message.remove(FROM);

            ErrorCode::NoConsumers.insert(&mut message);

            self.send_message(&self.sessions.conns[token], message);

            return
        }

        // client event
        // {
        //     CHAN: CLIENT_SEND,
        //     VALUE: $message
        // }
        let event_message = msg!{
            CHAN: CLIENT_SEND,
            VALUE: message
        };

        self.relay_super_message(token, CLIENT_SEND, event_message);

        // send reply message
        if let Some(reply_message) = reply_message {
            self.send_message(&self.sessions.conns[token], reply_message);
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
                        self.send_message(conn, message);
                    }
                }
            }
        }
    }
}

impl<T> Drop for QueenInner<T> {
    fn drop(&mut self) {
        self.run.store(false, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub struct Sessions {
    pub conns: Slab<Session>,
    pub chans: HashMap<String, HashSet<usize>>,
    pub client_ids: HashMap<MessageId, usize>,
}

impl Sessions {
    pub fn new() -> Sessions {
        Sessions {
            conns: Slab::new(),
            chans: HashMap::new(),
            client_ids: HashMap::new()
        }
    }
}

pub struct Session {
    pub token: usize,
    pub auth: bool,
    pub supe: bool,
    pub chans: HashMap<String, HashSet<String>>,
    pub stream: Stream<Message>,
    pub id: MessageId, // 默认情况下会随机生成一个，可以在认证时修改
    pub label: Message,
    pub send_messages: Cell<usize>,
    pub recv_messages: Cell<usize>
}

impl Session {
    pub fn new(token: usize, stream: Stream<Message>) -> Session {
        Session {
            token,
            auth: false,
            supe: false,
            chans: HashMap::new(),
            stream,
            id: MessageId::new(),
            label: Message::new(),
            send_messages: Cell::new(0),
            recv_messages: Cell::new(0)
        }
    }

    pub fn send(&self, message: &mut Option<Message>) -> Result<()> {
        self.stream.send(message).map(|m| {
            self.send_messages.set(self.send_messages.get() + 1);
            m
        })
    }

    pub fn recv(&self) -> Result<Message> {
        self.stream.recv().map(|m| {
            self.recv_messages.set(self.recv_messages.get() + 1);
            m
        })
    }
}
