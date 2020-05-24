use std::collections::{HashMap, HashSet};
use std::cell::Cell;
use std::result;

use queen_io::{
    epoll::{Epoll, Token, Ready, EpollOpt}
};

use nson::{
    Message, msg,
    message_id::MessageId
};

use slab::Slab;

use rand::{SeedableRng, seq::SliceRandom, rngs::SmallRng};

use crate::Wire;
use crate::dict::*;
use crate::error::{ErrorCode, Result, SendError, RecvError};

use super::Hook;

pub struct Slot {
    // 索引，CHAN，客户端 Token
    pub chans: HashMap<String, HashSet<usize>>,
    // 索引，客户端 ID， 客户端 Token
    pub client_ids: HashMap<MessageId, usize>,
    // 保存客户端
    pub clients: Slab<Client>,
    // 发送的消息数
    pub send_messages: Cell<usize>,
    // 接收的消息数
    pub recv_messages: Cell<usize>,
    // 随机数，用于发送共享消息
    rand: SmallRng
}

pub struct Client {
    // 客户端内部 ID
    pub token: usize,
    // 默认情况下会随机生成一个，可以在认证时修改
    pub id: MessageId,
    // 客户端的一些属性，可以在认证时设置
    pub label: Message,
    // 是否认证
    pub auth: bool,
    // 是否具有超级权限
    pub root: bool,
    // 客户端 ATTACH 的 CHAN，以及 LABEL
    pub chans: HashMap<String, HashSet<String>>,
    // 发送的消息数
    pub send_messages: Cell<usize>,
    // 接收的消息数
    pub recv_messages: Cell<usize>,
    // 线
    pub wire: Wire<Message>
}

impl Slot {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            chans: HashMap::new(),
            client_ids: HashMap::new(),
            clients: Slab::new(),
            send_messages: Cell::new(0),
            recv_messages: Cell::new(0),
            rand: SmallRng::from_entropy()
        }
    }

    pub(crate) fn add_client(
        &mut self,
        epoll: &Epoll,
        hook: &impl Hook,
        wire: Wire<Message>
    ) -> Result<()> {
        let entry = self.clients.vacant_entry();
        let client = Client::new(entry.key(), wire);

        // 此处可以验证一下客户端的属性，不过目前只能验证 wire.attr
        let success = hook.accept(&client);

        if success && matches!(client.wire.send(msg!{OK: 0i32}), Ok(_)) {
            epoll.add(&client.wire, Token(entry.key()), Ready::readable(), EpollOpt::level())?;
            entry.insert(client);
        } else {
            client.wire.close();
        }

        Ok(())
    }

    pub(crate) fn del_client(
        &mut self,
        epoll: &Epoll,
        hook: &impl Hook,
        token: usize
    ) -> Result<()> {
        if self.clients.contains(token) {
            let client = self.clients.remove(token);
            // client.wire.close();
            epoll.delete(&client.wire)?;

            for chan in client.chans.keys() {
                if let Some(ids) = self.chans.get_mut(chan) {
                    ids.remove(&token);

                    if ids.is_empty() {
                        self.chans.remove(chan);
                    }
                }
            }

            // 这里要记得移除 clinet_id，因为 wire 在一开始建立连接时就会默认分配一个
            // 认证成功时可以修改
            self.client_ids.remove(&client.id);

            hook.remove(&client);

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
                CLIENT_ID: client.id,
                LABEL: client.label.clone(),
                ATTR: client.wire.attr().clone()
            };

            self.relay_root_message(hook, token, CLIENT_BREAK, event_message);
        }

        Ok(())
    }

    pub(crate) fn recv_message(
        &mut self,
        epoll: &Epoll,
        hook: &impl Hook,
        socket_id: &MessageId,
        token: usize,
        mut message: Message
    ) -> Result<()> {
        self.recv_messages.set(self.recv_messages.get() + 1);

        let success = hook.recv(&self.clients[token], &mut message);

        if !success {
            ErrorCode::RefuseReceiveMessage.insert(&mut message);

            self.send_message(hook, token, message);

            return Ok(())
        }

        let chan = match message.get_str(CHAN) {
            Ok(chan) => chan,
            Err(_) => {
                ErrorCode::CannotGetChanField.insert(&mut message);

                self.send_message(hook, token, message);

                return Ok(())
            }
        };

        if chan.starts_with('_') {
            match chan {
                AUTH => self.auth(hook, socket_id, token, message),
                ATTACH => self.attach(hook, token, message),
                DETACH => self.detach(hook, token, message),
                PING => self.ping(hook, token, message),
                MINE => self.mine(hook, token, message),
                QUERY => self.query(hook, token, message),
                CUSTOM => self.custom(hook, token, message),
                CLIENT_KILL => self.kill(epoll, hook, token, message)?,
                _ => {
                    ErrorCode::UnsupportedChan.insert(&mut message);

                    self.send_message(hook, token, message);
                }
            }
        } else {
            self.relay_message(hook, token, chan.to_string(), message);
        }

        Ok(())
    }

    pub(crate) fn send_message(
        &self,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) {
        self.send_messages.set(self.send_messages.get() + 1);

        if let Some(client) = self.clients.get(token) {
            let success = hook.send(client, &mut message);

            if success {
                let _ = client.send(message);
            }
        }
    }

    pub(crate) fn relay_root_message(
        &self,
        hook: &impl Hook,
        token: usize,
        chan: &str, message: Message
    ) {
        if let Some(tokens) = self.chans.get(chan) {
            for other_token in tokens {
                if token == *other_token {
                    continue;
                }

                if let Some(client) = self.clients.get(*other_token) {
                    let mut message = message.clone();

                    let success = hook.send(&client, &mut message);

                    if success {
                        self.send_message(hook, client.token, message);
                    }
                }
            }
        }
    }

    #[allow(clippy::cognitive_complexity)]
    pub(crate) fn relay_message(
        &mut self,
        hook: &impl Hook,
        token: usize,
        chan: String,
        mut message: Message
    ) {
        // 认证之前，不能发送消息
        if !self.clients[token].auth {
            ErrorCode::Unauthorized.insert(&mut message);

            self.send_message(hook, token, message);

            return
        }

        let success = hook.emit(&self.clients[token], &mut message);

        if !success {
            ErrorCode::Unauthorized.insert(&mut message);

            self.send_message(hook, token, message);

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
                if !self.client_ids.contains_key(to_id) {
                    ErrorCode::TargetClientIdNotExist.insert(&mut message);
                    message.insert(CLIENT_ID, to_id);

                    self.send_message(hook, token, message);

                    return
                }

                to_ids.push(to_id.clone());
            } else if let Some(to_array) = to.as_array() {
                let mut not_exist_ids = vec![];

                for to in to_array {
                    if let Some(to_id) = to.as_message_id() {
                        if self.client_ids.contains_key(to_id) {
                            to_ids.push(to_id.clone());
                        } else {
                            not_exist_ids.push(to_id.clone());
                        }
                    } else {
                        ErrorCode::InvalidToFieldType.insert(&mut message);

                        self.send_message(hook, token, message);

                        return
                    }
                }

                if !not_exist_ids.is_empty() {
                    ErrorCode::TargetClientIdNotExist.insert(&mut message);
                    message.insert(CLIENT_ID, not_exist_ids);

                    self.send_message(hook, token, message);

                    return
                }
            } else {
                ErrorCode::InvalidToFieldType.insert(&mut message);

                self.send_message(hook, token, message);

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

                        self.send_message(hook, token, message);

                        return
                    }
                }
            } else {
                ErrorCode::InvalidLabelFieldType.insert(&mut message);

                self.send_message(hook, token, message);

                return
            }
        }

        message.insert(FROM, &self.clients[token].id.clone());

        macro_rules! send {
            ($self: ident, $hook: ident, $client: ident, $message: ident) => {
                let success = $hook.push(&$client, &mut $message);

                if success {
                    $self.send_message($hook, $client.token, $message.clone());

                    // client event
                    // {
                    //     CHAN: CLIENT_RECV,
                    //     VALUE: $message
                    // }
                    let event_message = msg!{
                        CHAN: CLIENT_RECV,
                        VALUE: $message.clone(),
                        TO: $client.id.clone()
                    };

                    let id = $client.token;

                    $self.relay_root_message($hook, id, CLIENT_RECV, event_message);
                }
            };
        }

        let mut no_consumers = true;

        // 发送 P2P 消息
        if !to_ids.is_empty() {
            no_consumers = false;

            for to in &to_ids {
                if let Some(client_id) = self.client_ids.get(to) {
                    if let Some(client) = self.clients.get(*client_id) {
                        send!(self, hook, client, message);
                    }
                }
            }
        // 如果存在 SHARE 字段，且值为 false，则只会随机给其中一个 CLIENT 发送消息
        // 会根据 LABEL 过滤
        } else if message.get_bool(SHARE).ok().unwrap_or(false) {
            let mut array: Vec<usize> = Vec::new();

            if let Some(ids) = self.chans.get(&chan) {
                // 这里没有进行过滤 `.filter(|id| **id != token )`
                // 也就是自己可以收到自己发送的消息
                for client_id in ids.iter() {
                    if let Some(client) = self.clients.get(*client_id) {
                        // filter labels
                        if !labels.is_empty() {
                            let client_labels = client.chans.get(&chan).expect("It shouldn't be executed here!");

                            // if !client_labels.iter().any(|l| labels.contains(l)) {
                            //     continue
                            // }
                            if (client_labels & &labels).is_empty() {
                                continue;
                            }
                        }

                        array.push(*client_id);
                    }
                }
            }

            if !array.is_empty() {
                no_consumers = false;

                if array.len() == 1 {
                    if let Some(client) = self.clients.get(array[0]) {
                        send!(self, hook, client, message);
                    }
                } else if let Some(id) = array.choose(&mut self.rand) {
                    if let Some(client) = self.clients.get(*id) {
                        send!(self, hook, client, message);
                    }
                }
            }

        // 给每个 CLIENT 发送消息
        // 会根据 LABEL 过滤
        } else if let Some(ids) = self.chans.get(&chan) {
            // 这里没有进行过滤 `.filter(|id| **id != token )`
            // 也就是自己可以收到自己发送的消息
            for client_id in ids.iter() {
                if let Some(client) = self.clients.get(*client_id) {
                    // filter labels
                    if !labels.is_empty() {
                        let client_labels = client.chans.get(&chan).expect("It shouldn't be executed here!");

                        if !client_labels.iter().any(|l| labels.contains(l)) {
                            continue
                        }
                    }

                    no_consumers = false;

                    send!(self, hook, client, message);
                }
            }
        }

        if no_consumers {
            message.remove(FROM);

            ErrorCode::NoConsumers.insert(&mut message);

            self.send_message(hook, token, message);

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

        self.relay_root_message(hook, token, CLIENT_SEND, event_message);

        // send reply message
        if let Some(reply_message) = reply_message {
            self.send_message(hook, token, reply_message);
        }
    }

    pub(crate) fn auth(
        &mut self,
        hook: &impl Hook,
        socket_id: &MessageId,
        token: usize,
        mut message: Message
    ) {
        let mut client = &mut self.clients[token];

        // 保存认证之前的状态
        struct TempSession {
            pub auth: bool,
            pub root: bool,
            pub id: MessageId,
            pub label: Message
        }

        let temp_session = TempSession {
            auth: client.auth,
            root: client.root,
            id: client.id.clone(),
            label: client.label.clone()
        };

        // ROOT
        if let Some(s) = message.get(ROOT) {
            // ROOT 字段的值只能为 bool
            if let Some(s) = s.as_bool() {
                client.root = s;
            } else {
                ErrorCode::InvalidRootFieldType.insert(&mut message);

                self.send_message(hook, token, message);

                return
            }
        }

        // LABEL
        if let Some(label) = message.get(LABEL) {
            // LABEL 字段的值只能为 Message
            if let Some(label) = label.as_message() {
                client.label = label.clone();
            } else {
                // 如果失败，记得恢复 ROOT
                client.root = temp_session.root;

                ErrorCode::InvalidLabelFieldType.insert(&mut message);

                self.send_message(hook, token, message);

                return
            }
        }

        // CLIENT_ID
        if let Some(client_id) = message.get(CLIENT_ID) {
            // CLIENT_ID 的类型必须为 MessageId
            if let Some(client_id) = client_id.as_message_id() {
                if let Some(other_token) = self.client_ids.get(client_id) {
                    // CLIENT ID 不能重复，且不能挤掉已存在的客户端
                    if *other_token != token {
                        // 如果失败，恢复 ROOT 和 LABEL
                        client.root = temp_session.root;
                        client.label = temp_session.label;

                        ErrorCode::DuplicateClientId.insert(&mut message);

                        self.send_message(hook, token, message);

                        return
                    }
                }

                // 认证时, 可以改变 CLIENT_ID
                self.client_ids.remove(&client.id);
                // 认证成功时，设置
                // 记得在 CLIENT 掉线时移除
                self.client_ids.insert(client_id.clone(), token);
                client.id = client_id.clone();

            } else {
                // 如果失败，恢复 ROOT 和 LABEL
                client.root = temp_session.root;
                client.label = temp_session.label;

                ErrorCode::InvalidClientIdFieldType.insert(&mut message);

                self.send_message(hook, token, message);

                return
            }
        } else {
            // 认证时，如果没有提供 CLIENT_ID, 可返回上次认证成功的 CLIENT_ID
            message.insert(CLIENT_ID, client.id.clone());
            // 认证成功时，设置
            // 记得在 CLIENT 掉线时移除
            self.client_ids.insert(client.id.clone(), token);
        }

        // 这里可以验证自定义字段的合法性
        // 这里可以验证超级用户的合法性
        // 这里可以验证 CLIENT_ID 的合法性
        let success = hook.auth(&client, &mut message);

        if !success {
            // 认证失败时, 移除
            self.client_ids.remove(&client.id);

            if temp_session.auth {
                // 如果上次认证成功，就将原来的插入
                self.client_ids.insert(temp_session.id.clone(), token);
            }

            client.auth = temp_session.auth;
            client.root = temp_session.root;
            client.id = temp_session.id;
            client.label = temp_session.label;

            ErrorCode::AuthenticationFailed.insert(&mut message);

            self.send_message(hook, token, message);

            return
        }

        client.auth = true;

        // 认证时，如果没有提供 LABEL, 可返回上次认证成功的 LABEL
        // 如果 LABEL 为空的话，就没必要返回了
        if !client.label.is_empty() {
            message.insert(LABEL, client.label.clone());
        }

        message.insert(SOCKET_ID, socket_id.clone());

        ErrorCode::OK.insert(&mut message);

        // 这里发一个事件，表示有 CLIENT 认证成功，准备好接收消息了
        // 注意，只有在 CLIENT_READY 和 CLIENT_BREAK 这两个事件才会返回
        // CLIENT 的 LABEL 和 ATTR
        // client event
        // {
        //     CHAN: CLIENT_READY,
        //     ROOT: $client.root,
        //     CLIENT_ID: $client_id,
        //     LABEL: $label,
        //     ATTR: $attr
        // }
        let event_message = msg!{
            CHAN: CLIENT_READY,
            ROOT: client.root,
            CLIENT_ID: client.id.clone(),
            LABEL: client.label.clone(),
            ATTR: client.wire.attr().clone()
        };

        // 之所以在这里才发送，是因为上面使用了 client 的几个字段
        self.send_message(hook, token, message);

        self.relay_root_message(hook, token, CLIENT_READY, event_message);
    }

    // ATTACH 的时候，可以附带自定义数据，可以通过 Hook.attach 或 CLIENT_ATTACH 事件获取
    pub(crate) fn attach(
        &mut self,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) {
        // check auth
        if !self.clients[token].auth {
            ErrorCode::Unauthorized.insert(&mut message);

            self.send_message(hook, token, message);

            return
        }

        if let Ok(chan) = message.get_str(VALUE).map(ToOwned::to_owned) {
            // check ROOT
            match chan.as_str() {
                CLIENT_READY | CLIENT_BREAK | CLIENT_ATTACH | CLIENT_DETACH | CLIENT_SEND | CLIENT_RECV => {

                    if !self.clients[token].root {
                        ErrorCode::Unauthorized.insert(&mut message);

                        self.send_message(hook, token, message);

                        return
                    }

                }
                _ => ()
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

                            self.send_message(hook, token, message);

                            return
                        }
                    }
                } else {
                    ErrorCode::InvalidLabelFieldType.insert(&mut message);

                    self.send_message(hook, token, message);

                    return
                }
            }

            // 这里可以验证该 CLIENT 是否有权限
            let success = hook.attach(&self.clients[token], &mut message, &chan, &labels);

            if !success {
                ErrorCode::Unauthorized.insert(&mut message);

                self.send_message(hook, token, message);

                return
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
                CLIENT_ID: self.clients[token].id.clone()
            };

            if let Some(label) = message.get(LABEL) {
                event_message.insert(LABEL, label.clone());
            }

            // session_attach
            let ids = self.chans.entry(chan.to_owned()).or_insert_with(HashSet::new);
            ids.insert(token);

            {
                let client = &mut self.clients[token];
                let set = client.chans.entry(chan).or_insert_with(HashSet::new);
                set.extend(labels);
            }

            self.relay_root_message(hook, token, CLIENT_ATTACH, event_message);

            ErrorCode::OK.insert(&mut message);
        } else {
            ErrorCode::CannotGetValueField.insert(&mut message);
        }

        self.send_message(hook, token, message);
    }

    // DETACH 的时候，可以附带自定义数据，可以通过 Hook.detach 或 CLIENT_DETACH 事件获取
    pub(crate) fn detach(
        &mut self,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) {
        // check auth
        if !self.clients[token].auth {
            ErrorCode::Unauthorized.insert(&mut message);

            self.send_message(hook, token, message);

            return
        }

        if let Ok(chan) = message.get_str(VALUE).map(ToOwned::to_owned) {
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

                            self.send_message(hook, token, message);

                            return
                        }
                    }
                } else {
                    ErrorCode::InvalidLabelFieldType.insert(&mut message);

                    self.send_message(hook, token, message);

                    return
                }
            }

            // 这里可以验证该 CLIENT 是否有权限
            // 可以让 CLIENT 不能 DETACH 某些 CHAN
            let success = hook.detach(&self.clients[token], &mut message, &chan, &labels);

            if !success {
                ErrorCode::Unauthorized.insert(&mut message);

                self.send_message(hook, token, message);

                return
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
                CLIENT_ID: self.clients[token].id.clone()
            };

            if let Some(label) = message.get(LABEL) {
                event_message.insert(LABEL, label.clone());
            }

            // session_detach
            {
                let client = &mut self.clients[token];

                // 注意，DETACH 的时候，如果 LABEL 为空的话，会将 CHAN 移除掉
                // 否则，只会移除掉对应的 LABEL
                if labels.is_empty() {
                    client.chans.remove(&chan);

                    if let Some(ids) = self.chans.get_mut(&chan) {
                        ids.remove(&token);

                        if ids.is_empty() {
                            self.chans.remove(&chan);
                        }
                    }
                } else if let Some(set) = client.chans.get_mut(&chan) {
                    *set = set.iter().filter(|label| !labels.contains(*label)).map(|s| s.to_string()).collect();
                }
            }

            self.relay_root_message(hook, token, CLIENT_DETACH, event_message);

            ErrorCode::OK.insert(&mut message);
        } else {
            ErrorCode::CannotGetValueField.insert(&mut message);
        }

        self.send_message(hook, token, message);
    }

    // PING 的时候可以附带自定义数据，可以通过 Hook.ping 获取
    pub(crate) fn ping(&mut self, hook: &impl Hook, token: usize, mut message: Message) {
        hook.ping(&self.clients[token], &mut message);

        // PING 的时候，会插入 OK: 0
        ErrorCode::OK.insert(&mut message);

        self.send_message(hook, token, message);
    }

    pub(crate) fn mine(&self, hook: &impl Hook, token: usize, mut message: Message) {
        if let Some(client) = self.clients.get(token) {
            let mut chans = Message::new();

            for (chan, labels) in &client.chans {
                let labels: Vec<&String> = labels.iter().collect();

                chans.insert(chan, labels);
            }

            let client = msg!{
                AUTH: client.auth,
                ROOT: client.root,
                CHANS: chans,
                CLIENT_ID: client.id.clone(),
                LABEL: client.label.clone(),
                ATTR: client.wire.attr().clone(),
                SEND_MESSAGES: client.send_messages.get() as u64,
                RECV_MESSAGES: client.recv_messages.get() as u64
            };

            message.insert(VALUE, client);
        }

        ErrorCode::OK.insert(&mut message);

        self.send_message(hook, token, message);
    }

    // 注意，QUERY 和 CUSTOM 的不同之处在于，前者必须具有 ROOT 权限，后者不需要
    // 可以在 Hook.query 自行定制返回数据
    pub(crate) fn query(&self, hook: &impl Hook, token: usize, mut message: Message) {
        {
            let client = &self.clients[token];

            if !client.auth || !client.root {
                ErrorCode::Unauthorized.insert(&mut message);

                self.send_message(hook, token, message);

                return
            }
        }

        hook.query(&self, token, &mut message);

        // QUERY 的时候，不会插入 OK: 0, 由 hook 函数决定

        self.send_message(hook, token, message);
    }

    // 注意，QUERY 和 CUSTOM 的不同之处在于，前者必须具有 ROOT 权限，后者不需要
    // 可以在 Hook.custom 自行定制返回数据
    pub(crate) fn custom(&self, hook: &impl Hook, token: usize, mut message: Message) {
        {
            let client = &self.clients[token];

            if !client.auth {
                ErrorCode::Unauthorized.insert(&mut message);

                self.send_message(hook, token, message);

                return
            }
        }

        hook.custom(&self, token, &mut message);

        // CUSTOM 的时候，不会插入 OK: 0, 由 hook 函数决定

        self.send_message(hook, token, message);
    }

    // 具有 ROOT 权限的客户端可以 KILL 掉其他客户端（包括自己）
    pub(crate) fn kill(
        &mut self,
        epoll: &Epoll,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) -> Result<()> {
        {
            let client = &self.clients[token];

            if !client.auth || !client.root {
                ErrorCode::Unauthorized.insert(&mut message);

                self.send_message(hook, token, message);

                return Ok(())
            }
        }

        let success = hook.kill(&self.clients[token], &mut message);

        if !success {
            ErrorCode::Unauthorized.insert(&mut message);

            self.send_message(hook, token, message);

            return Ok(())
        }

        let remove_token;

        if let Some(client_id) = message.get(CLIENT_ID) {
            if let Some(client_id) = client_id.as_message_id() {
                if let Some(other_token) = self.client_ids.get(client_id).cloned() {
                    remove_token = Some(other_token);
                } else {
                    ErrorCode::TargetClientIdNotExist.insert(&mut message);

                    self.send_message(hook, token, message);

                    return Ok(())
                }
            } else {
                ErrorCode::InvalidClientIdFieldType.insert(&mut message);

                self.send_message(hook, token, message);

                return Ok(())
            }
        } else {
            ErrorCode::CannotGetClientIdField.insert(&mut message);

            self.send_message(hook, token, message);

            return Ok(())
        }

        ErrorCode::OK.insert(&mut message);

        self.send_message(hook, token, message);

        if let Some(remove_token) = remove_token {
            self.del_client(epoll, hook, remove_token)?;
        }

        Ok(())
    }
}

impl Client {
    pub fn new(token: usize, wire: Wire<Message>) -> Self {
        Self {
            token,
            id: MessageId::new(),
            label: Message::new(),
            auth: false,
            root: false,
            chans: HashMap::new(),
            send_messages: Cell::new(0),
            recv_messages: Cell::new(0),
            wire
        }
    }

    pub fn send(&self, message: Message) -> result::Result<(), SendError<Message>> {
        self.wire.send(message).map(|m| {
            self.send_messages.set(self.send_messages.get() + 1);
            m
        })
    }

    pub fn recv(&self) -> result::Result<Message, RecvError> {
        self.wire.recv().map(|m| {
            self.recv_messages.set(self.recv_messages.get() + 1);
            m
        })
    }
}
