use std::collections::{HashMap, HashSet};
use std::cell::Cell;

use queen_io::{
    epoll::{Epoll, Token, Ready, EpollOpt},
    plus::slab::Slab
};

use nson::{
    Message, msg,
    message_id::MessageId,
    Array
};

use rand::{SeedableRng, seq::SliceRandom, rngs::SmallRng};

use crate::Wire;
use crate::dict::*;
use crate::error::{Code, Result};

use super::Hook;
use super::Slot;

pub struct Switch {
    pub socket_id: MessageId,
    // CHAN，Token
    pub chans: HashMap<String, HashSet<usize>>,
    pub share_chans: HashMap<String, HashSet<usize>>,
    // SLOT_ID，Token
    pub slot_ids: HashMap<MessageId, usize>,
    // SOCKET_ID, Token
    pub socket_ids: HashMap<MessageId, usize>,
    pub slots: Slab<Slot>,
    pub send_num: Cell<usize>,
    pub recv_num: Cell<usize>,
    rand: SmallRng
}

impl Switch {
    pub(crate) fn new(socket_id: MessageId) -> Self {
        Self {
            socket_id,
            chans: HashMap::new(),
            share_chans: HashMap::new(),
            slot_ids: HashMap::new(),
            socket_ids: HashMap::new(),
            slots: Slab::new(),
            send_num: Cell::new(0),
            recv_num: Cell::new(0),
            rand: SmallRng::from_entropy()
        }
    }

    pub(crate) fn add_slot(
        &mut self,
        epoll: &Epoll,
        hook: &impl Hook,
        id: MessageId,
        root: bool,
        wire: Wire<Message>
    ) -> Result<()> {
        if self.slot_ids.contains_key(&id) {
            let _ = wire.send(msg!{CODE: Code::DuplicateSlotId.code()});

            return Ok(())
        }

        let entry = self.slots.vacant_entry();
        let token = entry.key();

        let slot = Slot::new(token, id, root, wire);

        // 此处可以验证一下 SLOT 的属性，不过目前只能验证 wire.attr
        // 并且，wire.attr 是可以修改的
        // 但是，SLOT 的属性是不能在这里修改的
        let success = hook.accept(&slot);

        if success && slot.wire.send(msg!{CODE: Code::Ok.code()}) == Ok(()) {
            epoll.add(&slot.wire, Token(token), Ready::readable(), EpollOpt::level())?;

            self.slot_ids.insert(slot.id, token);

            // 这里发一个事件，表示有 SLOT 认证成功，准备好接收消息了
            // 注意，只有在 SLOT_READY 和 SLOT_BREAK 这两个事件才会返回
            // SLOT 的 TAG 和 ATTR
            // slot event
            // {
            //     CHAN: SLOT_READY,
            //     ROOT: $slot.root,
            //     SLOT_ID: $slot_id,
            //     ATTR: $attr
            // }
            let event_message = msg!{
                CHAN: SLOT_READY,
                SLOT_ID: slot.id,
                ROOT: slot.root,
                ATTR: slot.wire.attr().clone()
            };

            entry.insert(slot);

            self.relay_root_message(hook, token, SLOT_READY, event_message);
        } else {
            let _ = slot.wire.send(msg!{CODE: Code::AuthenticationFailed.code()});
        }

        Ok(())
    }

    pub(crate) fn del_slot(
        &mut self,
        epoll: &Epoll,
        hook: &impl Hook,
        token: usize
    ) -> Result<()> {
        if self.slots.contains(token) {
            let slot = self.slots.remove(token);
            // slot.wire.close(); 这里不需要主动关闭，离开作用域后会自动关闭
            epoll.delete(&slot.wire)?;

            for chan in slot.chans.keys() {
                if let Some(ids) = self.chans.get_mut(chan) {
                    ids.remove(&token);

                    if ids.is_empty() {
                        self.chans.remove(chan);
                    }
                }
            }

            // 移除共享订阅
            for chan in slot.share_chans.keys() {
                if let Some(ids) = self.share_chans.get_mut(chan) {
                    ids.remove(&token);

                    if ids.is_empty() {
                        self.chans.remove(chan);
                    }
                }
            }

            // 这里要记得移除 SLOT_ID，因为 wire 在一开始建立连接时就会默认分配一个
            // 认证成功时可以修改
            self.slot_ids.remove(&slot.id);

            // 清除 BIND
            for target_token in &slot.bind {
                if let Some(target_slot) = self.slots.get_mut(*target_token) {
                    target_slot.bound.remove(&slot.token);
                }
            }

            hook.remove(&slot);

            // 这里发一个事件，表示有 SLOT 断开
            // 注意，只有在 SLOT_READY 和 SLOT_BREAK 这两个事件才会返回
            // SLOT 的 TAG 和 ATTR
            // slot event
            // {
            //     CHAN: SLOT_BREAK,
            //     SLOT_ID: $slot_id,
            //     ATTR: $attr
            // }
            let event_message = msg!{
                CHAN: SLOT_BREAK,
                SLOT_ID: slot.id,
                ROOT: slot.root,
                ATTR: slot.wire.attr().clone()
            };

            self.relay_root_message(hook, token, SLOT_BREAK, event_message);
        }

        Ok(())
    }

    pub(crate) fn recv_message(
        &mut self,
        epoll: &Epoll,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) -> Result<()> {
        self.recv_num.set(self.recv_num.get() + 1);

        let success = hook.recv(&self.slots[token], &mut message);

        if !success {
            Code::PermissionDenied.set(&mut message);

            self.send_message(hook, token, message);

            return Ok(())
        }

        let chan = match message.get_str(CHAN) {
            Ok(chan) => chan,
            Err(_) => {
                Code::CannotGetChanField.set(&mut message);

                self.send_message(hook, token, message);

                return Ok(())
            }
        };

        if chan.starts_with('_') {
            match chan {
                ATTACH => self.attach(hook, token, message),
                DETACH => self.detach(hook, token, message),
                BIND => self.bind(hook, token, message),
                UNBIND => self.unbind(hook, token, message),
                JOIN => self.join(hook, token, message),
                UNJOIN => self.unjoin(hook, token, message),
                PING => self.ping(hook, token, message),
                MINE => self.mine(hook, token, message),
                QUERY => self.query(hook, token, message),
                CUSTOM => self.custom(hook, token, message),
                CTRL => self.ctrl(hook, token, message),
                SLOT_KILL => self.kill(epoll, hook, token, message)?,
                _ => {
                    Code::UnsupportedChan.set(&mut message);

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
        if let Some(slot) = self.slots.get(token) {
            let success = hook.send(slot, &mut message);

            if success && slot.wire.send(message).is_ok() {
                self.send_num.set(self.send_num.get() + 1);
            }
        }
    }

    fn relay_root_message(
        &self,
        hook: &impl Hook,
        token: usize,
        chan: &str,
        message: Message
    ) {
        if let Some(tokens) = self.chans.get(chan) {
            for other_token in tokens {
                if token == *other_token {
                    continue;
                }

                if let Some(slot) = self.slots.get(*other_token) {
                    let mut message = message.clone();

                    let success = hook.send(slot, &mut message);

                    if success {
                        self.send_message(hook, slot.token, message);
                    }
                }
            }
        }
    }

    #[allow(clippy::cognitive_complexity)]
    fn relay_message(
        &mut self,
        hook: &impl Hook,
        token: usize,
        chan: String,
        mut message: Message
    ) {
        let success = hook.emit(&self.slots[token], &mut message);

        if !success {
            Code::PermissionDenied.set(&mut message);

            self.send_message(hook, token, message);

            return
        }

        macro_rules! send_bind {
            ($self: ident, $hook: ident, $slot: ident, $message: ident) => {
                let success = $hook.push($slot, &mut $message);

                if success {
                    $self.send_message($hook, $slot.token, $message.clone());
                }
            };
        }

        macro_rules! send {
            ($self: ident, $hook: ident, $slot: ident, $message: ident) => {
                let success = $hook.push($slot, &mut $message);

                if success {
                    let mut message = $message.clone();

                    if $slot.joined {
                        // 如果对方已 JOIN，则填充 FROM_SOCKET
                        if !message.contains_key(FROM_SOCKET) {
                            message.insert(FROM_SOCKET, self.socket_id);
                        }
                    }

                    $self.send_message($hook, $slot.token, message.clone());

                    // BIND
                    // 此模式可以接收到所 BIND 的 SLOT 发送的消息
                    let bounds = &$slot.bound;

                    let mut bind_message = msg! {
                        CHAN: BIND_RECV,
                        SLOT_ID: $slot.id,
                        VALUE: message.clone()
                    };

                    for bound in bounds {
                        if let Some(slot) = self.slots.get(*bound) {
                            send_bind!(self, hook, slot, bind_message);
                        }
                    }

                    // slot event
                    // {
                    //     CHAN: SLOT_RECV,
                    //     VALUE: $message
                    // }
                    let event_message = msg!{
                        CHAN: SLOT_RECV,
                        VALUE: message.clone(),
                        TO: $slot.id
                    };

                    let id = $slot.token;

                    $self.relay_root_message($hook, id, SLOT_RECV, event_message);
                }
            };
        }

        if !message.contains_key(FROM) {
            message.insert(FROM, self.slots[token].id);
        }

        // BIND
        // 此模式可以接收到所 BIND 的 SLOT 发送的消息
        let bounds = &self.slots[token].bound;

        let mut bind_message = msg! {
            CHAN: BIND_SEND,
            SLOT_ID: self.slots[token].id,
            VALUE: message.clone()
        };

        for bound in bounds {
            if let Some(slot) = self.slots.get(*bound) {
                send_bind!(self, hook, slot, bind_message);
            }
        }

        // TO SOCKET
        let mut goon = true;

        if let Some(to_socket) = message.get(TO_SOCKET).cloned() {
            if let Some(to_socket_id) = to_socket.as_message_id() {
                // 目前 Socket ID 不等于本 Socket ID 时，该功能才生效
                if to_socket_id != &self.socket_id {
                    goon = false;

                    // 移除 TO
                    message.remove(TO_SOCKET);

                    if let Some(socket_token) = self.socket_ids.get(to_socket_id) {
                        if let Some(slot) = self.slots.get(*socket_token) {
                            send!(self, hook, slot, message);
                        }
                    }
                }
            } else {
                Code::InvalidToSocketFieldType.set(&mut message);

                self.send_message(hook, token, message);

                return
            }
        }

        if goon {
            // 如果 TO == 本 Socket ID，则移除
            if let Ok(to) = message.get_message_id(TO) {
                if to == &self.socket_id {
                    message.remove(TO);
                }
            }

            // 两种模式下，自己都可以收到自己发送的消息，如果不想处理，可以利用 `FROM` 进行过滤，
            // 也就是此时 FROM == 自己的 SLOT_ID

            // P2P 的优先级比较高
            // 不管 SLOT 是否 ATTACH，都可给其发送消息
            // 忽略 TAG
            // 自己可以收到自己发送的消息
            if let Some(to) = message.get(TO).cloned() {
                let mut to_ids = vec![];

                if let Some(to_id) = to.as_message_id() {
                    // TO 可以是单个 SLOT_ID
                    if self.slot_ids.contains_key(to_id) {
                        to_ids.push(*to_id);
                    }
                } else if let Some(to_array) = to.as_array() {
                    // TO 也可以是一个数组
                    for to in to_array {
                        if let Some(to_id) = to.as_message_id() {
                            if self.slot_ids.contains_key(to_id) {
                                to_ids.push(*to_id);
                            }
                        } else {
                            Code::InvalidToFieldType.set(&mut message);

                            self.send_message(hook, token, message);

                            return
                        }
                    }
                } else {
                    Code::InvalidToFieldType.set(&mut message);

                    self.send_message(hook, token, message);

                    return
                }

                // 移除 TO
                message.remove(TO);

                if !to_ids.is_empty() {
                    if message.get_bool(SHARE).ok().unwrap_or(false) {
                        if to_ids.len() == 1 {
                            if let Some(slot_token) = self.slot_ids.get(&to_ids[0]) {
                                if let Some(slot) = self.slots.get(*slot_token) {
                                    send!(self, hook, slot, message);
                                }
                            }
                        } else if let Some(to) = to_ids.choose(&mut self.rand) {
                            if let Some(slot_token) = self.slot_ids.get(to) {
                                if let Some(slot) = self.slots.get(*slot_token) {
                                    send!(self, hook, slot, message);
                                }
                            }
                        }
                    } else {
                        for to in &to_ids {
                            if let Some(slot_token) = self.slot_ids.get(to) {
                                if let Some(slot) = self.slots.get(*slot_token) {
                                    send!(self, hook, slot, message);
                                }
                            }
                        }
                    }
                }
            } else {
                // tags
                let mut tags = HashSet::new();

                if let Some(tag) = message.get(TAG) {
                    if let Some(tag) = tag.as_str() {
                        tags.insert(tag.to_string());
                    } else if let Some(tag_array) = tag.as_array() {
                        for v in tag_array {
                            if let Some(v) = v.as_str() {
                                tags.insert(v.to_string());
                            } else {
                                Code::InvalidTagFieldType.set(&mut message);

                                self.send_message(hook, token, message);

                                return
                            }
                        }
                    } else {
                        Code::InvalidTagFieldType.set(&mut message);

                        self.send_message(hook, token, message);

                        return
                    }
                }

                if let Some(tokens) = self.chans.get(&chan) {
                    if message.get_bool(SHARE).ok().unwrap_or(false) {
                        let mut array: Vec<usize> = Vec::new();

                        // 这里没有进行过滤 `.filter(|id| **id != token )`
                        // 也就是自己可以收到自己发送的消息
                        for slot_token in tokens.iter() {
                            if let Some(slot) = self.slots.get(*slot_token) {
                                // filter tags
                                if !tags.is_empty() {
                                    let slot_tags = slot.chans.get(&chan).expect("It shouldn't be executed here!");

                                    // 交集
                                    // if !slot_tags.iter().any(|l| tags.contains(l)) {
                                    //     continue
                                    // }
                                    if (slot_tags & &tags).is_empty() {
                                        continue;
                                    }
                                }

                                array.push(*slot_token);
                            }
                        }

                        if !array.is_empty() {
                            if array.len() == 1 {
                                if let Some(slot) = self.slots.get(array[0]) {
                                    send!(self, hook, slot, message);
                                }
                            } else if let Some(id) = array.choose(&mut self.rand) {
                                if let Some(slot) = self.slots.get(*id) {
                                    send!(self, hook, slot, message);
                                }
                            }
                        }
                    } else {
                        // 给每个 SLOT 发送消息
                        // 会根据 TAG 过滤
                        // 这里没有进行过滤 `.filter(|slot_token| **slot_token != token )`
                        // 也就是自己可以收到自己发送的消息
                        for slot_token in tokens.iter() {
                            if let Some(slot) = self.slots.get(*slot_token) {
                                // filter tags
                                if !tags.is_empty() {
                                    let slot_tags = slot.chans.get(&chan).expect("It shouldn't be executed here!");

                                    if (slot_tags & &tags).is_empty() {
                                        continue;
                                    }
                                }

                                send!(self, hook, slot, message);
                            }
                        }
                    }
                }

                // 共享订阅
                // 注意: 共享订阅与普通订阅是两套并行的机制，
                // 不管发送消息时有没有　SHARE　参数，共享订阅始终能收到消息
                if let Some(tokens) = self.share_chans.get(&chan) {
                    let mut array: Vec<usize> = Vec::new();

                    for slot_token in tokens.iter() {
                        if let Some(slot) = self.slots.get(*slot_token) {
                            // filter tags
                            if !tags.is_empty() {
                                let slot_tags = slot.share_chans.get(&chan).expect("It shouldn't be executed here!");

                                if (slot_tags & &tags).is_empty() {
                                    continue;
                                }
                            }

                            array.push(*slot_token);
                        }
                    }

                    if !array.is_empty() {
                        if array.len() == 1 {
                            if let Some(slot) = self.slots.get(array[0]) {
                                send!(self, hook, slot, message);
                            }
                        } else if let Some(slot_token) = array.choose(&mut self.rand) {
                            if let Some(slot) = self.slots.get(*slot_token) {
                                send!(self, hook, slot, message);
                            }
                        }
                    }
                }
            }

        } // end goon

        // slot event
        // {
        //     CHAN: SLOT_SEND,
        //     VALUE: $message
        // }
        let event_message = msg!{
            CHAN: SLOT_SEND,
            VALUE: message
        };

        self.relay_root_message(hook, token, SLOT_SEND, event_message);
    }

    // ATTACH 的时候，可以附带自定义数据，可以通过 Hook.attach 或 SLOT_ATTACH 事件获取
    fn attach(
        &mut self,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) {
        if let Ok(chan) = message.get_str(VALUE).map(ToOwned::to_owned) {
            // check ROOT
            match chan.as_str() {
                SLOT_READY | SLOT_BREAK | SLOT_ATTACH | SLOT_DETACH | SLOT_SEND | SLOT_RECV => {

                    if !self.slots[token].root {
                        Code::PermissionDenied.set(&mut message);

                        self.send_message(hook, token, message);

                        return
                    }

                }
                _ => ()
            }

            // tag
            let mut tags = HashSet::new();

            if let Some(tag) = message.get(TAG) {
                if let Some(tag) = tag.as_str() {
                    tags.insert(tag.to_string());
                } else if let Some(tag_array) = tag.as_array() {
                    for v in tag_array {
                        if let Some(v) = v.as_str() {
                            tags.insert(v.to_string());
                        } else {
                            Code::InvalidTagFieldType.set(&mut message);

                            self.send_message(hook, token, message);

                            return
                        }
                    }
                } else {
                    Code::InvalidTagFieldType.set(&mut message);

                    self.send_message(hook, token, message);

                    return
                }
            }

            // share
            let mut share = false;

            if let Some(share2) = message.get(SHARE) {
                if let Some(share2) = share2.as_bool() {
                    share = share2
                } else {
                    Code::InvalidShareFieldType.set(&mut message);

                    self.send_message(hook, token, message);

                    return
                }
            }

            // 这里可以验证该 SLOT 是否有权限
            let success = hook.attach(&self.slots[token], &mut message, &chan, &tags);

            if !success {
                Code::PermissionDenied.set(&mut message);

                self.send_message(hook, token, message);

                return
            }

            // slot event
            // {
            //     CHAN: SLOT_ATTACH,
            //     VALUE: $chan,
            //     TAG: $tag, // string or array
            //     slot_id: $slot_id
            // }
            let mut event_message = msg!{
                CHAN: SLOT_ATTACH,
                VALUE: &chan,
                SLOT_ID: self.slots[token].id
            };

            if let Some(tag) = message.get(TAG) {
                event_message.insert(TAG, tag.clone());
            }

            // session_attach
            if share {
                event_message.insert(SHARE, true);

                let ids = self.share_chans.entry(chan.to_owned()).or_insert_with(HashSet::new);
                ids.insert(token);

                {
                    let slot = &mut self.slots[token];
                    let set = slot.share_chans.entry(chan).or_insert_with(HashSet::new);
                    set.extend(tags);
                }
            } else {
                let ids = self.chans.entry(chan.to_owned()).or_insert_with(HashSet::new);
                ids.insert(token);

                {
                    let slot = &mut self.slots[token];
                    let set = slot.chans.entry(chan).or_insert_with(HashSet::new);
                    set.extend(tags);
                }
            }

            self.relay_root_message(hook, token, SLOT_ATTACH, event_message);

            Code::Ok.set(&mut message);
        } else {
            Code::CannotGetValueField.set(&mut message);
        }

        self.send_message(hook, token, message);
    }

    // DETACH 的时候，可以附带自定义数据，可以通过 Hook.detach 或 SLOT_DETACH 事件获取
    fn detach(
        &mut self,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) {
        if let Ok(chan) = message.get_str(VALUE).map(ToOwned::to_owned) {
            // tag
            let mut tags = HashSet::new();

            if let Some(tag) = message.get(TAG) {
                if let Some(tag) = tag.as_str() {
                    tags.insert(tag.to_string());
                } else if let Some(tag_array) = tag.as_array() {
                    for v in tag_array {
                        if let Some(v) = v.as_str() {
                            tags.insert(v.to_string());
                        } else {
                            Code::InvalidTagFieldType.set(&mut message);

                            self.send_message(hook, token, message);

                            return
                        }
                    }
                } else {
                    Code::InvalidTagFieldType.set(&mut message);

                    self.send_message(hook, token, message);

                    return
                }
            }

            // share
            let mut share = false;

            if let Some(share2) = message.get(SHARE) {
                if let Some(share2) = share2.as_bool() {
                    share = share2
                } else {
                    Code::InvalidShareFieldType.set(&mut message);

                    self.send_message(hook, token, message);

                    return
                }
            }

            // 这里可以验证该 SLOT 是否有权限
            // 可以让 SLOT 不能 DETACH 某些 CHAN
            let success = hook.detach(&self.slots[token], &mut message, &chan, &tags);

            if !success {
                Code::PermissionDenied.set(&mut message);

                self.send_message(hook, token, message);

                return
            }

            // slot event
            // {
            //     CHAN: SLOT_DETACH,
            //     VALUE: $chan,
            //     TAG: $tag, // string or array
            //     slot_id: $slot_id
            // }
            let mut event_message = msg!{
                CHAN: SLOT_DETACH,
                VALUE: &chan,
                SLOT_ID: self.slots[token].id
            };

            if let Some(tag) = message.get(TAG) {
                event_message.insert(TAG, tag.clone());
            }

            // session_detach
            if share {
                event_message.insert(SHARE, true);

                let slot = &mut self.slots[token];

                // 注意，DETACH 的时候，如果 TAG 为空的话，会将 CHAN 移除掉
                // 否则，只会移除掉对应的 TAG
                if tags.is_empty() {
                    slot.share_chans.remove(&chan);

                    if let Some(ids) = self.share_chans.get_mut(&chan) {
                        ids.remove(&token);

                        if ids.is_empty() {
                            self.share_chans.remove(&chan);
                        }
                    }
                } else if let Some(set) = slot.share_chans.get_mut(&chan) {
                    *set = set.iter().filter(|tag| !tags.contains(*tag)).map(|s| s.to_string()).collect();
                }
            } else {
                let slot = &mut self.slots[token];

                // 注意，DETACH 的时候，如果 TAG 为空的话，会将 CHAN 移除掉
                // 否则，只会移除掉对应的 TAG
                if tags.is_empty() {
                    slot.chans.remove(&chan);

                    if let Some(ids) = self.chans.get_mut(&chan) {
                        ids.remove(&token);

                        if ids.is_empty() {
                            self.chans.remove(&chan);
                        }
                    }
                } else if let Some(set) = slot.chans.get_mut(&chan) {
                    *set = set.iter().filter(|tag| !tags.contains(*tag)).map(|s| s.to_string()).collect();
                }
            }

            self.relay_root_message(hook, token, SLOT_DETACH, event_message);

            Code::Ok.set(&mut message);
        } else {
            Code::CannotGetValueField.set(&mut message);
        }

        self.send_message(hook, token, message);
    }

    fn bind(
        &mut self,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) {
        if let Ok(slot_id) = message.get_message_id(SLOT_ID).map(ToOwned::to_owned) {
            // 这里可以验证该 SLOT 是否有权限
            // 可以让 SLOT 不能 BIND 某些 SLOT_ID
            let success = hook.bind(&self.slots[token], &mut message, slot_id);

            if !success {
                Code::PermissionDenied.set(&mut message);

                self.send_message(hook, token, message);

                return
            }

            if let Some(target_token) = self.slot_ids.get(&slot_id).copied() {
                self.slots[token].bind.insert(target_token);
                self.slots[target_token].bound.insert(token);

                Code::Ok.set(&mut message);
            } else {
                Code::TargetSlotIdNotExist.set(&mut message);
            }
        } else {
            Code::CannotGetSlotIdField.set(&mut message);
        }

        self.send_message(hook, token, message);
    }

    fn unbind(
        &mut self,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) {
        if let Ok(slot_id) = message.get_message_id(SLOT_ID).map(ToOwned::to_owned) {
            // 这里可以验证该 SLOT 是否有权限
            // 可以让 SLOT 不能 BIND 某些 SLOT_ID
            let success = hook.unbind(&self.slots[token], &mut message, slot_id);

            if !success {
                Code::PermissionDenied.set(&mut message);

                self.send_message(hook, token, message);

                return
            }

            if let Some(target_token) = self.slot_ids.get(&slot_id).copied() {
                self.slots[token].bind.remove(&target_token);
                self.slots[target_token].bound.remove(&token);

                Code::Ok.set(&mut message);
            } else {
                Code::TargetSlotIdNotExist.set(&mut message);
            }
        } else {
            Code::CannotGetSlotIdField.set(&mut message);
        }

        self.send_message(hook, token, message);
    }

    fn join(
        &mut self,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) {
        // 这里可以验证该 SLOT 是否有权限
        let success = hook.join(&self.slots[token], &mut message);

        if !success {
            Code::PermissionDenied.set(&mut message);

            self.send_message(hook, token, message);

            return
        }

        self.socket_ids.insert(self.slots[token].id, token);
        self.slots[token].joined = true;

        Code::Ok.set(&mut message);

        self.send_message(hook, token, message);
    }

    fn unjoin(
        &mut self,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) {
        // 这里可以验证该 SLOT 是否有权限
        let success = hook.unjoin(&self.slots[token], &mut message);

        if !success {
            Code::PermissionDenied.set(&mut message);

            self.send_message(hook, token, message);

            return
        }

        self.socket_ids.remove(&self.slots[token].id);
        self.slots[token].joined = false;

        Code::Ok.set(&mut message);

        self.send_message(hook, token, message);
    }

    // PING 的时候可以附带自定义数据，可以通过 Hook.ping 获取
    fn ping(&mut self, hook: &impl Hook, token: usize, mut message: Message) {
        hook.ping(&self.slots[token], &mut message);

        // PING 的时候，会插入 OK: 0
        Code::Ok.set(&mut message);

        self.send_message(hook, token, message);
    }

    fn mine(&self, hook: &impl Hook, token: usize, mut message: Message) {
        if let Some(slot) = self.slots.get(token) {
            let mut chans = Message::new();
            for (chan, tags) in &slot.chans {
                let tags: Vec<&String> = tags.iter().collect();

                chans.insert(chan, tags);
            }

            let mut share_chans = Message::new();
            for (chan, tags) in &slot.share_chans {
                let tags: Vec<&String> = tags.iter().collect();

                share_chans.insert(chan, tags);
            }

            let mut binded = Array::new();

            for bind_token in &slot.bind {
                if let Some(bind_slot) = self.slots.get(*bind_token) {
                    binded.push(bind_slot.id);
                }
            }

            let mut bounded = Array::new();

            for bound_token in &slot.bound {
                if let Some(bound_slot) = self.slots.get(*bound_token) {
                    bounded.push(bound_slot.id);
                }
            }

            let slot = msg!{
                SOCKET_ID: self.socket_id,
                SLOT_ID: slot.id,
                ROOT: slot.root,
                ATTR: slot.wire.attr().clone(),
                CHANS: chans,
                SHARE_CHANS: share_chans,
                SEND_NUM: slot.wire.send_num() as u64,
                RECV_NUM: slot.wire.recv_num() as u64,
                BINDED: binded,
                BOUNDED: bounded,
                JOINED: slot.joined
            };

            message.insert(VALUE, slot);
        }

        Code::Ok.set(&mut message);

        self.send_message(hook, token, message);
    }

    // 注意，QUERY 和 CUSTOM 的不同之处在于，前者必须具有 ROOT 权限，后者不需要
    // 可以在 Hook.query 自行定制返回数据
    fn query(&self, hook: &impl Hook, token: usize, mut message: Message) {
        {
            let slot = &self.slots[token];

            if !slot.root {
                Code::PermissionDenied.set(&mut message);

                self.send_message(hook, token, message);

                return
            }
        }

        hook.query(self, token, &mut message);

        // QUERY 的时候，不会插入 CODE: 0, 由 hook 函数决定

        self.send_message(hook, token, message);
    }

    // 用于实现自定义功能。注意，QUERY 和 CUSTOM 的不同之处在于，前者必须具有 ROOT 权限，后者不需要
    // 可以在 Hook.custom 自行定制返回数据
    fn custom(&self, hook: &impl Hook, token: usize, mut message: Message) {
        hook.custom(self, token, &mut message);

        // CUSTOM 的时候，不会插入 CODE: 0, 由 hook 函数决定

        self.send_message(hook, token, message);
    }

    // 用于控制命令。必须具有 ROOT 权限
    // 可以在 Hook.ctrl 自行定制返回数据
    // 注意，在 Hook.ctrl 能修改 Switch 结构，需谨慎操作
    fn ctrl(&mut self, hook: &impl Hook, token: usize, mut message: Message) {
        {
            let slot = &self.slots[token];

            if !slot.root {
                Code::PermissionDenied.set(&mut message);

                self.send_message(hook, token, message);

                return
            }
        }

        hook.ctrl(self, token, &mut message);

        // QUERY 的时候，不会插入 CODE: 0, 由 hook 函数决定

        self.send_message(hook, token, message);
    }

    // 具有 ROOT 权限的 SLOT 可以 KILL 掉其他 SLOT（包括自己）
    fn kill(
        &mut self,
        epoll: &Epoll,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) -> Result<()> {
        {
            let slot = &self.slots[token];

            if !slot.root {
                Code::PermissionDenied.set(&mut message);

                self.send_message(hook, token, message);

                return Ok(())
            }
        }

        let success = hook.kill(&self.slots[token], &mut message);

        if !success {
            Code::PermissionDenied.set(&mut message);

            self.send_message(hook, token, message);

            return Ok(())
        }

        let remove_token;

        if let Some(slot_id) = message.get(SLOT_ID) {
            if let Some(slot_id) = slot_id.as_message_id() {
                if let Some(other_token) = self.slot_ids.get(slot_id).cloned() {
                    remove_token = Some(other_token);
                } else {
                    Code::TargetSlotIdNotExist.set(&mut message);

                    self.send_message(hook, token, message);

                    return Ok(())
                }
            } else {
                Code::InvalidSlotIdFieldType.set(&mut message);

                self.send_message(hook, token, message);

                return Ok(())
            }
        } else {
            Code::CannotGetSlotIdField.set(&mut message);

            self.send_message(hook, token, message);

            return Ok(())
        }

        Code::Ok.set(&mut message);

        self.send_message(hook, token, message);

        if let Some(remove_token) = remove_token {
            self.del_slot(epoll, hook, remove_token)?;
        }

        Ok(())
    }
}
