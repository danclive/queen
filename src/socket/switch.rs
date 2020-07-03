use std::collections::{HashMap, HashSet};
use std::cell::Cell;

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
use crate::error::{ErrorCode, Result};

use super::Hook;
use super::{Slot, SlotModify};

pub struct Switch {
    pub id: MessageId,
    // CHAN，Token
    pub chans: HashMap<String, HashSet<usize>>,
    // SLOT_ID，Token
    pub slot_ids: HashMap<MessageId, usize>,
    pub slots: Slab<Slot>,
    pub send_num: Cell<usize>,
    pub recv_num: Cell<usize>,
    rand: SmallRng
}

impl Switch {
    pub fn new(id: MessageId) -> Self {
        Self {
            id,
            chans: HashMap::new(),
            slot_ids: HashMap::new(),
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
        wire: Wire<Message>
    ) -> Result<()> {
        let entry = self.slots.vacant_entry();
        let slot = Slot::new(entry.key(), wire);

        // 此处可以验证一下 SLOT 的属性，不过目前只能验证 wire.attr
        // 并且，wire.attr 是可以修改的
        // 但是，为了避免不必要的问题，SLOT 的属性是不能在这里修改的
        let success = hook.accept(&slot);

        if success && matches!(slot.wire.send(msg!{OK: 0i32}), Ok(_)) {
            epoll.add(&slot.wire, Token(entry.key()), Ready::readable(), EpollOpt::level())?;
            entry.insert(slot);
        } else {
            slot.wire.close();
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
            // slot.wire.close();
            epoll.delete(&slot.wire)?;

            for chan in slot.chans.keys() {
                if let Some(ids) = self.chans.get_mut(chan) {
                    ids.remove(&token);

                    if ids.is_empty() {
                        self.chans.remove(chan);
                    }
                }
            }

            // 这里要记得移除 SLOT_ID，因为 wire 在一开始建立连接时就会默认分配一个
            // 认证成功时可以修改
            self.slot_ids.remove(&slot.id);

            hook.remove(&slot);

            // 这里发一个事件，表示有 SLOT 断开
            // 注意，只有在 SLOT_READY 和 SLOT_BREAK 这两个事件才会返回
            // SLOT 的 LABEL 和 ATTR
            // slot event
            // {
            //     CHAN: SLOT_BREAK,
            //     SLOT_ID: $slot_id,
            //     LABEL: $label,
            //     ATTR: $attr
            // }
            let event_message = msg!{
                CHAN: SLOT_BREAK,
                SLOT_ID: slot.id,
                LABEL: slot.label.clone(),
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
                AUTH => self.auth(hook, token, message),
                ATTACH => self.attach(hook, token, message),
                DETACH => self.detach(hook, token, message),
                PING => self.ping(hook, token, message),
                MINE => self.mine(hook, token, message),
                QUERY => self.query(hook, token, message),
                CUSTOM => self.custom(hook, token, message),
                SLOT_KILL => self.kill(epoll, hook, token, message)?,
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
        if let Some(slot) = self.slots.get(token) {
            let success = hook.send(slot, &mut message);

            if success {
                if slot.wire.send(message).is_ok() {
                    self.send_num.set(self.send_num.get() + 1);
                }
            }
        }
    }

    pub(crate) fn relay_root_message(
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

                    let success = hook.send(&slot, &mut message);

                    if success {
                        self.send_message(hook, slot.token, message);
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
        if !self.slots[token].auth {
            ErrorCode::Unauthorized.insert(&mut message);

            self.send_message(hook, token, message);

            return
        }

        let success = hook.emit(&self.slots[token], &mut message);

        if !success {
            ErrorCode::PermissionDenied.insert(&mut message);

            self.send_message(hook, token, message);

            return
        }

        // 如果消息中有 ACK 字段，就发送一条回复消息
        // ACK 可以是任意支持的类型
        // ACK 只是针对 SLOT -> SOCKET，以便于某些情况下 SLOT 控制自己的流速，
        // 在目前的实现中，如果 SLOT 发送过快，填满缓冲区，此时 SOCKET 已经没有
        // 能力进行处理，会丢失溢出的消息。在 SOCKET -> SLOT 时，如果缓冲区满了，也就
        // 意味着 SLOT 的网络拥堵，或者 SLOT 处理能力不足，会丢失溢出的消息，但
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
        // 也就是此时 FROM == 自己的 SLOT_ID

        // P2P 的优先级比较高
        // 不管 SLOT 是否 ATTACH，都可给其发送消息
        // 忽略 LABEL
        // 自己可以收到自己发送的消息
        let mut to_ids = vec![];

        if let Some(to) = message.get(TO).cloned() {
            if let Some(to_id) = to.as_message_id() {
                // TO 可以是单个 SLOT_ID，如果 SLOT_ID 不存在，应当反馈错误
                // 错误信息中应当包含不存在的 SLOT_ID
                if !self.slot_ids.contains_key(to_id) {
                    ErrorCode::TargetSlotIdNotExist.insert(&mut message);
                    message.insert(SLOT_ID, to_id);

                    self.send_message(hook, token, message);

                    return
                }

                to_ids.push(to_id.clone());
            } else if let Some(to_array) = to.as_array() {
                // TO 也可以是一个数组，但是只要其中一个 SLOT_ID 不存在，应当反馈错误
                // 错误信息中应当包含不存在的 SLOT_ID
                // 注意: 如果 TO 为数组，且为空，会按照常规消息发送
                let mut not_exist_ids = vec![];

                for to in to_array {
                    if let Some(to_id) = to.as_message_id() {
                        if self.slot_ids.contains_key(to_id) {
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
                    ErrorCode::TargetSlotIdNotExist.insert(&mut message);
                    message.insert(SLOT_ID, not_exist_ids);

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

        message.insert(FROM, &self.slots[token].id.clone());

        macro_rules! send {
            ($self: ident, $hook: ident, $slot: ident, $message: ident) => {
                let success = $hook.push(&$slot, &mut $message);

                if success {
                    $self.send_message($hook, $slot.token, $message.clone());

                    // slot event
                    // {
                    //     CHAN: SLOT_RECV,
                    //     VALUE: $message
                    // }
                    let event_message = msg!{
                        CHAN: SLOT_RECV,
                        VALUE: $message.clone(),
                        TO: $slot.id.clone()
                    };

                    let id = $slot.token;

                    $self.relay_root_message($hook, id, SLOT_RECV, event_message);
                }
            };
        }

        // 发送 P2P 消息
        // 注意: 如果 TO 为数组，且为空，会按照常规消息发送
        if !to_ids.is_empty() {
            // no_consumers = false;

            // 如果存在 SHARE 字段，且值为 true，则只会随机给其中一个 SLOT 发送消息
            if message.get_bool(SHARE).ok().unwrap_or(false) {
                if to_ids.len() == 1 {
                    if let Some(slot_id) = self.slot_ids.get(&to_ids[0]) {
                        if let Some(slot) = self.slots.get(*slot_id) {
                            send!(self, hook, slot, message);
                        }
                    }
                } else if let Some(to) = to_ids.choose(&mut self.rand) {
                    if let Some(slot_id) = self.slot_ids.get(to) {
                        if let Some(slot) = self.slots.get(*slot_id) {
                            send!(self, hook, slot, message);
                        }
                    }
                }
            } else {
                for to in &to_ids {
                    if let Some(slot_id) = self.slot_ids.get(to) {
                        if let Some(slot) = self.slots.get(*slot_id) {
                            send!(self, hook, slot, message);
                        }
                    }
                }
            }
        // 如果存在 SHARE 字段，且值为 true，则只会随机给其中一个 SLOT 发送消息
        // 会根据 LABEL 过滤
        } else if message.get_bool(SHARE).ok().unwrap_or(false) {
            let mut array: Vec<usize> = Vec::new();

            if let Some(ids) = self.chans.get(&chan) {
                // 这里没有进行过滤 `.filter(|id| **id != token )`
                // 也就是自己可以收到自己发送的消息
                for slot_id in ids.iter() {
                    if let Some(slot) = self.slots.get(*slot_id) {
                        // filter labels
                        if !labels.is_empty() {
                            let slot_labels = slot.chans.get(&chan).expect("It shouldn't be executed here!");

                            // if !slot_labels.iter().any(|l| labels.contains(l)) {
                            //     continue
                            // }
                            if (slot_labels & &labels).is_empty() {
                                continue;
                            }
                        }

                        array.push(*slot_id);
                    }
                }
            }

            if !array.is_empty() {
                // no_consumers = false;

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

        // 给每个 SLOT 发送消息
        // 会根据 LABEL 过滤
        } else if let Some(ids) = self.chans.get(&chan) {
            // 这里没有进行过滤 `.filter(|id| **id != token )`
            // 也就是自己可以收到自己发送的消息
            for slot_id in ids.iter() {
                if let Some(slot) = self.slots.get(*slot_id) {
                    // filter labels
                    if !labels.is_empty() {
                        let slot_labels = slot.chans.get(&chan).expect("It shouldn't be executed here!");

                        if !slot_labels.iter().any(|l| labels.contains(l)) {
                            continue
                        }
                    }

                    send!(self, hook, slot, message);
                }
            }
        }

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

        // send reply message
        if let Some(reply_message) = reply_message {
            self.send_message(hook, token, reply_message);
        }
    }

    pub(crate) fn auth(
        &mut self,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) {
        let mut slot = &mut self.slots[token];

        let mut modify = SlotModify::default();

        // ROOT
        if let Some(s) = message.get(ROOT) {
            // ROOT 字段的值只能为 bool
            if let Some(s) = s.as_bool() {
                modify.root = Some(s);
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
                modify.label = Some(label.clone());
            } else {
                ErrorCode::InvalidLabelFieldType.insert(&mut message);

                self.send_message(hook, token, message);

                return
            }
        }

        // SLOT_ID
        if let Some(slot_id) = message.get(SLOT_ID) {
            // SLOT_ID 的类型必须为 MessageId
            if let Some(slot_id) = slot_id.as_message_id() {
                if let Some(other_token) = self.slot_ids.get(slot_id) {
                    // SLOT ID 不能重复，且不能挤掉已存在的 SLOT
                    if *other_token != token {
                        ErrorCode::DuplicateSlotId.insert(&mut message);

                        self.send_message(hook, token, message);

                        return
                    }
                }

                modify.id = Some(slot_id.clone());
            } else {
                ErrorCode::InvalidSlotIdFieldType.insert(&mut message);

                self.send_message(hook, token, message);

                return
            }
        }

        // 这里可以验证自定义字段的合法性
        // 这里可以验证超级用户的合法性
        // 这里可以验证 SLOT_ID 的合法性
        let success = hook.auth(&slot, &modify, &mut message);

        if !success {
            ErrorCode::AuthenticationFailed.insert(&mut message);

            self.send_message(hook, token, message);

            return
        }

        if let Some(id) = modify.id {
            // 认证时, 可以改变 SLOT_ID
            self.slot_ids.remove(&slot.id);
            // 认证成功时，设置
            // 记得在 SLOT 掉线时移除
            self.slot_ids.insert(id.clone(), token);
            slot.id = id;
        } else {
            // 认证时，如果没有提供 SLOT_ID, 返回上次认证成功的 SLOT_ID
            message.insert(SLOT_ID, slot.id.clone());
            // 认证成功时，设置
            // 记得在 SLOT 掉线时移除
            self.slot_ids.insert(slot.id.clone(), token);
        }

        if let Some(label) = modify.label {
            slot.label = label;
        } else {
            // 如果没有提供 LABEL，返回上次认证成功的 LABEL
            // 如果 LABEL 为空的话，就没必要返回了
            if !slot.label.is_empty() {
                message.insert(LABEL, slot.label.clone());
            }
        }

        if let Some(root) = modify.root {
            slot.root = root;
        } else {
            // 如果没有提供 ROOT，返回上次认证成功的 ROOT
            message.insert(ROOT, slot.root);
        }

        slot.auth = true;

        message.insert(SOCKET_ID, self.id.clone());

        ErrorCode::OK.insert(&mut message);

        // 这里发一个事件，表示有 SLOT 认证成功，准备好接收消息了
        // 注意，只有在 SLOT_READY 和 SLOT_BREAK 这两个事件才会返回
        // SLOT 的 LABEL 和 ATTR
        // slot event
        // {
        //     CHAN: SLOT_READY,
        //     ROOT: $slot.root,
        //     SLOT_ID: $slot_id,
        //     LABEL: $label,
        //     ATTR: $attr
        // }
        let event_message = msg!{
            CHAN: SLOT_READY,
            ROOT: slot.root,
            SLOT_ID: slot.id.clone(),
            LABEL: slot.label.clone(),
            ATTR: slot.wire.attr().clone()
        };

        // 之所以在这里才发送，是因为上面使用了 slot 的几个字段
        self.send_message(hook, token, message);

        self.relay_root_message(hook, token, SLOT_READY, event_message);
    }

    // ATTACH 的时候，可以附带自定义数据，可以通过 Hook.attach 或 SLOT_ATTACH 事件获取
    pub(crate) fn attach(
        &mut self,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) {
        // check auth
        if !self.slots[token].auth {
            ErrorCode::Unauthorized.insert(&mut message);

            self.send_message(hook, token, message);

            return
        }

        if let Ok(chan) = message.get_str(VALUE).map(ToOwned::to_owned) {
            // check ROOT
            match chan.as_str() {
                SLOT_READY | SLOT_BREAK | SLOT_ATTACH | SLOT_DETACH | SLOT_SEND | SLOT_RECV => {

                    if !self.slots[token].root {
                        ErrorCode::PermissionDenied.insert(&mut message);

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

            // 这里可以验证该 SLOT 是否有权限
            let success = hook.attach(&self.slots[token], &mut message, &chan, &labels);

            if !success {
                ErrorCode::PermissionDenied.insert(&mut message);

                self.send_message(hook, token, message);

                return
            }

            // slot event
            // {
            //     CHAN: SLOT_ATTACH,
            //     VALUE: $chan,
            //     LABEL: $label, // string or array
            //     slot_id: $slot_id
            // }
            let mut event_message = msg!{
                CHAN: SLOT_ATTACH,
                VALUE: &chan,
                SLOT_ID: self.slots[token].id.clone()
            };

            if let Some(label) = message.get(LABEL) {
                event_message.insert(LABEL, label.clone());
            }

            // session_attach
            let ids = self.chans.entry(chan.to_owned()).or_insert_with(HashSet::new);
            ids.insert(token);

            {
                let slot = &mut self.slots[token];
                let set = slot.chans.entry(chan).or_insert_with(HashSet::new);
                set.extend(labels);
            }

            self.relay_root_message(hook, token, SLOT_ATTACH, event_message);

            ErrorCode::OK.insert(&mut message);
        } else {
            ErrorCode::CannotGetValueField.insert(&mut message);
        }

        self.send_message(hook, token, message);
    }

    // DETACH 的时候，可以附带自定义数据，可以通过 Hook.detach 或 SLOT_DETACH 事件获取
    pub(crate) fn detach(
        &mut self,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) {
        // check auth
        if !self.slots[token].auth {
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

            // 这里可以验证该 SLOT 是否有权限
            // 可以让 SLOT 不能 DETACH 某些 CHAN
            let success = hook.detach(&self.slots[token], &mut message, &chan, &labels);

            if !success {
                ErrorCode::PermissionDenied.insert(&mut message);

                self.send_message(hook, token, message);

                return
            }

            // slot event
            // {
            //     CHAN: SLOT_DETACH,
            //     VALUE: $chan,
            //     LABEL: $label, // string or array
            //     slot_id: $slot_id
            // }
            let mut event_message = msg!{
                CHAN: SLOT_DETACH,
                VALUE: &chan,
                SLOT_ID: self.slots[token].id.clone()
            };

            if let Some(label) = message.get(LABEL) {
                event_message.insert(LABEL, label.clone());
            }

            // session_detach
            {
                let slot = &mut self.slots[token];

                // 注意，DETACH 的时候，如果 LABEL 为空的话，会将 CHAN 移除掉
                // 否则，只会移除掉对应的 LABEL
                if labels.is_empty() {
                    slot.chans.remove(&chan);

                    if let Some(ids) = self.chans.get_mut(&chan) {
                        ids.remove(&token);

                        if ids.is_empty() {
                            self.chans.remove(&chan);
                        }
                    }
                } else if let Some(set) = slot.chans.get_mut(&chan) {
                    *set = set.iter().filter(|label| !labels.contains(*label)).map(|s| s.to_string()).collect();
                }
            }

            self.relay_root_message(hook, token, SLOT_DETACH, event_message);

            ErrorCode::OK.insert(&mut message);
        } else {
            ErrorCode::CannotGetValueField.insert(&mut message);
        }

        self.send_message(hook, token, message);
    }

    // PING 的时候可以附带自定义数据，可以通过 Hook.ping 获取
    pub(crate) fn ping(&mut self, hook: &impl Hook, token: usize, mut message: Message) {
        hook.ping(&self.slots[token], &mut message);

        // PING 的时候，会插入 OK: 0
        ErrorCode::OK.insert(&mut message);

        self.send_message(hook, token, message);
    }

    pub(crate) fn mine(&self, hook: &impl Hook, token: usize, mut message: Message) {
        if let Some(slot) = self.slots.get(token) {
            let mut chans = Message::new();

            for (chan, labels) in &slot.chans {
                let labels: Vec<&String> = labels.iter().collect();

                chans.insert(chan, labels);
            }

            let slot = msg!{
                AUTH: slot.auth,
                ROOT: slot.root,
                CHANS: chans,
                SLOT_ID: slot.id.clone(),
                LABEL: slot.label.clone(),
                ATTR: slot.wire.attr().clone(),
                SEND_NUM: slot.wire.send_num() as u64,
                RECV_NUM: slot.wire.recv_num() as u64
            };

            message.insert(VALUE, slot);
        }

        ErrorCode::OK.insert(&mut message);

        self.send_message(hook, token, message);
    }

    // 注意，QUERY 和 CUSTOM 的不同之处在于，前者必须具有 ROOT 权限，后者不需要
    // 可以在 Hook.query 自行定制返回数据
    pub(crate) fn query(&self, hook: &impl Hook, token: usize, mut message: Message) {
        {
            let slot = &self.slots[token];

            if !slot.auth {
                ErrorCode::Unauthorized.insert(&mut message);

                self.send_message(hook, token, message);

                return
            }

            if !slot.root {
                ErrorCode::PermissionDenied.insert(&mut message);

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
            let slot = &self.slots[token];

            if !slot.auth {
                ErrorCode::Unauthorized.insert(&mut message);

                self.send_message(hook, token, message);

                return
            }
        }

        hook.custom(&self, token, &mut message);

        // CUSTOM 的时候，不会插入 OK: 0, 由 hook 函数决定

        self.send_message(hook, token, message);
    }

    // 具有 ROOT 权限的 SLOT 可以 KILL 掉其他 SLOT（包括自己）
    pub(crate) fn kill(
        &mut self,
        epoll: &Epoll,
        hook: &impl Hook,
        token: usize,
        mut message: Message
    ) -> Result<()> {
        {
            let slot = &self.slots[token];

            if !slot.auth {
                ErrorCode::Unauthorized.insert(&mut message);

                self.send_message(hook, token, message);

                return Ok(())
            }

            if !slot.root {
                ErrorCode::PermissionDenied.insert(&mut message);

                self.send_message(hook, token, message);

                return Ok(())
            }
        }

        let success = hook.kill(&self.slots[token], &mut message);

        if !success {
            ErrorCode::PermissionDenied.insert(&mut message);

            self.send_message(hook, token, message);

            return Ok(())
        }

        let remove_token;

        if let Some(slot_id) = message.get(SLOT_ID) {
            if let Some(slot_id) = slot_id.as_message_id() {
                if let Some(other_token) = self.slot_ids.get(slot_id).cloned() {
                    remove_token = Some(other_token);
                } else {
                    ErrorCode::TargetSlotIdNotExist.insert(&mut message);

                    self.send_message(hook, token, message);

                    return Ok(())
                }
            } else {
                ErrorCode::InvalidSlotIdFieldType.insert(&mut message);

                self.send_message(hook, token, message);

                return Ok(())
            }
        } else {
            ErrorCode::CannotGetSlotIdField.insert(&mut message);

            self.send_message(hook, token, message);

            return Ok(())
        }

        ErrorCode::OK.insert(&mut message);

        self.send_message(hook, token, message);

        if let Some(remove_token) = remove_token {
            self.del_slot(epoll, hook, remove_token)?;
        }

        Ok(())
    }
}
