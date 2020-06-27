use std::collections::{HashMap, HashSet};

use nson::{
    Message,
    message_id::MessageId
};

use crate::Wire;

#[derive(Debug)]
pub struct Slot {
    pub token: usize,
    // 默认情况下会随机生成一个，可以在认证时修改
    pub id: MessageId,
    // 属性，可以在认证时设置
    pub label: Message,
    // 是否认证
    pub auth: bool,
    // 是否具有超级权限
    pub root: bool,
    // SLOT ATTACH 的 CHAN，以及 LABEL
    pub chans: HashMap<String, HashSet<String>>,
    // 线
    pub wire: Wire<Message>
}

impl Slot {
    pub fn new(token: usize, wire: Wire<Message>) -> Self {
        Self {
            token,
            id: MessageId::new(),
            label: Message::new(),
            auth: false,
            root: false,
            chans: HashMap::new(),
            wire
        }
    }
}

#[derive(Debug, Default)]
pub struct SlotModify {
    pub id: Option<MessageId>,
    pub label: Option<Message>,
    pub root: Option<bool>
}
