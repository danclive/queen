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
    // 是否具有超级权限
    pub root: bool,
    // SLOT ATTACH 的 CHAN，以及 LABEL
    pub chans: HashMap<String, HashSet<String>>,
    // 线
    pub wire: Wire<Message>
}

impl Slot {
    pub fn new(token: usize, id: MessageId, root: bool, wire: Wire<Message>) -> Self {
        Self {
            token,
            id,
            root,
            chans: HashMap::new(),
            wire
        }
    }
}
