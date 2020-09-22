use std::collections::{HashMap, HashSet};

use nson::{
    Message,
    message_id::MessageId
};

use crate::Wire;

#[derive(Debug)]
pub struct Slot {
    pub token: usize,
    pub id: MessageId,
    pub root: bool,
    pub chans: HashMap<String, HashSet<String>>,
    pub share_chans: HashMap<String, HashSet<String>>,
    pub wire: Wire<Message>
}

impl Slot {
    pub fn new(token: usize, id: MessageId, root: bool, wire: Wire<Message>) -> Self {
        Self {
            token,
            id,
            root,
            chans: HashMap::new(),
            share_chans: HashMap::new(),
            wire
        }
    }
}
