use std::collections::HashSet;

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
    pub joined: bool,
    pub chans: HashSet<String>,
    pub share_chans: HashSet<String>,
    pub bind: HashSet<usize>,
    pub bound: HashSet<usize>,
    pub wire: Wire<Message>
}

impl Slot {
    pub fn new(token: usize, id: MessageId, root: bool, wire: Wire<Message>) -> Self {
        Self {
            token,
            id,
            root,
            joined: false,
            chans: HashSet::new(),
            share_chans: HashSet::new(),
            bind: HashSet::new(),
            bound: HashSet::new(),
            wire
        }
    }
}
