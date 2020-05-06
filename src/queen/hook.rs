use std::collections::HashSet;

use nson::Message;

use crate::queen::{Slot, Client};

pub trait Hook: Send + 'static {
    fn accept(&self, _: &Client) -> bool { true }

    fn remove(&self, _: &Client) {}

    fn recv(&self, _: &Client, _: &mut Message) -> bool { true }

    fn send(&self, _: &Client, _: &mut Message) -> bool { true }

    fn auth(&self, _: &Client, _: &mut Message) -> bool { true }

    fn attach(&self, _: &Client, _: &mut Message, _chan: &str, _label: &HashSet<String>) -> bool { true }

    fn detach(&self, _: &Client, _: &mut Message, _chan: &str, _label: &HashSet<String>) -> bool { true }

    fn ping(&self, _: &Client, _: &mut Message) {}

    fn emit(&self, _: &Client, _: &mut Message) -> bool { true }

    fn push(&self, _: &Client, _: &mut Message) -> bool { true }

    fn kill(&self, _: &Client, _: &mut Message) -> bool { true }

    fn query(&self, _: &Slot, _token: usize, _: &mut Message) {}

    fn custom(&self, _: &Slot, _token: usize, _: &mut Message) {}
}

impl Hook for () {}
