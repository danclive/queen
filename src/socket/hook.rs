use std::collections::HashSet;

use nson::Message;

use super::{Switch, Slot, SlotModify};

pub trait Hook: Send + 'static {
    fn accept(&self, _: &Slot) -> bool { true }

    fn remove(&self, _: &Slot) {}

    fn recv(&self, _: &Slot, _: &mut Message) -> bool { true }

    fn send(&self, _: &Slot, _: &mut Message) -> bool { true }

    fn auth(&self, _: &Slot, _: &SlotModify, _: &mut Message) -> bool { true }

    fn attach(&self, _: &Slot, _: &mut Message, _chan: &str, _label: &HashSet<String>) -> bool { true }

    fn detach(&self, _: &Slot, _: &mut Message, _chan: &str, _label: &HashSet<String>) -> bool { true }

    fn ping(&self, _: &Slot, _: &mut Message) {}

    fn emit(&self, _: &Slot, _: &mut Message) -> bool { true }

    fn push(&self, _: &Slot, _: &mut Message) -> bool { true }

    fn kill(&self, _: &Slot, _: &mut Message) -> bool { true }

    fn query(&self, _: &Switch, _token: usize, _: &mut Message) {}

    fn custom(&self, _: &Switch, _token: usize, _: &mut Message) {}

    fn ctrl(&self, _: &mut Switch, _token: usize, _: &mut Message) {}

    fn stop(&self, _: &Switch) {}
}

pub struct NonHook;

impl Hook for NonHook {}
impl Hook for () {}
