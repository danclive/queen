use std::collections::HashSet;

use nson::Message;

use crate::queen::{Sessions, Session};

pub trait Hook: Send + 'static {
    fn accept(&self, _: &Session) -> bool { true }

    fn remove(&self, _: &Session) {}

    fn recv(&self, _: &Session, _: &mut Message) -> bool { true }

    fn send(&self, _: &Session, _: &mut Message) -> bool { true }

    fn auth(&self, _: &Session, _: &mut Message) -> bool { true }

    fn attach(&self, _: &Session, _: &mut Message, _chan: &str, _label: &HashSet<String>) -> bool { true }

    fn detach(&self, _: &Session, _: &mut Message, _chan: &str, _label: &HashSet<String>) -> bool { true }

    fn ping(&self, _: &Session, _: &mut Message) {}

    fn emit(&self, _: &Session, _: &mut Message) -> bool { true }

    fn push(&self, _: &Session, _: &mut Message) -> bool { true }

    fn kill(&self, _: &Session, _: &mut Message) -> bool { true }

    fn query(&self, _: &Sessions, _token: usize, _: &mut Message) {}

    fn custom(&self, _: &Sessions, _token: usize, _: &mut Message) {}
}

impl Hook for () {}
