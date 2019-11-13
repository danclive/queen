use std::sync::mpsc::Receiver;

use queen_io::queue::spsc::Queue;

use nson::{Message};

use super::{Packet, Port};

pub struct Recv {
    pub port: Port,
    pub id: usize,
    pub recv: Receiver<Message>
}

impl Iterator for Recv {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        self.recv.recv().ok()
    }
}

impl Drop for Recv {
    fn drop(&mut self) {
        self.port.queue.push(Packet::Detatch(self.id));
    }
}

pub struct AsyncRecv {
    pub port: Port,
    pub id: usize,
    pub recv: Queue<Message>
}

impl AsyncRecv {
    pub fn recv(&self) -> Option<Message> {
        self.recv.pop()
    }
}

impl Drop for AsyncRecv {
    fn drop(&mut self) {
        self.port.queue.push(Packet::Detatch(self.id));
    }
}
