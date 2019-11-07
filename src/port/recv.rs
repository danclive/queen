use std::os::unix::io::AsRawFd;
use std::sync::mpsc::Receiver;
use std::os::unix::io::RawFd;

use queen_io::queue::spsc::Queue;
use queen_io::queue::mpsc;

use nson::{Message};

use super::Packet;

pub struct Recv {
    pub(crate) queue: mpsc::Queue<Packet>,
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
        self.queue.push(Packet::Detatch(self.id));
    }
}

pub struct AsyncRecv {
    pub(crate) queue: mpsc::Queue<Packet>,
    pub id: usize,
    pub recv: Queue<Message>
}

impl AsyncRecv {
    pub fn recv(&self) -> Option<Message> {
        self.recv.pop()
    }
}

impl AsRawFd for AsyncRecv {
    fn as_raw_fd(&self) -> RawFd {
        self.recv.as_raw_fd()
    }
}

impl Drop for AsyncRecv {
    fn drop(&mut self) {
        self.queue.push(Packet::Detatch(self.id));
    }
}
