use std::io;
use std::fmt;

use queen_io::message_queue::MessagesQueue;

#[derive(Clone)]
pub struct BothQueue<T> where T: Send {
    pub tx: MessagesQueue<T>,
    pub rx: MessagesQueue<T>
}

impl<T> BothQueue<T> where T: Send {
    pub fn new(capacity: usize) -> io::Result<BothQueue<T>> {
        Ok(BothQueue {
            tx: MessagesQueue::with_capacity(capacity)?,
            rx: MessagesQueue::with_capacity(capacity)?,
        })
    }
}

impl<T> fmt::Debug for BothQueue<T> where T: Send {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", "both queue")
    }
}
