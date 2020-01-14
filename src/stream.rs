use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};

use queen_io::{
    epoll::{Epoll, Token, Ready, EpollOpt, Evented},
    queue::spsc::Queue
};

use nson::{Message, msg};

pub struct Stream {
    pub capacity: usize,
    rx: Queue<Message>,
    tx: Queue<Message>,
    close: Arc<AtomicBool>,
    pub attr: Message
}

impl Stream {
    pub fn pipe(capacity: usize, attr: Message) -> io::Result<(Stream, Stream)> {
        let queue1 = Queue::with_cache(capacity)?;
        let queue2 = Queue::with_cache(capacity)?;

        let close = Arc::new(AtomicBool::new(false));

        let stream1 = Stream {
            capacity,
            tx: queue1.clone(),
            rx: queue2.clone(),
            close: close.clone(),
            attr: attr.clone()
        };

        let stream2 = Stream {
            capacity,
            tx: queue2,
            rx: queue1,
            close,
            attr
        };

        Ok((stream1, stream2))
    }

    pub fn send(&self, message: Message) -> Option<Message> {
        if !self.is_close() {
            self.tx.push(message);
            return None
        }

        Some(message)
    }

    pub fn recv(&self) -> Option<Message> {
        self.rx.pop()
    }

    pub fn close(&self) {
        self.tx.push(msg!{});
        self.close.store(true, Ordering::Relaxed);
    }

    pub fn is_close(&self) -> bool {
        self.close.load(Ordering::Relaxed)
    }

    pub fn is_full(&self) -> bool {
        self.tx.pending() >= self.capacity
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.close()
    }
}

impl Evented for Stream {
    fn add(&self, epoll: &Epoll, token: Token, interest: Ready, opts: EpollOpt) -> io::Result<()> {
        self.rx.add(epoll, token, interest, opts)
    }

    fn modify(&self, epoll: &Epoll, token: Token, interest: Ready, opts: EpollOpt) -> io::Result<()> {
        self.rx.modify(epoll, token, interest, opts)
    }

    fn delete(&self, epoll: &Epoll) -> io::Result<()> {
       self.rx.delete(epoll)
    }
}
