use std::io::{self, ErrorKind::{WouldBlock, BrokenPipe, TimedOut}};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};
use std::time::Duration;
use std::os::unix::io::AsRawFd;

use queen_io::{
    epoll::{Epoll, Token, Ready, EpollOpt, Evented},
    queue::spsc::Queue
};

use queen_io::poll;

use nson::Message;

pub struct Stream<T: Send> {
    pub capacity: usize,
    rx: Queue<io::Result<T>>,
    tx: Queue<io::Result<T>>,
    close: Arc<AtomicBool>,
    pub attr: Message
}

impl<T: Send> Stream<T> {
    pub fn pipe(capacity: usize, attr: Message) -> io::Result<(Stream<T>, Stream<T>)> {
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

    pub fn send(&self, mut data: Option<T>) -> io::Result<()> {
        if !self.is_close() {
            self.tx.push(Ok(data.take().expect("The data sent must not be None")));
            return Ok(())
        }

        Err(io::Error::new(BrokenPipe, "Stream::send"))
    }

    pub fn recv(&self) -> io::Result<T> {
        match self.rx.pop() {
            Some(data) => data,
            None => Err(io::Error::new(WouldBlock, "Stream::recv"))
        }
    }

    pub fn close(&self) {
        self.tx.push(Err(io::Error::new(BrokenPipe, "Stream::close")));
        self.close.store(true, Ordering::Relaxed);
    }

    pub fn is_close(&self) -> bool {
        self.close.load(Ordering::Relaxed)
    }

    pub fn is_full(&self) -> bool {
        self.tx.pending() >= self.capacity
    }

    pub fn wait(&self, timeout: Option<Duration>) -> io::Result<T> {
        let mut events = poll::Events::new();

        events.put(self.rx.as_raw_fd(), poll::Ready::readable());

        poll::poll(&mut events, timeout)?;

        if events.get(0).unwrap().readiness().is_readable() {
            return self.recv()
        }

        Err(io::Error::new(TimedOut, "Stream::wait"))
    }
}

impl<T: Send> Drop for Stream<T> {
    fn drop(&mut self) {
        self.close()
    }
}

impl<T: Send> Evented for Stream<T> {
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

#[cfg(test)]
mod tests {
    use super::Stream;
    use std::thread;
    use std::time::Duration;
    use std::io::ErrorKind::{TimedOut, BrokenPipe};
    use nson::msg;

    #[test]
    fn test_wait_timeout() {
        let (stream1, stream2) = Stream::<i32>::pipe(1, msg!{}).unwrap();

        thread::spawn(move || {
            thread::sleep(Duration::from_secs(2));
            let _ = stream1.send(Some(1));
            thread::sleep(Duration::from_secs(2));
        });

        let ret = stream2.wait(Some(Duration::from_secs(1)));

        assert!(ret.is_err());
        match ret {
            Ok(_) => (),
            Err(err) => {
                assert!(err.kind() == TimedOut)
            }
        }

        let ret = stream2.wait(Some(Duration::from_secs(2)));

        assert!(ret.is_ok());

        thread::sleep(Duration::from_secs(2));

        let ret = stream2.wait(Some(Duration::from_secs(2)));

        assert!(ret.is_err());
        match ret {
            Ok(_) => (),
            Err(err) => {
                assert!(err.kind() == BrokenPipe)
            }
        }

        assert!(stream2.is_close());
    }
}
