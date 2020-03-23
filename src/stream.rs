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

pub use ext::StreamExt;

mod ext;

pub struct Stream<T: Send> {
    pub tx: StreamTx<T>,
    pub rx: StreamRx<T>
}

impl<T: Send> Stream<T> {
    pub fn pipe(capacity: usize, attr: Message) -> io::Result<(Stream<T>, Stream<T>)> {
        let queue1 = Queue::with_cache(capacity)?;
        let queue2 = Queue::with_cache(capacity)?;

        let close = Arc::new(AtomicBool::new(false));

        let stream1 = Stream {
            tx: StreamTx {
                capacity,
                tx: queue1.clone(),
                close: close.clone(),
                attr: attr.clone()
            },
            rx: StreamRx {
                capacity,
                rx: queue2.clone(),
                close: close.clone(),
                attr: attr.clone()
            }
        };

        let stream2 = Stream {
            tx: StreamTx {
                capacity,
                tx: queue2,
                close: close.clone(),
                attr: attr.clone()
            },
            rx: StreamRx {
                capacity,
                rx: queue1,
                close: close,
                attr: attr
            }
        };

        Ok((stream1, stream2))
    }

    pub fn split(self) -> (StreamTx<T>, StreamRx<T>) {
        (self.tx, self.rx)
    }

    pub fn attr(&self) -> &Message {
        &self.tx.attr
    }

    pub fn send(&self, data: &mut Option<T>) -> io::Result<()> {
        self.tx.send(data)
    }

    pub fn recv(&self) -> io::Result<T> {
        self.rx.recv()
    }

    pub fn close(&self) {
        self.tx.close()
    }

    pub fn is_close(&self) -> bool {
        self.tx.is_close()
    }

    pub fn wait(&self, timeout: Option<Duration>) -> io::Result<T> {
        self.rx.wait(timeout)
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

pub struct StreamTx<T: Send> {
    capacity: usize,
    tx: Queue<io::Result<T>>,
    close: Arc<AtomicBool>,
    attr: Message
}

impl<T: Send> StreamTx<T> {
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn attr(&self) -> &Message {
        &self.attr
    }

    pub fn send(&self, data: &mut Option<T>) -> io::Result<()> {
        if !self.is_close() {
            self.tx.push(Ok(data.take().expect("The data sent must not be None")));
            return Ok(())
        }

        Err(io::Error::new(BrokenPipe, "Stream::send"))
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

    pub fn pending(&self) -> usize {
        self.tx.pending()
    }
}

impl<T: Send> Drop for StreamTx<T> {
    fn drop(&mut self) {
        self.close()
    }
}

pub struct StreamRx<T: Send> {
    capacity: usize,
    rx: Queue<io::Result<T>>,
    close: Arc<AtomicBool>,
    attr: Message
}

impl<T: Send> StreamRx<T> {
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn attr(&self) -> &Message {
        &self.attr
    }

    pub fn recv(&self) -> io::Result<T> {
        match self.rx.pop() {
            Some(data) => data,
            None => Err(io::Error::new(WouldBlock, "Stream::recv"))
        }
    }

    pub fn close(&self) {
        self.close.store(true, Ordering::Relaxed);
    }

    pub fn is_close(&self) -> bool {
        self.close.load(Ordering::Relaxed)
    }

    pub fn is_full(&self) -> bool {
        self.rx.pending() >= self.capacity
    }

    pub fn pending(&self) -> usize {
        self.rx.pending()
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

impl<T: Send> Evented for StreamRx<T> {
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

impl<T: Send> Drop for StreamRx<T> {
    fn drop(&mut self) {
        self.close()
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
    fn send() {
        let (stream1, stream2) = Stream::<i32>::pipe(1, msg!{}).unwrap();

        let mut data = Some(1);

        assert!(stream1.send(&mut data).is_ok());
        assert!(data.is_none());

        drop(stream2);

        let mut data = Some(2);

        assert!(stream1.send(&mut data).is_err());
        assert!(data == Some(2));
    }

    #[test]
    fn test_wait_timeout() {
        let (stream1, stream2) = Stream::<i32>::pipe(1, msg!{}).unwrap();

        thread::spawn(move || {
            thread::sleep(Duration::from_secs(2));
            assert!(stream1.send(&mut Some(1)).is_ok());
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
