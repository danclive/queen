use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};
use std::time::Duration;
use std::os::unix::io::AsRawFd;
use std::marker::PhantomData;
use std::result;

use queen_io::{
    epoll::{Epoll, Token, Ready, EpollOpt, Evented},
    queue::spsc::Queue
};

use queen_io::poll;

use nson::Message;

use crate::util::lock::{Lock, LockGuard};
use crate::error::{Result, SendError, RecvError};

pub struct Stream<T: Send> {
    capacity: usize,
    tx: Queue<result::Result<T, RecvError>>,
    rx: Queue<result::Result<T, RecvError>>,
    close: Arc<AtomicBool>,
    attr: Arc<Lock<Message>>,
    _not_sync: PhantomData<*const ()>
}

impl<T: Send> Stream<T> {
    pub fn pipe(capacity: usize, attr: Message) -> Result<(Stream<T>, Stream<T>)> {
        let queue1 = Queue::with_cache(capacity)?;
        let queue2 = Queue::with_cache(capacity)?;

        let close = Arc::new(AtomicBool::new(false));
        let attr = Arc::new(Lock::new(attr));

        let stream1 = Stream {
            capacity,
            tx: queue1.clone(),
            rx: queue2.clone(),
            close: close.clone(),
            attr: attr.clone(),
            _not_sync: PhantomData
        };

        let stream2 = Stream {
            capacity,
            tx: queue2,
            rx: queue1,
            close: close,
            attr: attr,
            _not_sync: PhantomData
        };

        Ok((stream1, stream2))
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn attr(&self) -> LockGuard<Message> {
        loop {
            if let Some(attr) = self.attr.try_lock() {
                return attr
            }
        }
    }

    pub fn close(&self) {
        self.tx.push(Err(RecvError::Disconnected));
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

    pub fn send(&self, data: T) -> result::Result<(), SendError<T>> {
        if self.is_close() {
            return Err(SendError::Disconnected(data))
        }

        if self.is_full() {
            return Err(SendError::Full(data))
        }

        self.tx.push(Ok(data));

        Ok(())
    }

    pub fn recv(&self) -> result::Result<T, RecvError> {
        match self.rx.pop() {
            Some(data) => data,
            None => Err(RecvError::Empty)
        }
    }

    pub fn wait(&self, timeout: Option<Duration>) -> result::Result<T, RecvError> {
        match poll::wait(self.rx.as_raw_fd(), poll::Ready::readable(), timeout) {
            Ok(event) => {
                if event.is_readable() {
                    return self.recv()
                }

                Err(RecvError::TimedOut)
            },
            Err(_) => {
                self.close();
                Err(RecvError::Disconnected)
            }
        }
    }
}

impl<T: Send> Drop for Stream<T> {
    fn drop(&mut self) {
        self.close()
    }
}

unsafe impl<T: Send> Send for Stream<T> {}

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

    use nson::msg;
    use crate::error::{RecvError, SendError};

    #[test]
    fn send() {
        let (stream1, stream2) = Stream::<i32>::pipe(2, msg!{}).unwrap();

        assert!(stream1.send(1).is_ok());

        drop(stream2);

        assert!(stream1.send(2).err() == Some(SendError::Disconnected(2)));
    }

    #[test]
    fn send_full() {
        let (stream1, _stream2) = Stream::<i32>::pipe(1, msg!{}).unwrap();

        assert!(stream1.send(1).is_ok());
        assert!(stream1.send(2).err() == Some(SendError::Full(2)));
    }

    #[test]
    fn test_wait_timeout() {
        let (stream1, stream2) = Stream::<i32>::pipe(1, msg!{}).unwrap();

        thread::spawn(move || {
            thread::sleep(Duration::from_secs(2));
            assert!(stream1.send(1).is_ok());
            thread::sleep(Duration::from_secs(2));
        });

        let ret = stream2.wait(Some(Duration::from_secs(1)));

        assert!(ret.is_err());
        match ret {
            Ok(_) => (),
            Err(err) => {
                assert!(matches!(err, RecvError::TimedOut));
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
                assert!(matches!(err, RecvError::Disconnected));
            }
        }

        assert!(stream2.is_close());
    }

    #[test]
    fn test_modify_attr() {
        let (stream1, stream2) = Stream::<i32>::pipe(1, msg!{"a": 0}).unwrap();

        thread::spawn(move || {
            for _ in 0..1000 {
                let mut attr = stream1.attr();
                let a = attr.get_i32("a").unwrap();
                attr.insert("a", a + 1);
                drop(attr);
            }
        });

        for _ in 0..1000 {
            let mut attr = stream2.attr();
            let a = attr.get_i32("a").unwrap();
            attr.insert("a", a + 1);
            drop(attr);
        }

        thread::sleep(Duration::from_millis(100));

        let attr = stream2.attr();
        assert!(attr.get_i32("a").unwrap() == 2000);
    }
}
