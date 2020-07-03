use std::{io, result, fmt};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};
use std::time::Duration;
use std::os::unix::io::AsRawFd;
use std::marker::PhantomData;
use std::cell::Cell;

use queen_io::{
    epoll::{Epoll, Token, Ready, EpollOpt, Evented},
    queue::spsc::Queue
};

use queen_io::poll;

use nson::Message;

use crate::util::lock::{Lock, LockGuard};
use crate::error::{Result, SendError, RecvError};

pub struct Wire<T: Send> {
    capacity: usize,
    tx: Queue<result::Result<T, RecvError>>,
    rx: Queue<result::Result<T, RecvError>>,
    close: Arc<AtomicBool>,
    attr: Arc<Lock<Message>>,
    send_num: Cell<usize>,
    recv_num: Cell<usize>,
    _not_sync: PhantomData<*const ()>
}

impl<T: Send> fmt::Debug for Wire<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wire")
         .field("capacity", &self.capacity)
         .field("close", &self.is_close())
         .field("attr", &*self.attr())
         .field("send_num", &self.send_num.get())
         .field("recv_num", &self.recv_num.get())
         .finish()
    }
}

impl<T: Send> Wire<T> {
    pub fn pipe(capacity: usize, attr: Message) -> Result<(Wire<T>, Wire<T>)> {
        let queue1 = Queue::with_cache(capacity)?;
        let queue2 = Queue::with_cache(capacity)?;

        let close = Arc::new(AtomicBool::new(false));
        let attr = Arc::new(Lock::new(attr));

        let wire1 = Wire {
            capacity,
            tx: queue1.clone(),
            rx: queue2.clone(),
            close: close.clone(),
            attr: attr.clone(),
            send_num: Cell::new(0),
            recv_num: Cell::new(0),
            _not_sync: PhantomData
        };

        let wire2 = Wire {
            capacity,
            tx: queue2,
            rx: queue1,
            close,
            attr,
            send_num: Cell::new(0),
            recv_num: Cell::new(0),
            _not_sync: PhantomData
        };

        Ok((wire1, wire2))
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub fn attr(&self) -> LockGuard<Message> {
        self.attr.lock()
    }

    #[inline]
    pub fn close(&self) {
        self.tx.push(Err(RecvError::Disconnected));
        self.close.store(true, Ordering::Release);
    }

    #[inline]
    pub fn is_close(&self) -> bool {
        self.close.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.tx.pending() >= self.capacity
    }

    #[inline]
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

        self.send_num.set(self.send_num.get() + 1);

        Ok(())
    }

    #[inline]
    pub fn send_num(&self) -> usize {
        self.send_num.get()
    }

    pub fn recv(&self) -> result::Result<T, RecvError> {
        match self.rx.pop() {
            Some(data) => {
                if data.is_ok() {
                    self.recv_num.set(self.recv_num.get() + 1);
                }
                data
            },
            None => Err(RecvError::Empty)
        }
    }

    #[inline]
    pub fn recv_num(&self) -> usize {
        self.recv_num.get()
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

impl<T: Send> Drop for Wire<T> {
    fn drop(&mut self) {
        self.close()
    }
}

unsafe impl<T: Send> Send for Wire<T> {}

impl<T: Send> Evented for Wire<T> {
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
    use super::Wire;
    use std::thread;
    use std::time::Duration;

    use nson::msg;
    use crate::error::{RecvError, SendError};

    #[test]
    fn send() {
        let (wire1, wire2) = Wire::<i32>::pipe(2, msg!{}).unwrap();

        assert!(wire1.send(1).is_ok());

        drop(wire2);

        assert!(wire1.send(2).err() == Some(SendError::Disconnected(2)));
    }

    #[test]
    fn send_full() {
        let (wire1, _wire2) = Wire::<i32>::pipe(1, msg!{}).unwrap();

        assert!(wire1.send(1).is_ok());
        assert!(wire1.send(2).err() == Some(SendError::Full(2)));
    }

    #[test]
    fn test_wait_timeout() {
        let (wire1, wire2) = Wire::<i32>::pipe(1, msg!{}).unwrap();

        thread::spawn(move || {
            thread::sleep(Duration::from_secs(2));
            assert!(wire1.send(1).is_ok());
            thread::sleep(Duration::from_secs(2));
        });

        let ret = wire2.wait(Some(Duration::from_secs(1)));

        assert!(ret.is_err());
        match ret {
            Ok(_) => (),
            Err(err) => {
                assert!(matches!(err, RecvError::TimedOut));
            }
        }

        let ret = wire2.wait(Some(Duration::from_secs(2)));

        assert!(ret.is_ok());

        thread::sleep(Duration::from_secs(2));

        let ret = wire2.wait(Some(Duration::from_secs(2)));

        assert!(ret.is_err());
        match ret {
            Ok(_) => (),
            Err(err) => {
                assert!(matches!(err, RecvError::Disconnected));
            }
        }

        assert!(wire2.is_close());
    }

    #[test]
    fn test_modify_attr() {
        let (wire1, wire2) = Wire::<i32>::pipe(1, msg!{"a": 0}).unwrap();

        thread::spawn(move || {
            for _ in 0..1000 {
                let mut attr = wire1.attr();
                let a = attr.get_i32("a").unwrap();
                attr.insert("a", a + 1);
                drop(attr);
            }
        });

        for _ in 0..1000 {
            let mut attr = wire2.attr();
            let a = attr.get_i32("a").unwrap();
            attr.insert("a", a + 1);
            drop(attr);
        }

        thread::sleep(Duration::from_millis(100));

        let attr = wire2.attr();
        assert!(attr.get_i32("a").unwrap() == 2000);
    }
}
