use std::fmt::{self, Debug};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

pub fn oneshot<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner::new());

    let receiver = Receiver {
        inner: inner.clone()
    };

    let sender = Sender {
        inner,
        send: false
    };

    (sender, receiver)
}

struct Inner<T> {
    payload: Mutex<Option<T>>,
    cond: Condvar,
}

impl<T> Inner<T> {
    fn new() -> Inner<T> {
        Inner {
            payload: Mutex::new(None),
            cond: Condvar::new(),
        }
    }

    fn send(&self, t: T) {
        let mut lock = self.payload.lock().unwrap();

        *lock = Some(t);

        self.cond.notify_one();
    }

    fn wait(&self) -> T {
        let mut lock = self.payload.lock().unwrap();

        while lock.is_none() {
            lock = self.cond.wait(lock).unwrap();
        }

        lock.take().unwrap()
    }

    fn wait_timeout(&self, timeout: Duration) -> Option<T> {
        let lock = self.payload.lock().unwrap();

        let (mut lock, result) = self.cond.wait_timeout(lock, timeout).unwrap();

        if result.timed_out() {
            return None
        }

        Some(lock.take().unwrap())
    }
}

pub struct Receiver<T> {
    inner: Arc<Inner<Option<T>>>
}

impl<T> Receiver<T> {
    pub fn is_ready(&self) -> bool {
        // relaxed variant
        Arc::strong_count(&self.inner) == 1
    }

    pub fn wait(self) -> Option<T> {
        self.inner.wait()
    }

    pub fn wait_timeout(self, timeout: Duration) -> Option<T> {
        match self.inner.wait_timeout(timeout) {
            Some(ret) => ret,
            None => None
        }
    }

    pub fn try_recv(self) -> Result<Option<T>, Receiver<T>> {
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => Ok(inner.wait()),
            Err(inner) => Err(Receiver { inner }),
        }
    }
}
impl<T> Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Receiver")
    }
}

pub struct Sender<T> {
    inner: Arc<Inner<Option<T>>>,
    send: bool
}

impl<T> Sender<T> {
    pub fn is_needed(&self) -> bool {
        // relaxed variant
        Arc::strong_count(&self.inner) == 2
    }

    pub fn send(mut self, t: T) {
        self.inner.send(Some(t));
        self.send = true;
    }
}

impl<T> Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Sender")
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if !self.send {
            self.inner.send(None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::oneshot;
    use std::thread;
    use std::time::Duration;
    
    #[test]
    fn test_wait_setting() {
        let (tx, rx) = oneshot();

        let h = thread::spawn(move || {
            thread::sleep(Duration::from_millis(500));
            tx.send(5);
        });

        assert_eq!(rx.wait(), Some(5));

        h.join().unwrap();
    }
    
    #[test]
    fn test_wait_getting() {
        let (tx, rx) = oneshot();

        let h = thread::spawn(move || {
            tx.send(3);
        });

        thread::sleep(Duration::from_millis(500));

        assert_eq!(rx.wait(), Some(3));

        h.join().unwrap();
    }
    
    #[test]
    fn test_drop_setter() {
        let (tx, rx) = oneshot::<i32>();

        let h = thread::spawn(move || {
            let _tx = tx;
        });

        thread::sleep(Duration::from_millis(500));

        assert_eq!(rx.wait(), None);

        h.join().unwrap();
    }

    #[test]
    fn test_wait_timeout() {
        let (tx, rx) = oneshot::<i32>();

        let h = thread::spawn(move || {
            thread::sleep(Duration::from_millis(500));
            tx.send(123);
        });

        assert_eq!(rx.wait_timeout(Duration::from_millis(100)), None);

        h.join().unwrap();
    }

    #[test]
    fn test_wait_timeout2() {
        let (tx, rx) = oneshot::<i32>();

        let h = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            tx.send(123);
        });

        assert_eq!(rx.wait_timeout(Duration::from_millis(500)), Some(123));

        h.join().unwrap();
    }

    #[test]
    fn is_needed() {
        let (tx, rx) = oneshot::<i32>();

        assert_eq!(tx.is_needed(), true);

        drop(rx);

        assert_eq!(tx.is_needed(), false);
    }
}
