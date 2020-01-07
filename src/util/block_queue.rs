use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Condvar};
use std::time::Duration;

#[derive(Debug)]
pub struct BlockQueue<T> where T: Send {
    inner: Arc<BlockQueueInner<T>>
}

#[derive(Debug)]
struct BlockQueueInner<T> {
    queue: Mutex<VecDeque<T>>,
    condvar: Condvar
}

impl<T> BlockQueue<T> where T: Send {
    pub fn with_capacity(capacity: usize) -> BlockQueue<T> {
        BlockQueue {
            inner: Arc::new(BlockQueueInner {
                queue: Mutex::new(VecDeque::with_capacity(capacity)),
                condvar: Condvar::new()
            })
        }
    }

    pub fn push(&self, value: T) {
        let mut queue = self.inner.queue.lock().unwrap();
        queue.push_back(value);

        self.inner.condvar.notify_one();
    }

    pub fn pop(&self) -> T {
        let mut queue = self.inner.queue.lock().unwrap();

        loop {
            if let Some(elem) = queue.pop_front() {
                return elem;
            }

            queue = self.inner.condvar.wait(queue).unwrap();
        }
    }

    pub fn try_pop(&self) -> Option<T> {
        let mut queue = self.inner.queue.lock().unwrap();
        queue.pop_front()
    }

    pub fn pop_timeout(&self, timeout: Duration) -> Option<T> {
        let mut queue = self.inner.queue.lock().unwrap();

        loop {
            if let Some(elem) = queue.pop_front() {
                return Some(elem)
            }

            let result = self.inner.condvar.wait_timeout(queue, timeout).unwrap();
            
            if result.1.timed_out() {
                return None
            }

            queue = result.0;
        }
    }
}

impl<T> Clone for BlockQueue<T> where T: Send {
    fn clone(&self) -> BlockQueue<T> {
        BlockQueue {
            inner: self.inner.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BlockQueue;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn timeout() {
        let queue = BlockQueue::<i32>::with_capacity(4);

        let queue2 = queue.clone();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            queue2.push(1);
        });

        assert!(queue.pop_timeout(Duration::from_millis(10)).is_none());
        assert!(queue.pop_timeout(Duration::from_millis(20)).unwrap() == 1);
    }
}
