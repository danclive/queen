use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering, spin_loop_hint};

#[derive(Debug)]
pub struct Lock<T: ?Sized> {
    lock: AtomicBool,
    data: UnsafeCell<T>,
}

pub struct LockGuard<'a, T: ?Sized + 'a> {
    lock: &'a AtomicBool,
    data: &'a mut T
}

unsafe impl<T: Send> Send for Lock<T> {}
unsafe impl<T: Send> Sync for Lock<T> {}

impl<T> Lock<T> {
    pub fn new(t: T) -> Lock<T> {
        Lock {
            lock: AtomicBool::new(false),
            data: UnsafeCell::new(t),
        }
    }

    pub fn try_lock(&self) -> Option<LockGuard<T>> {
        if match self.lock.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed) {
            Ok(x) => x,
            Err(x) => x,
        } {
            return None
        }

        Some(LockGuard {
            lock: &self.lock,
            data: unsafe { &mut *self.data.get() }
        })
    }

    pub fn lock(&self) -> LockGuard<T> {
        while match self.lock.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed) {
            Ok(x) => x,
            Err(x) => x,
        } {
            while self.lock.load(Ordering::Relaxed) {
                spin_loop_hint();
            }
        }

        LockGuard {
            lock: &self.lock,
            data: unsafe { &mut *self.data.get() }
        }
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn force_unlock(&self) {
        self.lock.store(false, Ordering::Release);
    }

    pub fn is_lock(&self) -> bool {
        self.lock.load(Ordering::Relaxed)
    }
}

impl<'a, T: ?Sized> Deref for LockGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.data
    }
}

impl<'a, T: ?Sized> DerefMut for LockGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.data
    }
}

impl<'a, T: ?Sized> Drop for LockGuard<'a, T> {
    fn drop(&mut self) {
        self.lock.store(false, Ordering::Release);
    }
}

impl<T: ?Sized + Default> Default for Lock<T> {
    fn default() -> Lock<T> {
        Lock::new(Default::default())
    }
}

#[cfg(test)]
mod tests {
    use super::Lock;

    #[test]
    fn smoke() {
        let a = Lock::new(1);
        let mut a1 = a.try_lock().unwrap();
        assert!(a.try_lock().is_none());
        assert_eq!(*a1, 1);
        *a1 = 2;
        drop(a1);
        assert_eq!(*a.try_lock().unwrap(), 2);
        assert_eq!(*a.try_lock().unwrap(), 2);
    }
}
