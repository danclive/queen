use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::AtomicBool;
use std::marker::PhantomData;

#[derive(Debug)]
pub struct Lock<T> {
    locked: AtomicBool,
    data: UnsafeCell<T>,
}

pub struct LockGuard<'a, T> {
    lock: &'a Lock<T>,
    _p: PhantomData<std::rc::Rc<()>>,
}

unsafe impl<T: Send> Send for Lock<T> {}
unsafe impl<T: Send> Sync for Lock<T> {}

impl<T> Lock<T> {
    pub fn new(t: T) -> Lock<T> {
        Lock {
            locked: AtomicBool::new(false),
            data: UnsafeCell::new(t),
        }
    }

    pub fn try_lock(&self) -> Option<LockGuard<T>> {
        if self.locked.compare_exchange(false, true, SeqCst, SeqCst).is_err() {
            return None;
        }

        Some(LockGuard { lock: self, _p: PhantomData })
    }
}

impl<'a, T> Deref for LockGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.lock.data.get() }
    }
}

impl<'a, T> DerefMut for LockGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<'a, T> Drop for LockGuard<'a, T> {
    fn drop(&mut self) {
        self.lock.locked.store(false, SeqCst);
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
