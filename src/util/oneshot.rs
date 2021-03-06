// The MIT License

// Copyright (c) 2017 Takeru Ohta <phjgt308@gmail.com>

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// This code is copy pasted from https://github.com/sile/nbchan
// Thanks to the author.

use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::time::Duration;
use std::os::unix::io::AsRawFd;
use std::result;

use queen_io::{waker::Waker, poll};

use crate::error::{Result, Error, SendError, RecvError};

pub fn channel<T>() -> Result<(Sender<T>, Receiver<T>)> {
    let (shared0, shared1) = SharedBox::allocate();
    let waker = Waker::new()?;
    let tx = Sender { sb: shared0, waker: waker.clone() };
    let rx = Receiver { sb: shared1, waker };
    Ok((tx, rx))
}

/// The sending-half of an asynchronous oneshot channel.
#[derive(Debug)]
pub struct Sender<T> {
    sb: SharedBox<T>,
    waker: Waker
}

impl<T> Sender<T> {
    pub fn send(mut self, t: T) -> result::Result<(), SendError<T>> {
        let new = into_raw_ptr(t);
        let old = self.sb.swap(new);

        if old == mark_empty() {
            // Secceeded.
            self.sb.abandon();
            let _ = self.waker.wakeup();
            Ok(())
        } else {
            // Failed; the receiver already has dropped.
            debug_assert_eq!(old, mark_dropped());
            let t = from_raw_ptr(self.sb.load());
            self.sb.release();
            Err(SendError::Disconnected(t))
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.sb.is_available() {
            // This channel has not been used.
            let old = self.sb.swap(mark_dropped());
            if old == mark_dropped() {
                // The peer (i.e., receiver) dropped first.
                self.sb.release();
            }
        }
        let _ = self.waker.wakeup();
    }
}

unsafe impl<T: Send> Send for Sender<T> {}

/// The receiving-half of an asynchronous oneshot channel.
#[derive(Debug)]
pub struct Receiver<T> {
    sb: SharedBox<T>,
    waker: Waker
}

impl<T> Receiver<T> {
    /// Attempts to return a pending value on this receiver without blocking.
    pub fn try_recv(&mut self) -> result::Result<T, RecvError> {
        if !self.sb.is_available() {
            return Err(RecvError::Disconnected);
        }

        let ptr = self.sb.load();
        if ptr == mark_empty() {
            Err(RecvError::Empty)
        } else if ptr == mark_dropped() {
            self.sb.release();
            Err(RecvError::Disconnected)
        } else {
            let t = from_raw_ptr(ptr);
            self.sb.release();
            Ok(t)
        }
    }

    pub fn wait(&self, timeout: Option<Duration>) -> Result<()> {
        if poll::wait(self.waker.as_raw_fd(), poll::Ready::readable(), timeout)?.is_readable() {
            return Ok(())
        }

        Err(Error::TimedOut("Receiver.wait".to_string()))
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        if self.sb.is_available() {
            let old = self.sb.swap(mark_dropped());
            if old != mark_empty() {
                // The peer (i.e., sender) dropped first.
                if old != mark_dropped() {
                    // The channel has an unreceived item.
                    let _t = from_raw_ptr(old);
                }
                self.sb.release();
            }
        }
    }
}

unsafe impl<T: Send> Send for Receiver<T> {}

#[derive(Debug, Clone)]
struct SharedBox<T>(*mut AtomicPtr<T>);

impl<T> SharedBox<T> {
    #[inline]
    pub fn allocate() -> (Self, Self) {
        let ptr = into_raw_ptr(AtomicPtr::default());
        (SharedBox(ptr), SharedBox(ptr))
    }

    #[inline]
    pub fn release(&mut self) {
        debug_assert_ne!(self.0, ptr::null_mut());
        let _ = from_raw_ptr(self.0);
        self.0 = ptr::null_mut();
    }

    #[inline]
    pub fn abandon(&mut self) {
        debug_assert_ne!(self.0, ptr::null_mut());
        self.0 = ptr::null_mut();
    }

    #[inline]
    pub fn is_available(&self) -> bool {
        !self.0.is_null()
    }

    #[inline]
    pub fn swap(&self, value: *mut T) -> *mut T {
        debug_assert_ne!(self.0, ptr::null_mut());
        unsafe { &*self.0 }.swap(value, Ordering::SeqCst)
    }

    #[inline]
    pub fn load(&self) -> *mut T {
        unsafe { &*self.0 }.load(Ordering::SeqCst)
    }
}

unsafe impl<T: Send> Send for SharedBox<T> {}

#[inline]
fn mark_dropped<T>() -> *mut T {
    static MARK_DROPPED: &u8 = &0;
    MARK_DROPPED as *const _ as _
}

#[inline]
fn mark_empty<T>() -> *mut T {
    ptr::null_mut()
}

#[inline]
fn into_raw_ptr<T>(t: T) -> *mut T {
    Box::into_raw(Box::new(t))
}

#[inline]
fn from_raw_ptr<T>(ptr: *mut T) -> T {
    unsafe { *Box::from_raw(ptr) }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn send_and_recv_succeeds() {
        let (tx, mut rx) = channel().unwrap();
        tx.send(1).unwrap();
        assert_eq!(rx.try_recv(), Ok(1));
    }

    #[test]
    fn send_succeeds() {
        let (tx, rx) = channel().unwrap();
        tx.send(1).unwrap();
        drop(rx);
    }

    #[test]
    fn send_fails() {
        let (tx, rx) = channel().unwrap();
        drop(rx);
        assert_eq!(tx.send(1), Err(SendError::Disconnected(1)));
    }

    #[test]
    fn recv_fails() {
        let (tx, mut rx) = channel::<()>().unwrap();
        assert_eq!(rx.try_recv(), Err(RecvError::Empty));
        drop(tx);
        assert_eq!(rx.try_recv(), Err(RecvError::Disconnected));
    }

    #[test]
    fn unused() {
        channel::<()>().unwrap();
    }
}
