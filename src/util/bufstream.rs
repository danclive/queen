use std::io::{self, Read, Write};
use std::collections::VecDeque;
use std::cmp;

const DEFAULT_CAPACITY: usize = 2 * 1024;

#[derive(Debug, Default)]
pub struct Stream {
    pub reader: RingBuffer,
    pub writer: RingBuffer,
}

impl Stream {
    pub fn new() -> Stream {
        Stream::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Stream {
        Stream {
            reader: RingBuffer::with_capacity(capacity),
            writer: RingBuffer::with_capacity(capacity)
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RingBuffer {
    inner: VecDeque<u8>
}

impl RingBuffer {
    pub fn new() -> Self {
        RingBuffer {
            inner: VecDeque::new()
        }
    }

    pub fn with_capacity(n: usize) -> Self {
        RingBuffer {
            inner: VecDeque::with_capacity(n)
        }
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn pop(&mut self) -> Option<u8> {
        self.inner.pop_front()
    }

    pub fn push(&mut self, value: u8) {
        self.inner.push_back(value)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn clear(&mut self) {
        self.inner.clear()
    }

    pub fn set_position(&mut self, pos: usize) {
        // for _ in 0..pos {
        //     self.pop();
        // }
        self.inner.drain(0..pos);
    }

    pub fn seek(&self, buf: &mut [u8]) -> io::Result<usize> {
        let amt = cmp::min(buf.len(), self.inner.len());

        for (i, value) in buf[0..amt].iter_mut().enumerate() {
            let v = self.inner[i];
            *value = v
        }

        Ok(amt)
    }
}

impl Write for RingBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        for i in buf.iter() {
            self.push(*i);
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Read for RingBuffer {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let amt = cmp::min(buf.len(), self.inner.len());

        for i in buf[0..amt].iter_mut() {
            let v = self.pop().unwrap();
            *i = v;
        }

        Ok(amt)
    }
}
