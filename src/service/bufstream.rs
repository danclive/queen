use std::io::Read;
use std::io::Write;
use std::io::Result as IoResult;
use std::cmp;
use std::mem;

const DEFAULT_CAPACITY: usize = 16 * 1024;

pub struct Stream {
    pub reader: Vec<u8>,
    pub writer: Vec<u8>,
}

impl Stream {
    pub fn new() -> Stream {
        Stream::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Stream {
        Stream {
            reader: Vec::with_capacity(capacity),
            writer: Vec::with_capacity(capacity)
        }
    }
}

impl Read for Stream {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let amt = cmp::min(buf.len(), self.reader.len());
        
        let reader = mem::replace(&mut self.reader, Vec::new());
        let (a, b) = reader.split_at(amt);
        buf[..amt].copy_from_slice(a);
        self.reader = b.to_vec();
        
        Ok(amt)
    }
}

impl Write for Stream {
    #[inline]
    fn write(&mut self, data: &[u8]) -> IoResult<usize> {
        self.writer.write(data)
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

