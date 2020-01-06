use std::mem;
use std::io::{self, Read};
use std::io::ErrorKind::{BrokenPipe, InvalidData};

use crate::MAX_MESSAGE_LEN;

pub fn read_i32(buf: &[u8], start: usize) -> usize {
    (
        i32::from(buf[start]) |
        i32::from(buf[start + 1]) << 8 |
        i32::from(buf[start + 2]) << 16 |
        i32::from(buf[start + 3]) << 24
    ) as usize
}

pub fn read_nonblock(reader: &mut impl Read, buffer: &mut Vec<u8>) -> io::Result<Option<Vec<u8>>> {
    if buffer.is_empty() {
        let mut len_bytes = [0u8; 4];
        let size = reader.read(&mut len_bytes)?;

        if size == 0 {
            return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
        } else if size < 4 {
            buffer.extend_from_slice(&len_bytes[..size]);
            return Ok(None)
        }

        let len = i32::from_le_bytes(len_bytes) as usize;

        if len < 5 || len > MAX_MESSAGE_LEN {
            return Err(io::Error::new(InvalidData, format!("Invalid length of {}", len)))
        }

        let mut buf = vec![0u8; len];

        let size = reader.read(&mut buf[4..])?;

        if size == 0 {
            return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
        } else if size < len - 4 {
            buffer.extend_from_slice(&len_bytes);
            buffer.extend_from_slice(&buf[4..(4 + size)]);
            return Ok(None)
        }

        buf[..4].clone_from_slice(&len_bytes);

        Ok(Some(buf))
    } else {
        if buffer.len() < 4 {
            let need = 4 - buffer.len();
            let mut len_bytes = vec![0u8; need];

            let size = reader.read(&mut len_bytes)?;

            if size == 0 {
                return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
            }

            buffer.extend_from_slice(&len_bytes[0..size]);

            return Ok(None)
        }

        let len = read_i32(&buffer[..4], 0) as usize;

        if len < 5 || len > MAX_MESSAGE_LEN {
            return Err(io::Error::new(InvalidData, format!("Invalid length of {}", len)))
        }

        let mut buf = vec![0u8; len - buffer.len()];

        let size = reader.read(&mut buf)?;

        if size == 0 {
            return Err(io::Error::new(BrokenPipe, "BrokenPipe"))
        } else if size < buf.len() {
            buffer.extend_from_slice(&buf[0..size]);
            return Ok(None)
        }

        buffer.extend_from_slice(&buf);

        let mut vec = Vec::with_capacity(1024);

        mem::swap(buffer, &mut vec);

        Ok(Some(vec))
    }
}

pub fn read_block(reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes)?;

    let len = i32::from_le_bytes(len_bytes) as usize;

    if len < 5 || len > MAX_MESSAGE_LEN {
        return Err(io::Error::new(InvalidData, format!("Invalid length of {}", len)))
    }

    let mut buf = vec![0u8; len];

    reader.read_exact(&mut buf[4..])?;

    buf[..4].clone_from_slice(&len_bytes);

    assert!(len == buf.len());

    Ok(buf)
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::nson::msg;
    use super::{read_nonblock, read_block};

    #[test]
    fn test_empty_message() {
        let message = msg!{};
        let vec = message.to_vec().unwrap();
        let mut buffer: Vec<u8> = vec![];
        let mut reader = Cursor::new(&vec[..]);

        let ret = read_nonblock(&mut reader, &mut buffer).unwrap();

        assert!(ret.unwrap() == vec);
    }

    #[test]
    fn test_nonempty_message() {
        let message = msg!{
            "a": 1234,
            "b": 5678
        };
        let vec = message.to_vec().unwrap();
        let mut buffer: Vec<u8> = vec![];
        let mut reader = Cursor::new(&vec[..]);

        let ret = read_nonblock(&mut reader, &mut buffer).unwrap();

        assert!(ret.unwrap() == vec);
    }

    #[test]
    fn test_slice_data() {
        let message = msg!{
            "a": 1234,
            "b": 5678
        };
        let vec = message.to_vec().unwrap();
        let mut buffer: Vec<u8> = vec![];

        // slice 1
        let mut reader = Cursor::new(&vec[..6]);
        let ret = read_nonblock(&mut reader, &mut buffer).unwrap();
        assert!(ret.is_none());

        // slice 2
        let mut reader = Cursor::new(&vec[6..10]);
        let ret = read_nonblock(&mut reader, &mut buffer).unwrap();
        assert!(ret.is_none());

        // slice 3
        let mut reader = Cursor::new(&vec[10..17]);
        let ret = read_nonblock(&mut reader, &mut buffer).unwrap();
        assert!(ret.is_none());

        // slice 4
        let mut reader = Cursor::new(&vec[17..]);
        let ret = read_nonblock(&mut reader, &mut buffer).unwrap();
        assert!(ret.is_some());

        assert!(ret.unwrap() == vec);
    }

    #[test]
    fn test_slice_data2() {
        let message = msg!{
            "a": 1234,
            "b": 5678
        };
        let vec = message.to_vec().unwrap();
        let mut buffer: Vec<u8> = vec![];

        // slice 1
        let mut reader = Cursor::new(&vec[..2]);
        let ret = read_nonblock(&mut reader, &mut buffer).unwrap();
        assert!(ret.is_none());

        // slice 2
        let mut reader = Cursor::new(&vec[2..9]);
        let ret = read_nonblock(&mut reader, &mut buffer).unwrap();
        assert!(ret.is_none());
        let ret = read_nonblock(&mut reader, &mut buffer).unwrap();
        assert!(ret.is_none());

        // slice 3
        let mut reader = Cursor::new(&vec[9..15]);
        let ret = read_nonblock(&mut reader, &mut buffer).unwrap();
        assert!(ret.is_none());

        // slice 4
        let mut reader = Cursor::new(&vec[15..]);
        let ret = read_nonblock(&mut reader, &mut buffer).unwrap();
        assert!(ret.is_some());

        assert!(ret.unwrap() == vec);
    }

    #[test]
    fn test_bad_len() {
        let message = msg!{
            "a": 1234,
            "b": 5678
        };
        let mut vec = message.to_vec().unwrap();
        let mut buffer: Vec<u8> = vec![];

        vec[3] = 123;
        vec[4] = 234;

        let mut reader = Cursor::new(vec);

        let ret = read_nonblock(&mut reader, &mut buffer);
        assert!(ret.is_err());
    }

    #[test]
    fn test_read_block() {
        let message = msg!{
            "a": 1234,
            "b": 5678
        };
        let vec = message.to_vec().unwrap();
        let mut reader = Cursor::new(&vec);

        let ret = read_block(&mut reader).unwrap();

        assert!(ret == vec);
    }
}
