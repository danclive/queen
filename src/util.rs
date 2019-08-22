use std::mem;
use std::io::{self, Read, Write, Error, ErrorKind::InvalidData};

use crate::MAX_MESSAGE_LEN;
use crate::crypto::Aead;

#[inline]
pub fn get_length(buf: &[u8], start: usize) -> usize {
    (
        i32::from(buf[start]) |
        i32::from(buf[start + 1]) << 8 |
        i32::from(buf[start + 2]) << 16 |
        i32::from(buf[start + 3]) << 24
    ) as usize
}

pub fn slice_msg(buf1: &mut Vec<u8>, buf2: &[u8]) -> io::Result<Vec<Vec<u8>>>{
    let mut messages = Vec::new();

    if buf1.is_empty() {
        if buf2.len() < 4 {
            buf1.extend_from_slice(buf2);
        } else {
            let len = get_length(buf2, 0);

            if len > MAX_MESSAGE_LEN {
                return Err(Error::new(InvalidData, "InvalidData"))
            }

            if len > buf2.len() {
                buf1.extend_from_slice(buf2);
            } else if len == buf2.len() {
                messages.push(buf2.to_vec());
            } else {
                // len < buf2.len()
                let mut start = 0;
                let mut end = len;

                loop {
                    messages.push(buf2[start..end].to_vec());

                    if buf2.len() - end < 4 {
                        buf1.extend_from_slice(buf2);
                        break;
                    }

                    let len = get_length(buf2, end);

                    if len > MAX_MESSAGE_LEN {
                        return Err(Error::new(InvalidData, "InvalidData"))
                    }

                    if len > buf2.len() - end {
                        buf1.extend_from_slice(&buf2[end..]);
                        break;
                    } else if len == buf2.len() - end {
                        messages.push(buf2[end..].to_vec());
                        break;
                    } else {
                        // len < buf.len() - end
                        start = end;
                        end += len;
                    }
                }
            }
        }
    } else {
        buf1.extend_from_slice(buf2);

        if buf1.len() >= 4 {
            let len = get_length(buf1, 0);

            if len > MAX_MESSAGE_LEN {
                return Err(Error::new(InvalidData, "InvalidData"))
            }

            if len > buf1.len() {

            } else if len == buf1.len() {
                let data = mem::replace(buf1, Vec::with_capacity(4 * 1024));
                messages.push(data);
            } else {
                // len < buf1.len()
                let mut start = 0;
                let mut end = len;

                loop {
                    messages.push(buf1[start..end].to_vec());

                    if buf1.len() - end < 4 {
                        *buf1 = buf1[end..].to_vec();
                        break;
                    }

                    let len = get_length(buf1, end);

                    if len > MAX_MESSAGE_LEN {
                        return Err(Error::new(InvalidData, "InvalidData"))
                    }

                    if len > buf1.len() - end {
                        *buf1 = buf1[end..].to_vec();
                        break;
                    } else if len == buf1.len() - end {
                        messages.push(buf1[end..].to_vec());
                        mem::replace(buf1, Vec::with_capacity(4 * 1024));
                        break;
                    } else {
                        // len < buf.len() - end
                        start = end;
                        end += len;
                    }
                }
            }
        }
    }

    Ok(messages)
}

pub fn write_socket(writer: &mut impl Write, aead: &mut Aead, mut data: Vec<u8>) -> io::Result<usize> {
    if aead.encrypt(&mut data).is_err() {
        return Err(Error::new(InvalidData, "InvalidData"))
    }

    writer.write(&data)
}

pub fn write_socket_no_aead(writer: &mut impl Write, data: Vec<u8>) -> io::Result<usize> {
    writer.write(&data)
}

pub fn read_socket(reader: &mut impl Read, aead: &mut Aead) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;

    let len = get_length(&len_buf, 0);

    let mut buf = vec![0u8; len - 4];
    reader.read_exact(&mut buf)?;

    let mut data: Vec<u8> = Vec::with_capacity(128);

    data.extend_from_slice(&len_buf);
    data.extend_from_slice(&buf);

    if aead.encrypt(&mut data).is_err() {
        return Err(Error::new(InvalidData, "InvalidData"))
    }

    Ok(data)
}

pub fn read_socket_no_aead(reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;

    let len = get_length(&len_buf, 0);

    let mut buf = vec![0u8; len - 4];
    reader.read_exact(&mut buf)?;

    let mut data: Vec<u8> = Vec::with_capacity(128);

    data.extend_from_slice(&len_buf);
    data.extend_from_slice(&buf);

    Ok(data)
}

#[cfg(test)]
mod test {
    use nson::msg;
    use crate::util::slice_msg;

    #[test]
    fn test1() {
        let msg = msg!{
            "hello": "world",
            "foo": "bar"
        };

        let msg_vec = msg.to_vec().unwrap();

        let ret = slice_msg(&mut vec![], &msg_vec).unwrap();
    
        assert!(ret.len() == 1);
        assert!(ret[0] == msg_vec);
    }

    #[test]
    fn test2() {
        let msg = msg!{
            "hello": "world",
            "foo": "bar"
        };

        let msg_vec = msg.to_vec().unwrap();

        let mut buf1 = vec![];
        let ret = slice_msg(&mut buf1, &msg_vec[0..2]).unwrap();
        assert!(ret.len() == 0);
        assert!(buf1.len() == 2);

        let ret = slice_msg(&mut buf1, &msg_vec[2..10]).unwrap();
        assert!(ret.len() == 0);
        assert!(buf1.len() == 10);

        let ret = slice_msg(&mut buf1, &msg_vec[10..]).unwrap();
        assert!(ret.len() == 1);
        assert!(ret[0] == msg_vec);
        assert!(buf1.len() == 0);
    }

    #[test]
    fn test3() {
        let msg = msg!{
            "hello": "world",
            "foo": "bar"
        };

        let mut msg_vec = msg.to_vec().unwrap();
        msg_vec.extend_from_slice(&msg.to_vec().unwrap());

        let mut buf1 = vec![];
        let ret = slice_msg(&mut buf1, &msg_vec[0..4]).unwrap();
        assert!(ret.len() == 0);
        assert!(buf1.len() == 4);

        let ret = slice_msg(&mut buf1, &msg_vec[4..10]).unwrap();
        assert!(ret.len() == 0);
        assert!(buf1.len() == 10);

        let ret = slice_msg(&mut buf1, &msg_vec[10..40]).unwrap();
        assert!(ret.len() == 1);
        assert!(buf1.len() == 5);

        let ret = slice_msg(&mut buf1, &msg_vec[40..]).unwrap();
        assert!(ret.len() == 1);
        assert!(buf1.len() == 0);
    }
}
