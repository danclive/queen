use std::io::{self, Read};
use std::io::ErrorKind::InvalidData;


use crate::MAX_MESSAGE_LEN;

pub fn read_block(reader: &mut impl Read, max_len: Option<usize>) -> io::Result<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes)?;

    let len = u32::from_le_bytes(len_bytes) as usize;

    if len < 5 || len > max_len.unwrap_or(MAX_MESSAGE_LEN) {
        return Err(io::Error::new(InvalidData, format!("Invalid length of {}", len)))
    }

    let mut buf = vec![0u8; len];

    reader.read_exact(&mut buf[4..])?;

    buf[..4].clone_from_slice(&len_bytes);

    assert!(len == buf.len());

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::nson::msg;
    use super::read_block;

    #[test]
    fn test_read_block() {
        let message = msg!{
            "a": 1234,
            "b": 5678
        };
        let vec = message.to_bytes().unwrap();
        let mut reader = Cursor::new(&vec);

        let ret = read_block(&mut reader, None).unwrap();

        assert!(ret == vec);
    }
}
