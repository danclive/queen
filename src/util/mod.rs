use std::io::{Read, Write};

use byteorder::{ReadBytesExt, WriteBytesExt};

use error::Result;

pub mod bufstream;
pub mod both_queue;

pub fn write_cstring<W>(writer: &mut W, s: &str) -> Result<()>
    where W: Write + ?Sized
{
    writer.write_all(s.as_bytes())?;
    writer.write_u8(0)?;
    Ok(())
}

pub fn read_cstring<R: Read + ?Sized>(reader: &mut R) -> Result<String> {
    let mut v = Vec::new();

    loop {
        let c = reader.read_u8()?;
        if c == 0 {
            break;
        }
        v.push(c);
    }

    Ok(String::from_utf8(v)?)
}
