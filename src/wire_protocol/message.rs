use std::mem;
use std::io::{Read, Write};

use byteorder::{ReadBytesExt, WriteBytesExt};

use super::header::Header;
use super::header::OpCode;
use super::error::Result;

#[derive(Debug)]
pub struct Message {
    header: Header,
    target: String,
    origin: String,
    content_type: String,
    body: Vec<u8>
}

impl Message {
    pub fn new(request_id: i32, response_to: i32, op_code: OpCode, target: String, origin: String, content_type: String, body: Vec<u8>) -> Result<Message> {
        let header_length = mem::size_of::<Header>();
        let target_length = target.len() + 1;
        let origin_length = origin.len() + 1;
        let content_type_length = content_type.len() + 1;
        let body_length = body.len();

        let total_length = header_length + target_length + origin_length + content_type_length + body_length;

        let header = Header::new(total_length as i32, request_id, response_to, op_code);

        Ok(Message {
            header: header,
            target: target,
            origin: origin,
            content_type: content_type,
            body: body
        })
    }

    pub fn get_opcode(&self) -> &OpCode {
        &self.header.op_code
    }

    pub fn len(&self) -> usize {
        self.header.message_length as usize
    }

    pub fn write<W: Write>(&self, buffer: &mut W) -> Result<()> {
        self.header.write(buffer)?;

        write_cstring(buffer, &self.target)?;
        write_cstring(buffer, &self.origin)?;
        write_cstring(buffer, &self.content_type)?;

        buffer.write(&self.body)?;

        Ok(())
    }

    pub fn read<R: Read>(buffer: &mut R) -> Result<Option<Message>> {
        let header = Header::read(buffer)?;

        let mut length = header.message_length as usize - mem::size_of::<Header>();

        let target = read_cstring(buffer)?;
        length -= target.len() + 1;

        let origin = read_cstring(buffer)?;
        length -= origin.len() + 1;

        let content_type = read_cstring(buffer)?;
        length -= content_type.len() + 1;

        let mut body = vec![0u8; length];

        let read_size = buffer.read(&mut body)?;

        if read_size < length {
            return Ok(None)
        }

        Ok(Some(Message {
            header: header,
            target: target,
            origin: origin,
            content_type: content_type,
            body: body
        }))
    }
}


fn write_cstring<W>(writer: &mut W, s: &str) -> Result<()>
    where W: Write + ?Sized
{
    writer.write_all(s.as_bytes())?;
    writer.write_u8(0)?;
    Ok(())
}

fn read_cstring<R: Read + ?Sized>(reader: &mut R) -> Result<String> {
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
