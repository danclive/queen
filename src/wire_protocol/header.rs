use std::fmt;
use std::io::{Read, Write};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use super::error::Result;
use super::error::Error::ResponseError;

#[derive(Debug, Clone)]
pub enum OpCode {
    Request = 1,
    Response = 2
}

impl OpCode {
    pub fn from_i32(i: i32) -> Option<OpCode> {
        match i {
            1 => Some(OpCode::Request),
            2 => Some(OpCode::Response),
            _ => None
        }
    }
}

impl fmt::Display for OpCode {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            OpCode::Request => write!(fmt, "OP_REQUEST"),
            OpCode::Response => write!(fmt, "OP_RESPONSE")
        }
    }
}

#[derive(Debug, Clone)]
pub struct Header {
    pub message_length: i32,
    pub request_id: i32,
    pub response_to: i32,
    pub op_code: OpCode
}

impl Header {
    pub fn new(message_length: i32, request_id: i32, response_to: i32, op_code: OpCode) -> Header {
        Header {
            message_length: message_length,
            request_id: request_id,
            response_to: response_to,
            op_code: op_code
        }
    }

    pub fn new_request(message_length: i32, request_id: i32) -> Header {
        Header::new(message_length, request_id, 0, OpCode::Request)
    }

    pub fn new_response(message_length: i32, response_to: i32) -> Header {
        Header::new(message_length, 0, response_to, OpCode::Response)
    }

    pub fn write<W: Write>(&self, buffer: &mut W) -> Result<()> {
        buffer.write_i32::<LittleEndian>(self.message_length)?;
        buffer.write_i32::<LittleEndian>(self.request_id)?;
        buffer.write_i32::<LittleEndian>(self.response_to)?;
        buffer.write_i32::<LittleEndian>(self.op_code.clone() as i32)?;

        Ok(())
    }

    pub fn read<R: Read>(buffer: &mut R) -> Result<Header> {
        let message_length = buffer.read_i32::<LittleEndian>()?;
        let request_id = buffer.read_i32::<LittleEndian>()?;
        let response_to = buffer.read_i32::<LittleEndian>()?;
        let op_code_i32 = buffer.read_i32::<LittleEndian>()?;

        let op_code = match OpCode::from_i32(op_code_i32) {
            Some(code) => code,
            _ => return Err(ResponseError(format!("Invalid header opcode from server: {}.", op_code_i32)))
        };

        Ok(Header::new(message_length, request_id, response_to, op_code))
    }
}