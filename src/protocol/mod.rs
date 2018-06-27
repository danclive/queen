use std::io::{Read, Write};
use std::io::{Error, ErrorKind};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use error::Result;
use util::{write_cstring, read_cstring};

/// Struct of the message
///
/// ```
/// //  00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
/// // +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
/// // |                                         Message Length                                        |
/// // +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
/// // |                                           Message ID                                          |
/// // +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
/// // |                                             Origin                                            |
/// // +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
/// // |                                             Topic...                                          |
/// // +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
/// // |         OpCode        |     Content Type      |                   Reserved                    |
/// // +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
/// // |                                                                                               |
/// // //                                                                                             //
/// // //                                            Data                                             //
/// // //                                                                                             //
/// // |                                                                                               |
/// // +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
///
/// ```

bitflags! {
    pub struct OpCode: u8 {
        const CONNECT       = 0x01;
        const CONNACK       = 0x02;
        const PING          = 0x03;
        const PONG          = 0x04;

        const REQUEST       = 0x11;
        const RESPONSE      = 0x12;

        const SUBSCRIBE     = 0x21;
        const SUBACK        = 0x22;
        const UNSUBSCRIBE   = 0x23;
        const UNSUBACK      = 0x24;
        const PUBLISH       = 0x25;
        const PUBACK        = 0x26;

        const UNKNOW        = 0x00;
        const ERROR         = 0xFF;
    }
}

bitflags! {
    pub struct ContentType: u8 {
        const EMPTY         = 0x00;
        const BINARY        = 0x01;
        const NSON          = 0x02;
        const TEXT          = 0x03;
        const JSON          = 0x04;
    }
}

impl Default for OpCode {
    fn default() -> OpCode {
        OpCode::UNKNOW
    }
}

impl Default for ContentType {
    fn default() -> ContentType {
        ContentType::EMPTY
    }
}


#[derive(Debug, Clone, Default, PartialEq)]
pub struct Message {
    pub message_id: u32,
    pub origin: u32,
    pub topic: String,
    pub opcode: OpCode,
    pub content_type: u8,
    pub body: Vec<u8>
}

impl Message {
    pub fn new() -> Message {
        Message::default()
    }

    pub fn len(&self) -> usize {
        4 * 4 + self.topic.len() + 1 + self.body.len()
    }

    pub fn is_empty(&self) -> bool {
        self.body.is_empty()
    }

    pub fn write<W: Write>(&self, buffer: &mut W) -> Result<()> {
        let length = self.len();

        buffer.write_u32::<LittleEndian>(length as u32)?;
        buffer.write_u32::<LittleEndian>(self.message_id)?;
        buffer.write_u32::<LittleEndian>(self.origin)?;
        write_cstring(buffer, &self.topic)?;
        buffer.write_u8(self.opcode.bits())?;
        buffer.write_u8(self.content_type)?;
        buffer.write_u16::<LittleEndian>(0)?;
        buffer.write_all(&self.body)?;

        Ok(())
    }

    pub fn read<R: Read>(buffer: &mut R) -> Result<Message> {
        let length = buffer.read_u32::<LittleEndian>()?;

        let message_id = buffer.read_u32::<LittleEndian>()?;
        let origin = buffer.read_u32::<LittleEndian>()?;
        let topic = read_cstring(buffer)?;
        let opcode = buffer.read_u8()?;
        let content_type = buffer.read_u8()?;
        let _reserved = buffer.read_u16::<LittleEndian>();

        let body_legnth = (length - 4 * 4 - topic.len() as u32 - 1) as usize;

        let body = if body_legnth > 0 {
            let mut buf = vec![0u8; body_legnth];

            if buffer.read(&mut buf)? != body_legnth {
                return Err(Error::new(ErrorKind::UnexpectedEof, "Body length not enough").into())
            }

            buf
        } else {
            vec![]
        };

        let opcode = OpCode::from_bits(opcode).unwrap_or_default();

        Ok(Message {
            message_id,
            origin,
            topic,
            opcode,
            content_type,
            body
        })
    }
}


#[test]
fn message_write() {
    let mut buf: Vec<u8> = Vec::new();
    let message = Message::new();
    message.write(&mut buf).unwrap();
}

#[test]
fn message_read() {
    let mut message = Message::new();
    message.message_id = 1;
    message.origin = 2;
    message.topic = "topic".to_owned();
    message.opcode = OpCode::CONNECT;
    message.content_type = 5;
    message.body = vec![7, 8, 9];

    let mut buf: Vec<u8> = Vec::new();

    message.write(&mut buf).unwrap();

    let mut reader = ::std::io::Cursor::new(buf);

    let message2 = Message::read(&mut reader).unwrap();

    assert_eq!(message, message2);
}

#[test]
fn message_read_body_not_enogth() {
    let mut message = Message::new();
    message.message_id = 1;
    message.origin = 2;
    message.topic = "topic".to_owned();
    message.opcode = OpCode::CONNECT;
    message.content_type = 5;
    message.body = vec![7, 8, 9];

    let mut buf: Vec<u8> = Vec::new();

    message.write(&mut buf).unwrap();

    let mut reader = ::std::io::Cursor::new(&buf[0..20]);

    assert!(Message::read(&mut reader).is_err());
}
