use std::net::{TcpStream};
use std::io::{
    self, Write,
    ErrorKind::InvalidData
};

use crate::dict::*;
use crate::nson::{msg, Message};
use crate::crypto::Crypto;
use crate::net::CryptoOptions;
use crate::util::message::read_block;

#[derive(Debug)]
pub struct Stream(pub TcpStream);

impl Stream {
    pub fn read(&self) -> io::Result<Vec<u8>> {
        read_block(&mut &self.0)
    }

    pub fn write(&self, data: &[u8]) -> io::Result<()> {
        (&mut &self.0).write_all(&data)
    }

    pub fn handshake(&self, options: &Option<CryptoOptions>) -> io::Result<Message> {
        if let Some(options) = options {
            let message = msg!{
                CHAN: HANDSHAKE,
                METHOD: options.method.as_str(),
                ACCESS: options.access.clone()
            };

            let data = message.to_vec().expect("InvalidData");
            self.write(&data)?;
        } else {
            let message = msg!{
                CHAN: HANDSHAKE
            };

            let data = message.to_vec().expect("InvalidData");
            self.write(&data)?;
        }

        let data = self.read()?;
        let recv = Message::from_slice(&data);

        recv.map_err(|err| io::Error::new(InvalidData, format!("{}", err)))
    }

    pub fn ping(&self, crypto: &Option<Crypto>) -> io::Result<Message> {
        let ping_msg = msg!{
            CHAN: PING
        };

        let data = Crypto::encrypt_message(crypto, &ping_msg)?;
        self.write(&data)?;

        let data2 = self.read()?;
        let ret = Crypto::decrypt_message(crypto, data2)?;

        Ok(ret)
    }

    pub fn auth(&self, crypto: &Option<Crypto>, auth_message: Option<Message>) -> io::Result<Message> {
        let mut message = msg!{
            CHAN: AUTH
        };

        if let Some(auth_message) = auth_message {
            message.extend(auth_message);
        }

        let data = Crypto::encrypt_message(crypto, &message)?;
        self.write(&data)?;

        let data2 = self.read()?;
        let ret = Crypto::decrypt_message(crypto, data2)?;

        Ok(ret)
    }
}
