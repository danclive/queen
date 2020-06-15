use crate::crypto::Crypto;
use crate::error::{Result, Error};
use crate::nson::Message;

pub trait Codec: Send + 'static {
    fn new() -> Self;

    fn decode(&mut self, crypto: &Option<Crypto>, bytes: Vec<u8>) -> Result<Message>;

    fn encode(&mut self, crypto: &Option<Crypto>, message: Message) -> Result<Vec<u8>>;
}

pub struct NsonCodec;

impl Codec for NsonCodec {
    fn new() -> Self {
        NsonCodec
    }

    fn decode(&mut self, crypto: &Option<Crypto>, mut bytes: Vec<u8>) -> Result<Message> {
        if let Some(crypto) = &crypto {
            crypto.decrypt(&mut bytes).map_err(|err|
                Error::InvalidData(format!("{}", err))
            )?;
        }

        let recv = Message::from_slice(&bytes);

        recv.map_err(|err| Error::InvalidData(format!("{}", err)))
    }

    fn encode(&mut self, crypto: &Option<Crypto>, message: Message) -> Result<Vec<u8>> {
        let mut bytes = message.to_vec().map_err(|err| Error::InvalidData(format!("{}", err)) )?;

        if let Some(crypto) = &crypto {
            crypto.encrypt(&mut bytes).map_err(|err|
                Error::InvalidData(format!("{}", err))
            )?;
        }

        Ok(bytes)
    }
}
