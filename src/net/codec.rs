use crate::crypto::Crypto;
use crate::error::Result;
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

    fn decode(&mut self, crypto: &Option<Crypto>, bytes: Vec<u8>) -> Result<Message> {
        Crypto::decrypt_message(crypto, bytes)
    }

    fn encode(&mut self, crypto: &Option<Crypto>, message: Message) -> Result<Vec<u8>> {
        Crypto::encrypt_message(crypto, &message)
    }
}
