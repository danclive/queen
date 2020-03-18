use std::cmp;
use std::str::FromStr;
use std::io::{self, ErrorKind::InvalidData};

use ring::aead::{Algorithm, LessSafeKey, Nonce, UnboundKey, Aad};
use ring::aead::{AES_128_GCM, AES_256_GCM, CHACHA20_POLY1305};
use ring::digest;
use ring::error;

use rand::{self, thread_rng, Rng};

use nson::Message;

use crate::dict;

#[derive(Debug, Clone)]
pub enum Method {
    Aes128Gcm,
    Aes256Gcm,
    ChaCha20Poly1305
}

impl Method {
    pub fn algorithm(&self) -> &'static Algorithm {
        match self {
            Method::Aes128Gcm => &AES_128_GCM,
            Method::Aes256Gcm => &AES_256_GCM,
            Method::ChaCha20Poly1305 => &CHACHA20_POLY1305
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Method::Aes128Gcm => dict::AES_128_GCM,
            Method::Aes256Gcm => dict::AES_256_GCM,
            Method::ChaCha20Poly1305 => dict::CHACHA20_POLY1305
        }
    }
}

impl FromStr for Method {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            dict::AES_128_GCM => Ok(Method::Aes128Gcm),
            dict::AES_256_GCM => Ok(Method::Aes256Gcm),
            dict::CHACHA20_POLY1305 => Ok(Method::ChaCha20Poly1305),
            _ => Err(())
        }
    }
}

impl Default for Method {
    fn default() -> Self {
        Method::Aes128Gcm
    }
}

#[derive(Debug)]
pub struct Crypto {
    inner: LessSafeKey
}

impl Crypto {
    pub const NONCE_LEN: usize = 96 / 8;

    pub fn new(method: &Method, key: &[u8]) -> Self {
        let algorithm = method.algorithm();

        let key_len = algorithm.key_len();
        let key = digest::digest(&digest::SHA256, key).as_ref().to_vec();

        let key = UnboundKey::new(algorithm, &key[0..key_len]).expect("Fails if key_bytes.len() != algorithm.key_len()`.");

        Self {
            inner: LessSafeKey::new(key)
        }
    }

    pub fn encrypt(&self, in_out: &mut Vec<u8>) -> Result<(), error::Unspecified> {
        let nonce_bytes = Self::rand_nonce();
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);

        let tag = self.inner.seal_in_place_separate_tag(nonce, Aad::empty(), &mut in_out[4..])?;

        in_out.extend_from_slice(tag.as_ref());
        in_out.extend_from_slice(&nonce_bytes);

        let len = (in_out.len() as u32).to_le_bytes();
        in_out[..4].clone_from_slice(&len);

        Ok(())
    }

    pub fn encrypt_message(crypto: &Option<Crypto>, message: &Message) -> io::Result<Vec<u8>> {
        let mut data = message.to_vec().expect("InvalidData");

        if let Some(crypto) = &crypto {
            let _ = crypto.encrypt(&mut data).map_err(|err|
                io::Error::new(InvalidData, format!("{}", err)
            ));
        }

        Ok(data)
    }

    pub fn decrypt(&self, in_out: &mut Vec<u8>) -> Result<(), error::Unspecified> {
        let nonce = Nonce::try_assume_unique_for_key(&in_out[(in_out.len() - Self::NONCE_LEN)..])?;

        let end = in_out.len() - Self::NONCE_LEN;

        self.inner.open_in_place(nonce, Aad::empty(), &mut in_out[4..end]).map(|_| {})?;

        in_out.truncate(in_out.len() - self.inner.algorithm().tag_len() - Self::NONCE_LEN);

        let len = (in_out.len() as u32).to_le_bytes();
        in_out[..4].clone_from_slice(&len);

        Ok(())
    }

    pub fn decrypt_message(crypto: &Option<Crypto>, mut data: Vec<u8>) -> io::Result<Message> {
        if let Some(crypto) = &crypto {
            let _ = crypto.decrypt(&mut data).map_err(|err|
                io::Error::new(InvalidData, format!("{}", err)
            ));
        }

        let recv = Message::from_slice(&data);

        recv.map_err(|err| io::Error::new(InvalidData, format!("{}", err)))
    }

    pub fn init_nonce() -> [u8; Self::NONCE_LEN] {
        let mut nonce = [0u8; Self::NONCE_LEN];
        nonce[..5].clone_from_slice(&[113, 117, 101, 101, 110]);

        nonce
    }

    pub fn rand_nonce() -> [u8; Self::NONCE_LEN] {
        let mut buf = [0u8; Self::NONCE_LEN];
        thread_rng().fill(&mut buf);

        buf
    }

    pub fn increase_nonce(nonce: &mut [u8; Self::NONCE_LEN]) {
        for i in nonce {
            if std::u8::MAX == *i {
                *i = 0;
            } else {
                *i += 1;
                return;
            }
        }
    }

    pub fn set_nonce(nonce: &mut [u8; Self::NONCE_LEN], bytes: &[u8]) {
        let min = cmp::min(nonce.len(), bytes.len());
        nonce[..min].clone_from_slice(&bytes[..min]);
    }
}
