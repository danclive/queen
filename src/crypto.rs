use std::str::FromStr;

use ring::aead::{Algorithm, LessSafeKey, Nonce, UnboundKey, Aad};
use ring::aead::{AES_128_GCM, AES_256_GCM, CHACHA20_POLY1305};
use ring::digest;
use ring::error;

use rand::{self, thread_rng, Rng};

use crate::dict;

#[derive(Debug, Clone, Copy)]
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
        if in_out.len() <= 4 {
            return Err(error::Unspecified)
        }

        let nonce_bytes = Self::rand_nonce();
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);

        let tag = self.inner.seal_in_place_separate_tag(nonce, Aad::empty(), &mut in_out[4..])?;

        in_out.extend_from_slice(tag.as_ref());
        in_out.extend_from_slice(&nonce_bytes);

        let len = (in_out.len() as u32).to_le_bytes();
        in_out[..4].clone_from_slice(&len);

        Ok(())
    }

    pub fn decrypt(&self, in_out: &mut Vec<u8>) -> Result<(), error::Unspecified> {
        if in_out.len() <= 4 + Self::NONCE_LEN + self.inner.algorithm().tag_len() {
            return Err(error::Unspecified)
        }

        let nonce = Nonce::try_assume_unique_for_key(&in_out[(in_out.len() - Self::NONCE_LEN)..])?;

        let end = in_out.len() - Self::NONCE_LEN;

        self.inner.open_in_place(nonce, Aad::empty(), &mut in_out[4..end]).map(|_| {})?;

        in_out.truncate(in_out.len() - self.inner.algorithm().tag_len() - Self::NONCE_LEN);

        let len = (in_out.len() as u32).to_le_bytes();
        in_out[..4].clone_from_slice(&len);

        Ok(())
    }

    pub fn rand_nonce() -> [u8; Self::NONCE_LEN] {
        let mut buf = [0u8; Self::NONCE_LEN];
        thread_rng().fill(&mut buf);

        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_data() {
        let mut data: Vec<u8> = vec![];
        let key = "key123";

        let crypto = Crypto::new(&Method::Aes128Gcm, key.as_bytes());
        assert!(crypto.encrypt(&mut data).is_err());

        let mut data: Vec<u8> = vec![5, 0, 0, 0, 0];
        assert!(crypto.encrypt(&mut data).is_ok());

        assert!(crypto.decrypt(&mut data).is_ok());
        assert!(data == vec![5, 0, 0, 0, 0]);
    }
}
