use std::cmp;

use ring::aead::{Algorithm, LessSafeKey, Nonce, UnboundKey, Aad};
use ring::aead::{AES_128_GCM, AES_256_GCM, CHACHA20_POLY1305};
use ring::digest;
use ring::error;

#[derive(Debug, Clone)]
pub enum Method {
    Aes128Gcm,
    Aes256Gcm,
    ChaCha20Poly1305
}

impl Method {
    fn algorithm(&self) -> &'static Algorithm {
        match self {
            Method::Aes128Gcm => &AES_128_GCM,
            Method::Aes256Gcm => &AES_256_GCM,
            Method::ChaCha20Poly1305 => &CHACHA20_POLY1305
        }
    }
}

impl Default for Method {
    fn default() -> Self {
        Method::Aes256Gcm
    }
}

#[derive(Debug)]
pub struct Aead {
    aead: LessSafeKey,
    nonce: Vec<u8>
}

impl Aead {
    pub fn new(method: &Method, key: &[u8]) -> Aead {
        let algorithm = method.algorithm();

        let key_len = algorithm.key_len();
        let key = digest::digest(&digest::SHA256, key).as_ref().to_vec();

        let key = UnboundKey::new(algorithm, &key[0..key_len]).expect("Fails if key_bytes.len() != algorithm.key_len()`.");
        let aead = LessSafeKey::new(key);

        let nonce_len = algorithm.nonce_len();
        let mut nonce = vec![0u8; nonce_len];

        nonce[..5].clone_from_slice(&[113, 117, 101, 101, 110]);

        println!("{:?}", nonce);

        Aead {
            aead,
            nonce
        }
    }

    pub fn set_nonce(&mut self, nonce: &[u8]) {
        let min = cmp::min(self.nonce.len(), nonce.len());

        self.nonce[..min].clone_from_slice(&nonce[..min])
    }

    pub fn encrypt(&mut self, in_out: &mut Vec<u8>) -> Result<(), error::Unspecified> {
        let nonce = Nonce::try_assume_unique_for_key(&self.nonce).unwrap();

        let tag = self.aead.seal_in_place_separate_tag(nonce, Aad::empty(), &mut in_out[4..])?;

        in_out.extend_from_slice(tag.as_ref());

        let len = (in_out.len() as i32).to_le_bytes();
        in_out[..4].clone_from_slice(&len);

        Ok(())
    }

    pub fn decrypt(&mut self, in_out: &mut Vec<u8>) -> Result<(), error::Unspecified> {
        let nonce = Nonce::try_assume_unique_for_key(&self.nonce).unwrap();

        self.aead.open_in_place(nonce, Aad::empty(), &mut in_out[4..]).map(|_| {})?;

        unsafe {
            in_out.set_len(in_out.len() - self.aead.algorithm().tag_len());
        }

        let len = (in_out.len() as i32).to_le_bytes();
        in_out[..4].clone_from_slice(&len);

        Ok(())
    }
}
