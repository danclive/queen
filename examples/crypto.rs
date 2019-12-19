use queen::crypto::{Aead, Method};

fn main() {
    let mut aead = Aead::new(&Method::Aes256Gcm, b"hello");

    println!("{:?}", aead);

    let mut bytes: Vec<u8> = vec![9, 0, 0, 0, 104, 101, 108, 108, 111];

    println!("{:?}", bytes);

    aead.encrypt(&mut bytes).unwrap();

    println!("{:?}", bytes);

    aead.decrypt(&mut bytes).unwrap();

    println!("{:?}", bytes);
}
