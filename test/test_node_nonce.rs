use std::thread;
use std::net::TcpStream;
use std::time::Duration;

use rand::{self, RngCore};

use queen::{Node, node::NodeConfig};
use queen::nson::{msg, Message};
use queen::error::ErrorCode;
use queen::util::{write_socket, read_socket};
use queen::crypto::{Method, Aead};
use queen::dict::*;

use super::get_free_addr;

#[test]
fn set_nonce() {
    let mut nonce: Vec<u8> = vec![0u8; 12];
    let mut rand = rand::thread_rng();
    rand.fill_bytes(&mut nonce);

    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.add_tcp(addr2).unwrap();
        config.set_aead_key("queen");

        let mut node = Node::bind(config, ()).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    let mut socket = TcpStream::connect(addr).unwrap();
    let mut aead = Aead::new(&Method::default(), b"queen");

    // auth
    let msg = msg!{
        CHAN: AUTH,
        "username": "aaa",
        "password": "bbb",
        NONCE: 123
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::InvalidNonceFieldType));

    // auth
    let msg = msg!{
        CHAN: AUTH,
        "username": "aaa",
        "password": "bbb",
        NONCE: nonce.clone()
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    aead.set_nonce(&nonce);

    // attach
    let msg = msg!{
        CHAN: ATTACH,
        VALUE: "aaa"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0)
}
