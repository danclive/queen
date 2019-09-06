use std::thread;
use std::net::TcpStream;
use std::time::Duration;

use queen::{Node, node::NodeConfig};
use queen::nson::{msg, Message};
use queen::util::{write_socket, read_socket};
use queen::crypto::{Method, Aead};
use queen::dict::*;

use super::get_free_addr;

#[test]
fn no_aead() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.add_tcp(addr2).unwrap();

        let mut node = Node::bind(config, ()).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    let mut socket = TcpStream::connect(addr).unwrap();

    let msg = msg!{
        CHAN: PING
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);
}

#[test]
fn aead() {
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

    // no hmac
    let mut socket = TcpStream::connect(&addr).unwrap();

    let msg = msg!{
        CHAN: PING
    };

    assert!(msg.encode(&mut socket).is_err() || Message::decode(&mut socket).is_err());

    // hmac
    let mut socket = TcpStream::connect(addr).unwrap();
    let mut aead = Aead::new(&Method::default(), b"queen");

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);
}
