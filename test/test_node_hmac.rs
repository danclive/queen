use std::thread;
use std::net::TcpStream;
use std::time::Duration;

use queen::{Node, node::NodeConfig};
use queen::nson::{msg, Message};
use queen::util::{write_socket, read_socket};

use super::get_free_addr;

#[test]
fn no_hmac() {
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
        "_chan": "_ping"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);
}

#[test]
fn hmac() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.add_tcp(addr2).unwrap();
        config.set_hmac_key("queen");

        let mut node = Node::bind(config, ()).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    // no hmac
    let mut socket = TcpStream::connect(&addr).unwrap();

    let msg = msg!{
        "_chan": "_ping"
    };

    assert!(msg.encode(&mut socket).is_err() || Message::decode(&mut socket).is_err());

    // hmac
    let mut socket = TcpStream::connect(addr).unwrap();

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();
    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);
}
