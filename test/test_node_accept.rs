use std::thread;
use std::net::TcpStream;
use std::os::unix::net::UnixStream;
use std::time::Duration;
use std::fs;

use rand::random;

use queen::{Node, node::Callback, node::NodeConfig};
use queen::nson::{msg, Message};

use super::get_free_addr;

#[test]
fn tcp_accept() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.tcp(addr2).unwrap();

        let mut node = Node::bind(config).unwrap();

        let mut callback = Callback::default();

        callback.accept(|_id, _msg| {
            return false;
        });

        node.set_callback(callback);

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    let mut socket = TcpStream::connect(addr).unwrap();

    let msg = msg!{
        "_chan": "_ping",
        "_tmid": "aaa"
    };

    assert!(msg.encode(&mut socket).is_err() || Message::decode(&mut socket).is_err());
}

#[test]
fn unix_accept() {

    let rand_path = format!("/tmp/.queen-{}.socket", random::<u32>());

    let rand_path2 = rand_path.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.uds(rand_path2);

        let mut node = Node::bind(config).unwrap();

        let mut callback = Callback::default();

        callback.accept(|_id, _msg| {
            return false;
        });

        node.set_callback(callback);

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    let mut socket = UnixStream::connect(&rand_path).unwrap();

    let msg = msg!{
        "_chan": "_ping",
        "_tmid": "aaa"
    };

    assert!(msg.encode(&mut socket).is_err() || Message::decode(&mut socket).is_err());

    let _ = fs::remove_file(rand_path).unwrap();
}
