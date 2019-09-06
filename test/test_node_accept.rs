use std::thread;
use std::net::TcpStream;
use std::os::unix::net::UnixStream;
use std::time::Duration;
use std::fs;

use rand::random;

use queen::{Node, node::Callback, node::NodeConfig};
use queen::nson::msg;
use queen::util::{write_socket, read_socket};
use queen::crypto::{Method, Aead};
use queen::dict::*;

use super::get_free_addr;

#[test]
fn tcp_accept() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.add_tcp(addr2).unwrap();
        config.set_aead_key("queen");

        let mut node = Node::bind(config, ()).unwrap();

        let mut callback = Callback::default();

        callback.accept(|_id, _msg, _| {
            return false;
        });

        node.set_callback(callback);

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    let mut socket = TcpStream::connect(addr).unwrap();
    let mut aead = Aead::new(&Method::default(), b"queen");

    let msg = msg!{
        CHAN: PING
    };

    let data = msg.to_vec().unwrap();
    let r1 = write_socket(&mut socket, &mut aead, data).is_err();
    let r2 = read_socket(&mut socket, &mut aead).is_err();
    assert!(r1 || r2);
}

#[test]
fn unix_accept() {

    let rand_path = format!("/tmp/.queen-{}.socket", random::<u32>());

    let rand_path2 = rand_path.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.add_uds(rand_path2);
        config.set_aead_key("queen");

        let mut node = Node::bind(config, ()).unwrap();

        let mut callback = Callback::default();

        callback.accept(|_id, _msg, _| {
            return false;
        });

        node.set_callback(callback);

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    let mut socket = UnixStream::connect(&rand_path).unwrap();
    let mut aead = Aead::new(&Method::default(), b"queen");

    let msg = msg!{
        CHAN: PING
    };

    let data = msg.to_vec().unwrap();
    let r1 = write_socket(&mut socket, &mut aead, data).is_err();
    let r2 = read_socket(&mut socket, &mut aead).is_err();
    assert!(r1 || r2);

    let _ = fs::remove_file(rand_path).unwrap();
}
