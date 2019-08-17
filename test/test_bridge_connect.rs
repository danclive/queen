use std::thread;
use std::net::TcpStream;
use std::time::Duration;
use std::collections::HashSet;
use std::io::Read;

use queen::{Node, node::NodeConfig};
use queen::port::{Bridge, BridgeConfig};
use queen::nson::msg;
use queen::nson::Message;
use queen::util::{write_socket, read_socket, get_length};

use super::get_free_addr;

#[test]
fn connect() {
    let addr_a = get_free_addr();
    let addr_b = get_free_addr();

    // node a
    let addr_a2 = addr_a.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.add_tcp(addr_a2).unwrap();
        config.set_hmac_key("queen");

        let mut node = Node::bind(config, ()).unwrap();

        node.run().unwrap();
    });

    // node b
    let addr_b2 = addr_b.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.add_tcp(addr_b2).unwrap();
        // config.set_hmac_key("queen");

        let mut node = Node::bind(config, ()).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    // bridge
    let addr_a2 = addr_a.clone();
    let addr_b2 = addr_b.clone();
    thread::spawn(move || {
        let mut chans = HashSet::new();

        chans.insert("aaa".to_owned());

        let config = BridgeConfig {
            addr1: queen::net::Addr::tcp(addr_a2).unwrap(),
            auth_msg1: msg!{},
            hmac_key1: Some("queen".to_string()),
            addr2: queen::net::Addr::tcp(addr_b2).unwrap(),
            auth_msg2: msg!{},
            white_list: chans,
            hmac_key2: None
        };

        let mut bridge = Bridge::connect(config);

        bridge.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    // client b
    let mut socket_b = TcpStream::connect(addr_b).unwrap();

    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut socket_b).unwrap();
   
    let mut len_buf = [0u8; 4];
    socket_b.peek(&mut len_buf).unwrap();
    let len = get_length(&len_buf, 0);
    let mut data = vec![0u8; len];
    socket_b.read(&mut data).unwrap();

    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client b attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

    msg.encode(&mut socket_b).unwrap();
   
    let mut len_buf = [0u8; 4];
    socket_b.peek(&mut len_buf).unwrap();
    let len = get_length(&len_buf, 0);
    let mut data = vec![0u8; len];
    socket_b.read(&mut data).unwrap();

    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client a
    let mut socket_a = TcpStream::connect(addr_a).unwrap();

    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    write_socket(&mut socket_a, b"queen", msg.to_vec().unwrap()).unwrap();
   
    let data = read_socket(&mut socket_a, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client a send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_ack": 123
    };

    write_socket(&mut socket_a, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket_a, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client b try recv
    socket_b.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut len_buf = [0u8; 4];
    socket_b.peek(&mut len_buf).unwrap();
    let len = get_length(&len_buf, 0);
    let mut data = vec![0u8; len];
    socket_b.read(&mut data).unwrap();

    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");
}
