use std::thread;
use std::net::TcpStream;
use std::time::Duration;
use std::io::Read;

use queen::{Node, node::NodeConfig};
use queen::port::{Bridge, BridgeConfig};
use queen::nson::msg;
use queen::nson::Message;
use queen::util::{write_socket, read_socket, get_length};
use queen::crypto::{Method, Aead};
use queen::error::ErrorCode;
use queen::dict::*;

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
        config.set_aead_key("queen");

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
        let chans = vec![("aaa".to_string(), vec![])];

        let config = BridgeConfig {
            addr1: queen::net::Addr::tcp(addr_a2).unwrap(),
            auth_msg1: msg!{},
            aead_key1: Some("queen".to_string()),
            aead_method1: Method::default(),
            addr2: queen::net::Addr::tcp(addr_b2).unwrap(),
            auth_msg2: msg!{},
            white_list: chans,
            aead_key2: None,
            aead_method2: Method::default()
        };

        let mut bridge = Bridge::connect(config);

        bridge.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    // client b
    let mut socket_b = TcpStream::connect(addr_b).unwrap();

    let msg = msg!{
        CHAN: AUTH,
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
        CHAN: "_atta",
        VALUE: "aaa"
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
    let mut aead_a = Aead::new(&Method::default(), b"queen");

    let msg = msg!{
        CHAN: AUTH,
        "username": "aaa",
        "password": "bbb"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket_a, &mut aead_a, data).unwrap();
    let read_data = read_socket(&mut socket_a, &mut aead_a).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // client a send
    let msg = msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: 123
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket_a, &mut aead_a, data).unwrap();
    let read_data = read_socket(&mut socket_a, &mut aead_a).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

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

#[test]
fn test_with_label() {
    let addr_a = get_free_addr();
    let addr_b = get_free_addr();

    // node a
    let addr_a2 = addr_a.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.add_tcp(addr_a2).unwrap();
        config.set_aead_key("queen");

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
        let chans = vec![("aaa".to_string(), vec!["lable1".to_string()])];

        let config = BridgeConfig {
            addr1: queen::net::Addr::tcp(addr_a2).unwrap(),
            auth_msg1: msg!{},
            aead_key1: Some("queen".to_string()),
            aead_method1: Method::default(),
            addr2: queen::net::Addr::tcp(addr_b2).unwrap(),
            auth_msg2: msg!{},
            white_list: chans,
            aead_key2: None,
            aead_method2: Method::default()
        };

        let mut bridge = Bridge::connect(config);

        bridge.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    // client b
    let mut socket_b = TcpStream::connect(addr_b).unwrap();

    let msg = msg!{
        CHAN: AUTH,
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
        CHAN: "_atta",
        VALUE: "aaa",
        LABEL: "lable1"
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
    let mut aead_a = Aead::new(&Method::default(), b"queen");

    let msg = msg!{
        CHAN: AUTH,
        "username": "aaa",
        "password": "bbb"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket_a, &mut aead_a, data).unwrap();
    let read_data = read_socket(&mut socket_a, &mut aead_a).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // client a send
    let msg = msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: 123,
        LABEL: "lable1"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket_a, &mut aead_a, data).unwrap();
    let read_data = read_socket(&mut socket_a, &mut aead_a).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

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

    // client a send
    let msg = msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: 123,
        LABEL: "lable2"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket_a, &mut aead_a, data).unwrap();
    let read_data = read_socket(&mut socket_a, &mut aead_a).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::NoConsumers));
}
