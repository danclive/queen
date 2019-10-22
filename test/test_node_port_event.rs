use std::thread;
use std::net::TcpStream;
use std::time::Duration;

use queen::{Node, node::NodeConfig};
use queen::nson::{msg, Message, message_id::MessageId};
use queen::error::ErrorCode;
use queen::util::{write_socket, read_socket};
use queen::crypto::{Method, Aead};
use queen::dict::*;

use super::get_free_addr;

#[test]
fn supe() {
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

    // client 1
    let mut socket = TcpStream::connect(&addr).unwrap();
    let mut aead = Aead::new(&Method::default(), b"queen");

    // client 1 auth
    let msg = msg!{
        CHAN: AUTH,
        "username": "aaa",
        "password": "bbb",
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap(),
        SUPER: 123
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::InvalidSuperFieldType));

    // client 1 auth
    let msg = msg!{
        CHAN: AUTH,
        "username": "aaa",
        "password": "bbb",
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap(),
        SUPER: true
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    println!("{:?}", recv);
    assert!(recv.get_i32("ok").unwrap() == 0);
}

#[test]
fn auth() {
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

    // client 1
    let mut socket = TcpStream::connect(&addr).unwrap();
    let mut aead = Aead::new(&Method::default(), b"queen");

    // client 1 auth
    let msg = msg!{
        CHAN: AUTH,
        "username": "aaa",
        "password": "bbb",
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap()
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 attach
    let msg = msg!{
        CHAN: "_atta",
        VALUE: PORT_READY
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    // client 1 auth
    let msg = msg!{
        CHAN: AUTH,
        "username": "aaa",
        "password": "bbb",
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap(),
        SUPER: false
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 attach
    let msg = msg!{
        CHAN: "_atta",
        VALUE: PORT_READY
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    // client 1 auth
    let msg = msg!{
        CHAN: AUTH,
        "username": "aaa",
        "password": "bbb",
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap(),
        SUPER: true
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 attach
    let msg = msg!{
        CHAN: "_atta",
        VALUE: PORT_READY
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);
}

#[test]
fn event() {
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

    // client 1
    let mut socket = TcpStream::connect(&addr).unwrap();
    let mut aead = Aead::new(&Method::default(), b"queen");

    // client 1 auth
    let msg = msg!{
        CHAN: AUTH,
        "username": "aaa",
        "password": "bbb",
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap(),
        SUPER: true
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 attach
    let msg = msg!{
        CHAN: "_atta",
        VALUE: PORT_READY
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 attach
    let msg = msg!{
        CHAN: "_atta",
        VALUE: PORT_BREAK
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 attach
    let msg = msg!{
        CHAN: "_atta",
        VALUE: PORT_ATTACH
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 attach
    let msg = msg!{
        CHAN: "_atta",
        VALUE: PORT_DETACH
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 2
    let mut socket2 = TcpStream::connect(&addr).unwrap();
    let mut aead2 = Aead::new(&Method::default(), b"queen");

    // client 2 auth
    let msg = msg!{
        CHAN: AUTH,
        "username": "aaa",
        "password": "bbb",
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e5").unwrap()
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket2, &mut aead2, data).unwrap();
    let read_data = read_socket(&mut socket2, &mut aead2).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 try recv
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert_eq!(recv.get_str(CHAN).unwrap(), PORT_READY);
    assert_eq!(recv.get_message_id(PORT_ID).unwrap(), &MessageId::with_string("5932a005b4b4b4ac168cd9e5").unwrap());

    // client 2 attach
    let msg = msg!{
        CHAN: ATTACH,
        VALUE: "aaa"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket2, &mut aead2, data).unwrap();
    let read_data = read_socket(&mut socket2, &mut aead2).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 try recv
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert_eq!(recv.get_str(CHAN).unwrap(), PORT_ATTACH);
    assert_eq!(recv.get_message_id(PORT_ID).unwrap(), &MessageId::with_string("5932a005b4b4b4ac168cd9e5").unwrap());

    // client 2 detach
    let msg = msg!{
        CHAN: DETACH,
        VALUE: "aaa"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket2, &mut aead2, data).unwrap();
    let read_data = read_socket(&mut socket2, &mut aead2).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 try recv
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert_eq!(recv.get_str(CHAN).unwrap(), PORT_DETACH);
    assert_eq!(recv.get_message_id(PORT_ID).unwrap(), &MessageId::with_string("5932a005b4b4b4ac168cd9e5").unwrap());

    // client 2 disconnect
    drop(socket2);

    // client 1 try recv
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert_eq!(recv.get_str(CHAN).unwrap(), PORT_BREAK);
    assert_eq!(recv.get_message_id(PORT_ID).unwrap(), &MessageId::with_string("5932a005b4b4b4ac168cd9e5").unwrap());
}

#[test]
fn kill() {
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

    // client 1
    let mut socket = TcpStream::connect(&addr).unwrap();
    let mut aead = Aead::new(&Method::default(), b"queen");

    // client2
    let mut socket2 = TcpStream::connect(&addr).unwrap();
    let mut aead2 = Aead::new(&Method::default(), b"queen");

    // client 1 auth
    let msg = msg!{
        CHAN: AUTH,
        "username": "aaa",
        "password": "bbb",
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap()
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 2 auth
    let msg = msg!{
        CHAN: AUTH,
        "username": "aaa",
        "password": "bbb",
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e5").unwrap(),
        SUPER: true
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket2, &mut aead2, data).unwrap();
    let read_data = read_socket(&mut socket2, &mut aead2).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // socket 1 ping
    let msg = msg!{
        CHAN: PING,
        "_timeid": "aaa"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // kill socket 1
    let msg = msg!{
        CHAN: PORT_KILL,
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap()
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket2, &mut aead2, data).unwrap();
    let read_data = read_socket(&mut socket2, &mut aead2).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // try socket 1 ping
    let msg = msg!{
        CHAN: PING,
        "_timeid": "aaa"
    };

    let data = msg.to_vec().unwrap();
    let r1 = write_socket(&mut socket, &mut aead, data).is_err();
    let r2 = read_socket(&mut socket, &mut aead).is_err();
    assert!(r1 || r2);
}

