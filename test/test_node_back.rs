use std::thread;
use std::net::TcpStream;
use std::time::Duration;
use std::io::ErrorKind::WouldBlock;

use queen::{Node, node::NodeConfig};
use queen::nson::{msg, Message, decode::DecodeError};

use super::get_free_addr;

#[test]
fn back() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.tcp(addr2).unwrap();

        let mut node = Node::bind(config).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    let mut socket = TcpStream::connect(addr).unwrap();

    // auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_id": 123
    };

    msg.encode(&mut socket).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // try recv
    socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let recv = Message::decode(&mut socket);
    match recv {
       Ok(recv) => panic!("{:?}", recv),
       Err(err) => {
            if let DecodeError::IoError(err) = err {
                if let WouldBlock = err.kind() {
                    // pass
                } else {
                    panic!("{:?}", err);
                }
            } else {
                panic!("{:?}", err);
            }
       }
    }

    // send2
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_id": 123,
        "_back": true
    };

    msg.encode(&mut socket).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // try recv
    socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("_id").unwrap() == 123);
}

#[test]
fn back_time() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.tcp(addr2).unwrap();

        let mut node = Node::bind(config).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    let mut socket = TcpStream::connect(addr).unwrap();

    // auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_id": 123,
        "_time": 100u32
    };

    msg.encode(&mut socket).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // try recv
    socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let recv = Message::decode(&mut socket);
    match recv {
       Ok(recv) => panic!("{:?}", recv),
       Err(err) => {
            if let DecodeError::IoError(err) = err {
                if let WouldBlock = err.kind() {
                    // pass
                } else {
                    panic!("{:?}", err);
                }
            } else {
                panic!("{:?}", err);
            }
       }
    }

    // send2
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_id": 123,
        "_time": 100u32,
        "_back": true
    };

    msg.encode(&mut socket).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // try recv
    socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("_id").unwrap() == 123);
}
