use std::thread;
use std::net::TcpStream;
use std::time::Duration;
use std::io::ErrorKind::WouldBlock;

use queen::Node;
use queen::nson::{msg, Message, decode::DecodeError};
use queen::error::ErrorCode;

use super::get_free_addr;

#[test]
fn duplicate_clientid() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut node = Node::bind(Some(&addr2), None).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    // client 1
    let mut socket = TcpStream::connect(&addr).unwrap();

    // client 2
    let mut socket2 = TcpStream::connect(addr).unwrap();

    // client 1 auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb",
        "_clid": "abc"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 2 auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb",
        "_clid": "abc"
    };

    msg.encode(&mut socket2).unwrap();

    let recv = Message::decode(&mut socket2).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::DuplicateClientId));

    // client 2 auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb",
        "_clid": "def"
    };

    msg.encode(&mut socket2).unwrap();

    let recv = Message::decode(&mut socket2).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);
}

#[test]
fn to() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut node = Node::bind(Some(&addr2), None).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    // client 1
    let mut socket = TcpStream::connect(&addr).unwrap();

    // client 1 auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb",
        "_clid": "abc"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 send
    let msg = msg!{
        "_to": "def",
        "hello": "world"
    };

    msg.encode(&mut socket).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::ClientNotExist));

    // client 2
    let mut socket2 = TcpStream::connect(addr).unwrap();

    // client 2 auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb",
        "_clid": "def"
    };

    msg.encode(&mut socket2).unwrap();

    let recv = Message::decode(&mut socket2).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 send
    let msg = msg!{
        "_to": "def",
        "hello": "world"
    };

    socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    msg.encode(&mut socket).unwrap();
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

    // client 2 recv
    let recv = Message::decode(&mut socket2).unwrap();
    assert!(recv.get_str("_from").unwrap() == "abc");

    // client 1 send with _id
    let msg = msg!{
        "_to": "def",
        "hello": "world",
        "_id": 123
    };

    socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    msg.encode(&mut socket).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);
    assert!(recv.get_i32("_id").unwrap() == 123);

    // client 2 recv
    let recv = Message::decode(&mut socket2).unwrap();
    assert!(recv.get_str("_from").unwrap() == "abc");
    assert!(recv.get_i32("_id").unwrap() == 123);
}
