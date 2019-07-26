use std::thread;
use std::net::TcpStream;
use std::time::Duration;
use std::io::ErrorKind::WouldBlock;

use queen::Node;
use queen::nson::{msg, Message, decode::DecodeError};
use queen::error::ErrorCode;

use super::get_free_addr;

#[test]
fn timer() {
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
        "_chan": "node::auth",
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 attach
    let msg = msg!{
        "_chan": "node::attach",
        "_value": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // no time

    // client 1 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_id": 123,
        "_back": true
    };

    msg.encode(&mut socket).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 recv
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("_id").unwrap() == 123);

    // with time

    // client 1 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_id": 123,
        "_back": true,
        "_time": 1000 * 2u32
    };

    msg.encode(&mut socket).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 try recv
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

    // client 1 recv
    socket.set_read_timeout(None).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("_id").unwrap() == 123);
}

#[test]
fn del_time_id() {
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
        "_chan": "node::auth",
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 attach
    let msg = msg!{
        "_chan": "node::attach",
        "_value": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_id": 123,
        "_back": true,
        "_time": 1000u32,
        "_timeid": "123"
    };

    msg.encode(&mut socket).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 recv
    socket.set_read_timeout(None).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("_id").unwrap() == 123);

    // del time id

    // client 1 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_id": 123,
        "_back": true,
        "_time": 1000 * 2u32,
        "_timeid": "123"
    };

    msg.encode(&mut socket).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // del time
    let msg = msg!{
        "_chan": "node::deltime",
        "_timeid": "1234"
    };

    msg.encode(&mut socket).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::TimeidNotExist));

    let msg = msg!{
        "_chan": "node::deltime",
        "_timeid": "123"
    };

    msg.encode(&mut socket).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 try recv
    socket.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
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
}
