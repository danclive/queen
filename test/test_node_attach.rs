use std::thread;
use std::net::TcpStream;
use std::time::Duration;
use std::io::ErrorKind::WouldBlock;

use queen::{Node, node::NodeConfig};
use queen::nson::{msg, Message, decode::DecodeError};
use queen::error::ErrorCode;

use super::get_free_addr;

#[test]
fn attach() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.tcp(addr2).unwrap();

        let mut node = Node::bind(config).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    // client 1
    let mut socket = TcpStream::connect(&addr).unwrap();

    // client2
    let mut socket2 = TcpStream::connect(addr).unwrap();

    // client 1 auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 2 auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut socket2).unwrap();

    let recv = Message::decode(&mut socket2).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client2 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_id": 123
    };

    msg.encode(&mut socket2).unwrap();
    let recv = Message::decode(&mut socket2).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::NoConsumers));

    // client 1 attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client2 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_id": 123
    };

    msg.encode(&mut socket2).unwrap();
    let recv = Message::decode(&mut socket2).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client1 recv
    socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_str("_chan").unwrap() == "aaa");

    // client1 detach
    let msg = msg!{
        "_chan": "_deta",
        "_valu": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client2 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_id": 123
    };

    msg.encode(&mut socket2).unwrap();
    let recv = Message::decode(&mut socket2).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::NoConsumers));

    // client1 recv
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
}
