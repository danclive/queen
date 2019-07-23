use std::thread;
use std::net::TcpStream;
use std::time::Duration;


use queen::{Node};
use queen::nson::{msg, Message};
use queen::error::ErrorCode;

use super::get_free_addr;

#[test]
fn no_auth() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut node = Node::new(Some(&addr2), None).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    let mut socket = TcpStream::connect(addr).unwrap();

    // attach
    let msg = msg!{
        "chan": "node::attach",
        "value": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    // detach
    let msg = msg!{
        "chan": "node::detach",
        "value": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    // deltime
    let msg = msg!{
        "chan": "node::deltime",
        "_timeid": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    // ping
    let msg = msg!{
        "chan": "node::ping",
        "_timeid": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // send
    let msg = msg!{
        "chan": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));
}