use std::thread;
use std::net::TcpStream;
use std::time::Duration;

use queen::{Node, node::Callback};
use queen::nson::{msg, Message};
use queen::error::ErrorCode;

use super::get_free_addr;

#[test]
fn no_auth() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut node = Node::bind(Some(&addr2), None).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    let mut socket = TcpStream::connect(addr).unwrap();

    // attach
    let msg = msg!{
        "_chan": "node::attach",
        "_value": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    // detach
    let msg = msg!{
        "_chan": "node::detach",
        "_value": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    // deltime
    let msg = msg!{
        "_chan": "node::deltime",
        "_timeid": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    // ping
    let msg = msg!{
        "_chan": "node::ping",
        "_timeid": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // send
    let msg = msg!{
        "_chan": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));
}

#[test]
fn do_auth() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut node = Node::bind(Some(&addr2), None).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    let mut socket = TcpStream::connect(addr).unwrap();

    // attach
    let msg = msg!{
        "_chan": "node::attach",
        "_value": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    // auth
    let msg = msg!{
        "_chan": "node::auth",
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // attach
    let msg = msg!{
        "_chan": "node::attach",
        "_value": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0)
}

#[test]
fn can_auth() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut node = Node::bind(Some(&addr2), None).unwrap();

        let mut callback = Callback::default();

        callback.auth(|_id, msg| {
            let username = msg.get_str("username").unwrap();
            let password = msg.get_str("password").unwrap();

            if username == "aaa" && password == "bbb" {
                return true;
            }

            return false
        });

        node.set_callback(callback);

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    let mut socket = TcpStream::connect(addr).unwrap();

    // attach
    let msg = msg!{
        "_chan": "node::attach",
        "_value": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    let msg = msg!{
        "_chan": "node::auth",
        "username": "aaabbb",
        "password": "bbbccc"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::AuthenticationFailed));

    let msg = msg!{
        "_chan": "node::auth",
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // attach
    let msg = msg!{
        "_chan": "node::attach",
        "_value": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);
}
