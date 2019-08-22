use std::thread;
use std::net::TcpStream;
use std::time::Duration;

use queen::{Node, node::Callback, node::NodeConfig};
use queen::nson::{msg, Message};
use queen::error::ErrorCode;
use queen::util::{write_socket, read_socket};
use queen::crypto::{Method, Aead};

use super::get_free_addr;

#[test]
fn no_auth() {
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

    let mut socket = TcpStream::connect(addr).unwrap();
    let mut aead = Aead::new(&Method::default(), b"queen");

    // attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    // detach
    let msg = msg!{
        "_chan": "_deta",
        "_valu": "aaa"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    // ping
    let msg = msg!{
        "_chan": "_ping",
        "_timeid": "aaa"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // send
    let msg = msg!{
        "_chan": "aaa"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();

    let read_data = read_socket(&mut socket, &mut aead).unwrap();

    let recv = Message::from_slice(&read_data).unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));
}

#[test]
fn do_auth() {
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

    let mut socket = TcpStream::connect(addr).unwrap();
    let mut aead = Aead::new(&Method::default(), b"queen");

    // attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    // auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0)
}

#[test]
fn can_auth() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.add_tcp(addr2).unwrap();
        config.set_aead_key("queen");

        let mut node = Node::bind(config, ()).unwrap();

        let mut callback = Callback::default();

        callback.auth(|_id, _, msg, _,| {
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
    let mut aead = Aead::new(&Method::default(), b"queen");

    // attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    let msg = msg!{
        "_chan": "_auth",
        "username": "aaabbb",
        "password": "bbbccc"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::AuthenticationFailed));

    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);

    // attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

    let data = msg.to_vec().unwrap();
    write_socket(&mut socket, &mut aead, data).unwrap();
    let read_data = read_socket(&mut socket, &mut aead).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    assert!(recv.get_i32("ok").unwrap() == 0);
}
