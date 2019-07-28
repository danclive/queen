use std::thread;
use std::net::TcpStream;
use std::time::Duration;

use queen::Node;
use queen::nson::{msg, Message};

use super::get_free_addr;

#[test]
fn bridge() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut node = Node::bind(Some(&addr2), None).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    // bridge 1
    let mut bridge = TcpStream::connect(&addr).unwrap();

    // client 1
    let mut socket = TcpStream::connect(&addr).unwrap();

    // bridge 1 auth
    let msg = msg!{
        "_chan": "_auth",
        "_brge": true,
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut bridge).unwrap();

    let recv = Message::decode(&mut bridge).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // bridge 1 try recv
    bridge.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let recv = Message::decode(&mut bridge).unwrap();
    assert!(recv.get_str("_chan").unwrap() == "_brge_atta");
    assert!(recv.get_str("_valu").unwrap() == "aaa");

    // bridge 2
    let mut bridge2 = TcpStream::connect(&addr).unwrap();

    // bridge 2 auth
    let msg = msg!{
        "_chan": "_auth",
        "_brge": true,
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut bridge2).unwrap();

    let recv = Message::decode(&mut bridge2).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // bridge 2 try recv
    bridge2.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let recv = Message::decode(&mut bridge2).unwrap();
    assert!(recv.get_str("_chan").unwrap() == "_brge_atta");
    assert!(recv.get_str("_valu").unwrap() == "aaa");

    // client 1 detach
    let msg = msg!{
        "_chan": "_deta",
        "_valu": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // bridge 1 try recv
    bridge.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let recv = Message::decode(&mut bridge).unwrap();
    assert!(recv.get_str("_chan").unwrap() == "_brge_deta");
    assert!(recv.get_str("_valu").unwrap() == "aaa");

    // bridge 2 try recv
    bridge2.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let recv = Message::decode(&mut bridge2).unwrap();
    assert!(recv.get_str("_chan").unwrap() == "_brge_deta");
    assert!(recv.get_str("_valu").unwrap() == "aaa");
}
