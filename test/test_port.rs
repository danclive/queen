use std::thread;
use std::time::Duration;

use queen::{Socket, Node, Port};
use queen::node::Hook;
use queen::nson::{MessageId, msg, Message};
use queen::net::{CryptoOptions, NsonCodec};
use queen::crypto::Method;
use queen::dict::*;

use super::get_free_addr;

const ACCESS:    &str = "_acce";

#[test]
fn port() {
    // start node
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let addr = get_free_addr();

    let mut node = Node::<NsonCodec, ()>::new(
        socket.clone(),
        2,
        vec![addr.parse().unwrap()],
        ()
    ).unwrap();

    thread::spawn(move || {
        node.run().unwrap();
    });

    // start wire
    let wire1 = socket.connect(msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: AUTH
    });

    // start port
    let port = Port::<NsonCodec>::new().unwrap();

    let wire2 = port.connect(addr, msg!{}, None, None).unwrap();

    let _ = wire2.send(msg!{
        CHAN: AUTH
    });

    // wire1 recv
    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // wire2 recv
    let recv = wire2.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // wire1 attach
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "hello"
    });

    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // wire2 send
    let _ = wire2.send(msg!{
        CHAN: "hello",
        "hello": "world",
        ACK: true
    });

    // wire1 recv
    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");

    // wire2 recv
    let recv = wire2.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);
}

#[test]
fn port_secure() {
    // start node
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let addr = get_free_addr();

    struct MyHook;

    impl Hook for MyHook {
        fn enable_secure(&self) -> bool {
            true
        }

        fn hand(&self, message: &mut Message) -> bool {
            message.insert("lalala", 123);

            true
        }

        fn access(&self, message: &mut Message) -> Option<String> {
            message.insert("hello", "world");

            if let Ok(access) = message.get_str(ACCESS) {
                if access == "12d3eaf5e9effffb14fb213e" {
                    return Some("99557df09590ad6043ceefd1".to_string())
                }
            }

            None
        }
    }

    let mut node = Node::<NsonCodec, MyHook>::new(
        socket.clone(),
        2,
        vec![addr.parse().unwrap()],
        MyHook
    ).unwrap();

    thread::spawn(move || {
        node.run().unwrap();
    });

    // start wire
    let wire1 = socket.connect(msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: AUTH
    });

    // start port
    let port = Port::<NsonCodec>::new().unwrap();

    let crypto_options = CryptoOptions {
        method: Method::Aes128Gcm,
        secret: "99557df09590ad6043ceefd1".to_string()
    };

    let attr = msg!{
        ACCESS: "12d3eaf5e9effffb14fb213e"
    };

    let wire2 = port.connect(addr, attr, Some(crypto_options), None).unwrap();
    assert!(wire2.attr().get_i32("lalala").unwrap() == 123);
    assert!(wire2.attr().get_str("hello").unwrap() == "world");

    let _ = wire2.send(msg!{
        CHAN: AUTH
    });

    // wire1 recv
    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // wire2 recv
    let recv = wire2.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // wire1 attach
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "hello"
    });

    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // wire2 send
    let _ = wire2.send(msg!{
        CHAN: "hello",
        "hello": "world",
        ACK: true
    });

    // wire1 recv
    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");

    // wire2 recv
    let recv = wire2.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);
}

#[test]
fn port_secure2() {
    // start node
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let addr = get_free_addr();

    struct MyHook;

    impl Hook for MyHook {
        fn enable_secure(&self) -> bool {
            true
        }

        fn hand(&self, message: &mut Message) -> bool {
            message.insert("lalala", 123);

            true
        }

        fn access(&self, message: &mut Message) -> Option<String> {
            message.insert("hello", "world");

            if let Ok(access) = message.get_str(ACCESS) {
                if access == "12d3eaf5e9effffb14fb213e" {
                    return Some("99557df09590ad6043ceefd1".to_string())
                }
            }

            None
        }
    }

    let mut node = Node::<NsonCodec, MyHook>::new(
        socket.clone(),
        2,
        vec![addr.parse().unwrap()],
        MyHook
    ).unwrap();

    thread::spawn(move || {
        node.run().unwrap();
    });

    // start wire
    let wire1 = socket.connect(msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: AUTH
    });

    // start port
    let port = Port::<NsonCodec>::new().unwrap();

    let wire2 = port.connect(addr, msg!{}, None, None);
    assert!(wire2.is_err());

    // wire1 recv
    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);
}
