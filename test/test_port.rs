use std::time::Duration;

use queen::{Socket, Node, Port, Wire};
use queen::node::Hook;
use queen::nson::{MessageId, msg, Message};
use queen::net::{CryptoOptions, NsonCodec, KeepAlive};
use queen::crypto::Method;
use queen::dict::*;

use super::get_free_addr;

const ACCESS:    &str = "_acce";

#[test]
fn port() {
    // start node
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let addr = get_free_addr();

    let _node = Node::<NsonCodec>::new(
        socket.clone(),
        2,
        vec![addr.parse().unwrap()],
        KeepAlive::default(),
        ()
    ).unwrap();

    // start wire
    let wire1 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: PING
    });

    // start port
    let port = Port::<NsonCodec>::new(KeepAlive::default()).unwrap();

    let wire2 = port.connect(addr, MessageId::new(), false, msg!{}, None, None).unwrap();

    let _ = wire2.send(msg!{
        CHAN: PING
    });

    // wire1 recv
    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);

    // wire2 recv
    let recv = wire2.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);

    // wire1 attach
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "hello"
    });

    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);

    // wire2 send
    let _ = wire2.send(msg!{
        CHAN: "hello",
        "hello": "world"
    });

    // wire1 recv
    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");
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

        fn start(&self, _slot_id: MessageId, _root: bool, message: &mut Message) -> bool {
            message.insert("lalala", 123);

            true
        }

        fn access(&self, _slot_id: MessageId, _root: bool, message: &mut Message) -> Option<String> {
            message.insert("hello", "world");

            if let Ok(access) = message.get_str(ACCESS) {
                if access == "12d3eaf5e9effffb14fb213e" {
                    return Some("99557df09590ad6043ceefd1".to_string())
                }
            }

            None
        }

        fn finish(&self, _slot_id: MessageId, _root: bool, message: &mut Message, wire: &Wire<Message>) {
            message.insert("wawawa", 456);
            wire.attr().insert("aaa", "bbb");
        }
    }

    let _node = Node::<NsonCodec>::new(
        socket.clone(),
        2,
        vec![addr.parse().unwrap()],
        KeepAlive::default(),
        MyHook
    ).unwrap();

    // start wire
    let wire1 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: PING
    });

    // start port
    let port = Port::<NsonCodec>::new(KeepAlive::default()).unwrap();

    let crypto_options = CryptoOptions {
        method: Method::Aes128Gcm,
        secret: "99557df09590ad6043ceefd1".to_string()
    };

    let attr = msg!{
        ACCESS: "12d3eaf5e9effffb14fb213e"
    };

    let wire2 = port.connect(addr, MessageId::new(), false, attr, Some(crypto_options), None).unwrap();
    assert!(wire2.attr().get_i32("lalala").unwrap() == 123);
    assert!(wire2.attr().get_i32("wawawa").unwrap() == 456);
    assert!(wire2.attr().get_str("hello").unwrap() == "world");

    let _ = wire2.send(msg!{
        CHAN: PING
    });

    // wire1 recv
    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);

    // wire2 recv
    let recv = wire2.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);

    // wire1 attach
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "hello"
    });

    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);

    // wire2 send
    let _ = wire2.send(msg!{
        CHAN: "hello",
        "hello": "world"
    });

    // wire1 recv
    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");
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

        fn access(&self, _slot_id: MessageId, _root: bool, message: &mut Message) -> Option<String> {
            if let Ok(access) = message.get_str(ACCESS) {
                if access == "12d3eaf5e9effffb14fb213e" {
                    return Some("99557df09590ad6043ceefd1".to_string())
                }
            }

            None
        }
    }

    let _node = Node::<NsonCodec>::new(
        socket.clone(),
        2,
        vec![addr.parse().unwrap()],
        KeepAlive::default(),
        MyHook
    ).unwrap();

    // start wire
    let wire1 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: PING
    });

    // start port
    let port = Port::<NsonCodec>::new(KeepAlive::default()).unwrap();

    let wire2 = port.connect(addr, MessageId::new(), false, msg!{}, None, None);
    assert!(wire2.is_err());

    // wire1 recv
    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);
}
