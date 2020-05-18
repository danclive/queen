use std::thread;
use std::time::Duration;

use queen::{Queen, Node, Port};
use queen::node::Hook;
use queen::nson::{MessageId, msg};
use queen::net::{CryptoOptions, NsonCodec};
use queen::crypto::Method;
use queen::dict::*;
use queen::error::{Error, Result};

use super::get_free_addr;

#[test]
fn port() {
    // start node
    let queen = Queen::new(MessageId::new(), ()).unwrap();

    let addr = get_free_addr();

    let mut node = Node::<NsonCodec, ()>::new(
        queen.clone(),
        2,
        vec![addr.parse().unwrap()],
        ()
    ).unwrap();

    thread::spawn(move || {
        node.run().unwrap();
    });

    // start stream
    let stream1 = queen.connect(msg!{}, None, None).unwrap();

    let _ = stream1.send(msg!{
        CHAN: AUTH
    });

    // start port
    let port = Port::<NsonCodec>::new().unwrap();

    let stream2 = port.connect(addr, msg!{}, None, None).unwrap();

    let _ = stream2.send(msg!{
        CHAN: AUTH
    });

    thread::sleep(Duration::from_millis(100));

    // stream1 recv
    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // stream2 recv
    let recv = stream2.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // stream1 attach
    let _ = stream1.send(msg!{
        CHAN: ATTACH,
        VALUE: "hello"
    });

    thread::sleep(Duration::from_millis(100));

    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // stream2 send
    let _ = stream2.send(msg!{
        CHAN: "hello",
        "hello": "world",
        ACK: true
    });

    thread::sleep(Duration::from_millis(100));

    // stream1 recv
    let recv = stream1.recv().unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");

    // stream2 recv
    let recv = stream2.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);
}

#[test]
fn port_secure() {
    // start node
    let queen = Queen::new(MessageId::new(), ()).unwrap();

    let addr = get_free_addr();

    struct MyHook;

    impl Hook for MyHook {
        fn enable_secure(&self) -> bool {
            true
        }

        fn access(&self, access: &str) -> Result<String> {
            if access == "12d3eaf5e9effffb14fb213e" {
                return Ok("99557df09590ad6043ceefd1".to_string())
            }

            Err(Error::PermissionDenied("".to_string()))
        }
    }

    let mut node = Node::<NsonCodec, MyHook>::new(
        queen.clone(),
        2,
        vec![addr.parse().unwrap()],
        MyHook
    ).unwrap();

    thread::spawn(move || {
        node.run().unwrap();
    });

    // start stream
    let stream1 = queen.connect(msg!{}, None, None).unwrap();

    let _ = stream1.send(msg!{
        CHAN: AUTH
    });

    // start port
    let port = Port::<NsonCodec>::new().unwrap();

    let crypto_options = CryptoOptions {
        method: Method::Aes128Gcm,
        access: "12d3eaf5e9effffb14fb213e".to_string(),
        secret: "99557df09590ad6043ceefd1".to_string()
    };

    let stream2 = port.connect(addr, msg!{}, Some(crypto_options), None).unwrap();

    let _ = stream2.send(msg!{
        CHAN: AUTH
    });

    thread::sleep(Duration::from_millis(100));

    // stream1 recv
    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // stream2 recv
    let recv = stream2.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // stream1 attach
    let _ = stream1.send(msg!{
        CHAN: ATTACH,
        VALUE: "hello"
    });

    thread::sleep(Duration::from_millis(100));

    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // stream2 send
    let _ = stream2.send(msg!{
        CHAN: "hello",
        "hello": "world",
        ACK: true
    });

    thread::sleep(Duration::from_millis(100));

    // stream1 recv
    let recv = stream1.recv().unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");

    // stream2 recv
    let recv = stream2.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);
}

#[test]
fn port_secure2() {
    // start node
    let queen = Queen::new(MessageId::new(), ()).unwrap();

    let addr = get_free_addr();

    struct MyHook;

    impl Hook for MyHook {
        fn enable_secure(&self) -> bool {
            true
        }

        fn access(&self, access: &str) -> Result<String> {
            if access == "12d3eaf5e9effffb14fb213e" {
                return Ok("99557df09590ad6043ceefd1".to_string())
            }

            Err(Error::PermissionDenied("".to_string()))
        }
    }

    let mut node = Node::<NsonCodec, MyHook>::new(
        queen.clone(),
        2,
        vec![addr.parse().unwrap()],
        MyHook
    ).unwrap();

    thread::spawn(move || {
        node.run().unwrap();
    });

    // start stream
    let stream1 = queen.connect(msg!{}, None, None).unwrap();

    let _ = stream1.send(msg!{
        CHAN: AUTH
    });

    // start port
    let port = Port::<NsonCodec>::new().unwrap();

    let stream2 = port.connect(addr, msg!{}, None, None);
    assert!(stream2.is_err());

    thread::sleep(Duration::from_millis(100));

    // stream1 recv
    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);
}
