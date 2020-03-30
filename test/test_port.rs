use std::thread;
use std::time::Duration;

use queen::{Queen, Node, Port};
use queen::nson::{MessageId, msg};
use queen::net::CryptoOptions;
use queen::crypto::Method;
use queen::dict::*;

use super::get_free_addr;

#[test]
fn port() {
    // start node
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let addr = get_free_addr();

    let mut node = Node::new(
        queen.clone(),
        2,
        vec![addr.parse().unwrap()]
    ).unwrap();

    thread::spawn(move || {
        node.run().unwrap();
    });

    // start stream
    let stream1 = queen.connect(msg!{}, None, None).unwrap();

    let _ = stream1.send(&mut Some(msg!{
        CHAN: AUTH
    }));

    // start port
    let port = Port::new().unwrap();

    let stream2 = port.connect(addr, None, None).unwrap();

    let _ = stream2.send(&mut Some(msg!{
        CHAN: AUTH
    }));

    thread::sleep(Duration::from_secs(2));

    // stream1 recv
    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // stream2 recv
    let recv = stream2.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // stream1 attach
    let _ = stream1.send(&mut Some(msg!{
        CHAN: ATTACH,
        VALUE: "hello"
    }));

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // stream2 send
    let _ = stream2.send(&mut Some(msg!{
        CHAN: "hello",
        "hello": "world",
        ACK: true
    }));

    thread::sleep(Duration::from_secs(1));

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
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let addr = get_free_addr();

    let mut node = Node::new(
        queen.clone(),
        2,
        vec![addr.parse().unwrap()]
    ).unwrap();

    node.set_access_fn(|key|{
        assert!(key == "12d3eaf5e9effffb14fb213e");
        Some("99557df09590ad6043ceefd1".to_string())
    });

    thread::spawn(move || {
        node.run().unwrap();
    });

    // start stream
    let stream1 = queen.connect(msg!{}, None, None).unwrap();

    let _ = stream1.send(&mut Some(msg!{
        CHAN: AUTH
    }));

    // start port
    let port = Port::new().unwrap();

    let crypto_options = CryptoOptions {
        method: Method::Aes128Gcm,
        access: "12d3eaf5e9effffb14fb213e".to_string(),
        secret: "99557df09590ad6043ceefd1".to_string()
    };

    let stream2 = port.connect(addr, Some(crypto_options), None).unwrap();

    let _ = stream2.send(&mut Some(msg!{
        CHAN: AUTH
    }));

    thread::sleep(Duration::from_secs(1));

    // stream1 recv
    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // stream2 recv
    let recv = stream2.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // stream1 attach
    let _ = stream1.send(&mut Some(msg!{
        CHAN: ATTACH,
        VALUE: "hello"
    }));

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // stream2 send
    let _ = stream2.send(&mut Some(msg!{
        CHAN: "hello",
        "hello": "world",
        ACK: true
    }));

    thread::sleep(Duration::from_secs(1));

    // stream1 recv
    let recv = stream1.recv().unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");

    // stream2 recv
    let recv = stream2.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);
}
