use std::time::Duration;
use std::thread;

use nson::{msg, MessageId};

use queen::{Queen, Port, Connector, Node};
use queen::crypto::Method;
use queen::net::{Addr, CryptoOptions};
use queen::dict::*;
use queen::error::{Error, ErrorCode};

use super::get_free_addr;

#[test]
fn connect_queen() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let (port1, _) = Port::connect(
        MessageId::new(),
        Connector::Queen(queen.clone(), msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    let (port2, _) = Port::connect(
        MessageId::new(),
        Connector::Queen(queen, msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = port1.async_recv(
        "aaa", None, Some(Duration::from_secs(1))).unwrap();

    port2.send("aaa", msg!{"hello": "world"},
        None, Some(Duration::from_secs(1))).unwrap();

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());
}

#[test]
fn connect_node() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut node = Node::new(
            queen,
            2,
            vec![Addr::tcp(&addr2).unwrap()]
        ).unwrap();

        node.run().unwrap();
    });

    let (port1, _) = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), None),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

     let (port2, _) = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), None),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = port1.async_recv(
        "aaa", None, Some(Duration::from_secs(1))).unwrap();

    port2.send("aaa", msg!{"hello": "world"},
        None, Some(Duration::from_secs(1))).unwrap();

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());
}

#[test]
fn connect_node_crypto() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let options = CryptoOptions {
        method: Method::Aes256Gcm,
        access: "access_key".to_string(),
        secret: "secret_key".to_string()
    };

    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut node = Node::new(
            queen,
            2,
            vec![Addr::tcp(&addr2).unwrap()]
        ).unwrap();

        node.set_access_fn(|access_key| {
            assert!(access_key == "access_key");
            Some("secret_key".to_string())
        });

        node.run().unwrap();
    });

    let (port1, _) = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), Some(options.clone())),
        msg!{},
        2
    ).unwrap();

    let (port2, _) = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), Some(options)),
        msg!{},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = port1.async_recv("aaa", None, Some(Duration::from_secs(1))).unwrap();

    port2.send("aaa", msg!{"hello": "world"}, None, Some(Duration::from_secs(1))).unwrap();

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());
}

#[test]
fn connect_node_crypto2() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let options = CryptoOptions {
        method: Method::Aes256Gcm,
        access: "access_key".to_string(),
        secret: "secret_key".to_string()
    };

    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut node = Node::new(
            queen,
            2,
            vec![Addr::tcp(&addr2).unwrap()]
        ).unwrap();

        // node.set_access_fn(|access_key| {
        //     assert!(access_key == access_key);

        //     Some("secret_key".to_string())
        // });

        node.run().unwrap();
    });

    let (port1, _) = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), None),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    let (port2, _) = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), Some(options)),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = port1.async_recv(
        "aaa", None, Some(Duration::from_secs(1))).unwrap();

    let ret = port2.send("aaa", msg!{"hello": "world"},
        None, Some(Duration::from_secs(1)));

    assert!(ret.is_err());

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_none());
}

#[test]
fn connect_node_crypto3() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let options = CryptoOptions {
        method: Method::Aes256Gcm,
        access: "access_key".to_string(),
        secret: "secret_key".to_string()
    };

    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut node = Node::new(
            queen,
            2,
            vec![Addr::tcp(&addr2).unwrap()]
        ).unwrap();

        node.set_access_fn(|access_key| {
            assert!(access_key == access_key);

            Some("secret_key".to_string())
        });

        node.run().unwrap();
    });

    let (port1, _) = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), Some(options.clone())),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    let (port2, _) = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), None),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = port1.async_recv(
        "aaa", None, Some(Duration::from_secs(1))).unwrap();

    let ret = port2.send("aaa", msg!{"hello": "world"},
        None, Some(Duration::from_secs(1)));

    assert!(ret.is_err());

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_none());
}

#[test]
fn connect_mulit_node() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let addr1 = get_free_addr();
    let addr2 = get_free_addr();

    let queen2 = queen.clone();
    let addr = addr1.clone();
    thread::spawn(move || {
        let mut node = Node::new(
            queen2,
            2,
            vec![Addr::tcp(&addr).unwrap()]
        ).unwrap();

        node.run().unwrap();
    });

    let addr = addr2.clone();
    thread::spawn(move || {
        let mut node = Node::new(
            queen,
            2,
            vec![Addr::tcp(&addr).unwrap()]
        ).unwrap();

        node.run().unwrap();
    });

    let (port1, _) = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr1).unwrap(), None),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

     let (port2, _) = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr2).unwrap(), None),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = port1.async_recv(
        "aaa", None, Some(Duration::from_secs(1))).unwrap();

    port2.send("aaa", msg!{"hello": "world"},
        None, Some(Duration::from_secs(1))).unwrap();

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());
}

#[test]
fn recv() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let (port1, _) = Port::connect(
        MessageId::new(),
        Connector::Queen(queen.clone(), msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    let (port2, _) = Port::connect(
        MessageId::new(),
        Connector::Queen(queen, msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = port1.async_recv(
        "aaa", None, Some(Duration::from_secs(1))).unwrap();

    port2.send("aaa", msg!{"hello": "world"},
        None, Some(Duration::from_secs(1))).unwrap();

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());

    let recv2 = port1.async_recv(
        "aaa", None, Some(Duration::from_secs(2))).unwrap();

    port2.send("aaa", msg!{"hello": "world"},
        None, Some(Duration::from_secs(1))).unwrap();

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());
    assert!(recv2.recv().is_some());

    drop(recv2);

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"},
        None, Some(Duration::from_secs(1))).unwrap();

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());

    drop(recv);

    thread::sleep(Duration::from_secs(1));

    let ret = port2.send("aaa", msg!{"hello": "world"},
        None, Some(Duration::from_secs(1)));

    assert!(ret.is_err());

    if let Error::ErrorCode(ErrorCode::NoConsumers) = ret.err().unwrap() {

    } else {
        panic!("");
    }
}

#[test]
fn labels() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let (port1, _) = Port::connect(
        MessageId::new(),
        Connector::Queen(queen.clone(), msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    let (port2, _) = Port::connect(
        MessageId::new(),
        Connector::Queen(queen, msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));


    let recv = port1.async_recv(
        "aaa",
        Some(vec!["label1".to_string()]),
        Some(Duration::from_secs(1))
    ).unwrap();

    port2.send("aaa", msg!{"hello": "world"},
        None, Some(Duration::from_secs(1))).unwrap();

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());


    port2.send("aaa", msg!{"hello": "world"},
        Some(vec!["label1".to_string()]),
        Some(Duration::from_secs(1))).unwrap();

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());


    let ret = port2.send(
        "aaa",
        msg!{"hello": "world"},
        Some(vec!["label2".to_string()]),
        Some(Duration::from_secs(1))
    );
    assert!(ret.is_err());

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_none());


    let recv2 = port1.async_recv(
        "aaa",
        Some(vec!["label2".to_string()]),
        Some(Duration::from_secs(1))
    ).unwrap();

    port2.send("aaa", msg!{"hello": "world"},
        Some(vec!["label2".to_string()]),
        Some(Duration::from_secs(1))
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_none());
    assert!(recv2.recv().is_some());


    drop(recv);

    port2.send("aaa", msg!{"hello": "world"},
        Some(vec!["label2".to_string()]),
        Some(Duration::from_secs(1))
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    assert!(recv2.recv().is_some());


    let ret = port2.send("aaa", msg!{"hello": "world"},
        Some(vec!["label1".to_string()]),
        Some(Duration::from_secs(1))
    );
    assert!(ret.is_err());

    thread::sleep(Duration::from_secs(1));

    assert!(recv2.recv().is_none());


    let recv3 = port1.async_recv("aaa",
        None, Some(Duration::from_secs(1))).unwrap();

    port2.send("aaa", msg!{"hello": "world"},
        Some(vec!["label2".to_string()]),
        Some(Duration::from_secs(1))
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    assert!(recv2.recv().is_some());
    assert!(recv3.recv().is_none());


    port2.send("aaa", msg!{"hello": "world"},
        None, Some(Duration::from_secs(1))).unwrap();

    thread::sleep(Duration::from_secs(1));

    assert!(recv2.recv().is_some());
    assert!(recv3.recv().is_some());


    drop(recv3);

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"},
        None, Some(Duration::from_secs(1))).unwrap();

    thread::sleep(Duration::from_secs(1));

    assert!(recv2.recv().is_some());
}

#[test]
fn unknow_topic() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let (port1, _) = Port::connect(
        MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap(),
        Connector::Queen(queen.clone(), msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    let (port2, _) = Port::connect(
        MessageId::with_string("5932a005b4b4b4ac168cd9e5").unwrap(),
        Connector::Queen(queen, msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = port1.async_recv(UNKNOWN, None, Some(Duration::from_secs(1))).unwrap();

    port2.send("aaa", msg!{
        "hello": "world",
        TO: MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap()
    }, None, Some(Duration::from_secs(1))).unwrap();

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());
}
