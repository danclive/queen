use std::time::Duration;
use std::thread;

use nson::{msg, MessageId};

use queen::{Queen, Port, Connector, Node};
use queen::crypto::Method;
use queen::net::Addr;
use queen::dict::*;

use super::get_free_addr;

#[test]
fn connect_queen() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let port1 = Port::connect(
        MessageId::new(),
        Connector::Queen(queen.clone(), msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    let port2 = Port::connect(
        MessageId::new(),
        Connector::Queen(queen, msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = port1.async_recv("aaa", None).unwrap();

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"}, None);

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

    let port1 = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), None),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

     let port2 = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), None),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = port1.async_recv("aaa", None).unwrap();

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());
}

#[test]
fn connect_node_crypto() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let crypto = (Method::Aes256Gcm, "access_key".to_string(), "secret_key".to_string());
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

    let port1 = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), Some(crypto.clone())),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    let port2 = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), Some(crypto)),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = port1.async_recv("aaa", None).unwrap();

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());
}

#[test]
fn connect_node_crypto2() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let crypto = (Method::Aes256Gcm, "access_key".to_string(), "secret_key".to_string());
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

    let port1 = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), None),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    let port2 = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), Some(crypto)),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = port1.async_recv("aaa", None).unwrap();

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_none());
}

#[test]
fn connect_node_crypto3() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let crypto = (Method::Aes256Gcm, "access_key".to_string(), "secret_key".to_string());
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

    let port1 = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), Some(crypto.clone())),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    let port2 = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr).unwrap(), None),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = port1.async_recv("aaa", None).unwrap();

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"}, None);

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

    let port1 = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr1).unwrap(), None),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

     let port2 = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp(&addr2).unwrap(), None),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = port1.async_recv("aaa", None).unwrap();

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());
}

#[test]
fn recv() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let port1 = Port::connect(
        MessageId::new(),
        Connector::Queen(queen.clone(), msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    let port2 = Port::connect(
        MessageId::new(),
        Connector::Queen(queen, msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let reply = port1.async_recv(REPLY, None).unwrap();
    let reply2 = port2.async_recv(REPLY, None).unwrap();

    let recv = port1.async_recv("aaa", None).unwrap();

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());
    assert!(reply.recv().is_none());
    assert!(reply2.recv().is_none());

    let recv2 = port1.async_recv("aaa", None).unwrap();

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());
    assert!(recv2.recv().is_some());
    assert!(reply.recv().is_none());
    assert!(reply2.recv().is_none());

    drop(recv2);

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());
    assert!(reply.recv().is_none());
    assert!(reply2.recv().is_none());

    drop(recv);

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    assert!(reply2.recv().is_some());
}

#[test]
fn labels() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let port1 = Port::connect(
        MessageId::new(),
        Connector::Queen(queen.clone(), msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    let port2 = Port::connect(
        MessageId::new(),
        Connector::Queen(queen, msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let reply = port1.async_recv(REPLY, None).unwrap();
    let reply2 = port2.async_recv(REPLY, None).unwrap();

    let recv = port1.async_recv("aaa", Some(vec!["label1".to_string()])).unwrap();

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());
    assert!(reply.recv().is_none());
    assert!(reply2.recv().is_none());

    port2.send("aaa", msg!{"hello": "world"}, Some(vec!["label1".to_string()]));

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());
    assert!(reply.recv().is_none());
    assert!(reply2.recv().is_none());

    port2.send("aaa", msg!{"hello": "world"}, Some(vec!["label2".to_string()]));

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_none());
    assert!(reply.recv().is_none());
    assert!(reply2.recv().is_some());

    let recv2 = port1.async_recv("aaa", Some(vec!["label2".to_string()])).unwrap();

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"}, Some(vec!["label2".to_string()]));

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_none());
    assert!(recv2.recv().is_some());
    assert!(reply.recv().is_none());
    assert!(reply2.recv().is_none());

    drop(recv);

    port2.send("aaa", msg!{"hello": "world"}, Some(vec!["label2".to_string()]));

    thread::sleep(Duration::from_secs(1));

    assert!(recv2.recv().is_some());
    assert!(reply.recv().is_none());
    assert!(reply2.recv().is_none());

    port2.send("aaa", msg!{"hello": "world"}, Some(vec!["label1".to_string()]));

    thread::sleep(Duration::from_secs(1));

    assert!(recv2.recv().is_none());
    assert!(reply.recv().is_none());
    assert!(reply2.recv().is_some());

    let recv3 = port1.async_recv("aaa", None).unwrap();

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"}, Some(vec!["label2".to_string()]));

    thread::sleep(Duration::from_secs(1));

    assert!(recv2.recv().is_some());
    assert!(recv3.recv().is_none());
    assert!(reply.recv().is_none());
    assert!(reply2.recv().is_none());

    port2.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    assert!(recv2.recv().is_some());
    assert!(recv3.recv().is_some());
    assert!(reply.recv().is_none());
    assert!(reply2.recv().is_none());

    drop(recv3);

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    assert!(recv2.recv().is_some());
    assert!(reply.recv().is_none());
    assert!(reply2.recv().is_none());
}

#[test]
fn unknow_topic() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let port1 = Port::connect(
        MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap(),
        Connector::Queen(queen.clone(), msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    let port2 = Port::connect(
        MessageId::with_string("5932a005b4b4b4ac168cd9e5").unwrap(),
        Connector::Queen(queen, msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = port1.async_recv(UNKNOWN, None).unwrap();

    port2.send("aaa", msg!{
        "hello": "world",
        TO: MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap()
    }, None);

    thread::sleep(Duration::from_secs(1));

    assert!(recv.recv().is_some());
}
