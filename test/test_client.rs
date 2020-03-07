use std::thread;
use std::time::Duration;

use queen::{Queen, Node};
use queen::client::{Client, ClientOptions};
use queen::nson::{MessageId, msg};
use queen::dict::*;
use queen::error::{Error, ErrorCode};
use queen::net::CryptoOptions;
use queen::crypto::Method;

use super::get_free_addr;

#[test]
fn client() {
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
    let stream1 = queen.connect(msg!{}, None).unwrap();

    stream1.send(msg!{
        CHAN: AUTH
    });

    // start client
    let options = ClientOptions {
        addr: addr.parse().unwrap(),
        crypto_options: None,
        auth_message: None,
        works: 2
    };

    thread::sleep(Duration::from_secs(1));

    let client = Client::new(options).unwrap();

    thread::sleep(Duration::from_secs(2));

    // stream recv
    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // client send
    let ret = client.send("hello", msg!{"hahaha": "lalala"}, None);
    assert!(ret.is_err());

    match ret.err().unwrap() {
        Error::IoError(err) => panic!("{:?}", err),
        Error::ErrorCode(err) => {
            assert!(err == ErrorCode::NoConsumers);
        }
    }

    // stream attach
    stream1.send(msg!{
        CHAN: ATTACH,
        VALUE: "hello"
    });

    thread::sleep(Duration::from_millis(500));

    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // client send
    let ret = client.send("hello", msg!{"hahaha": "lalala"}, None);
    assert!(ret.is_ok());

    let recv = stream1.recv().unwrap();
    assert!(recv.get_str("hahaha").unwrap() == "lalala");
}

#[test]
fn client_secure() {
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
    let stream1 = queen.connect(msg!{}, None).unwrap();

    stream1.send(msg!{
        CHAN: AUTH
    });

    // start client
    let options = ClientOptions {
        addr: addr.parse().unwrap(),
        crypto_options: Some(CryptoOptions {
            method: Method::Aes128Gcm,
            access: "12d3eaf5e9effffb14fb213e".to_string(),
            secret: "99557df09590ad6043ceefd1".to_string()
        }),
        auth_message: None,
        works: 2
    };

    let client = Client::new(options).unwrap();

    thread::sleep(Duration::from_secs(1));

    // stream recv
    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // client send
    let ret = client.send("hello", msg!{"hahaha": "lalala"}, None);
    assert!(ret.is_err());

    match ret.err().unwrap() {
        Error::IoError(err) => panic!("{:?}", err),
        Error::ErrorCode(err) => {
            assert!(err == ErrorCode::NoConsumers);
        }
    }

    // stream attach
    stream1.send(msg!{
        CHAN: ATTACH,
        VALUE: "hello"
    });

    thread::sleep(Duration::from_millis(500));

    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // client send
    let ret = client.send("hello", msg!{"hahaha": "lalala"}, None);
    assert!(ret.is_ok());

    let recv = stream1.recv().unwrap();
    assert!(recv.get_str("hahaha").unwrap() == "lalala");
}

#[test]
fn client_send_recv() {
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

    // start client
    let options = ClientOptions {
        addr: addr.parse().unwrap(),
        crypto_options: None,
        auth_message: None,
        works: 2
    };

    thread::sleep(Duration::from_secs(1));

    let client = Client::new(options).unwrap();

    thread::sleep(Duration::from_secs(1));

    // client send
    let ret = client.send("hello", msg!{"hahaha": "lalala"}, None);
    assert!(ret.is_err());

    match ret.err().unwrap() {
        Error::IoError(err) => panic!("{:?}", err),
        Error::ErrorCode(err) => {
            assert!(err == ErrorCode::NoConsumers);
        }
    }

    // start client 2
    let options = ClientOptions {
        addr: addr.parse().unwrap(),
        crypto_options: None,
        auth_message: None,
        works: 2
    };

    thread::sleep(Duration::from_secs(1));

    let client2 = Client::new(options).unwrap();

    thread::sleep(Duration::from_secs(1));

    let recv = client2.recv("hello", None).unwrap();

    // client send
    let ret = client.send("hello", msg!{"hahaha": "lalala"}, None);
    assert!(ret.is_ok());
    assert!(recv.recv.recv().ok().unwrap().get_str("hahaha").unwrap() == "lalala");

    drop(recv);

    thread::sleep(Duration::from_secs(1));

    // client send
    let ret = client.send("hello", msg!{"hahaha": "lalala"}, None);
    assert!(ret.is_err());

    match ret.err().unwrap() {
        Error::IoError(err) => panic!("{:?}", err),
        Error::ErrorCode(err) => {
            assert!(err == ErrorCode::NoConsumers);
        }
    }
}

#[test]
fn client_call_add() {
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

    // start client
    let options = ClientOptions {
        addr: addr.parse().unwrap(),
        crypto_options: None,
        auth_message: None,
        works: 2
    };

    thread::sleep(Duration::from_secs(1));

    let client = Client::new(options).unwrap();

    thread::sleep(Duration::from_secs(1));

    // client call
    let ret = client.call("hello", msg!{"hahaha": "lalala"}, None);
    assert!(ret.is_err());

    match ret.err().unwrap() {
        Error::IoError(err) => panic!("{:?}", err),
        Error::ErrorCode(err) => {
            assert!(err == ErrorCode::NoConsumers);
        }
    }

    // start client 2
    let options = ClientOptions {
        addr: addr.parse().unwrap(),
        crypto_options: None,
        auth_message: None,
        works: 2
    };

    thread::sleep(Duration::from_secs(1));

    let client2 = Client::new(options).unwrap();

    thread::sleep(Duration::from_secs(1));

    let id = client2.add_handle("hello", |message| {
        assert!(message.get_str("hahaha").unwrap() == "lalala");

        msg!{"hahahaha": "lalalala"}
    }, None).unwrap();

    // client call
    let ret = client.call("hello", msg!{"hahaha": "lalala"}, None);
    assert!(ret.is_ok());
    assert!(ret.unwrap().get_str("hahahaha").unwrap() == "lalalala");

    client2.remove_handle(id).unwrap();

    // client call
    let ret = client.call("hello", msg!{"hahaha": "lalala"}, None);
    assert!(ret.is_err());

    match ret.err().unwrap() {
        Error::IoError(err) => panic!("{:?}", err),
        Error::ErrorCode(err) => {
            assert!(err == ErrorCode::NoConsumers);
        }
    }
}
