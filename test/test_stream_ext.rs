use std::time::Duration;
use std::thread;
use std::sync::{Arc, Mutex};

use queen::{Queen, Node, Port};
use queen::nson::{msg, MessageId};
use queen::error::{Error, ErrorCode};
use queen::net::CryptoOptions;
use queen::crypto::Method;
use queen::dict::*;

use queen::stream::StreamExt;

use super::get_free_addr;

#[test]
fn stream() {
    // start queen
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let stream1 = queen.connect(msg!{}, None).unwrap();
    let stream2 = queen.connect(msg!{}, None).unwrap();

    // stream1 auth
    let _ = stream1.send(&mut Some(msg!{
        CHAN: AUTH
    }));

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();

    assert!(recv.get_i32(OK).unwrap() == 0);

    // stream ext
    let stream_ext = StreamExt::new(stream2);

    // ping
    assert!(stream_ext.ping(msg!{}).is_ok());

    // try attach
    assert!(matches!(
        stream_ext.attach("hello", None),
        Err(Error::ErrorCode(ErrorCode::Unauthorized))
    ));

    assert!(!stream_ext.authed());

    // auth
    assert!(stream_ext.auth(msg!{}).is_ok());

    assert!(stream_ext.authed());

    // try attach
    assert!(stream_ext.attach("hello", None).is_ok());

    // recv
    let state = Arc::new(Mutex::new(false));
    let state2 = state.clone();
    stream_ext.recv(move |chan, message| {
        if chan == "hello" && message.get_str("hello").unwrap() == "world" {
            let mut state = state2.lock().unwrap();
            *state = true;
        }
    });

    // stream1 send
    let _ = stream1.send(&mut Some(msg!{
        CHAN: "hello",
        "hello": "world",
        ACK: "123"
    }));

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();

    assert!(recv.get_i32(OK).unwrap() == 0);

    // check state
    let state2 = state.lock().unwrap();
    assert!(*state2 == true);
    
    // stream ext close
    stream_ext.close();

    thread::sleep(Duration::from_secs(1));

    // stream1 send
    let _ = stream1.send(&mut Some(msg!{
        CHAN: "hello",
        "hello": "world",
        ACK: "123"
    }));

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::NoConsumers));
}

#[test]
fn stream_over_port() {
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

    let _ = stream1.send(&mut Some(msg!{
        CHAN: AUTH
    }));

    // start port
    let port = Port::new().unwrap();

    let stream2 = port.connect(addr, None).unwrap();

    thread::sleep(Duration::from_secs(1));

    // stream1 recv
    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // stream ext
    let stream_ext = StreamExt::new(stream2);

    // ping
    assert!(stream_ext.ping(msg!{}).is_ok());

    // try attach
    assert!(matches!(
        stream_ext.attach("hello", None),
        Err(Error::ErrorCode(ErrorCode::Unauthorized))
    ));

    assert!(!stream_ext.authed());

    // auth
    assert!(stream_ext.auth(msg!{}).is_ok());

    assert!(stream_ext.authed());

    // try attach
    assert!(stream_ext.attach("hello", None).is_ok());

    // recv
    let state = Arc::new(Mutex::new(false));
    let state2 = state.clone();
    stream_ext.recv(move |chan, message| {
        if chan == "hello" && message.get_str("hello").unwrap() == "world" {
            let mut state = state2.lock().unwrap();
            *state = true;
        }
    });

    // stream1 send
    let _ = stream1.send(&mut Some(msg!{
        CHAN: "hello",
        "hello": "world",
        ACK: "123"
    }));

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();

    assert!(recv.get_i32(OK).unwrap() == 0);

    // check state
    let state2 = state.lock().unwrap();
    assert!(*state2 == true);
    
    // stream ext close
    stream_ext.close();

    thread::sleep(Duration::from_secs(1));

    // stream1 send
    let _ = stream1.send(&mut Some(msg!{
        CHAN: "hello",
        "hello": "world",
        ACK: "123"
    }));

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::NoConsumers));
}

#[test]
fn stream_over_port_secure() {
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

    let stream2 = port.connect(addr, Some(crypto_options)).unwrap();

    thread::sleep(Duration::from_secs(1));

    // stream1 recv
    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    // stream ext
    let stream_ext = StreamExt::new(stream2);

    // ping
    assert!(stream_ext.ping(msg!{}).is_ok());

    // try attach
    assert!(matches!(
        stream_ext.attach("hello", None),
        Err(Error::ErrorCode(ErrorCode::Unauthorized))
    ));

    assert!(!stream_ext.authed());

    // auth
    assert!(stream_ext.auth(msg!{}).is_ok());

    assert!(stream_ext.authed());

    // try attach
    assert!(stream_ext.attach("hello", None).is_ok());

    // recv
    let state = Arc::new(Mutex::new(false));
    let state2 = state.clone();
    stream_ext.recv(move |chan, message| {
        if chan == "hello" && message.get_str("hello").unwrap() == "world" {
            let mut state = state2.lock().unwrap();
            *state = true;
        }
    });

    // stream1 send
    let _ = stream1.send(&mut Some(msg!{
        CHAN: "hello",
        "hello": "world",
        ACK: "123"
    }));

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();

    assert!(recv.get_i32(OK).unwrap() == 0);

    // check state
    let state2 = state.lock().unwrap();
    assert!(*state2 == true);
    
    // stream ext close
    stream_ext.close();

    thread::sleep(Duration::from_secs(1));

    // stream1 send
    let _ = stream1.send(&mut Some(msg!{
        CHAN: "hello",
        "hello": "world",
        ACK: "123"
    }));

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::NoConsumers));
}
