use std::thread;
use std::time::Duration;

use queen::{Socket, Node, Port};
use queen::net::NsonCodec;
use queen::dict::*;
use queen::nson::{MessageId, msg};
use queen::error::Code;

fn main() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    // start wire 1
    let wire1 = socket.connect(msg!{}, None, None).unwrap();

    wire1.send(msg!{
        CHAN: AUTH
    }).unwrap();

    let ret = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    if let Some(err) = Code::get(&ret) {
        if err != Code::Ok {
            println!("wire 1 auth error: {:?}", err);
            return
        }
    }

    println!("wire 1 auth ret: {:?}", ret);

    // start port
    let mut node = Node::<NsonCodec, ()>::new(
        socket.clone(),
        1,
        vec!["127.0.0.1:8888".parse().unwrap()],
        ()
    ).unwrap();

    thread::spawn(move || {
        node.run().unwrap();
    });

    // start port
    let port = Port::<NsonCodec>::new().unwrap();

    // start wire 2
    let wire2 = port.connect("127.0.0.1:8888", msg!{}, None, None).unwrap();

    wire2.send(msg!{
        CHAN: AUTH
    }).unwrap();

    let ret = wire2.wait(Some(Duration::from_secs(1))).unwrap();
    if let Some(err) = Code::get(&ret) {
        if err != Code::Ok {
            println!("wire 2 auth error: {:?}", err);
            return
        }
    }

    println!("wire 2 auth ret: {:?}", ret);

    // wire 1 attach
    wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "hello"
    }).unwrap();

    let ret = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    if let Some(err) = Code::get(&ret) {
        if err != Code::Ok {
            println!("wire 1 attach error: {:?}", err);
            return
        }
    }

    println!("wire 1 attach ret: {:?}", ret);

    // wire 2 send
    wire2.send(msg!{
        ID: MessageId::new(),
        CHAN: "hello",
        ACK: true,
        "hello": "world"
    }).unwrap();

    let ret = wire2.wait(Some(Duration::from_secs(1))).unwrap();
    if let Some(err) = Code::get(&ret) {
        if err != Code::Ok {
            println!("wire 2 send error: {:?}", err);
            return
        }
    }

    println!("wire 2 send ret: {:?}", ret);

    // wire 1 recv
    let ret = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    if let Some(err) = Code::get(&ret) {
        if err != Code::Ok {
            println!("wire 2 recv error: {:?}", err);
            return
        }
    }

    println!("wire 2 recv ret: {:?}", ret);
}
