# Queen

[![crates.io](https://meritbadge.herokuapp.com/queen)](https://crates.io/crates/queen)
[![Build Status](https://travis-ci.org/danclive/queen.svg?branch=master)](https://travis-ci.org/danclive/queen)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Released API docs](https://docs.rs/queen/badge.svg)](https://docs.rs/queen)

Queen is a message bus

## features

* subscribe-publish & request-response
* one to one, one to many
* use [nson](https://github.com/danclive/nson) as a data format
* support message encryption
* ... more

## example

```rust
use std::time::Duration;

use queen::{Socket, Node, Port, NonHook};
use queen::net::{NsonCodec, KeepAlive};
use queen::dict::*;
use queen::nson::{MessageId, msg};
use queen::error::Code;

fn main() {
    let socket = Socket::new(MessageId::new(), NonHook).unwrap();

    // start wire 1
    let wire1 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    wire1.send(msg!{
        CHAN: PING
    }).unwrap();

    let ret = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    println!("wire 1 ping ret: {:?}", ret);

    // start node
    let _node = Node::<NsonCodec>::new(
        socket.clone(),
        4,
        vec!["127.0.0.1:8888".parse().unwrap()],
        KeepAlive::default(),
        ()
    ).unwrap();

    // start port
    let port = Port::<NsonCodec>::new(KeepAlive::default()).unwrap();

    // start wire 2
    let wire2 = port.connect("127.0.0.1:8888", None, MessageId::new(), false, msg!{}, None).unwrap();

    wire2.send(msg!{
        CHAN: PING
    }).unwrap();

    let ret = wire2.wait(Some(Duration::from_secs(1))).unwrap();
    if let Some(err) = Code::get(&ret) {
        if err != Code::Ok {
            println!("wire 2 ping error: {:?}", err);
            return
        }
    }

    println!("wire 2 ping ret: {:?}", ret);

    // wire 1 attach
    wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "hello"
    }).unwrap();

    let ret = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    println!("wire 1 attach ret: {:?}", ret);

    // wire 2 send
    wire2.send(msg!{
        ID: MessageId::new(),
        CHAN: "hello",
        "hello": "world"
    }).unwrap();

    // wire 1 recv
    let ret = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    if let Some(err) = Code::get(&ret) {
        if err != Code::Ok {
            println!("wire 1 recv error: {:?}", err);
            return
        }
    }

    println!("wire 1 recv ret: {:?}", ret);
}
```
