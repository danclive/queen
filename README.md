# Queen

[![crates.io](https://meritbadge.herokuapp.com/queen)](https://crates.io/crates/queen)
[![Build Status](https://travis-ci.org/danclive/queen.svg?branch=master)](https://travis-ci.org/danclive/queen)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Released API docs](https://docs.rs/queen/badge.svg)](https://docs.rs/queen)

Queen 是一个支持订阅发布模式、一对一和一对多的数据总线。

## 安装

在 `Cargo.toml` 文件中加入

```
queen = "0.12"
```

## 功能特性

* 订阅发布
* 一对一、一对多
* 使用 [nson](https://github.com/danclive/nson) 作为数据格式
* 提供若干 Hook 函数，开发者可以按需定制鉴权，ACL等功能
* 支持消息加密
* ... 待补充

## 示例

```rust
use std::thread;
use std::time::Duration;

use queen::{Socket, Node, Port, NonHook};
use queen::net::NsonCodec;
use queen::dict::*;
use queen::nson::{MessageId, msg};
use queen::error::Code;

fn main() {
    let socket = Socket::new(MessageId::new(), NonHook).unwrap();

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
