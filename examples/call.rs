#![allow(unused_imports)]
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
        CHAN: PING
    }).unwrap();

    let ret = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    println!("wire 1 ping ret: {:?}", ret);

    // start wire 2
    let wire2 = socket.connect(msg!{}, None, None).unwrap();

    wire2.send(msg!{
        CHAN: PING
    }).unwrap();

    let ret = wire2.wait(Some(Duration::from_secs(1))).unwrap();
    println!("wire 2 ping ret: {:?}", ret);


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

    thread::spawn(move || {
        loop {
            if let Ok(ret) = wire1.wait(Some(Duration::from_secs(1))) {
                if let Some(err) = Code::get(&ret) {
                    if err != Code::Ok {
                        println!("wire 1 recv error: {:?}", err);
                        return
                    }
                }

                println!("wire 1 recv ret: {:?}", ret);

                if let Some(_) = ret.get("aaa") {
                    wire1.send(msg!{
                        CHAN: "hello",
                        CODE: 0i32,
                        TO: ret.get_message_id(FROM).unwrap(),
                        "lalala": "wawawa"
                    }).unwrap();
                }
            }
        }
    });


    // wire 2 send
    wire2.send(msg!{
        ID: MessageId::new(),
        CHAN: "hello",
        "aaa": true,
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
}
