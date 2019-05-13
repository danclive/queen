#![allow(unused)]
use std::net::TcpStream;
use std::io::{Read, Write};


use queen::{Queen, Context};
use queen::nson::msg;
use queen::nson::Message;

fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:8888").unwrap();

    // handle
    let msg = msg!{
        "event": "node:hand",
        "username": "aaa",
        "password": "bbb",
        "su": true
    };

    msg.encode(&mut stream).unwrap();

    let recv = Message::decode(&mut stream).unwrap();

    println!("{:?}", recv);


    // listen
    /*
    let msg = msg!{
        "e": "s:n",
        "m": {
            "e": "s:listen",
            "protocol": "tcp",
            "addr": "0.0.0.0:8889",
            "_time": 2000u32
        }
    };

    msg.encode(&mut stream).unwrap();

    let recv = Message::decode(&mut stream).unwrap();

    println!("{:?}", recv);

    // unlisten
    let msg = msg!{
        "e": "s:n",
        "m": {
            "e": "s:unlisten",
            "listen_id": 101
        }
    };

    msg.encode(&mut stream).unwrap();

    let recv = Message::decode(&mut stream).unwrap();

    println!("{:?}", recv);
    */
// loop {
    // timer
    let msg = msg!{
        "event": "pub:hello",
        "aaa": "bbb",
        "_time": 2000u32,
        "_id": 123
    };

    msg.encode(&mut stream).unwrap();

    let recv = Message::decode(&mut stream).unwrap();

    println!("{:?}", recv);
// }
}
