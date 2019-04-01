use std::net::TcpStream;
use std::io::{Read, Write};


use queen::{Queen, Context};
use queen::node::Control;
use queen::nson::msg;
use queen::nson::Message;

use queen_log;
use log::LevelFilter;
use log::{debug, error, info, warn, trace};

fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:8888").unwrap();

    let msg = msg!{
        "e": "s:h",
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut stream).unwrap();

    let recv = Message::decode(&mut stream).unwrap();

    println!("{:?}", recv);

    let msg = msg!{
        "e": "s:a",
        "v": "p:hello"
    };

    msg.encode(&mut stream).unwrap();

    let recv = Message::decode(&mut stream).unwrap();

    println!("{:?}", recv);

    // let msg = msg!{
    //     "e": "s:d",
    //     "v": "p:hello"
    // };

    // msg.encode(&mut stream).unwrap();

    // let recv = Message::decode(&mut stream).unwrap();

    // println!("{:?}", recv);

    loop {
        let recv = Message::decode(&mut stream).unwrap();
        println!("{:?}", recv);
    }
}
