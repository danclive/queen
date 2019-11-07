#![allow(unused_imports)]

use std::net::TcpStream;
use std::time::{Instant, Duration};
use std::thread;
use std::io::{Write, Read};

use queen::nson::msg;
use queen::nson::Message;
use queen::util::message::{write_socket, read_socket, read_socket_no_aead};
use queen::crypto::{Method, Aead};
use queen::dict::*;

fn main() {
    let mut socket = TcpStream::connect("127.0.0.1:8888").unwrap();

    println!("{:?}", socket);

    let msg = msg!{
        CHAN: AUTH,
        "username": "aaa",
        "password": "bbb"
    };

    let data = msg.to_vec().unwrap();
    socket.write(&data).unwrap();

    let r_data = read_socket_no_aead(&mut socket).unwrap();
    let recv = Message::from_slice(&r_data).unwrap();

    println!("{:?}", recv);

    let time1 = Instant::now();

    for i in 0..3000000 {

        let msg = msg!{
            CHAN: AUTH,
            "hello": "world",
            // ACK: 123,
            "i": i
        };

        let data = msg.to_vec().unwrap();
        socket.write(&data).unwrap();

    }

    loop {
        let r_data = read_socket_no_aead(&mut socket).unwrap();
        let recv = Message::from_slice(&r_data).unwrap();

        println!("{:?}", recv);
    }

    let time2 = Instant::now();

    println!("{:?}", time2 - time1);

    thread::sleep(Duration::from_secs(2));
}
