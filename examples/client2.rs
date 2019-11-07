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

    let msg = msg!{
        CHAN: ATTACH,
        VALUE: "aaa"
    };

    let data = msg.to_vec().unwrap();
    socket.write(&data).unwrap();

    let r_data = read_socket_no_aead(&mut socket).unwrap();
    let recv = Message::from_slice(&r_data).unwrap();

    println!("{:?}", recv);

    loop {
        let r_data = read_socket_no_aead(&mut socket).unwrap();
        let recv = Message::from_slice(&r_data).unwrap();

        // println!("{:?}", recv);
    }

}
