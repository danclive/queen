#![allow(unused_imports)]

use std::net::TcpStream;
use std::time::{Instant, Duration};
use std::thread;
use std::io::{Write, Read};

use queen::nson::msg;
use queen::nson::{Message, MessageId};
use queen::util::message::{write_socket, read_socket, read_socket_no_aead};
use queen::crypto::{Method, Aead};
use queen::dict::*;
use queen::port::{Port, Connector};
use queen::net::{NetStream, Listen, Addr};
use queen::rpc::Rpc;


fn main() {
    let crypto = (Method::Aes256Gcm, "sep-centre".to_string());

    let port = Port::connect(MessageId::new(), Connector::Net(Addr::tcp("127.0.0.1:8888").unwrap(), Some(crypto)), msg!{"user": "test-user", "pass": "test-pass"}).unwrap();

    let rpc = Rpc::new(port, 2).unwrap();

    rpc.add("hello", |message| {
        println!("{:?}", message);

        msg!{"hehehe": "lalala"}
    });

    thread::sleep(Duration::from_secs(60 * 10));
}
