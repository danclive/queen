#![allow(unused_imports)]

use std::time::{Duration, Instant};
use std::thread;

use nson::message_id::MessageId;
use nson::{Message, msg};

use queen::{Queen, Callback};
use queen::node::Node;
use queen::net::{NetStream, Listen, Addr};
use queen::crypto::Method;

fn main() {
    let mut callback = Callback::<()>::new();

    callback.accept(|port, _| {
        println!("accept, port attr: {:?}", port.stream.attr);

        return true
    });

    callback.remove(|port, _| {
        println!("remove, port attr: {:?}", port.stream.attr);
    });

    callback.auth(move |port, message, _| {
        println!("auth, port attr: {:?}, message: {:?}", port.stream.attr, message);

        if let Ok(u) = message.get_str("user") {
            if u != "test-user" {
                return false
            }
        } else {
            return false
        }

        if let Ok(p) = message.get_str("pass") {
            if p != "test-pass" {
                return false
            }
        } else {
            return false
        }

        return true;
    });

    let queen = Queen::new(MessageId::new(), (), Some(callback)).unwrap();

    let crypto = (Method::Aes256Gcm, "sep-centre".to_string());

    let mut node = Node::new(queen, 2, vec![Addr::tcp("127.0.0.1:8888").unwrap()], Some(crypto)).unwrap();

    node.run().unwrap();

    println!("{:?}", "exit");

    thread::sleep(Duration::from_secs(60 * 10));
}
