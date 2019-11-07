#![allow(unused_imports)]

use std::time::{Duration, Instant};
use std::thread;

use nson::message_id::MessageId;
use nson::{Message, msg};

use queen::queen::Queen;
use queen::node::Node;
use queen::net::{NetStream, Listen, Addr};

fn main() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let mut node = Node::new(queen, 2, vec![Addr::tcp("127.0.0.1:8888").unwrap()]).unwrap();

    node.run().unwrap();

    println!("{:?}", "exit");

    thread::sleep(Duration::from_secs(60 * 10));
}
