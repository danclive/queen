use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use nson::msg;

use queen::Queen;
use queen::Node;

fn main() {
    let mut node = Node::new().unwrap();

    node.run().unwrap();
}
