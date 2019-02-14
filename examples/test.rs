use std::thread;
use std::time::Duration;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use nson::msg;

use queen::queen::Queen;
use queen::client;
use queen::Message;

use queen::center::Center;

fn main() {
    let center = Center::new();

    center.on("aaa", |context| {
        //println!("{:?}", context);
        let mut i = context.value.as_i32().unwrap();
        i += 1;
        println!("{:?}", i);
        context.center.insert("aaa", i.into());
    });

    center.insert("aaa", 1.into());

    center.run(4, true);
}
