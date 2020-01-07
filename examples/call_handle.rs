use std::thread;
use std::time::Duration;

use queen::client::{Client, ClientOptions};
use queen::nson::msg;

fn main() {
    let options = ClientOptions {
        addr: "127.0.0.1:8888".parse().unwrap(),
        crypto_options: None,
        auth_message: None,
        works: 2
    };

    let client = Client::new(options).unwrap();

    while !client.is_connect() {
        thread::sleep(Duration::from_millis(100));
    }

    println!("{:?}", client.is_connect());

    println!("{:?}", client.call("hello", msg!{"hahaha": "lalala"}, None));

    thread::sleep(Duration::from_secs(10));
}
