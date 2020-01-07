use std::thread;
use std::time::Duration;

use queen::client::{Client, ClientOptions};

fn main() {
    let options = ClientOptions {
        addr: "127.0.0.1:8888".parse().unwrap(),
        crypto_options: None,
        auth_message: None,
        works: 2
    };

    let client = Client::new(options).unwrap();

    while !client.is_connect() {
        thread::sleep(Duration::from_secs(1));
    }

    println!("{:?}", client.is_connect());

    let recv = client.recv("hello", None);
    println!("{:?}", recv);

    let recv = recv.unwrap();

    for ret in recv {
        println!("{:?}", ret);
    }
}
