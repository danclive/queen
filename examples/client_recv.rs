#![allow(unused_imports)]
use std::io::stdout;
use std::thread;
use std::time::Duration;

use queen::client::{Client, ClientOptions};
use queen::net::CryptoOptions;
use queen::crypto::Method;

fn init_logger() {
    use log::LevelFilter;

    // init logger
    let mut builder = queen_log::filter::Builder::new();

    if let Ok(ref filter) = std::env::var("LOG_LEVEL") {
        builder.parse(filter);
    }

    queen_log::init_with_logger(
        LevelFilter::max(),
        queen_log::QueenLogger::new(stdout(), builder.build(), true)
    ).unwrap();
}

fn main() {
    init_logger();

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

    // loop {
    //     match recv.recv_timeout(Duration::from_secs(10)) {
    //         Ok(message) => {
    //             println!("recv: {:?}", message);
    //         }
    //         Err(err) => {
    //             println!("{:?}", err);
    //             client.stop();
    //         }
    //     }
    // }
}
