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
        thread::sleep(Duration::from_secs(1));
    }

    let ret = client.add_handle("hello", |message| {
        println!("{:?}", message);

        msg!{"1111": "2222"}
    }, None);

    println!("{:?}", ret);


    client.wait();
}
