use std::time::Duration;
use std::thread;

use queen::nson::msg;
use queen::nson::MessageId;
use queen::crypto::Method;
use queen::net::{Addr};
use queen::{Port, Connector};

fn main() {
    let crypto = (Method::Aes256Gcm, "sep-centre".to_string());

    let rpc = Port::connect(
        MessageId::new(),
        Connector::Net(Addr::tcp("127.0.0.1:8888").unwrap(), Some(crypto)),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
        ).unwrap();

    rpc.add("hello", None, |message| {
        println!("{:?}", message);

        msg!{"hehehe": "lalala"}
    });

    thread::sleep(Duration::from_secs(60 * 10));
}
