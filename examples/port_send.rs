use std::thread;
use std::time::Duration;

use queen::{Port, Connector};
use queen::net::Addr;

use nson::{MessageId, msg};

fn main() {
    let port1 = Port::connect(
        MessageId::new(),
        Connector::Net(
            Addr::tcp("127.0.0.1:8888").unwrap(),
            None
        ),
        msg!{},
        2
    ).unwrap();

    loop {
        thread::sleep(Duration::from_secs(1));
        println!("{:?}", "send");
        port1.send("aaa", msg!{"hello": "world"}, None);
    }
}
