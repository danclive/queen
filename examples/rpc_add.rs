use std::time::Duration;
use std::thread;

use queen::{Port, Connector};
use queen::net::Addr;

use nson::{MessageId, msg};

fn main() {
    let (port1, join_handle) = Port::connect(
        MessageId::new(),
        Connector::Net(
            Addr::tcp("127.0.0.1:8888").unwrap(),
            None
        ),
        msg!{},
        2
    ).unwrap();

    port1.add("hello", None, move |message| {
        println!("{:?}", message);
        thread::sleep(Duration::from_secs(3));

        msg!{"hehehe": "lalala"}
    }, Some(Duration::from_secs(1))).unwrap();

    println!("{:?}", join_handle.join());
}
