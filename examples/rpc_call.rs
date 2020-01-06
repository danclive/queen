use std::time::Duration;

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

    let res = port1.call("hello", None, msg!{"hello": "world"},
        Some(<Duration>::from_secs(1)));

    println!("{:?}", res);

    println!("{:?}", join_handle.join());
}
