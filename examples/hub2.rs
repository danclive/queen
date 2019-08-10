use std::thread;
use std::time::Duration;

use queen::nson::msg;

use queen::net::Addr;
use queen::port::{Hub, Recv};

fn main() {

    let addr = Addr::tcp("127.0.0.1:8888").unwrap();

    let hub = Hub::connect(addr, msg!{}, None).unwrap();

    thread::sleep(Duration::from_millis(1000));

    let mut i = 0;

    loop {
        hub.send("aaa", msg!{"a": 123, "i": i});
        i += 1;
    }

    thread::sleep(Duration::from_millis(1000));
}
