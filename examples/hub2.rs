use std::thread;
use std::time::{Duration, Instant};

use queen::nson::msg;

use queen::net::Addr;
use queen::port::{Hub, Recv};

fn main() {

    let addr = Addr::tcp("127.0.0.1:8888").unwrap();

    let hub = Hub::connect(addr, msg!{}, None).unwrap();

    thread::sleep(Duration::from_millis(1000));

    let time1 = Instant::now();

    //let mut i = 0;

    for i in 0..100000 {
        // hub.send("switch.esp8266.control", msg!{"s2": false});
        hub.send("aaa", msg!{"s2": false, "i": i});
        //i += 1;
    }

    let time2 = Instant::now();

    println!("{:?}", time2 - time1);

    thread::sleep(Duration::from_millis(1000));
}
