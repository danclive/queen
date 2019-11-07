#![allow(unused_imports)]

use std::time::{Duration, Instant};
use std::thread;

use queen::queen::Queen;
use queen::dict::*;

use nson::message_id::MessageId;
use nson::{Message, msg};

fn main() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let stream1 = queen.connect(msg!{}, None).unwrap();

    let time1 = Instant::now();

    for i in 0..1000000 {
        stream1.send(msg!{
            CHAN: AUTH,
            "i": i
        });

        stream1.send(msg!{
            "aaa": "bbb",
            "i": i
        });

        //stream1.recv();
    }

    drop(stream1);

    let time2 = Instant::now();

    println!("{:?}", time2 - time1);

    thread::sleep(Duration::from_secs(1000));
}
