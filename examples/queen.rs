use std::time::Duration;
use std::thread;

use queen::queen::Queen;

use nson::message_id::MessageId;
use nson::{Message, msg};

fn main() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let stream1 = queen.connect(msg!{}, None).unwrap();

    stream1.send(msg!{
        "aaa": "bbb"
    });

    thread::sleep(Duration::from_secs(1));

    println!("{:?}", stream1.recv());

    stream1.close();
    stream1.close();

    // let queen2 = queen.clone();

    thread::sleep(Duration::from_secs(2));
}
