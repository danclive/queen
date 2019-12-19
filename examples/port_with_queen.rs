use std::time::Duration;
use std::thread;

use nson::{msg, MessageId};

use queen::{Queen, Port, Connector};

fn main() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let port1 = Port::connect(
        MessageId::new(),
        Connector::Queen(queen.clone(), msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    let port2 = Port::connect(
        MessageId::new(),
        Connector::Queen(queen, msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let mut recv = port1.recv("aaa", None);

    thread::sleep(Duration::from_secs(1));

    port2.send("aaa", msg!{"hello": "world"}, None);

    assert!(recv.next().is_some());
}
