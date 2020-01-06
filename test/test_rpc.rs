use std::time::Duration;
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use nson::{msg, MessageId};

use queen::{Queen, Port, Connector};

#[test]
fn recv() {
    let atomic = Arc::new(AtomicUsize::new(0));

    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let (rpc1, _) = Port::connect(
        MessageId::new(),
        Connector::Queen(queen.clone(), msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
        ).unwrap();

    let atomic2 = atomic.clone();
    rpc1.add("hello", None, move |_message| {
        
        atomic2.fetch_add(1, Ordering::SeqCst);
        msg!{"hehehe": "lalala"}
    }, Some(Duration::from_secs(1))).unwrap();

    let (rpc2, _) = Port::connect(
        MessageId::new(),
        Connector::Queen(queen.clone(), msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
        ).unwrap();

    let atomic2 = atomic.clone();
    rpc2.add("hello", None, move |_message| {
        
        atomic2.fetch_add(1, Ordering::SeqCst);
        msg!{"hehehe": "lalala"}
    }, Some(Duration::from_secs(1))).unwrap();

    thread::sleep(Duration::from_secs(1));


    let (rpc3, _) = Port::connect(
        MessageId::new(),
        Connector::Queen(queen, msg!{}),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
    ).unwrap();

    thread::sleep(Duration::from_secs(1));

    let res = rpc3.call("hello", None, msg!{"hello": "world"},
        Some(<Duration>::from_secs(10)));
    assert!(res.is_ok());

    assert!(atomic.load(Ordering::SeqCst) == 1);
}
