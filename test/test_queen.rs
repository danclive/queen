use std::time::Duration;
use std::thread;

use nson::{msg, MessageId};

use queen::{Socket, Hook, Slot};
use queen::dict::*;
use queen::error::{Code, Error, RecvError};

#[test]
fn conn() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    for _ in 0..10000 {
        let _wire = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();
    }

    struct MyHook;

    impl Hook for MyHook {
        fn accept(&self, _: &Slot) -> bool {
            false
        }
    }

    let socket = Socket::new(MessageId::new(), MyHook).unwrap();

    let ret = socket.connect(MessageId::new(), false, msg!{}, None, None);
    assert!(ret.is_err());

    match ret {
        Ok(_) => unreachable!(),
        Err(err) => {
            assert!(matches!(err, Error::ErrorCode(Code::AuthenticationFailed)));
        }
    }
}

#[test]
fn connect() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();
    let wire2 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: PING
    });

    let _ = wire2.send(msg!{
        CHAN: PING
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    let _ = wire1.send(msg!{
        "aaa": "bbb"
    });

    let _ = wire2.send(msg!{
        CHAN: 123
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(Code::get(&recv) == Some(Code::CannotGetChanField));

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(Code::get(&recv) == Some(Code::CannotGetChanField));
}

#[test]
fn attach_detach() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();
    let wire2 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: PING
    });

    let _ = wire2.send(msg!{
        CHAN: PING
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // attach
    let _ = wire2.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa"
    });

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world"
    });

    // recv
    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).is_ok());

    // send with from
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        FROM: 123
    });

    // recv
    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_i32(FROM) == Ok(123));

    // detach
    let _ = wire2.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa"
    });

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world"
    });

    // recv
    assert!(wire2.wait(Some(Duration::from_millis(100))) == Err(RecvError::TimedOut));
}

#[test]
fn label() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();
    let wire2 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();
    let wire3 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: PING
    });

    let _ = wire2.send(msg!{
        CHAN: PING
    });

    let _ = wire3.send(msg!{
        CHAN: PING
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // attach
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa"
    });

    let _ = wire2.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa",
        LABEL: "label1"
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world"
    });

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get(FROM).is_some());

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get(FROM).is_some());

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: "label1"
    });

    // recv
    assert!(wire1.wait(Some(Duration::from_millis(100))) == Err(RecvError::TimedOut));

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    // detach
    let _ = wire2.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa",
        LABEL: "label1"
    });

    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: "label1"
    });

    // recv
    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());

    // send
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world"
    });

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    let wire4 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    let _ = wire4.send(msg!{
        CHAN: PING
    });

    // attach
    let _ = wire4.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa",
        LABEL: "label1"
    });

    let _ = wire4.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa",
        LABEL: "label2"
    });

    assert!(wire4.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire4.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire4.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: "label1"
    });

    // recv
    let recv = wire4.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    // detach
    let _ = wire4.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa",
        LABEL: "label1"
    });

    assert!(wire4.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: "label2"
    });

    // recv
    let recv = wire4.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
}

#[test]
fn labels() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();
    let wire2 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();
    let wire3 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: PING
    });

    let _ = wire2.send(msg!{
        CHAN: PING
    });

    let _ = wire3.send(msg!{
        CHAN: PING
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // attach
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa",
        LABEL: ["label1", "label2", "label3", "label1"]
    });

    let _ = wire2.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa",
        LABEL: ["label2", "label3", "label4"]
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world"
    });

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get(FROM).is_some());

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get(FROM).is_some());

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: "label5"
    });

    // recv
    assert!(wire1.wait(Some(Duration::from_millis(100))).is_err());

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label5", "label6"]
    });

    // recv
    assert!(wire1.wait(Some(Duration::from_millis(100))).is_err());

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: "label1"
    });

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label1"]
    });

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label1", "label5"]
    });

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label1", "label4"]
    });

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    // detach
    let _ = wire1.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa",
        LABEL: "label1"
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label1", "label4"]
    });

    // recv
    assert!(wire1.wait(Some(Duration::from_millis(100))).is_err());

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    // detach
    let _ = wire2.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa",
        LABEL: ["label2", "label3"]
    });

    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: "label2"
    });

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label2", "label3"]
    });

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());

    let wire4 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    let _ = wire4.send(msg!{
        CHAN: PING
    });

    // attach
    let _ = wire4.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa",
        LABEL: ["label1", "label2"]
    });

    let _ = wire4.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa",
        LABEL: ["label3", "label4"]
    });

    assert!(wire4.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire4.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire4.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: "label1"
    });

    // recv
    let recv = wire4.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    // detach
    let _ = wire4.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa",
        LABEL: ["label2", "label3"]
    });

    assert!(wire4.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: "label1"
    });

    // recv
    let recv = wire4.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
}

#[test]
fn share() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();
    let wire2 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();
    let wire3 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();
    let wire4 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: PING
    });

    let _ = wire2.send(msg!{
        CHAN: PING
    });

    let _ = wire3.send(msg!{
        CHAN: PING
    });

    let _ = wire4.send(msg!{
        CHAN: PING
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire4.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // attatch
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa"
    });

    let _ = wire2.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa"
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world"
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get(FROM).is_some());

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get(FROM).is_some());

    // send with share
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        SHARE: true
    });

    let mut read_num = 0;

    if wire1.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    if wire2.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    assert!(read_num == 1);

    // with label

    // attatch
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "bbb",
        LABEL: "label1"
    });

    let _ = wire2.send(msg!{
        CHAN: ATTACH,
        VALUE: "bbb",
        LABEL: "label1"
    });

    let _ = wire3.send(msg!{
        CHAN: ATTACH,
        VALUE: "bbb"
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send with share
    let _ = wire4.send(msg!{
        CHAN: "bbb",
        "hello": "world",
        LABEL: "label1"
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "bbb");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get(FROM).is_some());

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "bbb");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get(FROM).is_some());

    assert!(wire3.wait(Some(Duration::from_millis(100))).is_err());

    // send with share
    let _ = wire4.send(msg!{
        CHAN: "bbb",
        "hello": "world",
        LABEL: "label1",
        SHARE: true
    });

    let mut read_num = 0;

    if wire1.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    if wire2.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    assert!(read_num == 1);

    assert!(wire3.wait(Some(Duration::from_millis(100))).is_err());
}

#[test]
fn share_attach() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();
    let wire2 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();
    let wire3 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();
    let wire4 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: PING
    });

    let _ = wire2.send(msg!{
        CHAN: PING
    });

    let _ = wire3.send(msg!{
        CHAN: PING
    });

    let _ = wire4.send(msg!{
        CHAN: PING
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire4.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // attatch
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa",
        SHARE: true
    });

    let _ = wire2.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa",
        SHARE: true
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world"
    });

    let mut read_num = 0;

    if wire1.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    if wire2.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    assert!(read_num == 1);

    // detach
    let _ = wire1.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa"
    });

    let _ = wire2.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa"
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world"
    });

    let mut read_num = 0;

    if wire1.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    if wire2.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    assert!(read_num == 1);

    // detach
    let _ = wire1.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa",
        SHARE: true
    });

    let _ = wire2.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa",
        SHARE: true
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world"
    });

    let mut read_num = 0;

    if wire1.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    if wire2.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    assert!(read_num == 0);

    // with label

    // attatch
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "bbb",
        LABEL: "label1",
        SHARE: true
    });

    let _ = wire2.send(msg!{
        CHAN: ATTACH,
        VALUE: "bbb",
        LABEL: "label1",
        SHARE: true
    });

    let _ = wire3.send(msg!{
        CHAN: ATTACH,
        VALUE: "bbb",
        SHARE: true
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire4.send(msg!{
        CHAN: "bbb",
        "hello": "world",
        LABEL: "label1"
    });

    let mut read_num = 0;

    if wire1.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    if wire2.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    assert!(read_num == 1);

    assert!(wire3.wait(Some(Duration::from_millis(100))).is_err());

    // detach
    let _ = wire1.send(msg!{
        CHAN: DETACH,
        VALUE: "bbb",
        LABEL: "label1"
    });

    let _ = wire2.send(msg!{
        CHAN: DETACH,
        VALUE: "bbb",
        LABEL: "label1"
    });

    let _ = wire3.send(msg!{
        CHAN: DETACH,
        VALUE: "bbb"
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire4.send(msg!{
        CHAN: "bbb",
        "hello": "world",
        LABEL: "label1"
    });

    let mut read_num = 0;

    if wire1.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    if wire2.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    assert!(read_num == 1);

    assert!(wire3.wait(Some(Duration::from_millis(100))).is_err());

    // detach
    let _ = wire1.send(msg!{
        CHAN: DETACH,
        VALUE: "bbb",
        LABEL: "label1",
        SHARE: true
    });

    let _ = wire2.send(msg!{
        CHAN: DETACH,
        VALUE: "bbb",
        LABEL: "label1",
        SHARE: true
    });

    let _ = wire3.send(msg!{
        CHAN: DETACH,
        VALUE: "bbb",
        SHARE: true
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire4.send(msg!{
        CHAN: "bbb",
        "hello": "world",
        LABEL: "label1"
    });

    let mut read_num = 0;

    if wire1.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    if wire2.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    assert!(read_num == 0);

    assert!(wire3.wait(Some(Duration::from_millis(100))).is_err());

    // attach
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "ccc",
        SHARE: true
    });

    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "ccc"
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire3.send(msg!{
        CHAN: "ccc",
        "hello": "world"
    });

    let mut read_num = 0;

    if wire1.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    if wire1.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    assert!(read_num == 2);

    // detach
    let _ = wire1.send(msg!{
        CHAN: DETACH,
        VALUE: "ccc"
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire3.send(msg!{
        CHAN: "ccc",
        "hello": "world"
    });

    let mut read_num = 0;

    if wire1.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    if wire1.wait(Some(Duration::from_millis(100))).is_ok() {
        read_num += 1;
    }

    assert!(read_num == 1);
}

#[test]
fn wire_to_wire() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let slot_id = MessageId::with_string("016f9dd00d746c7f89ce3423").unwrap();
    let wire1 = socket.connect(slot_id, false, msg!{}, None, None).unwrap();

    let slot_id = MessageId::with_string("016f9dd0c97338e09f5c61e9").unwrap();
    let wire2 = socket.connect(slot_id, false, msg!{}, None, None).unwrap();

    let slot_id = MessageId::with_string("016f9dd0c97338e09f5c61e9").unwrap();
    let ret = socket.connect(slot_id, false, msg!{}, None, None);

    assert!(matches!(ret, Err(Error::ErrorCode(Code::DuplicateSlotId))));

    let _ = wire1.send(msg!{
        CHAN: PING
    });

    let _ = wire2.send(msg!{
        CHAN: PING
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    let slot_id = MessageId::with_string("016f9dd11953dba9c0943f8c").unwrap();
    let wire3 = socket.connect(slot_id, false, msg!{}, None, None).unwrap();

    let _ = wire3.send(msg!{
        CHAN: PING
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // slot to slot
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        TO: MessageId::with_string("016f9dd00d746c7f89ce3423").unwrap()
    });

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).unwrap() == &MessageId::with_string("016f9dd11953dba9c0943f8c").unwrap());

    // slot to slots
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        TO: [MessageId::with_string("016f9dd00d746c7f89ce3423").unwrap(), MessageId::with_string("016f9dd0c97338e09f5c61e9").unwrap()]
    });

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).unwrap() == &MessageId::with_string("016f9dd11953dba9c0943f8c").unwrap());

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).unwrap() == &MessageId::with_string("016f9dd11953dba9c0943f8c").unwrap());

    // share
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        SHARE: true,
        TO: [MessageId::with_string("016f9dd00d746c7f89ce3423").unwrap(), MessageId::with_string("016f9dd0c97338e09f5c61e9").unwrap()]
    });

    // recv
    let mut num = 0;

    if wire1.wait(Some(Duration::from_millis(100))).is_ok() {
        num += 1;
    }

    if wire2.wait(Some(Duration::from_millis(100))).is_ok() {
        num += 1;
    }

    assert_eq!(num, 1);
}

#[test]
fn slot_event() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(MessageId::new(), true, msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: PING
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // attach
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: SLOT_READY
    });

    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: SLOT_BREAK
    });

    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: SLOT_ATTACH
    });

    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: SLOT_DETACH
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    let wire2 = socket.connect(MessageId::new(), true, msg!{}, None, None).unwrap();

    let _ = wire2.send(msg!{
        CHAN: PING
    });

    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == SLOT_READY);
    assert!(recv.get_bool(ROOT).unwrap() == true);
    assert!(recv.get_message_id(SLOT_ID).is_ok());
    assert!(recv.get_message(ATTR).is_ok());

    // attach
    let _ = wire2.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa"
    });

    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == SLOT_ATTACH);
    assert!(recv.get_str(VALUE).unwrap() == "aaa");
    assert!(recv.get_message_id(SLOT_ID).is_ok());

    let _ = wire2.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa",
        LABEL: "label1"
    });

    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == SLOT_ATTACH);
    assert!(recv.get_str(VALUE).unwrap() == "aaa");
    assert!(recv.get_str(LABEL).unwrap() == "label1");
    assert!(recv.get_message_id(SLOT_ID).is_ok());

    // detach
    let _ = wire2.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa",
        LABEL: "label1"
    });

    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == SLOT_DETACH);
    assert!(recv.get_str(VALUE).unwrap() == "aaa");
    assert!(recv.get_str(LABEL).unwrap() == "label1");
    assert!(recv.get_message_id(SLOT_ID).is_ok());

    let _ = wire2.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa"
    });

    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == SLOT_DETACH);
    assert!(recv.get_str(VALUE).unwrap() == "aaa");
    assert!(recv.get_message_id(SLOT_ID).is_ok());

    drop(wire2);

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == SLOT_BREAK);
    assert!(recv.get_message_id(SLOT_ID).is_ok());
    assert!(recv.get_message(ATTR).is_ok());

    let slot_id = MessageId::with_string("016f9dd11953dba9c0943f8c").unwrap();
    let wire2 = socket.connect(slot_id, false, msg!{}, None, None).unwrap();

    let _ = wire2.send(msg!{
        CHAN: PING
    });

    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    let _ = wire1.send(msg!{
        CHAN: SLOT_KILL,
        SLOT_ID: MessageId::with_string("016f9dd11953dba9c0943f8c").unwrap()
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == SLOT_READY);
    assert!(recv.get_bool(ROOT).unwrap() == false);
    assert!(recv.get_message_id(SLOT_ID).unwrap() == &MessageId::with_string("016f9dd11953dba9c0943f8c").unwrap());

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == SLOT_KILL);
    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_message_id(SLOT_ID).unwrap() == &MessageId::with_string("016f9dd11953dba9c0943f8c").unwrap());

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == SLOT_BREAK);
    assert!(recv.get_message_id(SLOT_ID).unwrap() == &MessageId::with_string("016f9dd11953dba9c0943f8c").unwrap());

    thread::sleep(Duration::from_millis(100));

    assert!(wire2.is_close());
    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());

    let _ = wire1.send(msg!{
        CHAN: SLOT_KILL,
        SLOT_ID: MessageId::with_string("016f9dd11953dba9c0943f8c").unwrap()
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(Code::get(&recv) == Some(Code::TargetSlotIdNotExist));
    assert!(recv.get_message_id(SLOT_ID).unwrap() == &MessageId::with_string("016f9dd11953dba9c0943f8c").unwrap());

    let wire3 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    let _ = wire3.send(msg!{
        CHAN: PING
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    let _ = wire3.send(msg!{
        CHAN: ATTACH,
        VALUE: SLOT_READY
    });

    let recv = wire3.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(Code::get(&recv) == Some(Code::PermissionDenied));
}

#[test]
fn mine() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    // mine
    let _ = wire1.send(msg!{
        CHAN: MINE,
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);

    let value = recv.get_message(VALUE).unwrap();

    assert!(value.get_bool(ROOT).unwrap() == false);
    assert!(value.get_message(CHANS).unwrap().is_empty());
    assert!(value.is_null(SLOT_ID) == false);
    assert!(value.get_u64(SEND_NUM).unwrap() == 1);
    assert!(value.get_u64(RECV_NUM).unwrap() == 1);
    assert!(value.get_array(BINDED).is_ok());
    assert!(value.get_array(BOUNDED).is_ok());
    assert!(value.get_bool(JOINED).is_ok());

    // attach
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "hello",
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // mine
    let _ = wire1.send(msg!{
        CHAN: MINE,
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);

    let value = recv.get_message(VALUE).unwrap();

    assert!(value.get_bool(ROOT).unwrap() == false);
    assert!(value.get_message(CHANS).unwrap().get_array("hello").is_ok());
    assert!(value.get_message_id(SLOT_ID).is_ok());
    assert!(value.get_u64(SEND_NUM).unwrap() == 3);
    assert!(value.get_u64(RECV_NUM).unwrap() == 3);
}

#[test]
fn slot_event_send_recv() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();
    let wire2 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();
    let wire3 = socket.connect(MessageId::new(), true, msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: PING
    });

    let _ = wire2.send(msg!{
        CHAN: PING
    });

    let _ = wire3.send(msg!{
        CHAN: PING
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // attach slot event
    let _ = wire3.send(msg!{
        CHAN: ATTACH,
        VALUE: SLOT_SEND,
    });

    let _ = wire3.send(msg!{
        CHAN: ATTACH,
        VALUE: SLOT_RECV,
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // try send
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world"
    });

    let recv = wire3.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == SLOT_SEND);
    assert!(recv.get_message(VALUE).unwrap().get_str(CHAN).unwrap() == "aaa");

    // attach
    let _ = wire2.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa"
    });

    // send
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world"
    });

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);

    // recv
    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    let recv = wire3.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_str(CHAN).unwrap() == SLOT_RECV);

    let recv = wire3.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_str(CHAN).unwrap() == SLOT_SEND);
}

#[test]
fn self_send_recv() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: PING
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // attach
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa"
    });

    // send
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world"
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_str(CHAN).unwrap() == ATTACH);
    assert!(recv.get_i32(CODE).is_ok());

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).is_ok());
    assert!(recv.get_i32(CODE).is_err());

    assert!(wire1.wait(Some(Duration::from_millis(100))).is_err());

    // wire2
    let wire2 = socket.connect(MessageId::new(), false, msg!{}, None, None).unwrap();

    let _ = wire2.send(msg!{
        CHAN: PING
    });

    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // attach
    let _ = wire2.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa"
    });

    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world"
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).is_ok());
    assert!(recv.get_i32(CODE).is_err());

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).is_ok());
    assert!(recv.get_i32(CODE).is_err());

    assert!(wire1.wait(Some(Duration::from_millis(100))).is_err());
    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());
}

#[test]
fn bind_unbind() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let slot_id1 = MessageId::new();
    let slot_id2 = MessageId::new();
    let wire1 = socket.connect(slot_id1, false, msg!{}, None, None).unwrap();
    let wire2 = socket.connect(slot_id2, false, msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: PING
    });

    let _ = wire2.send(msg!{
        CHAN: PING
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // BIND_SEND
    // bind
    let _ = wire2.send(msg!{
        CHAN: BIND,
        SLOT_ID: slot_id1
    });

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world"
    });

    // recv
    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == BIND_SEND);
    assert!(recv.get_message_id(SLOT_ID).unwrap() == &slot_id1);
    assert!(recv.get_message(VALUE).unwrap().get_str("hello").unwrap() == "world");
    assert!(recv.get_message(VALUE).unwrap().get_message_id(FROM).is_ok());

    // unbind
    let _ = wire2.send(msg!{
        CHAN: UNBIND,
        SLOT_ID: slot_id1
    });

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world"
    });

    // recv
    assert!(wire2.wait(Some(Duration::from_millis(100))) == Err(RecvError::TimedOut));


    // BIND_RECV
    let slot_id3 = MessageId::new();
    let wire3 = socket.connect(slot_id3, false, msg!{}, None, None).unwrap();

    let _ = wire3.send(msg!{
        CHAN: PING
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // bind
    let _ = wire2.send(msg!{
        CHAN: BIND,
        SLOT_ID: slot_id3
    });

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        TO: slot_id3
    });

    // recv
    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == BIND_RECV);
    assert!(recv.get_message_id(SLOT_ID).unwrap() == &slot_id3);
    assert!(recv.get_message(VALUE).unwrap().get_str("hello").unwrap() == "world");
    assert!(recv.get_message(VALUE).unwrap().get_message_id(FROM).is_ok());

    // unbind
    let _ = wire2.send(msg!{
        CHAN: UNBIND,
        SLOT_ID: slot_id3
    });

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        TO: slot_id3
    });

    // recv
    assert!(wire2.wait(Some(Duration::from_millis(100))) == Err(RecvError::TimedOut));
}

#[test]
fn to_socket() {
    let socket_id = MessageId::new();
    let socket = Socket::new(socket_id, ()).unwrap();

    let slot_id1 = MessageId::new();
    let slot_id2 = MessageId::new();
    let slot_id3 = MessageId::new();
    let wire1 = socket.connect(slot_id1, false, msg!{}, None, None).unwrap();
    let wire2 = socket.connect(slot_id2, false, msg!{}, None, None).unwrap();
    let wire3 = socket.connect(slot_id3, false, msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: PING
    });

    let _ = wire2.send(msg!{
        CHAN: PING
    });

    let _ = wire3.send(msg!{
        CHAN: PING
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        TO: slot_id3,
        TO_SOCKET: 123
    });

    // err
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(Code::get(&recv) == Some(Code::InvalidToSocketFieldType));

    // send
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        TO: slot_id3,
        TO_SOCKET: socket_id
    });

    // recv
    let recv = wire3.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_str(CHAN).unwrap() == "aaa");

    // join
    let _ = wire2.send(msg!{
        CHAN: JOIN
    });

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        TO: slot_id3,
        TO_SOCKET: slot_id2
    });

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM_SOCKET).unwrap() == &socket_id);
    assert!(recv.get_message_id(TO_SOCKET).unwrap() == &slot_id2);

    // unjoin
    let _ = wire2.send(msg!{
        CHAN: UNJOIN
    });

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);

    // send
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        TO: slot_id3,
        TO_SOCKET: slot_id2
    });

    assert!(wire2.wait(Some(Duration::from_millis(100))) == Err(RecvError::TimedOut));
}
