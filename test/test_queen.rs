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
        let _wire = socket.connect(msg!{}, None, None).unwrap();
    }

    struct MyHook;

    impl Hook for MyHook {
        fn accept(&self, _: &Slot) -> bool {
            false
        }
    }

    let socket = Socket::new(MessageId::new(), MyHook).unwrap();

    let ret = socket.connect(msg!{}, None, None);
    assert!(ret.is_err());

    match ret {
        Ok(_) => unreachable!(),
        Err(err) => {
            assert!(matches!(err, Error::ConnectionRefused(_)));
        }
    }
}

#[test]
fn connect() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(msg!{}, None, None).unwrap();
    let wire2 = socket.connect(msg!{}, None, None).unwrap();

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
fn auth() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: ATTACH
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(Code::get(&recv) == Some(Code::Unauthorized));

    let _ = wire1.send(msg!{
        CHAN: AUTH
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_message_id(SLOT_ID).is_ok());
    assert!(recv.get(LABEL).is_none());

    let _ = wire1.send(msg!{
        CHAN: AUTH,
        LABEL: msg!{
            "aa": "bb"
        }
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_message_id(SLOT_ID).is_ok());
    assert!(recv.get_message(LABEL).unwrap() == &msg!{"aa": "bb"});

    let slot_id = MessageId::new();

    let _ = wire1.send(msg!{
        CHAN: AUTH,
        SLOT_ID: slot_id.clone()
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_message_id(SLOT_ID).unwrap() == &slot_id);
    assert!(recv.get_message(LABEL).unwrap() == &msg!{"aa": "bb"});

    let _ = wire1.send(msg!{
        CHAN: AUTH,
        LABEL: msg!{
            "cc": "dd"
        }
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_message_id(SLOT_ID).is_ok());
    assert!(recv.get_message(LABEL).unwrap() == &msg!{"cc": "dd"});

    // wire2
    let wire2 = socket.connect(msg!{}, None, None).unwrap();
    let _ = wire2.send(msg!{
        CHAN: AUTH,
        SLOT_ID: slot_id.clone()
    });

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(Code::get(&recv) == Some(Code::DuplicateSlotId));

    let _ = wire2.send(msg!{
        CHAN: MINE,
    });

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);

    let value = recv.get_message(VALUE).unwrap();

    assert!(value.get_bool(AUTH).unwrap() == false);
    assert!(value.get_bool(ROOT).unwrap() == false);
    assert!(value.get_message(CHANS).unwrap().is_empty());
    assert!(value.get_message_id(SLOT_ID).unwrap() != &slot_id);
    assert!(value.get_u64(SEND_NUM).unwrap() == 2);
    assert!(value.get_u64(RECV_NUM).unwrap() == 2);

    drop(wire1);

    thread::sleep(Duration::from_millis(100));

    // wire 2 auth again
    let _ = wire2.send(msg!{
        CHAN: AUTH,
        SLOT_ID: slot_id.clone()
    });

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_message_id(SLOT_ID).unwrap() == &slot_id);
}

#[test]
fn attach_detach() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(msg!{}, None, None).unwrap();
    let wire2 = socket.connect(msg!{}, None, None).unwrap();

    // auth
    let _ = wire1.send(msg!{
        CHAN: AUTH
    });

    let _ = wire2.send(msg!{
        CHAN: AUTH
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // try send
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123"
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_str(ACK).unwrap() == "123");

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
        "hello": "world",
        ACK: "123"
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);

    // recv
    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).is_ok());

    // send with from
    let _ = wire1.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123",
        FROM: 123
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);

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
        "hello": "world",
        ACK: "123"
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_str(ACK).unwrap() == "123");

    // recv
    assert!(wire2.wait(Some(Duration::from_millis(100))) == Err(RecvError::TimedOut));
}

#[test]
fn label() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(msg!{}, None, None).unwrap();
    let wire2 = socket.connect(msg!{}, None, None).unwrap();
    let wire3 = socket.connect(msg!{}, None, None).unwrap();

    // auth
    let _ = wire1.send(msg!{
        CHAN: AUTH
    });

    let _ = wire2.send(msg!{
        CHAN: AUTH
    });

    let _ = wire3.send(msg!{
        CHAN: AUTH
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

    // send
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123"
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

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
        LABEL: "label1",
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

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
        LABEL: "label1",
        ACK: "123"
    });

    let recv = wire3.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_str(ACK).unwrap() == "123");

    // recv
    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());

    // send
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    let wire4 = socket.connect(msg!{}, None, None).unwrap();

    // auth
    let _ = wire4.send(msg!{
        CHAN: AUTH
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
        LABEL: "label1",
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

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
        LABEL: "label2",
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // recv
    let recv = wire4.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
}

#[test]
fn labels() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(msg!{}, None, None).unwrap();
    let wire2 = socket.connect(msg!{}, None, None).unwrap();
    let wire3 = socket.connect(msg!{}, None, None).unwrap();

    // auth
    let _ = wire1.send(msg!{
        CHAN: AUTH
    });

    let _ = wire2.send(msg!{
        CHAN: AUTH
    });

    let _ = wire3.send(msg!{
        CHAN: AUTH
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
        "hello": "world",
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

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
        LABEL: "label5",
        ACK: "123"
    });

    let recv = wire3.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_str(ACK).unwrap() == "123");

    // recv
    assert!(wire1.wait(Some(Duration::from_millis(100))).is_err());

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label5", "label6"],
        ACK: "123"
    });

    let recv = wire3.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_str(ACK).unwrap() == "123");

    // recv
    assert!(wire1.wait(Some(Duration::from_millis(100))).is_err());

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: "label1",
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label1"],
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label1", "label5"],
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label1", "label4"],
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

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
        LABEL: ["label1", "label4"],
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

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
        LABEL: "label2",
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());

    // send with label
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label2", "label3"],
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());

    let wire4 = socket.connect(msg!{}, None, None).unwrap();

    // auth
    let _ = wire4.send(msg!{
        CHAN: AUTH
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
        LABEL: "label1",
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

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
        LABEL: "label1",
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // recv
    let recv = wire4.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
}

#[test]
fn share() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(msg!{}, None, None).unwrap();
    let wire2 = socket.connect(msg!{}, None, None).unwrap();
    let wire3 = socket.connect(msg!{}, None, None).unwrap();
    let wire4 = socket.connect(msg!{}, None, None).unwrap();

    // auth
    let _ = wire1.send(msg!{
        CHAN: AUTH
    });

    let _ = wire2.send(msg!{
        CHAN: AUTH
    });

    let _ = wire3.send(msg!{
        CHAN: AUTH
    });

    let _ = wire4.send(msg!{
        CHAN: AUTH
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
        "hello": "world",
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

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
        SHARE: true,
        ACK: "123"
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

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
        LABEL: "label1",
        ACK: "123"
    });

    assert!(wire4.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

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
        SHARE: true,
        ACK: "123"
    });

    assert!(wire4.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

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
fn wire_to_wire() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(msg!{}, None, None).unwrap();
    let wire2 = socket.connect(msg!{}, None, None).unwrap();
    let wire3 = socket.connect(msg!{}, None, None).unwrap();

    // auth
    let _ = wire1.send(msg!{
        CHAN: AUTH,
        SLOT_ID: MessageId::with_string("016f9dd00d746c7f89ce342387e4c462").unwrap()
    });

    let _ = wire2.send(msg!{
        CHAN: AUTH,
        SLOT_ID: MessageId::with_string("016f9dd0c97338e09f5c61e91e43f7c0").unwrap()
    });

    // duplicate
    let _ = wire3.send(msg!{
        CHAN: AUTH,
        SLOT_ID: MessageId::with_string("016f9dd0c97338e09f5c61e91e43f7c0").unwrap()
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    // assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    let recv = wire3.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(Code::get(&recv) == Some(Code::DuplicateSlotId));

    let _ = wire3.send(msg!{
        CHAN: AUTH,
        SLOT_ID: MessageId::with_string("016f9dd11953dba9c0943f8c7ba0924b").unwrap()
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // type
    let wire4 = socket.connect(msg!{}, None, None).unwrap();

    let _ = wire4.send(msg!{
        CHAN: AUTH,
        SLOT_ID: 123
    });

    let recv = wire4.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(Code::get(&recv) == Some(Code::InvalidSlotIdFieldType));

    // slot to slot
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123",
        TO: MessageId::with_string("016f9dd25e24d713c22ec04881afd5d2").unwrap()
    });

    let recv = wire3.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(Code::get(&recv) == Some(Code::TargetSlotIdNotExist));
    assert!(recv.get_message_id(SLOT_ID).unwrap() == &MessageId::with_string("016f9dd25e24d713c22ec04881afd5d2").unwrap());

    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123",
        TO: MessageId::with_string("016f9dd00d746c7f89ce342387e4c462").unwrap()
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).unwrap() == &MessageId::with_string("016f9dd11953dba9c0943f8c7ba0924b").unwrap());

    // slot to slots
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123",
        TO: [MessageId::with_string("016f9dd00d746c7f89ce342387e4c463").unwrap(), MessageId::with_string("016f9dd25e24d713c22ec04881afd5d2").unwrap()]
    });

    let recv = wire3.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(Code::get(&recv) == Some(Code::TargetSlotIdNotExist));
    let array = recv.get_array(SLOT_ID).unwrap();
    assert!(array.len() == 2);
    assert!(array.contains(&MessageId::with_string("016f9dd00d746c7f89ce342387e4c463").unwrap().into()));
    assert!(array.contains(&MessageId::with_string("016f9dd25e24d713c22ec04881afd5d2").unwrap().into()));

    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123",
        TO: [MessageId::with_string("016f9dd00d746c7f89ce342387e4c462").unwrap(), MessageId::with_string("016f9dd0c97338e09f5c61e91e43f7c0").unwrap()]
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // recv
    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).unwrap() == &MessageId::with_string("016f9dd11953dba9c0943f8c7ba0924b").unwrap());

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).unwrap() == &MessageId::with_string("016f9dd11953dba9c0943f8c7ba0924b").unwrap());

    // share
    let _ = wire3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123",
        SHARE: true,
        TO: [MessageId::with_string("016f9dd00d746c7f89ce342387e4c462").unwrap(), MessageId::with_string("016f9dd0c97338e09f5c61e91e43f7c0").unwrap()]
    });

    assert!(wire3.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

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

    let wire1 = socket.connect(msg!{}, None, None).unwrap();

    let _ = wire1.send(msg!{
        CHAN: AUTH,
        ROOT: 123
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(Code::get(&recv) == Some(Code::InvalidRootFieldType));

    let _ = wire1.send(msg!{
        CHAN: AUTH,
        ROOT: true
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // supe attach
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

    // auth
    let wire2 = socket.connect(msg!{}, None, None).unwrap();

    let _ = wire2.send(msg!{
        CHAN: AUTH,
        ROOT: true
    });

    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == SLOT_READY);
    assert!(recv.get_bool(ROOT).unwrap() == true);
    assert!(recv.get_message_id(SLOT_ID).is_ok());
    assert!(recv.get_message(LABEL).is_ok());
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
    assert!(recv.get_message(LABEL).is_ok());
    assert!(recv.get_message(ATTR).is_ok());

    // auth
    let wire2 = socket.connect(msg!{}, None, None).unwrap();

    let _ = wire2.send(msg!{
        CHAN: AUTH,
        SLOT_ID: MessageId::with_string("016f9dd11953dba9c0943f8c7ba0924b").unwrap()
    });

    assert!(wire2.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    let _ = wire1.send(msg!{
        CHAN: SLOT_KILL,
        SLOT_ID: MessageId::with_string("016f9dd11953dba9c0943f8c7ba0924b").unwrap()
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == SLOT_READY);
    assert!(recv.get_bool(ROOT).unwrap() == false);
    assert!(recv.get_message_id(SLOT_ID).unwrap() == &MessageId::with_string("016f9dd11953dba9c0943f8c7ba0924b").unwrap());

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == SLOT_KILL);
    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_message_id(SLOT_ID).unwrap() == &MessageId::with_string("016f9dd11953dba9c0943f8c7ba0924b").unwrap());

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_str(CHAN).unwrap() == SLOT_BREAK);
    assert!(recv.get_message_id(SLOT_ID).unwrap() == &MessageId::with_string("016f9dd11953dba9c0943f8c7ba0924b").unwrap());

    thread::sleep(Duration::from_millis(100));

    assert!(wire2.is_close());
    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());

    let _ = wire1.send(msg!{
        CHAN: SLOT_KILL,
        SLOT_ID: MessageId::with_string("016f9dd11953dba9c0943f8c7ba0924c").unwrap()
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(Code::get(&recv) == Some(Code::TargetSlotIdNotExist));
    assert!(recv.get_message_id(SLOT_ID).unwrap() == &MessageId::with_string("016f9dd11953dba9c0943f8c7ba0924c").unwrap());
}

#[test]
fn mine() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(msg!{}, None, None).unwrap();

    // mine
    let _ = wire1.send(msg!{
        CHAN: MINE,
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);

    let value = recv.get_message(VALUE).unwrap();

    assert!(value.get_bool(AUTH).unwrap() == false);
    assert!(value.get_bool(ROOT).unwrap() == false);
    assert!(value.get_message(CHANS).unwrap().is_empty());
    assert!(value.is_null(SLOT_ID) == false);
    assert!(value.get_u64(SEND_NUM).unwrap() == 1);
    assert!(value.get_u64(RECV_NUM).unwrap() == 1);

    // auth
    let _ = wire1.send(msg!{
        CHAN: AUTH,
        ROOT: true
    });

    // attach
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "hello",
    });

    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);
    assert!(wire1.wait(Some(Duration::from_millis(100))).unwrap().get_i32(CODE).unwrap() == 0);

    // mine
    let _ = wire1.send(msg!{
        CHAN: MINE,
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);

    let value = recv.get_message(VALUE).unwrap();

    assert!(value.get_bool(AUTH).unwrap() == true);
    assert!(value.get_bool(ROOT).unwrap() == true);
    assert!(value.get_message(CHANS).unwrap().get_array("hello").is_ok());
    assert!(value.get_message_id(SLOT_ID).is_ok());
    assert!(value.get_u64(SEND_NUM).unwrap() == 4);
    assert!(value.get_u64(RECV_NUM).unwrap() == 4);
}

#[test]
fn slot_event_send_recv() {
    let socket = Socket::new(MessageId::new(), ()).unwrap();

    let wire1 = socket.connect(msg!{}, None, None).unwrap();
    let wire2 = socket.connect(msg!{}, None, None).unwrap();
    let wire3 = socket.connect(msg!{}, None, None).unwrap();

    // auth
    let _ = wire1.send(msg!{
        CHAN: AUTH
    });

    let _ = wire2.send(msg!{
        CHAN: AUTH
    });

    let _ = wire3.send(msg!{
        CHAN: AUTH,
        ROOT: true
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
        "hello": "world",
        ACK: "123"
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_str(ACK).unwrap() == "123");

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
        "hello": "world",
        ACK: "123"
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();

    assert!(recv.get_i32(CODE).unwrap() == 0);

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

    let wire1 = socket.connect(msg!{}, None, None).unwrap();

    // auth
    let _ = wire1.send(msg!{
        CHAN: AUTH
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
        "hello": "world",
        ACK: "123"
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).is_ok());
    assert!(recv.get_i32(CODE).is_err());

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_str(ACK).unwrap() == "123");

    assert!(wire1.wait(Some(Duration::from_millis(100))).is_err());

    // wire2
    let wire2 = socket.connect(msg!{}, None, None).unwrap();

    // auth
    let _ = wire2.send(msg!{
        CHAN: AUTH
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
        "hello": "world",
        ACK: "123"
    });

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).is_ok());
    assert!(recv.get_i32(CODE).is_err());

    let recv = wire2.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).is_ok());
    assert!(recv.get_i32(CODE).is_err());

    let recv = wire1.wait(Some(Duration::from_millis(100))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_str(ACK).unwrap() == "123");

    assert!(wire1.wait(Some(Duration::from_millis(100))).is_err());
    assert!(wire2.wait(Some(Duration::from_millis(100))).is_err());
}
