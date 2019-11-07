use std::time::Duration;
use std::thread;

use nson::message_id::MessageId;
use nson::msg;

use queen::queen::Queen;
use queen::dict::*;
use queen::error::ErrorCode;

#[test]
fn connect() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let stream1 = queen.connect(msg!{}, None).unwrap();
    let stream2 = queen.connect(msg!{}, None).unwrap();

    stream1.send(msg!{
        CHAN: PING
    });

    stream2.send(msg!{
        CHAN: PING
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream1.recv().unwrap().get_i32(OK).unwrap() == 0);
    assert!(stream2.recv().unwrap().get_i32(OK).unwrap() == 0);

    stream1.send(msg!{
        "aaa": "bbb"
    });

    stream2.send(msg!{
        CHAN: 123
    });

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::CannotGetChanField));

    let recv = stream2.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::CannotGetChanField));
}

#[test]
fn auth() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let stream1 = queen.connect(msg!{}, None).unwrap();

    stream1.send(msg!{
        CHAN: ATTACH
    });

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    stream1.send(msg!{
        CHAN: AUTH
    });

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();

    assert!(recv.get_i32(OK).unwrap() == 0);
}

#[test]
fn attach_detach() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let stream1 = queen.connect(msg!{}, None).unwrap();
    let stream2 = queen.connect(msg!{}, None).unwrap();

    // auth
    stream1.send(msg!{
        CHAN: AUTH
    });

    stream2.send(msg!{
        CHAN: AUTH
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream1.recv().unwrap().get_i32(OK).unwrap() == 0);
    assert!(stream2.recv().unwrap().get_i32(OK).unwrap() == 0);

    // try send
    stream1.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::NoConsumers));

    // attach
    stream2.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa"
    });

    // send
    stream1.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();

    assert!(recv.get_i32(OK).unwrap() == 0);

    let recv = stream2.recv().unwrap();

    assert!(recv.get_i32(OK).unwrap() == 0);

    // recv
    let recv = stream2.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    // detach
    stream2.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa"
    });

    // send
    stream1.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    let recv = stream2.recv().unwrap();

    assert!(recv.get_i32(OK).unwrap() == 0);

    let recv = stream1.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::NoConsumers));
}

#[test]
fn label() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let stream1 = queen.connect(msg!{}, None).unwrap();
    let stream2 = queen.connect(msg!{}, None).unwrap();
    let stream3 = queen.connect(msg!{}, None).unwrap();

    // auth
    stream1.send(msg!{
        CHAN: AUTH
    });

    stream2.send(msg!{
        CHAN: AUTH
    });

    stream3.send(msg!{
        CHAN: AUTH
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream1.recv().unwrap().get_i32(OK).unwrap() == 0);
    assert!(stream2.recv().unwrap().get_i32(OK).unwrap() == 0);
    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // attach
    stream1.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa"
    });

    stream2.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa",
        LABEL: "label1"
    });

    // send
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream1.recv().unwrap().get_i32(OK).unwrap() == 0);
    assert!(stream2.recv().unwrap().get_i32(OK).unwrap() == 0);
    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // recv
    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    let recv = stream2.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    // send with label
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: "label1",
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // recv
    assert!(stream1.recv().is_none());

    let recv = stream2.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    // detach
    stream2.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa",
        LABEL: "label1"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream2.recv().unwrap().get_i32(OK).unwrap() == 0);

    // send with label
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: "label1",
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    let recv = stream3.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::NoConsumers));

    // send
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // recv
    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    let recv = stream2.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
}

#[test]
fn labels() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let stream1 = queen.connect(msg!{}, None).unwrap();
    let stream2 = queen.connect(msg!{}, None).unwrap();
    let stream3 = queen.connect(msg!{}, None).unwrap();

    // auth
    stream1.send(msg!{
        CHAN: AUTH
    });

    stream2.send(msg!{
        CHAN: AUTH
    });

    stream3.send(msg!{
        CHAN: AUTH
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream1.recv().unwrap().get_i32(OK).unwrap() == 0);
    assert!(stream2.recv().unwrap().get_i32(OK).unwrap() == 0);
    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // attach
    stream1.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa",
        LABEL: ["label1", "label2", "label3"]
    });

    stream2.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa",
        LABEL: ["label2", "label3", "label4"]
    });

    // send
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream1.recv().unwrap().get_i32(OK).unwrap() == 0);
    assert!(stream2.recv().unwrap().get_i32(OK).unwrap() == 0);
    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // recv
    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    let recv = stream2.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    // send with label
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: "label5",
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    let recv = stream3.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::NoConsumers));

    // send with label
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label5", "label6"],
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    let recv = stream3.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::NoConsumers));

    // send with label
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: "label1",
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // recv
    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(stream2.recv().is_none());

    // send with label
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label1"],
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // recv
    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(stream2.recv().is_none());

    // send with label
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label1", "label5"],
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // recv
    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(stream2.recv().is_none());

    // send with label
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label1", "label4"],
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // recv
    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    let recv = stream2.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    // detach
    stream1.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa",
        LABEL: "label1"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream1.recv().unwrap().get_i32(OK).unwrap() == 0);

    // send with label
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label1", "label4"],
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // recv
    assert!(stream1.recv().is_none());

    let recv = stream2.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    // detach
    stream2.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa",
        LABEL: ["label2", "label3"]
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream2.recv().unwrap().get_i32(OK).unwrap() == 0);

    // send with label
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: "label2",
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // recv
    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(stream2.recv().is_none());

    // send with label
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        LABEL: ["label2", "label3"],
        ACK: "123"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // recv
    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(stream2.recv().is_none());
}

#[test]
fn port_id() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let stream1 = queen.connect(msg!{}, None).unwrap();
    let stream2 = queen.connect(msg!{}, None).unwrap();
    let stream3 = queen.connect(msg!{}, None).unwrap();

    // auth
    stream1.send(msg!{
        CHAN: AUTH,
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap()
    });

    stream2.send(msg!{
        CHAN: AUTH,
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e5").unwrap()
    });

    // duplicate
    stream3.send(msg!{
        CHAN: AUTH,
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e5").unwrap()
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream1.recv().unwrap().get_i32(OK).unwrap() == 0);
    assert!(stream2.recv().unwrap().get_i32(OK).unwrap() == 0);
    // assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);
    let recv = stream3.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::DuplicatePortId));

    stream3.send(msg!{
        CHAN: AUTH,
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e6").unwrap()
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // type
    let stream4 = queen.connect(msg!{}, None).unwrap();

    stream4.send(msg!{
        CHAN: AUTH,
        PORT_ID: 123
    });

    thread::sleep(Duration::from_secs(1));

    let recv = stream4.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::InvalidPortIdFieldType));

    // port to port
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123",
        TO: MessageId::with_string("5932a005b4b4b4ac168cd9e7").unwrap()
    });

    thread::sleep(Duration::from_secs(1));

    let recv = stream3.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::TargetPortIdNotExist));

    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123",
        TO: MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap()
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // recv
    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).unwrap() == &MessageId::with_string("5932a005b4b4b4ac168cd9e6").unwrap());

    // port to ports
    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123",
        TO: [MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap(), MessageId::with_string("5932a005b4b4b4ac168cd9e7").unwrap()]
    });

    thread::sleep(Duration::from_secs(1));

    let recv = stream3.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::TargetPortIdNotExist));

    stream3.send(msg!{
        CHAN: "aaa",
        "hello": "world",
        ACK: "123",
        TO: [MessageId::with_string("5932a005b4b4b4ac168cd9e4").unwrap(), MessageId::with_string("5932a005b4b4b4ac168cd9e5").unwrap()]
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream3.recv().unwrap().get_i32(OK).unwrap() == 0);

    // recv
    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).unwrap() == &MessageId::with_string("5932a005b4b4b4ac168cd9e6").unwrap());

    let recv = stream2.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == "aaa");
    assert!(recv.get_str("hello").unwrap() == "world");
    assert!(recv.get_message_id(FROM).unwrap() == &MessageId::with_string("5932a005b4b4b4ac168cd9e6").unwrap());

    // generate message id
    let stream4 = queen.connect(msg!{}, None).unwrap();

    stream4.send(msg!{
        CHAN: AUTH
    });

    thread::sleep(Duration::from_secs(1));

    let recv = stream4.recv().unwrap();

    assert!(recv.get_message_id(PORT_ID).is_ok());
}

#[test]
fn port_event() {
    let queen = Queen::new(MessageId::new(), (), None).unwrap();

    let stream1 = queen.connect(msg!{}, None).unwrap();

    stream1.send(msg!{
        CHAN: AUTH,
        SUPER: 123
    });

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();

    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::InvalidSuperFieldType));

    stream1.send(msg!{
        CHAN: AUTH,
        SUPER: true
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream1.recv().unwrap().get_i32(OK).unwrap() == 0);

    // supe attach
    stream1.send(msg!{
        CHAN: ATTACH,
        VALUE: PORT_READY
    });

    stream1.send(msg!{
        CHAN: ATTACH,
        VALUE: PORT_BREAK
    });

    stream1.send(msg!{
        CHAN: ATTACH,
        VALUE: PORT_ATTACH
    });

    stream1.send(msg!{
        CHAN: ATTACH,
        VALUE: PORT_DETACH
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream1.recv().unwrap().get_i32(OK).unwrap() == 0);
    assert!(stream1.recv().unwrap().get_i32(OK).unwrap() == 0);
    assert!(stream1.recv().unwrap().get_i32(OK).unwrap() == 0);
    assert!(stream1.recv().unwrap().get_i32(OK).unwrap() == 0);

    // auth
    let stream2 = queen.connect(msg!{}, None).unwrap();

    stream2.send(msg!{
        CHAN: AUTH,
        SUPER: true
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream2.recv().unwrap().get_i32(OK).unwrap() == 0);

    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == PORT_READY);
    assert!(recv.get_bool(SUPER).unwrap() == true);
    assert!(recv.get_message_id(PORT_ID).is_ok());

    // attach
    stream2.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream2.recv().unwrap().get_i32(OK).unwrap() == 0);

    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == PORT_ATTACH);
    assert!(recv.get_str(VALUE).unwrap() == "aaa");
    assert!(recv.get_message_id(PORT_ID).is_ok());

    stream2.send(msg!{
        CHAN: ATTACH,
        VALUE: "aaa",
        LABEL: "label1"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream2.recv().unwrap().get_i32(OK).unwrap() == 0);

    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == PORT_ATTACH);
    assert!(recv.get_str(VALUE).unwrap() == "aaa");
    assert!(recv.get_str(LABEL).unwrap() == "label1");
    assert!(recv.get_message_id(PORT_ID).is_ok());

    // detach
    stream2.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa",
        LABEL: "label1"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream2.recv().unwrap().get_i32(OK).unwrap() == 0);

    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == PORT_DETACH);
    assert!(recv.get_str(VALUE).unwrap() == "aaa");
    assert!(recv.get_str(LABEL).unwrap() == "label1");
    assert!(recv.get_message_id(PORT_ID).is_ok());

    stream2.send(msg!{
        CHAN: DETACH,
        VALUE: "aaa"
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream2.recv().unwrap().get_i32(OK).unwrap() == 0);

    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == PORT_DETACH);
    assert!(recv.get_str(VALUE).unwrap() == "aaa");
    assert!(recv.get_message_id(PORT_ID).is_ok());

    drop(stream2);

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == PORT_BREAK);
    assert!(recv.get_message_id(PORT_ID).is_ok());

    // auth
    let stream2 = queen.connect(msg!{}, None).unwrap();

    stream2.send(msg!{
        CHAN: AUTH,
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e6").unwrap()
    });

    thread::sleep(Duration::from_secs(1));

    assert!(stream2.recv().unwrap().get_i32(OK).unwrap() == 0);

    stream1.send(msg!{
        CHAN: PORT_KILL,
        PORT_ID: MessageId::with_string("5932a005b4b4b4ac168cd9e6").unwrap()
    });

    thread::sleep(Duration::from_secs(1));

    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == PORT_READY);
    assert!(recv.get_bool(SUPER).unwrap() == false);
    assert!(recv.get_message_id(PORT_ID).unwrap() == &MessageId::with_string("5932a005b4b4b4ac168cd9e6").unwrap());

    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == PORT_KILL);
    assert!(recv.get_i32(OK).unwrap() == 0);
    assert!(recv.get_message_id(PORT_ID).unwrap() == &MessageId::with_string("5932a005b4b4b4ac168cd9e6").unwrap());
   
    let recv = stream1.recv().unwrap();

    assert!(recv.get_str(CHAN).unwrap() == PORT_BREAK);
    assert!(recv.get_message_id(PORT_ID).unwrap() == &MessageId::with_string("5932a005b4b4b4ac168cd9e6").unwrap());

    println!("{:?}", stream2.is_close());
    println!("{:?}", stream2.recv());
}
