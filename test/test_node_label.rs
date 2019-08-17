use std::thread;
use std::net::TcpStream;
use std::time::Duration;
use std::io::ErrorKind::WouldBlock;

use queen::{Node, node::NodeConfig};
use queen::nson::{msg, Message};
use queen::error::ErrorCode;
use queen::util::{write_socket, read_socket};

use super::get_free_addr;

#[test]
fn label() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.add_tcp(addr2).unwrap();
        config.set_hmac_key("queen");

        let mut node = Node::bind(config, ()).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    // client 1
    let mut socket = TcpStream::connect(&addr).unwrap();

    // client2
    let mut socket2 = TcpStream::connect(&addr).unwrap();

    // client3
    let mut socket3 = TcpStream::connect(addr).unwrap();

    // client 1 auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 2 auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    write_socket(&mut socket2, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket2, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 3 auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    write_socket(&mut socket3, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket3, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa",
        "_labe": "label1"
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 2 attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

    write_socket(&mut socket2, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket2, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    //// no label
    // client 3 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_ack": 123,
    };

    write_socket(&mut socket3, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket3, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 try recv
    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");

    // client 2 try recv
    let data = read_socket(&mut socket2, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");

    //// with label
    // client 3 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_labe": "label1",
        "_ack": 123
    };

    write_socket(&mut socket3, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket3, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 tyy recv
    socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");

    // client 2 tyy recv
    socket2.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let recv = read_socket(&mut socket2, b"queen");
    match recv {
       Ok(recv) => panic!("{:?}", recv),
       Err(err) => {
            if let WouldBlock = err.kind() {
                // pass
            } else {
                panic!("{:?}", err);
            }        
       }
    }
}

#[test]
fn labels() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.add_tcp(addr2).unwrap();
        config.set_hmac_key("queen");

        let mut node = Node::bind(config, ()).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    // client 1
    let mut socket = TcpStream::connect(&addr).unwrap();

    // client2
    let mut socket2 = TcpStream::connect(&addr).unwrap();

     // client 1 auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 2 auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    write_socket(&mut socket2, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket2, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa",
        "_labe": ["label1", "label2"]
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 2 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_labe": "label1",
        "_ack": 123
    };

    write_socket(&mut socket2, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket2, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 try recv
    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");

    // client 2 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_labe": "label2",
        "_ack": 123
    };

    write_socket(&mut socket2, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket2, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 try recv
    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_str("hello").unwrap() == "world");

    // client 2 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_labe": "label3",
        "_ack": 123
    };

    write_socket(&mut socket2, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket2, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::NoConsumers));

    // client 1 try recv
    socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let recv = read_socket(&mut socket, b"queen");
    match recv {
       Ok(recv) => panic!("{:?}", recv),
       Err(err) => {
            if let WouldBlock = err.kind() {
                // pass
            } else {
                panic!("{:?}", err);
            }        
       }
    }
}
