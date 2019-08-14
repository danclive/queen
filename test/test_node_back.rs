use std::thread;
use std::net::TcpStream;
use std::time::Duration;
use std::io::ErrorKind::WouldBlock;

use queen::{Node, node::NodeConfig};
use queen::nson::{msg, Message};
use queen::util::{write_socket, read_socket};

use super::get_free_addr;

#[test]
fn back() {
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

    let mut socket = TcpStream::connect(addr).unwrap();

    // auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

     write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

     write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_ack": 123
    };

     write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // try recv
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

    // send2
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_ack": 123,
        "_back": true,
        "id": 456
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // try recv
    socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("id").unwrap() == 456);
}

#[test]
fn back_time() {
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

    let mut socket = TcpStream::connect(addr).unwrap();

    // auth
    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_ack": 123,
        "_time": 100u32
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // try recv
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

    // send2
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_ack": 123,
        "_time": 100u32,
        "_back": true,
        "id": 456
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // try recv
    socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("id").unwrap() == 456);
}
