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
fn timer() {
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

    // client 1 attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // no time

    // client 1 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_id": 123,
        "_back": true
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 recv
    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("_id").unwrap() == 123);

    // with time

    // client 1 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_id": 123,
        "_back": true,
        "_time": 1000 * 2u32
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

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

    // client 1 recv
    socket.set_read_timeout(None).unwrap();
    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("_id").unwrap() == 123);
}

#[test]
fn del_time_id() {
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

    // client 1 attach
    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_id": 123,
        "_back": true,
        "_time": 1000u32,
        "_tmid": "123"
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // client 1 recv
    socket.set_read_timeout(None).unwrap();
    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("_id").unwrap() == 123);

    // del time id

    // client 1 send
    let msg = msg!{
        "_chan": "aaa",
        "hello": "world",
        "_id": 123,
        "_back": true,
        "_time": 1000 * 2u32,
        "_tmid": "123"
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

    // del time
    let msg = msg!{
        "_chan": "_delt",
        "_tmid": "1234"
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::TimeidNotExist));

    let msg = msg!{
        "_chan": "_delt",
        "_tmid": "123"
    };

    write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, b"queen").unwrap();
    let recv = Message::from_slice(&data).unwrap();
    assert!(recv.get_i32("ok").unwrap() == 0);

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
