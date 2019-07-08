use std::net::TcpStream;

use queen::nson::msg;
use queen::nson::Message;

fn main() {
    let mut socket = TcpStream::connect("127.0.0.1:8888").unwrap();

    println!("{:?}", socket);
    let mut i = 0;

    let msg = msg!{
        "event": "node::auth",
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();

    //println!("{:?}", recv);
    i += 1;
    println!("i: {:?}", i);

    //std::thread::sleep_ms(1000 * 5);


    let msg = msg!{
        "event": "aaa",
        "haha": "bb",
        "_id": 123,
        "_time": 1000u32,
        "_timeid": "123"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    println!("{:?}", recv);

    let msg = msg!{
        "event": "aaa",
        "haha": "bb",
        "_id": 123,
        "_time": 1000u32,
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    println!("{:?}", recv);


    let msg = msg!{
        "event": "node::deltime",
        "_timeid": "123"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    println!("{:?}", recv);
}
