use std::net::TcpStream;

use queen::nson::msg;
use queen::nson::Message;

fn main() {
    let mut socket = TcpStream::connect("m.danclive.com:8888").unwrap();

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
        "event": "node::attach",
        "value": "aaa"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();
    println!("{:?}", recv);


    loop {
        let recv = Message::decode(&mut socket).unwrap();
        println!("{:?}", recv);
    }
}
