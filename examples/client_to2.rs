use std::net::TcpStream;

use queen::nson::msg;
use queen::nson::Message;

fn main() {
    let mut socket = TcpStream::connect("127.0.0.1:8888").unwrap();

    println!("{:?}", socket);

    let msg = msg!{
        "event": "node::auth",
        "username": "aaa",
        "password": "bbb",
        "_clientid": "def"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();

    println!("{:?}", recv);


    let msg = msg!{
        "_to": "abc",
        "hello": "world"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();

    println!("{:?}", recv);
}
