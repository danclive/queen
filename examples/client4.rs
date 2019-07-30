use std::net::TcpStream;
use std::time::Instant;

use queen::nson::msg;
use queen::nson::Message;

fn main() {
    let mut socket = TcpStream::connect("127.0.0.1:8889").unwrap();

    println!("{:?}", socket);

    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut socket).unwrap();

    let recv = Message::decode(&mut socket).unwrap();

    println!("{:?}", recv);

    let time1 = Instant::now();

    let mut i = 100000;
    while i > 0 {
        let msg = msg!{
            "_chan": "aaa",
            "haha": "bb",
            "i": i,
            "_id": 123,
            "_share": true
        };

        msg.encode(&mut socket).unwrap();

        let recv = Message::decode(&mut socket).unwrap();
        //println!("{:?}", recv);

        i -= 1;
    }

    let time2 = Instant::now();

    println!("{:?}", time2 - time1);
}
