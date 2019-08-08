use std::net::TcpStream;
use std::time::Instant;

use queen::nson::msg;
use queen::nson::Message;
use queen::util::{write_socket, read_socket};

fn main() {
    let key = "queen";

    let mut socket = TcpStream::connect("127.0.0.1:8888").unwrap();

    println!("{:?}", socket);

    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    write_socket(&mut socket, key.as_ref(), msg.to_vec().unwrap()).unwrap();

    let data = read_socket(&mut socket, key.as_ref()).unwrap();

    let recv = Message::from_slice(&data).unwrap();
    println!("{:?}", recv);

    let time1 = Instant::now();

    let mut i = 1000000;
    while i > 0 {
        let msg = msg!{
            "_chan": "aaa",
            "haha": "bb",
            "i": i,
            "_id": 123,
            "_share": true
        };

        write_socket(&mut socket, b"queen", msg.to_vec().unwrap()).unwrap();

        let data = read_socket(&mut socket, b"queen").unwrap();

        let recv = Message::from_slice(&data).unwrap();
        // println!("{:?}", recv);

        i -= 1;
    }

    let time2 = Instant::now();

    println!("{:?}", time2 - time1);
}
