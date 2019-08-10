use std::io::Read;
use std::net::TcpStream;
use std::time::Instant;

use queen::nson::msg;
use queen::nson::Message;
use queen::util::get_length;

fn main() {
    let mut socket = TcpStream::connect("127.0.0.1:8888").unwrap();

    println!("{:?}", socket);

    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    msg.encode(&mut socket).unwrap();
   
    let mut len_buf = [0u8; 4];
    socket.peek(&mut len_buf).unwrap();
    let len = get_length(&len_buf, 0);
    let mut data = vec![0u8; len];
    socket.read(&mut data).unwrap();

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

        msg.encode(&mut socket).unwrap();

        let mut len_buf = [0u8; 4];
        socket.peek(&mut len_buf).unwrap();
        let len = get_length(&len_buf, 0);
        let mut data = vec![0u8; len];
        socket.read(&mut data).unwrap();

        let recv = Message::from_slice(&data).unwrap();
        // println!("{:?}", recv);

        i -= 1;
    }

    let time2 = Instant::now();

    println!("{:?}", time2 - time1);
}
