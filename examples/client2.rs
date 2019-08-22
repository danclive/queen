use std::net::TcpStream;
use std::time::Instant;

use queen::nson::msg;
use queen::nson::Message;
use queen::util::{write_socket_no_aead, read_socket_no_aead};

fn main() {
    let mut socket = TcpStream::connect("127.0.0.1:8888").unwrap();

    println!("{:?}", socket);

    let msg = msg!{
        "_chan": "_auth",
        "username": "aaa",
        "password": "bbb"
    };

    let data = msg.to_vec().unwrap();
    write_socket_no_aead(&mut socket, data).unwrap();
    let read_data = read_socket_no_aead(&mut socket).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    println!("{:?}", recv);

    let msg = msg!{
        "_chan": "_atta",
        "_valu": "aaa"
    };

    let data = msg.to_vec().unwrap();
    write_socket_no_aead(&mut socket, data).unwrap();
    let read_data = read_socket_no_aead(&mut socket).unwrap();
    let recv = Message::from_slice(&read_data).unwrap();

    println!("{:?}", recv);


    // loop {
    //     let recv = Message::decode(&mut socket).unwrap();
    //     println!("{:?}", recv);
    // }
    let time1 = Instant::now();

    for i in 0..1000000 {
        let msg = msg!{
            "_chan": "aaa",
            "haha": "bb",
            "i": i,
            "_id": 123
        };

        let data = msg.to_vec().unwrap();
        write_socket_no_aead(&mut socket, data).unwrap();
        let read_data = read_socket_no_aead(&mut socket).unwrap();
        let recv = Message::from_slice(&read_data).unwrap();

        // println!("{:?}", recv);
    }

    let time2 = Instant::now();

    println!("{:?}", time2 - time1);
}
