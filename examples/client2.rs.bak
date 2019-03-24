use std::net::TcpStream;

use nson::msg;

use queen::Message;

fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:8888").unwrap();

    // let handed = msg!{"event": "sys:handed"};
    // handed.encode(&mut stream).unwrap();
    // let handed_r = Message::decode(&mut stream);
    // println!("{:?}", handed_r);

    let hand = msg!{"event": "sys:hand", "u": "admin", "p": "admin123"};
    hand.encode(&mut stream).unwrap();
    let hand_r = Message::decode(&mut stream);
    println!("{:?}", hand_r);

    // let handed = msg!{"event": "sys:handed"};
    // handed.encode(&mut stream).unwrap();
    // let handed_r = Message::decode(&mut stream);
    // println!("{:?}", handed_r);

    let bbb = msg!{"event": "sys:attach", "v": "pub:arduino"};
    bbb.encode(&mut stream).unwrap();
    let bbb_r = Message::decode(&mut stream);
    println!("{:?}", bbb_r);

    loop {
        let msg = Message::decode(&mut stream);
        println!("{:?}", msg);
    }
}

