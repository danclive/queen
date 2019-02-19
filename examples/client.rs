use std::net::TcpStream;

use nson::msg;

use queen::Message;

fn main() {
    //let mut stream = TcpStream::connect("172.184.1.10:8888").unwrap();
    let mut stream = TcpStream::connect("m.danclive.com:8888").unwrap();

    let hand = msg!{"event": "sys:hand", "u": "admin", "p": "admin123"};
    hand.encode(&mut stream).unwrap();
    let hand_r = Message::decode(&mut stream);
    println!("{:?}", hand_r);

    let mut id = 0i32;
    loop {
        let aaa = msg!{"event": "pub:bbb", "hello": "world", "event_id": id};
        //println!("{:?}", aaa);
        aaa.encode(&mut stream).unwrap();

        let _aaa_r = Message::decode(&mut stream);
        //println!("{:?}", aaa_r);
        id += 1;
        println!("{:?}", id);
    }

    // let bbb = msg!{"event": "sys:attach", "v": "pub:bbb"};
    // bbb.encode(&mut stream).unwrap();
    // let aaa_r = Message::decode(&mut stream);
    // println!("{:?}", aaa_r);

    // loop {
    //  let msg = Message::decode(&mut stream);
    //  println!("{:?}", msg);
    // }
    // detach
}
