use std::net::TcpStream;

use queen::nson::msg;
use queen::nson::Message;

fn main() {
    let msg = msg!{
        "_chan": "_auth",
        "aaa": 12.3f32,
        "bbb": 45.6f32,
        "ccc": 78.9f32,
        "ddd": 10.1f32,
        "eee": 23.4f32,
        "fff": 56.7f32
    };

    println!("{:?}", msg.to_vec());
    println!("{:?}", msg.to_vec().unwrap().len());

    println!("{:?}", 255 * 255 * 255);
}
