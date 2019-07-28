use std::net::TcpStream;

use queen::nson::msg;
use queen::nson::Message;


fn main() {
    let msg = msg!{
        "a": true,
        "_chan": "esp8266",
        "key": 123456779i32,
        "aaa": 456u32,
        "bbb": 1.234f32,
    };

    println!("{:?}", msg.to_vec());
    println!("{:?}", msg.to_vec().unwrap().len());

    println!("{:?}", 255 * 255 * 255);
}
