use std::time::Duration;

use queen::nson::{msg, message_id::MessageId};
use queen::crypto::Method;

use queen::net::Addr;
use queen::port::{Point, PointConfig};



fn main() {
    let addr = Addr::tcp("127.0.0.1:8888").unwrap();

    let config = PointConfig {
        addr,
        auth_msg: msg!{},
        aead_key: Some("queen".to_string()),
        aead_method: Method::default(),
        port_id: MessageId::new(),
        worker_num: 2
    };

    let point = Point::connect(config).unwrap();

    // std::thread::sleep_ms(2000);

    let r = point.call("aaa", "bbb", msg!{"hello": "world"}, Some(Duration::from_secs(10)));

    println!("{:?}", r);

    // let r = point.call("aaa", "bbb", msg!{"hello": "world"}, Some(Duration::from_secs(1)));

    // println!("{:?}", r);

    // std::thread::sleep_ms(100000);
}
