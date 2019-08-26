use std::thread;
use std::time::Duration;

use queen::{Node, node::NodeConfig};
use queen::nson::msg;
use queen::port::{Point, PointConfig};
use queen::net::Addr;

use super::get_free_addr;

#[test]
fn connect() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.add_tcp(addr2).unwrap();
        config.set_aead_key("queen");

        let mut node = Node::bind(config, ()).unwrap();

        node.run().unwrap();
    });

    let addr = Addr::tcp(addr).unwrap();

    // point 1
    let config = PointConfig::new(addr.clone(), msg!{}, Some("queen".to_string()));
    let point = Point::connect(config).unwrap();

    // point 2
    let config = PointConfig::new(addr.clone(), msg!{}, Some("queen".to_string()));
    let point2 = Point::connect(config).unwrap();

    point.add("aaa", "bbb", |_message| {
        msg!{
            "hello": "request"
        }
    });

    thread::sleep(Duration::from_secs(1));

    // call
    let r = point2.call("aaa", "bbb", msg!{"hello": "world"}, Some(Duration::from_secs(10))).unwrap();

    assert_eq!(r.get_str("hello").unwrap(), "request");

    // call
    let r = point.call("aaa", "bbb", msg!{"hello": "world"}, Some(Duration::from_secs(10))).unwrap();

    assert_eq!(r.get_str("hello").unwrap(), "request");
}
