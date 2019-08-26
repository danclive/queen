use std::thread;
use std::time::Duration;

use queen::{Node, node::NodeConfig};
use queen::nson::msg;
use queen::port::{Hub, HubConfig};
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

    // hub1
    let config = HubConfig::new(addr.clone(), msg!{}, Some("queen".to_string()));
    let hub = Hub::connect(config.clone()).unwrap();

    // hub2
    let config = HubConfig::new(addr, msg!{}, Some("queen".to_string()));
    let hub2 = Hub::connect(config).unwrap();


    // hub 2 recv
    let recv = hub2.async_recv("aaa").unwrap();

    thread::sleep(Duration::from_secs(1));

    // hub 1 send
    hub.send("aaa", msg!{"hello": "world"});

    thread::sleep(Duration::from_secs(1));

    // hub 2 recv
    let msg = recv.recv().unwrap();

    assert_eq!(msg.get_str("hello").unwrap(), "world");

    // hub 2 send
    hub2.send("aaa", msg!{"hello": "world"});

    thread::sleep(Duration::from_secs(1));

    // hub 2 recv
    let msg = recv.recv().unwrap();

    assert_eq!(msg.get_str("hello").unwrap(), "world");
}
