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

    thread::sleep(Duration::from_secs(1));

    let addr = Addr::tcp(addr).unwrap();

    // hub1
    let config = HubConfig::new(addr.clone(), msg!{}, Some("queen".to_string()));
    let hub = Hub::connect(config.clone()).unwrap();

    // hub2
    let config = HubConfig::new(addr, msg!{}, Some("queen".to_string()));
    let hub2 = Hub::connect(config).unwrap();


    // hub 2 recv
    let recv = hub2.async_recv("aaa", None).unwrap();

    thread::sleep(Duration::from_secs(1));

    // hub 1 send
    hub.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    // hub 2 recv
    let msg = recv.recv().unwrap();

    assert_eq!(msg.get_str("hello").unwrap(), "world");

    // hub 2 send
    hub2.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    // hub 2 recv
    let msg = recv.recv().unwrap();

    assert_eq!(msg.get_str("hello").unwrap(), "world");
}

#[test]
fn with_label() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.add_tcp(addr2).unwrap();
        config.set_aead_key("queen");

        let mut node = Node::bind(config, ()).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    let addr = Addr::tcp(addr).unwrap();

    // hub1
    let config = HubConfig::new(addr.clone(), msg!{}, Some("queen".to_string()));
    let hub = Hub::connect(config.clone()).unwrap();

    // hub2
    let config = HubConfig::new(addr, msg!{}, Some("queen".to_string()));
    let hub2 = Hub::connect(config).unwrap();

    // hub 2 recv
    let recv = hub2.async_recv("aaa", Some(vec!["label1".to_string()])).unwrap();

    thread::sleep(Duration::from_secs(1));

    // group 1
    // hub 1 send
    hub.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    // hub 2 recv
    let msg = recv.recv().unwrap();

    assert_eq!(msg.get_str("hello").unwrap(), "world");

    // group 2
    // hub 1 send
    hub.send("aaa", msg!{"hello": "world"},  Some(vec!["label1".to_string()]));

    thread::sleep(Duration::from_secs(1));

    // hub 2 recv
    let msg = recv.recv().unwrap();

    assert_eq!(msg.get_str("hello").unwrap(), "world");

    // group 3
    // hub 1 send
    hub.send("aaa", msg!{"hello": "world"},  Some(vec!["label2".to_string()]));

    thread::sleep(Duration::from_secs(1));

    // hub 2 recv
    assert!(recv.recv().is_none());
}

#[test]
fn detach() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.add_tcp(addr2).unwrap();
        config.set_aead_key("queen");

        let mut node = Node::bind(config, ()).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    let addr = Addr::tcp(addr).unwrap();

    // hub1
    let config = HubConfig::new(addr.clone(), msg!{}, Some("queen".to_string()));
    let hub = Hub::connect(config.clone()).unwrap();

    // hub2
    let config = HubConfig::new(addr.clone(), msg!{}, Some("queen".to_string()));
    let hub2 = Hub::connect(config).unwrap();


    // hub 2 recv
    let recv = hub2.async_recv("aaa", None).unwrap();
    // hub 3 recv
    let recv2 = hub2.async_recv("aaa", None).unwrap();

    thread::sleep(Duration::from_secs(1));

    
    // group 1
    // hub 1 send
    hub.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    // hub 2 recv
    let msg = recv.recv().unwrap();

    assert_eq!(msg.get_str("hello").unwrap(), "world");

    // hub 3 recv
    let msg = recv2.recv().unwrap();

    assert_eq!(msg.get_str("hello").unwrap(), "world");

    drop(recv);
    thread::sleep(Duration::from_secs(1));

    // group 2
    // hub 1 send
    hub.send("aaa", msg!{"hello": "world"}, None);

    thread::sleep(Duration::from_secs(1));

    // hub 3 recv
    let msg = recv2.recv().unwrap();

    assert_eq!(msg.get_str("hello").unwrap(), "world");
}

#[test]
fn with_label_detach() {
    let addr = get_free_addr();

    let addr2 = addr.clone();
    thread::spawn(move || {
        let mut config = NodeConfig::new();

        config.add_tcp(addr2).unwrap();
        config.set_aead_key("queen");

        let mut node = Node::bind(config, ()).unwrap();

        node.run().unwrap();
    });

    thread::sleep(Duration::from_secs(1));

    let addr = Addr::tcp(addr).unwrap();

    // hub1
    let config = HubConfig::new(addr.clone(), msg!{}, Some("queen".to_string()));
    let hub = Hub::connect(config.clone()).unwrap();

    // hub2
    let config = HubConfig::new(addr.clone(), msg!{}, Some("queen".to_string()));
    let hub2 = Hub::connect(config).unwrap();


    // hub 2 recv
    let recv = hub2.async_recv("aaa", Some(vec!["label1".to_string()])).unwrap();
    // hub 3 recv
    let recv2 = hub2.async_recv("aaa", Some(vec!["label1".to_string()])).unwrap();

    thread::sleep(Duration::from_secs(1));

    
    // group 1
    // hub 1 send
    hub.send("aaa", msg!{"hello": "world"}, Some(vec!["label1".to_string()]));

    thread::sleep(Duration::from_secs(1));

    // hub 2 recv
    let msg = recv.recv().unwrap();

    assert_eq!(msg.get_str("hello").unwrap(), "world");

    // hub 3 recv
    let msg = recv2.recv().unwrap();

    assert_eq!(msg.get_str("hello").unwrap(), "world");


    drop(recv2);
    thread::sleep(Duration::from_secs(1));

    // group 2
    // hub 1 send
    hub.send("aaa", msg!{"hello": "world"}, Some(vec!["label1".to_string()]));

    thread::sleep(Duration::from_secs(1));

    // hub 2 recv
    let msg = recv.recv().unwrap();

    assert_eq!(msg.get_str("hello").unwrap(), "world");
}
