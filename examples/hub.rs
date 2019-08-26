use queen::nson::msg;

use queen::net::Addr;
use queen::port::{Hub, HubConfig};

fn main() {

    let addr = Addr::tcp("127.0.0.1:8888").unwrap();

    let config = HubConfig::new(addr, msg!{}, Some("queen".to_string()));

    let hub = Hub::connect(config).unwrap();

    for msg in hub.recv("aaa") {
        println!("{:?}", msg);
    } 
}
