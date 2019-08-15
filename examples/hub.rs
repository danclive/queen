use queen::nson::msg;

use queen::net::Addr;
use queen::port::{Hub, Recv};

fn main() {

    let addr = Addr::tcp("127.0.0.1:8888").unwrap();

    let hub = Hub::connect(addr, msg!{}, None).unwrap();

    for msg in hub.recv("aaa") {
        println!("{:?}", msg);
    } 
}
