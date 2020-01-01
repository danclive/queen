use queen::{Port, Connector};
use queen::net::Addr;

use nson::{MessageId, msg};

fn main() {
    let port1 = Port::connect(
        MessageId::new(),
        Connector::Net(
            Addr::tcp("127.0.0.1:8888").unwrap(),
            None
        ),
        msg!{},
        2
    ).unwrap();

    let mut recv = port1.recv("aaa", None, None).unwrap();

    loop {
        println!("recv: {:?}", recv.next());
    }
}
