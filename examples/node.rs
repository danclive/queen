use queen::{Socket, Node};
use queen::net::NsonCodec;

use queen::nson::MessageId;

fn main() {
    let queen = Socket::new(MessageId::new(), ()).unwrap();

    let mut node = Node::<NsonCodec, ()>::new(
        queen.clone(),
        2,
        vec!["127.0.0.1:8888".parse().unwrap()],
        ()
    ).unwrap();

    node.run().unwrap();
}
