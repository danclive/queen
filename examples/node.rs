use queen::{Queen, Node};

use queen::nson::MessageId;

fn main() {
    let queen = Queen::new(MessageId::new(), ()).unwrap();

    let mut node = Node::new(
        queen.clone(),
        2,
        vec!["127.0.0.1:8888".parse().unwrap()]
    ).unwrap();

    node.run().unwrap();
}
