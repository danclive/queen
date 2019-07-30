use queen::node::*;

fn main() {
    let mut node = Node::bind(Some("127.0.0.1:8889"), None).unwrap();

    node.run().unwrap();
}
