use queen::node::*;

fn main() {
    let mut node = Node::new(Some("0.0.0.0:8888"), None).unwrap();

    node.run().unwrap();
}
