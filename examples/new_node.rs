use queen::node::*;

fn main() {
    let mut node = Node::new("127.0.0.1:8888").unwrap();

    node.run().unwrap();
}
