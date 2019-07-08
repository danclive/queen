use queen::node::*;

fn main() {
    let mut node = Node::new(Some("127.0.0.1:8888"), Some("/tmp/sock")).unwrap();

    node.run().unwrap();
}
