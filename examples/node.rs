use queen::node::*;

fn main() {

    let mut config = NodeConfig::new();

    config.tcp("127.0.0.1:8889").unwrap();

    let mut node = Node::bind(config).unwrap();

    node.run().unwrap();
}
