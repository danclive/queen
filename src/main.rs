use queen::node::*;

fn main() {

    let mut config = NodeConfig::new();

    config.tcp("0.0.0.0:8888").unwrap();

    let mut node = Node::bind(config, ()).unwrap();

    node.run().unwrap();
}
