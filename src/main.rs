use queen::node::*;

fn main() {

    let mut config = NodeConfig::new();

    config.add_tcp("0.0.0.0:8888").unwrap();
    config.set_aead_key("queen");

    let mut node = Node::bind(config, ()).unwrap();

    node.run().unwrap();
}
