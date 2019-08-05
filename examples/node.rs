use queen::node::*;

fn main() {

    let mut config = NodeConfig::new();

    config.tcp("0.0.0.0:8888").unwrap();

    let mut node = Node::bind(config).unwrap();

    let mut callback = Callback::default();

    callback.accept(|id, addr| {
        println!("accept, id: {:?}, addr: {:?}", id, addr);
        return true
    });

    callback.recv(|id, msg| {
        println!("recv:, id: {:?}, addr: {:?}", id, msg);
        return true;
    });

    callback.auth(|id, msg| {
        println!("auth, id: {:?}, addr: {:?}", id, msg);
        return true;
    });

    node.set_callback(callback);

    node.run().unwrap();
}
