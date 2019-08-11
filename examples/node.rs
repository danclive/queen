use queen::node::*;

fn main() {

    let mut config = NodeConfig::new();

    config.add_tcp("0.0.0.0:8888").unwrap();
    // config.set_hmac_key("queen");

    let mut node = Node::bind(config, ()).unwrap();

    let mut callback = Callback::new();

    callback.accept(move |id, addr, _| {
        println!("accept, id: {:?}, addr: {:?}", id, addr);
        return true
    });

    callback.remove(move |id, addr, _| {
        println!("remove, id: {:?}, addr: {:?}", id, addr);
    });

    callback.recv(|id, _addr, msg, _| {
        println!("recv:, id: {:?}, addr: {:?}", id, msg);
        return true;
    });

    callback.send(|id, _addr, msg, _| {
        println!("send:, id: {:?}, addr: {:?}", id, msg);
        return true;
    });

    callback.auth(|id, _addr, msg, _| {
        println!("auth, id: {:?}, addr: {:?}", id, msg);
        return true;
    });

    node.set_callback(callback);

    node.run().unwrap();
}
