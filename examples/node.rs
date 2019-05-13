use std::rc::Rc;

use queen::node::Node;
use queen::nson::msg;

fn main() {


    let mut node = Node::new().unwrap();


    node.send(msg!{
        "event": "net:listen",
        "proto": "tcp",
        "addr": "0.0.0.0:8888"
    }).unwrap();

    node.callback.listen_fn = Some(Rc::new(|_node, message| {
        println!("{:?}", message);
    }));

    node.callback.accept_fn = Some(Rc::new(|_node, message| {
        println!("{:?}", message);

        true
    }));



    node.run().unwrap();
}
