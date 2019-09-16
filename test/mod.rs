use std::net::TcpListener;

mod test_event_emit;
mod test_node_accept;
mod test_node_auth;
mod test_node_attach;
mod test_node_aead;
mod test_node_port_id;
mod test_node_label;
mod test_node_nonce;
mod test_node_port_event;
mod test_bridge_connect;
mod test_port_hub;
mod test_port_point;

pub fn get_free_addr() -> String {
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap().to_string()
}
