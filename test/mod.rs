use std::net::TcpListener;

mod test_event_emit;
mod test_node_accept;
mod test_node_auth;
mod test_node_attach;
mod test_node_back;
mod test_node_timer;
mod test_node_hmac;
mod test_node_port_id;
mod test_bridge_connect;

pub fn get_free_addr() -> String {
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap().to_string()
}
