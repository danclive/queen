use queen::nson::msg;
use queen::nson::MessageId;
use queen::crypto::Method;
use queen::net::{Addr};
use queen::{Rpc, Connector};

fn main() {
    let crypto = (Method::Aes256Gcm, "sep-centre".to_string());

    let rpc = Rpc::new(
        MessageId::new(),
        Connector::Net(Addr::tcp("127.0.0.1:8888").unwrap(), Some(crypto)),
        msg!{"user": "test-user", "pass": "test-pass"},
        2
        ).unwrap();

    let res = rpc.call("hello", None, msg!{"hello": "owlrd"}, None);

    println!("{:?}", res);
}
