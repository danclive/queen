extern crate queen;

use queen::server;

fn main() {
    println!("{:?}", "hello world");

    let mut queen = server::Queen::new().unwrap();

    queen.listen("0.0.0.0:9000").unwrap();

    queen.run().unwrap();

}
