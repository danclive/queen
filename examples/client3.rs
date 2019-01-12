use queen::client::Queen;
use bsonrs::doc;

fn main() {
    let queen = Queen::new().unwrap();

    queen.on("sys:link", |context| {
        println!("{:?}", context);

        context.queen.emit("sys:hand", doc!{"u": "admin", "p": "admin123"});
    });

    queen.on("pub:hello", |context| {
        println!("{:?}", context);
    });

    queen.emit("sys:link", doc!{"protocol": "tcp", "addr": "127.0.0.1:8888"});
    queen.run(4, false);





    let queen2 = Queen::new().unwrap();

    queen2.on("sys:link", |context| {
        println!("{:?}", context);

        context.queen.emit("sys:hand", doc!{"u": "admin", "p": "admin123"});
    });

    queen2.on("sys:hand", |context| {
        context.queen.emit("pub:hello", doc!{"hello": "world"});
    });

    queen2.emit("sys:link", doc!{"protocol": "tcp", "addr": "127.0.0.1:8888"});

    queen2.run(4, true);
}
