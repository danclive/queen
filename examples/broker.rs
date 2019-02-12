use queen::queen::Queen;
use nson::msg;

fn main() {
    let queen = Queen::new().unwrap();

    queen.on("sys:listen", |_context| {
        //println!("{:?}", _message);
        println!("{:?}", _context);
        println!("{}", _context);
        //std::process::exit(0);
    });

    queen.on("sys:accept", |_context| {
        //println!("{:?}", _message);
    });

    queen.on("sys:hand", |context| {
        //println!("event: {:?}", event);
        //println!("message: {:?}", message);

        let mut message = context.message;
        message.insert("ok", true);
        message.insert("client_id", 123456778);
        context.queen.emit("sys:hand", message);
    });

    queen.on("pub:hello", |context| {
        println!("{:?}", context);
    });

    // queen.on("pub:aaa", |_queen, _id, event, message| {
    //  //println!("{:?}", event);
    //  //println!("{:?}", message);
    // });

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "0.0.0.0:8888"});

    queen.run(4, true);
}
