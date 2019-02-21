use queen::Queen;
use queen::node;
use nson::msg;

fn main() {
    let queen = Queen::new().unwrap();
    let control = node::Control::new(&queen).unwrap();
    control.run();

    queen.on("sys:link", |context| {
        println!("event: {:?}", context.event);
        println!("message: {:?}", context.message);

        if let Ok(ok) = context.message.get_bool("ok") {
            if ok {
                let conn_id = context.message.get_i32("conn_id").expect("Can't get conn_id!");

                context.queen.emit("sys:hand", msg!{"conn_id": conn_id, "node": true, "u": "admin", "p": "admin123"});
            } else {
                println!("{:?}", "aaaaa");
                context.queen.emit("sys:link", msg!{"protocol": "tcp", "addr": "127.0.0.1:8888"});
                println!("{:?}", "bbbbb");
            }
        }
    });

    queen.on("sys:hand", |context| {
        println!("event: {:?}", context.event);
        println!("[[[[[[[[[[[message: {:?}", context.message);

        if let Ok(_ok) = context.message.get_bool("ok") {

        }

        println!("{:?}", "ss");
        let a = context.queen.on("pub:aaa", |_context| {});
        let b = context.queen.on("pub:aaa", |_context| {});
        println!("{:?}", "ff");

        context.queen.off(a);
        context.queen.off(b);

        //let c = c;
    });

    let a = queen.on("pub:aaa", |context| {
        println!("{:?}", context.event);
        println!("{:?}", context.message);
    });

    queen.off(a);

    queen.on("sys:remove", |context| {
        println!("{:?}", context.event);
        println!("{:?}", context.message);

        if let Ok(my) = context.message.get_bool("my") {
            if my {
                context.queen.emit("sys:link", msg!{"protocol": "tcp", "addr": "127.0.0.1:8888"});
            }
        }
    });

    queen.emit("sys:link", msg!{"protocol": "tcp", "addr": "127.0.0.1:8888"});

    queen.run(4, true);
}
