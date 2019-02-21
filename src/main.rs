use queen::Queen;
use queen::Message;
use queen::node::Control;
use nson::msg;

fn main() {
    let queen = Queen::new().unwrap();

    let control = Control::new(&queen).unwrap();
    control.run();

    queen.on("sys:listen", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            if ok {
                println!("Service run: {:?}", context.message);
            } else {
                panic!("Can't start service: {:?}", context.message);
            }
        }
    });

    queen.on("sys:hand", |context| {
        let mut message = context.message;
        message.insert("ok", true);
        context.queen.emit("sys:hand", message);
    });

    queen.on("sys:recv", |context| {
        let data = context.message.get_binary("data").unwrap();
        let message = Message::from_slice(&data);

        println!("sys:recv: {:?}", message);
    });

    queen.on("sys:send", |context| {
        let data = context.message.get_binary("data").unwrap();
        let message = Message::from_slice(&data);

        println!("sys:send: {:?}", message);
    });

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "0.0.0.0:8888"});

    queen.run(4, true);
}
