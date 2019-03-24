use queen::{Queen, Context};
use queen::node::Control;
use queen::nson::msg;

use queen_log;
use log::LevelFilter;
//use log::{debug, error, info, warn, trace};

fn main() {

    queen_log::init(LevelFilter::Trace).unwrap();

    let queen = Queen::new().unwrap();

    Control::bind(&queen).unwrap();

    queen.emit("sys:link", msg!{
        "protocol": "tcp",
        "addr": "127.0.0.1:8888"
    });

    queen.on("sys:link", |context| {
        let message = context.message;
        println!("sys:link: {:?}", message);

        if let Ok(ok) = message.get_bool("ok") {
            if ok {
                let conn_id = message.get_i32("conn_id").unwrap();
                context.queen.emit("sys:unlink", msg!{"conn_id": conn_id});
            }
        }
    });

    queen.run(4, true);
}

