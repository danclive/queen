use queen::{Queen, Context};
use queen::node::Control;
use queen::nson::msg;

use queen_log;
use log::LevelFilter;
use log::{debug, error, info, warn, trace};

fn main() {

    queen_log::init(LevelFilter::Trace).unwrap();

    error!("{:?}", "error");
    warn!("{:?}", "warn");
    debug!("{:?}", "debug");
    info!("{:?}", "info");
    trace!("{:?}", "trace");


    let queen = Queen::new().unwrap();

    Control::bind(&queen).unwrap();

    queen.emit("s:listen", msg!{
        "protocol": "tcp",
        "addr": "0.0.0.0:8888"
    });

    queen.on("s:listen", |context| {
        let message = context.message;
        println!("s:listen: {:?}", message);

        if let Ok(ok) = message.get_bool("ok") {
            if ok {
                // let listen_id = message.get_i32("listen_id").unwrap();
                // context.queen.emit("sys:unlisten", msg!{"listen_id": listen_id});
            }
        }
    });

    queen.on("s:unlisten", |context| {
        println!("s:unlisten: {:?}", context.message);
    });

    queen.on("s:h", |context| {
        println!("{:?}", context);

        let mut message = context.message;
        message.insert("ok", true);
        context.queen.emit("s:h", message);
    });

    queen.on("s:a", |context| {
        println!("{:?}", context);

        let mut message = context.message;
        message.insert("ok", true);
        context.queen.emit("s:a", message);
    });

    queen.run(4, true);
}
