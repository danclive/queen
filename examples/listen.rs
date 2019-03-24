use queen::{Queen, Context};
use queen::node::Control;
use queen::nson::msg;

use queen_log;
use log::LevelFilter;
//use log::{debug, error, info, warn, trace};

fn main() {

    queen_log::init(LevelFilter::Trace).unwrap();

    // error!("{:?}", "error");
    // warn!("{:?}", "warn");
    // debug!("{:?}", "debug");
    // info!("{:?}", "info");
    // trace!("{:?}", "trace");


    let queen = Queen::new().unwrap();

    Control::bind(&queen).unwrap();

    queen.emit("sys:listen", msg!{
        "protocol": "tcp",
        "addr": "0.0.0.0:8888"
    });

    queen.on("sys:listen", |context| {
        let message = context.message;
        println!("sys:listen: {:?}", message);

        if let Ok(ok) = message.get_bool("ok") {
            if ok {
                // let listen_id = message.get_i32("listen_id").unwrap();
                // context.queen.emit("sys:unlisten", msg!{"listen_id": listen_id});
            }
        }
    });

    queen.on("sys:unlisten", |context| {
        println!("sys:unlisten: {:?}", context.message);
    });




    queen.run(4, true);
}
