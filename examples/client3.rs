use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use queen::Queen;
use queen::node;
use queen::client;
use nson::msg;

fn main() {
    let addr_node1: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let addr_node2: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let hello: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));

    // node 1
    let queen = Queen::new().unwrap();
    let control = node::Control::new(&queen).unwrap();
    control.run();

    let addr = addr_node1.clone();
    queen.on("sys:listen", move |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            let mut addr = addr.lock().unwrap();
            *addr = Some(context.message.get_str("addr").unwrap().to_string());
        } else {
            panic!("{:?}", context);
        }
    });

    queen.on("sys:hand", |context| {
        let mut message = context.message;
        message.insert("ok", true);
        context.queen.emit("sys:hand", message);
    });

    let hello2 = hello.clone();
    queen.on("pub:hello", move |_context| {
        let mut hello = hello2.lock().unwrap();
        *hello += 1;
    });

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "0.0.0.0:0"});

    queen.run(4, false);


    // node 2
    let queen = Queen::new().unwrap();
    let control = node::Control::new(&queen).unwrap();
    control.run();

    let addr = addr_node2.clone();
    queen.on("sys:listen", move |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            let mut addr = addr.lock().unwrap();
            *addr = Some(context.message.get_str("addr").unwrap().to_string());
        } else {
            panic!("{:?}", context);
        }
    });

    queen.on("sys:hand", |context| {
        let mut message = context.message;
        message.insert("ok", true);
        context.queen.emit("sys:hand", message);
    });

    let hello2 = hello.clone();
    queen.on("pub:hello", move |_context| {
        let mut hello = hello2.lock().unwrap();
        *hello += 1;
    });

    queen.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            let conn_id = context.message.get_i32("conn_id").unwrap();

            context.queen.emit("sys:hand", msg!{ "conn_id": conn_id, "u": "admin", "p": "admin123"});
        } else {
            panic!("{:?}", context);
        }
    });

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "0.0.0.0:0"});
    loop {
        let addr = addr_node1.lock().unwrap();
        if let Some(ref addr) = *addr {
            queen.emit("sys:link", msg!{"protocol": "tcp", "addr": addr});
            break;
        }
    }

    queen.run(4, false);


    // client 1
    let queen = Queen::new().unwrap();
    let control = client::Control::new(&queen).unwrap();
    control.run();

    queen.on("sys:link", |context| {
        context.queen.emit("sys:hand", msg!{"u": "admin", "p": "admin123"});
    });

    let hello2 = hello.clone();
    queen.on("pub:hello", move |_context| {
        let mut hello = hello2.lock().unwrap();
        *hello += 1;
    });

    loop {
        let addr = addr_node1.lock().unwrap();
        if let Some(ref addr) = *addr {
            queen.emit("sys:link", msg!{"protocol": "tcp", "addr": addr});
            break;
        }
    }

    queen.run(4, false);


    // client 2
    let queen = Queen::new().unwrap();
    let control = client::Control::new(&queen).unwrap();
    control.run();

    queen.on("sys:link", |context| {
        context.queen.emit("sys:hand", msg!{"u": "admin", "p": "admin123"});
    });

    queen.on("sys:hand", |context| {
        context.queen.emit("pub:hello", msg!{"hello": "world"});
    });

    let hello2 = hello.clone();
    queen.on("pub:hello", move |_context| {
        let mut hello = hello2.lock().unwrap();
        *hello += 1;
    });

    loop {
        let addr = addr_node2.lock().unwrap();
        if let Some(ref addr) = *addr {
            queen.emit("sys:link", msg!{"protocol": "tcp", "addr": addr});
            break;
        }
    }

    queen.run(4, false);
    thread::sleep(Duration::from_secs(1));

    let hello = hello.lock().unwrap();
    assert!(*hello == 4);
}
