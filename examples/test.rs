use std::thread;
use std::time::Duration;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use nson::msg;

use queen::queen::Queen;
use queen::client;
use queen::Message;

fn main() {
    node_not_handshake();
}

fn node_not_handshake() {
    let addr: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let hello: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));

    // node 1
    let queen = Queen::new().unwrap();

    let addr2 = addr.clone();
    queen.on("sys:listen", move |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            let mut addr = addr2.lock().unwrap();
            *addr = Some(context.message.get_str("addr").unwrap().to_string());
        } else {
            panic!("{:?}", context);
        }
    });
    
    let hello2 = hello.clone();
    queen.on("pub:hello", move |_context| {
        println!("11111111111{:?}", _context);
        let mut hello = hello2.lock().unwrap();
        *hello += 1;
    });

    queen.on("sys:hand", |context| {
        let mut message = context.message;
        message.insert("ok", true);
        context.queen.emit("sys:hand", message);
    });

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "127.0.0.1:0"});
    queen.run(2, false);

    // client 1
    let queen2 = client::Queen::new().unwrap();

    queen2.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            context.queen.emit("sys:hand", msg!{"u": "admin", "p": "admin123"});
        } else {
            panic!("{:?}", context);
        }
    });

    queen2.on("sys:hand", |context| {
        context.queen.emit("pub:hello", msg!{"hello": "world", "aa": "bb"});
    });

    let hello2 = hello.clone();
    queen2.on("pub:hello", move |_context| {
        println!("2222222222{:?}", _context);
        let mut hello = hello2.lock().unwrap();
        *hello += 1;
    });

    loop {
        let addr = addr.lock().unwrap();
        if let Some(ref addr) = *addr {
            queen2.emit("sys:link", msg!{"protocol": "tcp", "addr": addr});
            break;
        }
    }

    queen2.run(2, false);

    // client 2
    let queen3 = client::Queen::new().unwrap();

    queen3.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            context.queen.emit("sys:hand", msg!{"u": "admin", "p": "admin123"});
        } else {
            panic!("{:?}", context);
        }
    });

    queen3.on("sys:hand", |context| {
        context.queen.emit("pub:hello", msg!{"hello": "world", "cc": "dd"});
    });

    queen3.on("pub:hello", |context| {
        println!("3333333333{:?}", context);
    });

    loop {
        let addr = addr.lock().unwrap();
        if let Some(ref addr) = *addr {
            queen3.emit("sys:link", msg!{"protocol": "tcp", "addr": addr});
            break;
        }
    }

    queen3.run(2, false);
    thread::sleep(Duration::from_secs(1));
    let hello = hello.lock().unwrap();
    assert!(*hello == 4);
}
