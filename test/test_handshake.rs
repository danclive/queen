use std::thread;
use std::time::Duration;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use nson::msg;

use queen::queen::Queen;
use queen::client;
use queen::Message;

#[test]
fn node_not_handshake() {
    let addr: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let hello: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

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
        let mut hello = hello2.lock().unwrap();
        *hello = true;
    });

    queen.on("sys:hand", |context| {
        let mut message = context.message;
        message.insert("ok", true);
        context.queen.emit("sys:hand", message);
    });

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "127.0.0.1:0"});
    queen.run(2, false);

    // node 2
    let queen2 = Queen::new().unwrap();

    queen2.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            context.queen.emit("pub:hello", msg!{"hello": "world"});
        } else {
            panic!("{:?}", context);
        }
    });

    loop {
        let addr = addr.lock().unwrap();
        if let Some(ref addr) = *addr {
            queen2.emit("sys:link", msg!{"protocol": "tcp", "addr": addr});
            break;
        }
    }

    queen2.run(2, false);

    thread::sleep(Duration::from_secs(1));

    let hello = hello.lock().unwrap();
    assert!(!(*hello));
}

#[test]
fn node_has_handshake() {
    let addr: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let hello: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

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
        let mut hello = hello2.lock().unwrap();
        *hello = true;
    });

    queen.on("sys:hand", |context| {
        let mut message = context.message;
        message.insert("ok", true);
        context.queen.emit("sys:hand", message);
    });

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "127.0.0.1:0"});
    queen.run(2, false);

    // node 2
    let queen2 = Queen::new().unwrap();

    queen2.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            let conn_id = context.message.get_i32("conn_id").unwrap();

            context.queen.emit("sys:hand", msg!{ "conn_id": conn_id, "u": "admin", "p": "admin123"});
        } else {
            panic!("{:?}", context);
        }
    });

    queen2.on("sys:hand", |context| {
        context.queen.emit("pub:hello", msg!{"hello": "world"});
    });

    loop {
        let addr = addr.lock().unwrap();
        if let Some(ref addr) = *addr {
            queen2.emit("sys:link", msg!{"protocol": "tcp", "addr": addr});
            break;
        }
    }

    queen2.run(2, false);

    thread::sleep(Duration::from_secs(1));

    let hello = hello.lock().unwrap();
    assert!(*hello);
}

#[test]
fn client_not_handshake() {
    let addr: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let hello: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

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
        let mut hello = hello2.lock().unwrap();
        *hello = true;
    });

    queen.on("sys:hand", |context| {
        let mut message = context.message;
        message.insert("ok", true);
        context.queen.emit("sys:hand", message);
    });

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "127.0.0.1:0"});
    queen.run(2, false);

    // client
    let queen2 = client::Queen::new().unwrap();

    queen2.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            context.queen.emit("pub:hello", msg!{"hello": "world"});
        } else {
            panic!("{:?}", context);
        }
    });

    loop {
        let addr = addr.lock().unwrap();
        if let Some(ref addr) = *addr {
            queen2.emit("sys:link", msg!{"protocol": "tcp", "addr": addr});
            break;
        }
    }

    queen2.run(2, false);

    thread::sleep(Duration::from_secs(1));

    let hello = hello.lock().unwrap();
    assert!(!(*hello));
}

#[test]
fn client_has_handshake() {
    let addr: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let hello: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    // node
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
        let mut hello = hello2.lock().unwrap();
        *hello = true;
    });

    queen.on("sys:hand", |context| {
        let mut message = context.message;
        message.insert("ok", true);
        context.queen.emit("sys:hand", message);
    });

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "127.0.0.1:0"});
    queen.run(2, false);

    // client
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
        context.queen.emit("pub:hello", msg!{"hello": "world"});
    });

    loop {
        let addr = addr.lock().unwrap();
        if let Some(ref addr) = *addr {
            queen2.emit("sys:link", msg!{"protocol": "tcp", "addr": addr});
            break;
        }
    }

    queen2.run(2, false);

    thread::sleep(Duration::from_secs(1));

    let hello = hello.lock().unwrap();
    assert!(*hello);
}

#[test]
fn sinple_client_handshake() {
    let addr: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let hello: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    // node
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
        let mut hello = hello2.lock().unwrap();
        *hello = true;
    });

    queen.on("sys:hand", |context| {
        let mut message = context.message;
        message.insert("ok", true);
        context.queen.emit("sys:hand", message);
    });

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "127.0.0.1:0"});
    queen.run(2, false);

    // client 1
    loop {
        let addr = addr.lock().unwrap();
        if let Some(ref addr) = *addr {
            
            let mut stream = TcpStream::connect(addr).unwrap();

            let hand = msg!{"event": "sys:hand", "u": "admin", "p": "admin123"};
            hand.encode(&mut stream).unwrap();
            let hand_r = Message::decode(&mut stream).unwrap();
            assert!(hand_r.get_bool("ok").unwrap());

            break;
        }
    }

    // client 2
    loop {
        let addr = addr.lock().unwrap();
        if let Some(ref addr) = *addr {
            
            let mut stream = TcpStream::connect(addr).unwrap();

            let hand = msg!{"event": "sys:hand"};
            hand.encode(&mut stream).unwrap();
            let hand_r = Message::decode(&mut stream).unwrap();
            assert!(hand_r.get_str("error").unwrap() == "Can't get u from message!");

            break;
        }
    }
}

