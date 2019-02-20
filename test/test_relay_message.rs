use std::thread;
use std::time::Duration;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use nson::msg;

use queen::node::Queen;
use queen::client;
use queen::Message;

#[test]
fn simple_client_relay_message() {
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
    let addr2 = addr.clone();
    thread::spawn(move || {
        loop {

            let mut has_addr = false;
            let mut addr3 = String::new();

            {
                let addr = addr2.lock().unwrap();
                if let Some(ref addr) = *addr {
                    addr3 = addr.clone();
                    has_addr = true;
                }
            }

            if has_addr {
                
                let mut stream = TcpStream::connect(addr3).unwrap();

                let hand = msg!{"event": "sys:hand", "u": "admin", "p": "admin123"};
                hand.encode(&mut stream).unwrap();
                let hand_r = Message::decode(&mut stream).unwrap();
                assert!(hand_r.get_bool("ok").unwrap());

                let attach = msg!{"event": "sys:attach", "v": "pub:hello"};
                attach.encode(&mut stream).unwrap();
                let attach_r = Message::decode(&mut stream).unwrap();
                assert!(attach_r.get_bool("ok").unwrap());

                let hello = Message::decode(&mut stream).unwrap();
                assert!(hello.get_str("hello").unwrap() == "world");

                break;
            }
        }
    });

    thread::sleep(Duration::from_secs(1));

    loop {
        let addr = addr.lock().unwrap();
        if let Some(ref addr) = *addr {
            
            let mut stream = TcpStream::connect(addr).unwrap();

            let hand = msg!{"event": "sys:hand", "u": "admin", "p": "admin123"};
            hand.encode(&mut stream).unwrap();
            let hand_r = Message::decode(&mut stream).unwrap();
            assert!(hand_r.get_bool("ok").unwrap());

            let hello = msg!{"event": "pub:hello", "hello": "world"};
            hello.encode(&mut stream).unwrap();
            let hello_r = Message::decode(&mut stream).unwrap();
            assert!(hello_r.get_bool("ok").unwrap());

            break;
        }
    }

    thread::sleep(Duration::from_secs(1));  
}

#[test]
fn client_relay_message() {
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
        context.queen.emit("pub:hello", msg!{"hello": "world"});
    });

    let hello2 = hello.clone();
    queen2.on("pub:hello", move |_context| {
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
        context.queen.emit("pub:hello", msg!{"hello": "world"});
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

#[test]
fn client_relay_message_off() {
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
        context.queen.emit("pub:hello", msg!{"hello": "world"});
    });

    let hello2 = hello.clone();
    queen2.on("pub:hello", move |context| {
        let mut hello = hello2.lock().unwrap();
        *hello += 1;
        context.queen.off(context.id);
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
        context.queen.emit("pub:hello", msg!{"hello": "world"});
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
    assert!(*hello == 3);
}

#[test]
fn client_node_node_client() {
    let addr_node1: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let addr_node2: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let hello: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));

    // node 1
    let queen = Queen::new().unwrap();

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
    let queen = client::Queen::new().unwrap();

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
    let queen = client::Queen::new().unwrap();

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
