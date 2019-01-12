use std::thread;
use std::time::Duration;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use bsonrs::doc;

use queen::queen::Queen;
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

    queen.emit("sys:listen", doc!{"protocol": "tcp", "addr": "127.0.0.1:0"});
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

                let hand = doc!{"event": "sys:hand", "u": "admin", "p": "admin123"};
                hand.encode(&mut stream).unwrap();
                let hand_r = Message::decode(&mut stream).unwrap();
                assert!(hand_r.get_bool("ok").unwrap());

                let attach = doc!{"event": "sys:attach", "v": "pub:hello"};
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

            let hand = doc!{"event": "sys:hand", "u": "admin", "p": "admin123"};
            hand.encode(&mut stream).unwrap();
            let hand_r = Message::decode(&mut stream).unwrap();
            assert!(hand_r.get_bool("ok").unwrap());

            let hello = doc!{"event": "pub:hello", "hello": "world"};
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

    queen.emit("sys:listen", doc!{"protocol": "tcp", "addr": "127.0.0.1:0"});
    queen.run(2, false);

    // client 1
    let queen2 = client::Queen::new().unwrap();

    queen2.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            context.queen.emit("sys:hand", doc!{"u": "admin", "p": "admin123"});
        } else {
            panic!("{:?}", context);
        }
    });

    queen2.on("sys:hand", |context| {
        context.queen.emit("pub:hello", doc!{"hello": "world"});
    });

    let hello2 = hello.clone();
    queen2.on("pub:hello", move |_context| {
        let mut hello = hello2.lock().unwrap();
        *hello += 1;
    });

    loop {
        let addr = addr.lock().unwrap();
        if let Some(ref addr) = *addr {
            queen2.emit("sys:link", doc!{"protocol": "tcp", "addr": addr});
            break;
        }
    }

    queen2.run(2, false);

    // client 2
    let queen3 = client::Queen::new().unwrap();

    queen3.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            context.queen.emit("sys:hand", doc!{"u": "admin", "p": "admin123"});
        } else {
            panic!("{:?}", context);
        }
    });

    queen3.on("sys:hand", |context| {
        context.queen.emit("pub:hello", doc!{"hello": "world"});
    });

    loop {
        let addr = addr.lock().unwrap();
        if let Some(ref addr) = *addr {
            queen3.emit("sys:link", doc!{"protocol": "tcp", "addr": addr});
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

    queen.emit("sys:listen", doc!{"protocol": "tcp", "addr": "127.0.0.1:0"});
    queen.run(2, false);

    // client 1
    let queen2 = client::Queen::new().unwrap();

    queen2.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            context.queen.emit("sys:hand", doc!{"u": "admin", "p": "admin123"});
        } else {
            panic!("{:?}", context);
        }
    });

    queen2.on("sys:hand", |context| {
        context.queen.emit("pub:hello", doc!{"hello": "world"});
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
            queen2.emit("sys:link", doc!{"protocol": "tcp", "addr": addr});
            break;
        }
    }

    queen2.run(2, false);

    // client 2
    let queen3 = client::Queen::new().unwrap();

    queen3.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            context.queen.emit("sys:hand", doc!{"u": "admin", "p": "admin123"});
        } else {
            panic!("{:?}", context);
        }
    });

    queen3.on("sys:hand", |context| {
        context.queen.emit("pub:hello", doc!{"hello": "world"});
    });

    loop {
        let addr = addr.lock().unwrap();
        if let Some(ref addr) = *addr {
            queen3.emit("sys:link", doc!{"protocol": "tcp", "addr": addr});
            break;
        }
    }

    queen3.run(2, false);
    thread::sleep(Duration::from_secs(1));
    let hello = hello.lock().unwrap();
    assert!(*hello == 3);
}
