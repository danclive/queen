use std::thread;
use std::time::Duration;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use nson::msg;

use queen::queen::Queen;
use queen::client;

#[test]
fn node_unlisten() {
    let addr: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let queen = Queen::new().unwrap();

    let addr2 = addr.clone();
    queen.on("sys:listen", move |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            let mut addr = addr2.lock().unwrap();
            *addr = Some(context.message.get_str("addr").unwrap().to_string());

            context.queen.emit("sys:unlisten", msg!{"listen_id": context.message.get_i32("listen_id").unwrap()});
        } else {
            panic!("{:?}", context);
        }
    });

    queen.on("sys:unlisten", |context| {
        assert!(context.message.get_bool("ok").unwrap());
    });

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "127.0.0.1:0"});
    queen.run(2, false);

    thread::sleep(Duration::from_secs(1));

    loop {
        let addr = addr.lock().unwrap();
        if let Some(ref addr) = *addr {
            
            assert!(TcpStream::connect(addr).is_err());

            break;
        }
    }
}

#[test]
fn node_ublink() {
    let addr: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let remove: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let unlink: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

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

    let remove2 = remove.clone();
    queen.on("sys:remove", move |_context| {
        let mut remove = remove2.lock().unwrap();
        *remove = true;
    });

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "127.0.0.1:0"});
    queen.run(2, false);

    let queen2 = Queen::new().unwrap();

    queen2.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            let conn_id = context.message.get_i32("conn_id").unwrap();
            context.queen.emit("sys:unlink", msg!{"conn_id": conn_id});
            
        } else {
            panic!("{:?}", context);
        }
    });

    let unlink2 = unlink.clone();
    queen2.on("sys:unlink", move |context| {
        assert!(context.message.get_bool("ok").unwrap());
        let mut unlink = unlink2.lock().unwrap();
        *unlink = true;
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

    let remove = remove.lock().unwrap();
    assert!(*remove == true);

    let unlink = unlink.lock().unwrap();
    assert!(*unlink == true);
}

#[test]
fn client_unlink() {
    let addr: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let remove: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let unlink: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

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

    let remove2 = remove.clone();
    queen.on("sys:remove", move |_context| {
        let mut remove = remove2.lock().unwrap();
        *remove = true;
    });

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "127.0.0.1:0"});
    queen.run(2, false);

    let queen2 = client::Queen::new().unwrap();

    queen2.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

            context.queen.emit("sys:unlink", msg!{});
        } else {
            panic!("{:?}", context);
        }
    });

    let unlink2 = unlink.clone();
    queen2.on("sys:unlink", move |context| {
        assert!(context.message.get_bool("ok").unwrap());
        let mut unlink = unlink2.lock().unwrap();
        *unlink = true;
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

    let remove = remove.lock().unwrap();
    assert!(*remove == true);

    let unlink = unlink.lock().unwrap();
    assert!(*unlink == true);
}

#[test]
fn node_unlink_client_remove() {
    let addr: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let remove: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let unlink: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

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

    queen.on("sys:accept", |context| {
        let conn_id = context.message.get_i32("conn_id").unwrap();
        context.queen.emit("sys:unlink", msg!{"conn_id": conn_id});
    });

    let unlink2 = unlink.clone();
    queen.on("sys:unlink", move |context| {
        assert!(context.message.get_bool("ok").unwrap());
        let mut unlink = unlink2.lock().unwrap();
        *unlink = true;
    });

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "127.0.0.1:0"});
    queen.run(2, false);

    let queen2 = client::Queen::new().unwrap();

    queen2.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);

        } else {
            panic!("{:?}", context);
        }
    });

    let remove2 = remove.clone();
    queen2.on("sys:remove", move |_context| {
        let mut remove = remove2.lock().unwrap();
        *remove = true;
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

    let remove = remove.lock().unwrap();
    assert!(*remove == true);

    let unlink = unlink.lock().unwrap();
    assert!(*unlink == true);
}
