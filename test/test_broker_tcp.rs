use std::thread;
use std::time::Duration;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use nson::msg;

use queen::queen::Queen;
use queen::client;

#[test]
fn test_broker_tcp_listen() {
    let queen = Queen::new().unwrap();

    queen.on("sys:listen", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);
        } else {
            panic!("{:?}", context);
        }
    });

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "127.0.0.1:0"});
    queen.run(2, false);

    thread::sleep(Duration::from_secs(1));
}

#[test]
fn test_broker_tcp_link() {
    let addr: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let addr2 = addr.clone();
    thread::spawn(move || {
        let server = TcpListener::bind("127.0.0.1:0").unwrap();
        {
            let mut addr = addr2.lock().unwrap();
            *addr = Some(server.local_addr().unwrap().to_string());
        }

        for _ in server.incoming() {}
    });

    let queen = Queen::new().unwrap();

    queen.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);
        } else {
            panic!("{:?}", context);
        }
    });

    loop {
        let addr = addr.lock().unwrap();
        if let Some(ref addr) = *addr {
            queen.emit("sys:link", msg!{"protocol": "tcp", "addr": addr});
            break;
        }
    }

    queen.run(2, false);

    thread::sleep(Duration::from_secs(1));
}

#[test]
fn test_broker_tcp_link_broker() {
    let addr: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

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

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "127.0.0.1:0"});
    queen.run(2, false);

    // node 2
    let queen2 = Queen::new().unwrap();

    queen2.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);
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
}

#[test]
fn test_client_tcp_link_broker() {
    let addr: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

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

    queen.emit("sys:listen", msg!{"protocol": "tcp", "addr": "127.0.0.1:0"});
    queen.run(2, false);

    // client
    let queen2 = client::Queen::new().unwrap();

    queen2.on("sys:link", |context| {
        if let Ok(ok) = context.message.get_bool("ok") {
            assert!(ok);
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
}
