use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use nson::msg;

use queen::Queen;

#[test]
fn on() {
    let hasemit: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    let queen = Queen::new().unwrap();
    queen.run(2, false);

    let hasemit2 = hasemit.clone();
    queen.on("hello", move |_context| {
        let mut hasemit = hasemit2.lock().unwrap();
        *hasemit = true;
    });

    queen.emit("hello", msg!{"hello": "world"});

    thread::sleep(Duration::from_secs(1));
    let hasemit = hasemit.lock().unwrap();
    assert!(*hasemit);
}

#[test]
fn off() {
    let hasemit: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    let queen = Queen::new().unwrap();
    queen.run(2, false);

    let hasemit2 = hasemit.clone();
    let id = queen.on("hello", move |_context| {
        let mut hasemit = hasemit2.lock().unwrap();
        *hasemit = true;
    });

    queen.off(id);

    queen.emit("hello", msg!{"hello": "world"});

    thread::sleep(Duration::from_secs(1));
    let hasemit = hasemit.lock().unwrap();
    assert!(!*hasemit);
}

#[test]
fn sys_pub_on() {
    let hasemit: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    let queen = Queen::new().unwrap();
    queen.run(2, false);

    let hasemit2 = hasemit.clone();
    queen.on("queen", move |_context| {
        let mut hasemit = hasemit2.lock().unwrap();
        *hasemit = true;
    });

    queen.on("sys:hello", |_|{});

    thread::sleep(Duration::from_secs(1));
    let hasemit = hasemit.lock().unwrap();
    assert!(*hasemit);


    let hasemit: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    let queen = Queen::new().unwrap();
    queen.run(2, false);

    let hasemit2 = hasemit.clone();
    queen.on("queen", move |_context| {
        let mut hasemit = hasemit2.lock().unwrap();
        *hasemit = true;
    });

    queen.on("pub:hello", |_|{});

    thread::sleep(Duration::from_secs(1));
    let hasemit = hasemit.lock().unwrap();
    assert!(*hasemit);
}

#[test]
fn sys_pub_off() {
    let hasemit: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    let queen = Queen::new().unwrap();
    queen.run(2, false);

    let hasemit2 = hasemit.clone();
    queen.on("queen", move |_context| {
        let mut hasemit = hasemit2.lock().unwrap();
        *hasemit = true;
    });

    let id = queen.on("sys:hello", |_|{});
    queen.off(id);

    thread::sleep(Duration::from_secs(1));
    let hasemit = hasemit.lock().unwrap();
    assert!(*hasemit);


    let hasemit: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    let queen = Queen::new().unwrap();
    queen.run(2, false);

    let hasemit2 = hasemit.clone();
    queen.on("queen", move |_context| {
        let mut hasemit = hasemit2.lock().unwrap();
        *hasemit = true;
    });

    let id = queen.on("pub:hello", |_|{});
    queen.off(id);

    thread::sleep(Duration::from_secs(1));
    let hasemit = hasemit.lock().unwrap();
    assert!(*hasemit);
}

#[test]
fn push() {
    let hasemit: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    let queen = Queen::new().unwrap();
    queen.run(2, false);

    let hasemit2 = hasemit.clone();
    queen.on("hello", move |_context| {
        let mut hasemit = hasemit2.lock().unwrap();
        *hasemit = true;
    });

    queen.emit("hello", msg!{"hello": "world"});

    thread::sleep(Duration::from_secs(1));
    let hasemit = hasemit.lock().unwrap();
    assert!(*hasemit);


    let hasemit: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    let queen = Queen::new().unwrap();
    queen.run(2, false);

    let hasemit2 = hasemit.clone();
    queen.on("pub:hello", move |_context| {
        let mut hasemit = hasemit2.lock().unwrap();
        *hasemit = true;
    });

    queen.push("pub:hello", msg!{"hello": "world"});

    thread::sleep(Duration::from_secs(1));
    let hasemit = hasemit.lock().unwrap();
    assert!(*hasemit);


    let hasemit: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));

    let queen = Queen::new().unwrap();
    queen.run(2, false);

    let hasemit2 = hasemit.clone();
    queen.on("pub:hello", move |_context| {
        let mut hasemit = hasemit2.lock().unwrap();
        *hasemit += 1;
    });

    let hasemit2 = hasemit.clone();
    queen.on("queen", move |_context| {
        let mut hasemit = hasemit2.lock().unwrap();
        *hasemit += 1;
    });

    queen.emit("pub:hello", msg!{"hello": "world"});

    thread::sleep(Duration::from_secs(1));
    let hasemit = hasemit.lock().unwrap();
    assert!(*hasemit == 3);
}
