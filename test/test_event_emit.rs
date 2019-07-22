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
}
