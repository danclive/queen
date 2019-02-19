use std::thread;
use std::time::Duration;
use std::sync::{Arc, Mutex};

use queen::center::Center;

fn main() {
    let a = Arc::new(Mutex::new(0));
    let center = Center::new();

    let a2 = a.clone();
    center.all(move |context| {
        let mut a = a2.lock().unwrap();
        *a = context.value.as_i32().unwrap();
    });

    center.run(2, false);

    center.insert("hello", 123.into());

    thread::sleep(Duration::from_secs(1));
    let a = a.lock().unwrap();
    assert!(*a == 123);
}
