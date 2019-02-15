use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use queen::center::Center;

#[test]
fn insert_get_remove() {
    let center = Center::new();

    center.insert("hello", "world".into());

    assert_eq!(center.get("hello"), Some("world".into()));
    assert_eq!(center.get("hello2"), None);
    assert_eq!(center.remove("hello"), Some("world".into()));
    assert_eq!(center.get("hello"), None);

    center.insert("hello", "world".into());
    assert_eq!(center.get("hello"), Some("world".into()));
    center.insert("hello", "world2".into());
    assert_eq!(center.get("hello"), Some("world2".into()));
}

#[test]
fn on_and_off() {
    let a = Arc::new(Mutex::new(0));
    let center = Center::new();

    let a2 = a.clone();
    center.on("hello", move |_context| {
        let mut a = a2.lock().unwrap();
        *a += 1;
    });

    center.run(2, false);

    center.insert("hello", 1.into());

    thread::sleep(Duration::from_secs(1));
    let a = a.lock().unwrap();
    assert!(*a == 1);
}

#[test]
fn on_and_off2() {
    let a = Arc::new(Mutex::new(0));
    let center = Center::new();

    let a2 = a.clone();
    center.on("hello", move |_context| {
        let mut a = a2.lock().unwrap();
        *a += 1;
    });

    let a2 = a.clone();
    center.on("hello", move |_context| {
        let mut a = a2.lock().unwrap();
        *a += 1;
    });

    center.run(2, false);

    center.insert("hello", 1.into());

    thread::sleep(Duration::from_secs(1));
    let a = a.lock().unwrap();
    assert!(*a == 2);
}

#[test]
fn on_and_off3() {
    let a = Arc::new(Mutex::new(0));
    let center = Center::new();

    let a2 = a.clone();
    center.on("hello", move |context| {
        let mut a = a2.lock().unwrap();
        *a = context.value.as_i32().unwrap();
    });

    center.run(2, false);

    center.insert("hello", 123.into());

    thread::sleep(Duration::from_secs(1));
    let a = a.lock().unwrap();
    assert!(*a == 123);
}

#[test]
fn on_and_off4() {
    let a = Arc::new(Mutex::new(0));
    let center = Center::new();

    let a2 = a.clone();
    let id = center.on("hello", move |context| {
        let mut a = a2.lock().unwrap();
        *a = context.value.as_i32().unwrap();
    });

    assert!(center.off(id));

    center.run(2, false);

    center.insert("hello", 123.into());

    thread::sleep(Duration::from_secs(1));
    let a = a.lock().unwrap();
    assert!(*a == 0);
}
