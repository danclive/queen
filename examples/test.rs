use std::thread;
use std::time::Duration;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use nson::msg;

use queen::queen::Queen;
use queen::client;
use queen::Message;

use queen::center::Center;

use std::sync::{Once, ONCE_INIT};

static mut FOO: Option<Arc<&'static Vec<i32>>> = None;
static ONCE : Once = ONCE_INIT;

fn init(a: &Vec<i32>) {
    ONCE.call_once(|| {
        unsafe {
            let b = Box::new(a);
            let p: *const Vec<i32> = &**b;
            std::mem::forget(b);
            FOO = Some(Arc::new(&*p));
            // let p: *const Vec<i32> = a;
            // std::mem::forget(a);
            // FOO = Some(Arc::new(&*p));
        }
    });
}

fn foo() {
    let a = vec![1, 2, 3];
    println!("{:?}", a);
    init(&a.clone());
}

fn main() {
    // let center = Center::new();

    // center.on("aaa", |context| {
    //     //println!("{:?}", context);
    //     let mut i = context.value.as_i32().unwrap();
    //     i += 1;
    //     println!("{:?}", i);
    //     context.center.insert("aaa", i.into());
    // });

    // center.insert("aaa", 1.into());

    // center.run(4, true);

    // let a = vec![1, 2, 3];
    // println!("{:?}", a);
    // println!("{:?}", unsafe {&FOO});
    // init(&a);
    // println!("{:?}", unsafe {&FOO});
    println!("{:?}", unsafe {&FOO});
    foo();
    println!("{:?}", unsafe {&FOO});
}

// pub fn leak<T>(v: T) -> &'static T {
//     unsafe {
//         let b = Box::new(v);
//         let p: *const T = &*b;
//         std::mem::forget(b); // leak our reference, so that `b` is never freed
//         &*p
//     }
// }
