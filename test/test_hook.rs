use std::sync::{
    Arc,
    atomic::{AtomicUsize, AtomicBool, Ordering}
};
use std::time::Duration;
use std::thread;
use std::collections::HashSet;

use queen::{Socket, Hook, Switch, Slot, SlotModify};
use queen::nson::{msg, MessageId, Message};
use queen::dict::*;
use queen::error::Code;

#[test]
fn test_hook() {
    #[derive(Clone)]
    struct MyHook {
        inner: Arc<MyHookInner>
    }

    struct MyHookInner {
        pings: AtomicUsize,
        slots: AtomicUsize,
        recvs: AtomicUsize,
        sends: AtomicUsize,
        run: AtomicBool
    }

    impl MyHook {
        fn new() -> Self {
            MyHook {
                inner: Arc::new(MyHookInner {
                    pings: AtomicUsize::new(0),
                    slots: AtomicUsize::new(0),
                    recvs: AtomicUsize::new(0),
                    sends: AtomicUsize::new(0),
                    run: AtomicBool::new(true)
                })
            }
        }

        fn pings(&self) -> usize {
            self.inner.pings.load(Ordering::SeqCst)
        }

        fn slots(&self) -> usize {
            self.inner.slots.load(Ordering::SeqCst)
        }

        fn recvs(&self) -> usize {
            self.inner.recvs.load(Ordering::SeqCst)
        }

        fn sends(&self) -> usize {
            self.inner.recvs.load(Ordering::SeqCst)
        }

        fn run(&self) -> bool {
            !self.inner.run.load(Ordering::SeqCst)
        }
    }

    impl Hook for MyHook {
        fn accept(&self, _: &Slot) -> bool {
            self.inner.slots.fetch_add(1, Ordering::SeqCst);

            true
        }

        fn remove(&self, _: &Slot) {
            self.inner.slots.fetch_sub(1, Ordering::SeqCst);
        }

        fn recv(&self, conn: &Slot, message: &mut Message) -> bool {
            self.inner.recvs.fetch_add(1, Ordering::SeqCst);

            if !conn.auth {
                if let Ok(chan) = message.get_str(CHAN) {
                    if chan == PING {
                        return false
                    }
                }
            }

            true
        }

        fn send(&self, _: &Slot, _: &mut Message) -> bool {
            self.inner.sends.fetch_add(1, Ordering::SeqCst);

            true
        }

        fn auth(&self, _: &Slot, _: &SlotModify, message: &mut Message) -> bool {
            if let (Ok(user), Ok(pass)) = (message.get_str("user"), message.get_str("pass")) {
                if user == "aaa" && pass == "bbb" {
                    return true
                }
            }

            return false
        }

        fn attach(&self, _: &Slot, message: &mut Message, chan: &str, _ : &HashSet<String>) -> bool {
            if chan == "123" {
                return false
            }

            message.insert("123", "456");

            return true
        }

        fn detach(&self, _: &Slot, message: &mut Message, chan: &str, _ : &HashSet<String>) -> bool {
            if chan == "123" {
                return false
            }

            message.insert("456", "789");

            return true
        }

        fn ping(&self, _: &Slot, message: &mut Message) {
            self.inner.pings.fetch_add(1, Ordering::SeqCst);
            message.insert("hello", "world");
        }

        fn custom(&self, switch: &Switch, _token: usize, message: &mut Message) {
            message.insert("hahaha", "wawawa");
            message.insert("slots", switch.slot_ids.len() as u32);

            Code::Ok.set(message);
        }

        fn stop(&self, _switch: &Switch) {
            self.inner.run.store(false, Ordering::SeqCst);
        }
    }

    let hook = MyHook::new();

    let socket = Socket::new(MessageId::new(), hook.clone()).unwrap();

    let wire1 = socket.connect(msg!{}, None, None).unwrap();

    assert!(hook.slots() == 1);

    // ping
    let _ = wire1.send(msg!{
        CHAN: PING
    });

    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(Code::get(&recv) == Some(Code::RefuseReceiveMessage));

    assert!(hook.recvs() == 1);
    assert!(hook.sends() == 1);

    // auth
    let _ = wire1.send(msg!{
        CHAN: AUTH
    });

    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(Code::get(&recv) == Some(Code::AuthenticationFailed));

    let _ = wire1.send(msg!{
        CHAN: AUTH,
        "user": "aaa",
        "pass": "bbb"
    });

    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);

    assert!(hook.recvs() == 3);
    assert!(hook.sends() == 3);

    // ping
    let _ = wire1.send(msg!{
        CHAN: PING
    });

    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(hook.pings() == 1);

    // attach
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "123"
    });

    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(Code::get(&recv) == Some(Code::PermissionDenied));

    // attach
    let _ = wire1.send(msg!{
        CHAN: ATTACH,
        VALUE: "456"
    });

    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_str("123").unwrap() == "456");

    // detach
    let _ = wire1.send(msg!{
        CHAN: DETACH,
        VALUE: "123"
    });

    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(Code::get(&recv) == Some(Code::PermissionDenied));

    // detach
    let _ = wire1.send(msg!{
        CHAN: DETACH,
        VALUE: "456"
    });

    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_str("456").unwrap() == "789");

    // custom
    let _ = wire1.send(msg!{
        CHAN: CUSTOM,
    });

    let recv = wire1.wait(Some(Duration::from_secs(1))).unwrap();
    assert!(recv.get_i32(CODE).unwrap() == 0);
    assert!(recv.get_str("hahaha").unwrap() == "wawawa");
    assert!(recv.get_u32("slots").unwrap() == 1);

    drop(wire1);

    thread::sleep(Duration::from_millis(100));

    assert!(hook.slots() == 0);

    socket.stop();

    thread::sleep(Duration::from_millis(100));

    assert!(hook.run() == false);
}
