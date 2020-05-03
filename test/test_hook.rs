use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering}
};
use std::time::Duration;
use std::thread;
use std::collections::HashSet;

use queen::{Queen, Hook, Sessions, Session};
use queen::nson::{msg, MessageId, Message};
use queen::dict::*;
use queen::error::ErrorCode;

#[test]
fn test_hook() {
    #[derive(Clone)]
    struct MyHook {
        inner: Arc<MyHookInner>
    }

    struct MyHookInner {
        pings: AtomicUsize,
        clients: AtomicUsize,
        recvs: AtomicUsize,
        sends: AtomicUsize
    }

    impl MyHook {
        fn new() -> Self {
            MyHook {
                inner: Arc::new(MyHookInner {
                    pings: AtomicUsize::new(0),
                    clients: AtomicUsize::new(0),
                    recvs: AtomicUsize::new(0),
                    sends: AtomicUsize::new(0)
                })
            }
        }

        fn pings(&self) -> usize {
            self.inner.pings.load(Ordering::SeqCst)
        }

        fn clients(&self) -> usize {
            self.inner.clients.load(Ordering::SeqCst)
        }

        fn recvs(&self) -> usize {
            self.inner.recvs.load(Ordering::SeqCst)
        }

        fn sends(&self) -> usize {
            self.inner.recvs.load(Ordering::SeqCst)
        }
    }

    impl Hook for MyHook {
        fn accept(&self, _: &Session) -> bool {
            self.inner.clients.fetch_add(1, Ordering::SeqCst);

            true
        }

        fn remove(&self, _: &Session) {
            self.inner.clients.fetch_sub(1, Ordering::SeqCst);
        }

        fn recv(&self, conn: &Session, message: &mut Message) -> bool {
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

        fn send(&self, _: &Session, _: &mut Message) -> bool {
            self.inner.sends.fetch_add(1, Ordering::SeqCst);

            true
        }

        fn auth(&self, _: &Session, message: &mut Message) -> bool {
            if let (Ok(user), Ok(pass)) = (message.get_str("user"), message.get_str("pass")) {
                if user == "aaa" && pass == "bbb" {
                    return true
                }
            }

            return false
        }

        fn attach(&self, _: &Session, message: &mut Message, chan: &str, _ : &HashSet<String>) -> bool {
            if chan == "123" {
                return false
            }

            message.insert("123", "456");

            return true
        }

        fn detach(&self, _: &Session, message: &mut Message, chan: &str, _ : &HashSet<String>) -> bool {
            if chan == "123" {
                return false
            }

            message.insert("456", "789");

            return true
        }

        fn ping(&self, _: &Session, message: &mut Message) {
            self.inner.pings.fetch_add(1, Ordering::SeqCst);
            message.insert("hello", "world");
        }

        fn custom(&self, sessions: &Sessions, _token: usize, message: &mut Message) {
            message.insert("hahaha", "wawawa");
            message.insert("clients", sessions.client_ids.len() as u32);

            ErrorCode::OK.insert(message);
        }
    }

    let hook = MyHook::new();

    let queen = Queen::new(MessageId::new(), hook.clone()).unwrap();

    let stream1 = queen.connect(msg!{}, None, None).unwrap();

    assert!(hook.clients() == 1);

    // ping
    let _ = stream1.send(msg!{
        CHAN: PING
    });

    thread::sleep(Duration::from_millis(100));

    let recv = stream1.recv().unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::RefuseReceiveMessage));

    assert!(hook.recvs() == 1);
    assert!(hook.sends() == 1);

    // auth
    let _ = stream1.send(msg!{
        CHAN: AUTH
    });

    thread::sleep(Duration::from_millis(100));

    let recv = stream1.recv().unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::AuthenticationFailed));

    let _ = stream1.send(msg!{
        CHAN: AUTH,
        "user": "aaa",
        "pass": "bbb"
    });

    thread::sleep(Duration::from_millis(100));

    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);

    assert!(hook.recvs() == 3);
    assert!(hook.sends() == 3);

    // ping
    let _ = stream1.send(msg!{
        CHAN: PING
    });

    thread::sleep(Duration::from_millis(100));

    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);
    assert!(recv.get_str("hello").unwrap() == "world");

    assert!(hook.pings() == 1);

    // attach
    let _ = stream1.send(msg!{
        CHAN: ATTACH,
        VALUE: "123"
    });

    thread::sleep(Duration::from_millis(100));

    let recv = stream1.recv().unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    // attach
    let _ = stream1.send(msg!{
        CHAN: ATTACH,
        VALUE: "456"
    });

    thread::sleep(Duration::from_millis(100));

    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);
    assert!(recv.get_str("123").unwrap() == "456");

    // detach
    let _ = stream1.send(msg!{
        CHAN: DETACH,
        VALUE: "123"
    });

    thread::sleep(Duration::from_millis(100));

    let recv = stream1.recv().unwrap();
    assert!(ErrorCode::has_error(&recv) == Some(ErrorCode::Unauthorized));

    // detach
    let _ = stream1.send(msg!{
        CHAN: DETACH,
        VALUE: "456"
    });

    thread::sleep(Duration::from_millis(100));

    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);
    assert!(recv.get_str("456").unwrap() == "789");

    // custom
    let _ = stream1.send(msg!{
        CHAN: CUSTOM,
    });

    thread::sleep(Duration::from_millis(100));

    let recv = stream1.recv().unwrap();
    assert!(recv.get_i32(OK).unwrap() == 0);
    assert!(recv.get_str("hahaha").unwrap() == "wawawa");
    assert!(recv.get_u32("clients").unwrap() == 1);

    drop(stream1);

    thread::sleep(Duration::from_millis(100));

    assert!(hook.clients() == 0);
}
