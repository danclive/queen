#![allow(dead_code)]
use std::time::Duration;
use std::thread;
use std::io;

use nson::Message;

use queen_io::queue::mpsc::Queue;

use crate::net::Addr;
use crate::oneshot::{oneshot, Sender};

pub struct Point {
    queue: Queue<Packet>
}

pub struct PointConfig {
    pub addrs: Vec<Addr>,
    pub node_addr: Addr,
    pub node_auth: Message,
    pub hmac_key: Option<String>
}

impl Point {
    pub fn new(config: PointConfig) -> io::Result<Point> {
        let queue = Queue::new()?;

        let mut inner = InnerPoint {
            config,
            queue: queue.clone()
        };

        thread::spawn(move || {
            inner.run()
        });

        Ok(Point {
            queue
        })
    }

    pub fn call(
        &self,
        service: &str,
        method: &str,
        request: Message,
        timeout: Option<Duration>
    ) -> Result<Message, ()> {
        let (tx, rx) = oneshot::<Message>();

        let packet = Packet::Call(
            service.to_string(),
            method.to_string(),
            request,
            tx
        );

        self.queue.push(packet);

        if let Some(timeout) = timeout {
            rx.wait_timeout(timeout).ok_or(())
        } else {
            rx.wait().ok_or(())
        }
    }

    pub fn add(
        &self,
        service: &str,
        method: &str,
        handle: impl Fn(Message) -> Message + Sync + Send + 'static
    ) {
        let packet = Packet::Add(
            service.to_string(),
            method.to_string(),
            Box::new(handle)
        );

        self.queue.push(packet);
    }
}

enum Packet {
    Call(String, String, Message, Sender<Message>),
    Add(String, String, Box<dyn Fn(Message) -> Message + Sync + Send + 'static>)
}

struct InnerPoint {
    config: PointConfig,
    queue: Queue<Packet>
}

impl InnerPoint {
    fn run(&mut self) {

    }
}
