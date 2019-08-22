use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use std::thread;
use std::io;
use std::os::unix::io::AsRawFd;

use nson::Message;

use queen_io::poll::{poll, Ready, Events};
use queen_io::queue::mpsc::Queue;

use crate::net::Addr;
use crate::oneshot::{oneshot, Sender};
use crate::port::Hub;

pub struct Point {
    pub hub: Hub,
    queue: Queue<Packet>,
}

pub struct PointConfig {
    pub addrs: Vec<Addr>,
    pub node_addr: Addr,
    pub node_auth: Message,
    pub aead_key: Option<String>
}

impl Point {
    pub fn new(hub: Hub, config: PointConfig) -> io::Result<Point> {
        let queue = Queue::new()?;

        let mut inner = InnerPoint {
            hub: hub.clone(),
            config,
            queue: queue.clone(),
            handles: HashMap::new(),
            services: HashMap::new(),
            un_call: VecDeque::new(),
            run: true
        };

        thread::spawn(move || {
            inner.run()
        });

        Ok(Point {
            hub,
            queue
        })
    }

    pub fn call(
        &self,
        module: &str,
        method: &str,
        request: Message,
        timeout: Option<Duration>
    ) -> Result<Message, ()> {
        let (tx, rx) = oneshot::<Message>();

        let packet = Packet::Call(
            module.to_string(),
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
        module: &str,
        method: &str,
        handle: impl Fn(Message) -> Message + Sync + Send + 'static
    ) {
        let packet = Packet::Add(
            module.to_string(),
            method.to_string(),
            Box::new(handle)
        );

        self.queue.push(packet);
    }
}

type Handle = dyn Fn(Message) -> Message + Sync + Send + 'static;

enum Packet {
    Call(String, String, Message, Sender<Message>),
    Add(String, String, Box<Handle>)
}

struct InnerPoint {
    hub: Hub,
    config: PointConfig,
    queue: Queue<Packet>,
    handles: HashMap<String, HandleBox>,
    // conns: HashMap<i32, Conn>,
    services: HashMap<String, Vec<Service>>,
    un_call: VecDeque<(String, Message, Sender<Message>)>,
    run: bool,
}

struct HandleBox {
    handle: Box<Handle>
}

impl HandleBox {
    fn new(handle: Box<Handle>) -> HandleBox {
        HandleBox {
            handle
        }
    }
}

struct Service {
    id: i32
}

impl InnerPoint {
    fn run(&mut self) -> io::Result<()> {
        while self.run {
            self.run_once()?;
        }

        Ok(())
    }

    fn run_once(&mut self) -> io::Result<()> {
        let mut events = Events::new();

        // queue
        events.put(self.queue.as_raw_fd(), Ready::readable());

        if poll(&mut events, Some(Duration::from_secs(1)))? > 0 {
            for event in &events {
                if event.fd() == self.queue.as_raw_fd() {
                    self.handle_message_from_queue()?;
                } else {

                }
            }
        }

        Ok(())
    }

    fn handle_message_from_queue(&mut self) -> io::Result<()> {
        if let Some(packet) = self.queue.pop() {
            match packet {
                Packet::Call(module, method, message, tx) => {
                    let key = format!("{}.{}", module, method);

                    if let Some(_conn_id) = self.services.get_mut(&key) {

                    } else {
                        self.un_call.push_back((key, message, tx));

                        // todo
                    }
                }
                Packet::Add(module, method, handle) => {
                    let key = format!("{}.{}", module, method);
                    self.handles.insert(key, HandleBox::new(handle));
                }
            }
        }

        Ok(())
    }
}
