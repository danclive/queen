use std::time::Duration;
use std::io::{self};

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::sync::mpsc::{channel};

use queen_io::queue::spsc::Queue;
use queen_io::queue::mpsc;

use nson::{Message};
use nson::message_id::MessageId;

use crate::{Queen};
use crate::net::{Addr};
use crate::dict::*;

pub use recv::{Recv, AsyncRecv};
use port_backend::{PortBackend, Packet};

mod recv;
mod port_backend;

#[derive(Clone)]
pub struct Port {
    recv_id: Arc<AtomicUsize>,
    queue: mpsc::Queue<Packet>,
    run: Arc<AtomicBool>
}

pub enum Connector {
    Net(Addr),
    Queen(Queen, Message)
}

impl Port {
    pub fn connect(connector: Connector) -> io::Result<Port> {
        let run = Arc::new(AtomicBool::new(true));

        let queue = mpsc::Queue::new()?;

        let queue2 = queue.clone();

        let mut inner = PortBackend::new(connector, queue2, run.clone())?;

        thread::Builder::new().name("port_backend".to_string()).spawn(move || {
            let ret = inner.run();
            println!("port_backend: {:?}", ret);
        }).unwrap();

        Ok(Port {
            recv_id: Arc::new(AtomicUsize::new(0)),
            queue,
            run
        })
    }

    pub fn recv(
        &self,
        chan: &str,
        lables: Option<Vec<String>>
    ) -> Recv { // iter
        let (tx, rx) = channel();

        let id = self.recv_id.fetch_add(1, Ordering::SeqCst);

        self.queue.push(Packet::AttatchBlock(id, chan.to_string(), lables, tx));

        Recv {
            queue: self.queue.clone(),
            id,
            recv: rx
        }
    }

    pub fn async_recv(
        &self,
        chan: &str,
        lables: Option<Vec<String>>
    ) -> io::Result<AsyncRecv> {
        let queue = Queue::with_cache(64)?;

        let id = self.recv_id.fetch_add(1, Ordering::SeqCst);

        self.queue.push(Packet::AttatchAsync(id, chan.to_string(), lables, queue.clone()));

        Ok(AsyncRecv {
            queue: self.queue.clone(),
            id,
            recv: queue
        })
    }

    pub fn send(
        &self,
        chan: &str,
        mut msg: Message,
        lable: Option<Vec<String>>
    ) {
        msg.insert(CHAN, chan);

        if let Some(lable) = lable {
            msg.insert(LABEL, lable);
        }

        if msg.get_message_id(ID).is_err() {
            msg.insert(ID, MessageId::new());
        }

        loop {
            if self.queue.pending() < 64 {
                self.queue.push(Packet::Send(msg));
                return
            }

            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn is_run(&self) -> bool {
        self.run.load(Ordering::Relaxed)
    }

    pub fn stop(&self) {
        self.run.store(false, Ordering::Relaxed);
    }
}

impl Drop for Port {
    fn drop(&mut self) {
        self.run.store(false, Ordering::Relaxed);
    }
}
