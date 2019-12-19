use std::time::Duration;
use std::io::{self};

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::sync::mpsc::channel;

use queen_io::queue::spsc::Queue;
use queen_io::queue::mpsc;
use queen_io::plus::block_queue::BlockQueue;

use nson::{Message};
use nson::message_id::MessageId;

use crate::dict::*;
use crate::Connector;
use crate::util::oneshot::oneshot;

pub use recv::{Recv, AsyncRecv};
use port_backend::{PortBackend, Packet, WorkerQueue};

mod recv;
mod port_backend;

#[derive(Clone)]
pub struct Port {
    inner: Arc<PortInner>,
    run: Arc<AtomicBool>
}

struct PortInner {
    id: MessageId,
    recv_id: AtomicUsize,
    queue: mpsc::Queue<Packet>
}

impl Port {
    pub fn connect(id: MessageId, connector: Connector, auth_msg: Message, works: usize) -> io::Result<Port> {
        let run = Arc::new(AtomicBool::new(true));

        let queue = mpsc::Queue::new()?;
        let queue2 = queue.clone();

        let worker_queue = BlockQueue::with_capacity(128);

        let mut inner = PortBackend::new(
                            id.clone(),
                            connector,
                            auth_msg,
                            queue2,
                            worker_queue.clone(),
                            run.clone()
                        )?;

        thread::Builder::new().name("port_backend".to_string()).spawn(move || {
            inner.run().unwrap();
        }).unwrap();

        for i in 0..works {
            let worker = Worker {
                worker_queue: worker_queue.clone(),
                queue: queue.clone()
            };

            worker.run(format!("rpc_worker: {:?}", i));
        }

        Ok(Port {
            inner: Arc::new(PortInner {
                id,
                recv_id: AtomicUsize::new(0),
                queue,
            }),
            run
        })
    }

    pub fn recv(
        &self,
        chan: &str,
        lables: Option<Vec<String>>
    ) -> Recv {
        let (tx, rx) = channel();

        let id = self.inner.recv_id.fetch_add(1, Ordering::SeqCst);

        self.inner.queue.push(Packet::AttachBlock(id, chan.to_string(), lables, tx));

        Recv {
            port: self.clone(),
            id,
            chan: chan.to_string(),
            recv: rx
        }
    }

    pub fn async_recv(
        &self,
        chan: &str,
        lables: Option<Vec<String>>
    ) -> io::Result<AsyncRecv> {
        let queue = Queue::with_cache(64)?;

        let id = self.inner.recv_id.fetch_add(1, Ordering::SeqCst);

        self.inner.queue.push(Packet::AttachAsync(id, chan.to_string(), lables, queue.clone()));

        Ok(AsyncRecv {
            port: self.clone(),
            id,
            chan: chan.to_string(),
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
            if self.inner.queue.pending() < 64 {
                self.inner.queue.push(Packet::Send(msg));
                return
            }

            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn call(
        &self,
        method: &str,
        lables: Option<Vec<String>>,
        mut request: Message,
        timeout: Option<Duration>
    ) -> Result<Message, ()> {
        let request_id = MessageId::new();

        request.insert(REQUEST_ID, request_id.clone());
        request.insert(SHARE, true);

        if let Some(lables) = lables {
            request.insert(LABEL, lables);
        }

        let (tx, rx) = oneshot::<Message>();

        let packet = Packet::Call(
            request_id.clone(),
            method.to_string(),
            request,
            tx
        );

        self.inner.queue.push(packet);

        if let Some(timeout) = timeout {
            let ret = rx.wait_timeout(timeout);

            if ret.is_none() {
                let packet = Packet::UnCall(request_id);
                self.inner.queue.push(packet);
            }

            ret.ok_or(())
        } else {
            rx.wait().ok_or(())
        }
    }

    pub fn add(
        &self,
        method: &str,
        lables: Option<Vec<String>>,
        handle: impl Fn(Message) -> Message + Sync + Send + 'static
    ) {
        let packet = Packet::Add(
            method.to_string(),
            Box::new(handle),
            lables
        );

        self.inner.queue.push(packet);
    }

    pub fn remove(&self, method: &str) {
        self.inner.queue.push(Packet::Remove(method.to_string()))
    }

    pub fn id(&self) -> &MessageId {
        &self.inner.id
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
        if Arc::strong_count(&self.inner) == 1 {
            self.run.store(false, Ordering::Relaxed);
        }
    }
}

struct Worker {
    worker_queue: WorkerQueue, // (req_key, from_id, message_id, handle, message)
    queue: mpsc::Queue<Packet>,
}

impl Worker {
    fn run(self, name: String) {
        thread::Builder::new().name(name).spawn(|| {
            let worker = self;

            loop {
                if let Some((from_id, request_id, req_message, handle)) = worker.worker_queue.pop() {
                    let mut res_message = handle(req_message);

                    res_message.insert(TO, from_id);
                    res_message.insert(REQUEST_ID, request_id);
                    res_message.insert(CHAN, RPC_RECV);

                    worker.queue.push(Packet::Response(res_message));
                } else {
                    return
                }
            }
        }).unwrap();
    }
}
