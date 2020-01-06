use std::time::Duration;
use std::io::{self, ErrorKind::UnexpectedEof};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread::{self, JoinHandle};
use std::sync::mpsc::channel;

use queen_io::queue::spsc::Queue;
use queen_io::queue::mpsc;
use queen_io::plus::block_queue::BlockQueue;

use nson::{Message, Value};
use nson::message_id::MessageId;

use crate::Queen;
use crate::dict::*;
use crate::util::oneshot::oneshot;
use crate::error::{Result, Error};
use crate::net::{Addr, CryptoOptions};

pub use recv::{Recv, AsyncRecv};
use port_backend::{PortBackend, Packet, WorkerQueue, WorkerQueueMessage};

mod recv;
mod port_backend;

#[derive(Clone, Debug)]
pub struct Port {
    inner: Arc<PortInner>,
    run: Arc<AtomicBool>
}

pub enum Connector {
    Net(Addr, Option<CryptoOptions>),
    Queen(Queen, Message)
}

struct PortInner {
    id: MessageId,
    recv_id: AtomicU32,
    queue: mpsc::Queue<Packet>
}

impl Port {
    pub fn connect(
        id: MessageId,
        connector: Connector,
        auth_message: Message,
        works: usize
    ) -> Result<(Port, JoinHandle<Result<()>>)> {
        let run = Arc::new(AtomicBool::new(true));

        let queue = mpsc::Queue::new()?;
        let queue2 = queue.clone();

        let worker_queue = BlockQueue::with_capacity(128);

        let mut inner = PortBackend::new(
                            id.clone(),
                            connector,
                            auth_message,
                            queue2,
                            works,
                            worker_queue.clone(),
                            run.clone()
                        )?;

        let join_handle = thread::Builder::new().name("port backend".to_string()).spawn(move || {
            inner.run().map_err(Error::from)
        }).unwrap();

        for i in 0..works {
            let worker = Worker {
                worker_queue: worker_queue.clone(),
                queue: queue.clone()
            };

            worker.run(format!("rpc_worker: {:?}", i));
        }

        Ok((Port {
            inner: Arc::new(PortInner {
                id,
                recv_id: AtomicU32::new(0),
                queue,
            }),
            run
        }, join_handle))
    }

    pub fn recv(
        &self,
        chan: &str,
        labels: Option<Vec<String>>,
        timeout: Option<Duration>
    ) -> Result<Recv> {
        let (tx, rx) = channel();
        let (tx2, rx2) = oneshot::<Result<()>>();

        let id = self.inner.recv_id.fetch_add(1, Ordering::SeqCst);

        self.inner.queue.push(Packet::AttachBlock {
            id,
            chan: chan.to_string(),
            labels,
            tx,
            ack_tx: tx2
        });

        // wait
        let timeout = timeout.unwrap_or(Duration::from_secs(60));
        let ret = rx2.wait_timeout(timeout);

        ret?.unwrap_or_else(|| Err(io::Error::new(UnexpectedEof, "UnexpectedEof").into()))?;

        Ok(Recv {
            port: self.clone(),
            id,
            chan: chan.to_string(),
            recv: rx
        })
    }

    pub fn async_recv(
        &self,
        chan: &str,
        labels: Option<Vec<String>>,
        timeout: Option<Duration>
    ) -> Result<AsyncRecv> {
        let (tx, rx) = oneshot::<Result<()>>();

        let queue = Queue::with_cache(64)?;

        let id = self.inner.recv_id.fetch_add(1, Ordering::SeqCst);

        self.inner.queue.push(Packet::AttachAsync {
            id,
            chan: chan.to_string(),
            labels,
            tx: queue.clone(),
            ack_tx: tx
        });

        // wait
        let timeout = timeout.unwrap_or(Duration::from_secs(60));
        let ret = rx.wait_timeout(timeout);

        ret?.unwrap_or_else(|| Err(io::Error::new(UnexpectedEof, "UnexpectedEof").into()))?;

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
        mut message: Message,
        label: Option<Vec<String>>,
        timeout: Option<Duration>
    ) -> Result<()> {
        message.insert(CHAN, chan);
        message.insert(ACK, Value::Null);

        if let Some(label) = label {
            message.insert(LABEL, label);
        }

        if message.get_message_id(ID).is_err() {
            message.insert(ID, MessageId::new());
        }

        let (tx, rx) = oneshot::<Result<()>>();

        self.inner.queue.push(Packet::Send {
            message,
            ack_tx: tx
        });

        // wait
        let timeout = timeout.unwrap_or(Duration::from_secs(60));
        let ret = rx.wait_timeout(timeout)?;

        ret.unwrap_or_else(|| Err(io::Error::new(UnexpectedEof, "UnexpectedEof").into()))
    }

    pub fn call(
        &self,
        method: &str,
        labels: Option<Vec<String>>,
        mut request: Message,
        timeout: Option<Duration>
    ) -> Result<Message> {
        let request_id = MessageId::new();

        request.insert(REQUEST_ID, request_id.clone());

        if let Some(labels) = labels {
            request.insert(LABEL, labels);
        }

        let (tx, rx) = oneshot::<Result<Message>>();

        let packet = Packet::Call {
            id: request_id.clone(),
            method: method.to_string(),
            message: request,
            ack_tx: tx
        };

        self.inner.queue.push(packet);

        // wait
        let timeout = timeout.unwrap_or(Duration::from_secs(60));
        let ret = rx.wait_timeout(timeout);

        if ret.is_err() {
            self.inner.queue.push(Packet::UnCall { id: request_id });
        }

        ret?.unwrap_or_else(|| Err(io::Error::new(UnexpectedEof, "UnexpectedEof").into()))
    }

    pub fn add(
        &self,
        method: &str,
        labels: Option<Vec<String>>,
        handle: impl Fn(Message) -> Message + Sync + Send + 'static,
        timeout: Option<Duration>
    ) -> Result<u32> {

        let id = self.inner.recv_id.fetch_add(1, Ordering::SeqCst);

        let (tx, rx) = oneshot::<Result<()>>();

        let packet = Packet::Add {
            id,
            method: method.to_string(),
            handle: Box::new(handle),
            labels,
            ack_tx: tx
        };

        self.inner.queue.push(packet);

        // wait
        let timeout = timeout.unwrap_or(Duration::from_secs(60));
        let ret = rx.wait_timeout(timeout);

        if ret.is_err() {
            self.inner.queue.push(Packet::Remove { id });
        }

        ret?.unwrap_or_else(|| Err(io::Error::new(UnexpectedEof, "UnexpectedEof").into()))?;

        Ok(id)
    }

    pub fn remove(&self, id: u32) {
        self.inner.queue.push(Packet::Remove { id })
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

impl fmt::Debug for PortInner {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("PortInner")
            .field("id", &self.id)
            .field("recv_id", &self.recv_id)
            .finish()
    }
}

struct Worker {
    worker_queue: WorkerQueue,
    queue: mpsc::Queue<Packet>
}

impl Worker {
    fn run(self, name: String) {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        thread::Builder::new().name(name).spawn(|| {
            let worker = self;

            loop {
                if let Some(WorkerQueueMessage { from_id, req_id, req_message, handle }) = worker.worker_queue.pop() {
                    let _ = catch_unwind(AssertUnwindSafe(|| {
                        let mut res_message = handle(req_message);

                        res_message.insert(TO, from_id);
                        res_message.insert(REQUEST_ID, req_id);
                        res_message.insert(CHAN, RPC_RECV);

                        worker.queue.push(Packet::Response { message: res_message });
                    }));
                } else {
                    return
                }
            }
        }).unwrap();
    }
}
