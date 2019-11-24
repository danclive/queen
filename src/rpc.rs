use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::io;
use std::thread;

use queen_io::queue::mpsc::Queue;
use queen_io::plus::block_queue::BlockQueue;

use nson::{Message, MessageId};

use crate::util::oneshot::oneshot;
use crate::dict::*;
use crate::Connector;

use rpc_backend::{RpcBackend, Packet, WorkerQueue};

mod rpc_backend;

#[derive(Clone)]
pub struct Rpc {
    inner: Arc<RpcInner>,
    run: Arc<AtomicBool>
}

struct RpcInner {
    id: MessageId,
    queue: Queue<Packet>,
    worker_queue: WorkerQueue,
    works: Arc<usize>
}

impl Rpc {
    pub fn new(id: MessageId, connector: Connector, auth_msg: Message, works: usize) -> io::Result<Rpc> {
        let run = Arc::new(AtomicBool::new(true));

        let queue = Queue::new()?;

        let worker_queue = BlockQueue::with_capacity(128);

        let mut inner = RpcBackend::new(
                            id.clone(),
                            connector,
                            auth_msg,
                            queue.clone(),
                            worker_queue.clone(),
                            run.clone()
                        )?;

        thread::Builder::new().name("rpc_backend".to_string()).spawn(move || {
            inner.run().unwrap();
        }).unwrap();

        for i in 0..works {
            let worker = Worker {
                worker_queue: worker_queue.clone(),
                queue: queue.clone()
            };

            worker.run(format!("rpc_worker: {:?}", i));
        }

        Ok(Rpc {
            inner: Arc::new(RpcInner {
                id,
                queue,
                works: Arc::new(works),
                worker_queue
            }),
            run
        })
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

impl Drop for Rpc {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            self.run.store(false, Ordering::Relaxed);

            for _ in 0..*self.inner.works {
                self.inner.worker_queue.push(None);
            }
        }
    }
}

struct Worker {
    worker_queue: WorkerQueue, // (req_key, from_id, message_id, handle, message)
    queue: Queue<Packet>,
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
