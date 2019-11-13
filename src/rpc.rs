use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::io;
use std::thread;
use std::collections::HashMap;

use queen_io::queue::mpsc::Queue;
use queen_io::plus::block_queue::BlockQueue;
use queen_io::epoll::{Epoll, Events, Token, Ready, EpollOpt};

use nson::{msg, Message, MessageId};

use crate::port::{Port, AsyncRecv};
use crate::util::oneshot::{oneshot, Sender};
use crate::dict::*;

pub struct Rpc {
    queue: Queue<Packet>,
    run: Arc<AtomicBool>
}

impl Rpc {
    pub fn new(port: Port, works: usize) -> io::Result<Rpc> {
        let run = Arc::new(AtomicBool::new(true));

        let queue = Queue::new()?;

        let worker_queue = BlockQueue::with_capacity(128);

        let mut inner = RpcInner::new(port.clone(), queue.clone(), worker_queue.clone(), run.clone())?;

        thread::Builder::new().name("rpc_backend".to_string()).spawn(move || {
            let ret = inner.run();
            println!("rpc_backend: {:?}", ret);
        }).unwrap();

        for i in 0..works {
            let worker = Worker {
                worker_queue: worker_queue.clone(),
                port: port.clone()
            };

            worker.run(format!("rpc_worker: {:?}", i));
        }

        Ok(Rpc {
            queue,
            run
        })
    }

    pub fn call(
        &self,
        method: &str,
        mut request: Message,
        timeout: Option<Duration>
    ) -> Result<Message, ()> {
        let request_id = MessageId::new();

        request.insert(REQUEST_ID, request_id.clone());
        request.insert(SHARE, true);

        let (tx, rx) = oneshot::<Message>();

        let packet = Packet::Call(
            request_id.clone(),
            method.to_string(),
            request,
            tx
        );

        self.queue.push(packet);

        if let Some(timeout) = timeout {
            let ret = rx.wait_timeout(timeout);

            if ret.is_none() {
                let packet = Packet::UnCall(request_id);
                self.queue.push(packet);
            }

            ret.ok_or(())
        } else {
            rx.wait().ok_or(())
        }
    }

    pub fn add(
        &self,
        method: &str,
        handle: impl Fn(Message) -> Message + Sync + Send + 'static
    ) {
        let packet = Packet::Add(
            method.to_string(),
            Box::new(handle)
        );

        self.queue.push(packet);
    }

    pub fn is_run(&self) -> bool {
        self.run.load(Ordering::Relaxed)
    }

    pub fn stop(&self) {
        self.run.store(false, Ordering::Relaxed);
    }
}

type Handle = dyn Fn(Message) -> Message + Sync + Send + 'static;

enum Packet {
    Call(MessageId, String, Message, Sender<Message>),
    UnCall(MessageId),
    Add(String, Box<Handle>)
}

struct RpcInner {
    port: Port,
    epoll: Epoll,
    events: Events,
    queue: Queue<Packet>,
    worker_queue: BlockQueue<(MessageId, MessageId, Message, Arc<Box<Handle>>)>,
    recv: Option<AsyncRecv>,
    req_recv: Option<AsyncRecv>,
    calling: HashMap<MessageId, Sender<Message>>,
    handles: HashMap<String, Arc<Box<Handle>>>,
    run: Arc<AtomicBool>
}

impl RpcInner {
    const QUEUE_TOKEN: usize = 0;
    const RECV_TOKEN: usize = 1;
    const REQ_RECV_TOKEN: usize = 2;

    fn new(
        port: Port,
        queue: Queue<Packet>,
        worker_queue: BlockQueue<(MessageId, MessageId, Message, Arc<Box<Handle>>)>,
        run: Arc<AtomicBool>
    ) -> io::Result<RpcInner> {
        Ok(RpcInner {
            port,
            epoll: Epoll::new()?,
            events: Events::with_capacity(64),
            queue,
            worker_queue,
            recv: None,
            req_recv: None,
            calling: HashMap::new(),
            handles: HashMap::new(),
            run
        })
    }

    fn run(&mut self) -> io::Result<()> {
        let recv = self.port.async_recv(RPC_RECV, None)?;
        let req_recv = self.port.async_recv(UNKNOWN, None)?;

        self.epoll.add(&recv.recv, Token(Self::RECV_TOKEN), Ready::readable(), EpollOpt::level())?;
        self.epoll.add(&req_recv.recv, Token(Self::REQ_RECV_TOKEN), Ready::readable(), EpollOpt::level())?;
        self.epoll.add(&self.queue, Token(Self::QUEUE_TOKEN), Ready::readable(), EpollOpt::level())?;

        self.recv = Some(recv);
        self.req_recv = Some(req_recv);

        while self.run.load(Ordering::Relaxed) {
            let size = self.epoll.wait(&mut self.events, Some(<Duration>::from_secs(1)))?;

            for i in 0..size {
                let event = self.events.get(i).unwrap();

                match event.token().0 {
                    Self::QUEUE_TOKEN => self.dispatch_queue(),
                    Self::RECV_TOKEN => self.dispatch_recv(),
                    Self::REQ_RECV_TOKEN => self.dispatch_req_recv(),
                    _ => ()
                }
            }
        }

        Ok(())
    }

    fn dispatch_queue(&mut self) {
        if let Some(packet) = self.queue.pop() {
            match packet {
                Packet::Call(id, method, message, tx) => {
                    let req_chan = format!("RPC/REQ/{}", method);

                    self.port.send(&req_chan, message, None);

                    self.calling.insert(id, tx);
                }
                Packet::UnCall(id) => {
                    self.calling.remove(&id);
                }
                Packet::Add(method, handle) => {
                    let req_chan = format!("RPC/REQ/{}", method);

                    self.port.send(ATTACH, msg!{VALUE: &req_chan}, None);

                    self.handles.insert(req_chan, Arc::new(handle));
                }
            }
        }
    }

    fn dispatch_recv(&mut self) {
        if let Some(message) = self.recv.as_ref().unwrap().recv() {
            let request_id = match message.get_message_id(REQUEST_ID) {
                Ok(id) => id,
                Err(_) => return
            };

            if let Some(tx) = self.calling.remove(request_id) {
                if tx.is_needed() {
                    tx.send(message);
                }
            }
        }
    }

    fn dispatch_req_recv(&mut self) {
        if let Some(message) = self.req_recv.as_ref().unwrap().recv() {
            let req_chan = match message.get_str(CHAN) {
                Ok(chan) => chan,
                Err(_) => return
            };

            let from_id = match message.get_message_id(FROM) {
                Ok(id) => id,
                Err(_) => return
            };

            let request_id = match message.get_message_id(REQUEST_ID) {
                Ok(id) => id,
                Err(_) => return
            };

            if let Some(handle) = self.handles.get(req_chan) {
                self.worker_queue.push((from_id.clone(), request_id.clone(), message, handle.clone()))
            }
        }
    }
}

struct Worker {
    worker_queue: BlockQueue<(MessageId, MessageId, Message, Arc<Box<Handle>>)>, // (req_key, from_id, message_id, handle, message)
    port: Port
}

impl Worker {
    fn run(self, name: String) {
        thread::Builder::new().name(name).spawn(|| {
            let worker = self;

            loop {
                let (from_id, request_id, req_message, handle) = worker.worker_queue.pop();

                let mut res_message = handle(req_message);

                res_message.insert(TO, from_id);
                res_message.insert(REQUEST_ID, request_id);

                worker.port.send(RPC_RECV, res_message, None);
            }
        }).unwrap();
    }
}
