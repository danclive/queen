use std::collections::HashSet;
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use std::io::{
    self,
    ErrorKind::{InvalidData, NotConnected, TimedOut, WouldBlock}
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU32, Ordering},
    mpsc::{channel, Receiver, RecvTimeoutError}
};

use crate::dict::*;
use crate::nson::{msg, Message, MessageId};
use crate::crypto::Crypto;
use crate::error::{Result, ErrorCode};
use crate::util::{
    oneshot::oneshot,
    block_queue::BlockQueue
};

use nson::Value;

use stream::Stream;
use session::Session;
use worker::{Worker, WorkerQueue, WorkerQueueMessage};
pub use options::{ClientOptions, SendOptions, RecvOptions, CallOptions, AddOptions};

mod session;
mod stream;
mod worker;
mod options;

#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

#[derive(Debug)]
struct ClientInner {
    options: ClientOptions,
    run: AtomicBool,
    id: AtomicU32,
    session: Mutex<Session>,
    crypto: Option<Crypto>,
    worker_queue: WorkerQueue
}

impl Client {
    pub fn new(options: ClientOptions) -> Result<Self> {
        let mut crypto = None;

        if let Some(options) = &options.crypto_options {
            crypto = Some(Crypto::new(&options.method, options.secret.as_bytes()));
        }

        let client = Client {
            inner: Arc::new(ClientInner {
                options,
                run: AtomicBool::new(true),
                id: AtomicU32::new(0),
                session: Mutex::new(Session::new()),
                crypto,
                worker_queue: BlockQueue::with_capacity(64)
            })
        };

        client.clone().heartbeat();

        for i in 0..client.inner.options.works {
            let worker = Worker {
                client: client.clone()
            };

            worker.run(format!("worker: {:?}", i));
        }

        Ok(client)
    }

    pub fn is_connect(&self) -> bool {
        self.inner.session.lock().unwrap().is_connect()
    }

    pub fn is_run(&self) -> bool {
        self.inner.run.load(Ordering::Relaxed)
    }

    pub fn stop(&self) {
        self.inner.run.store(false, Ordering::Relaxed);

        let mut session = self.inner.session.lock().unwrap();
        session.recvs.clear();
    }

    pub fn send(
        &self,
        chan: &str,
        mut message: Message,
        options: Option<SendOptions>
    ) -> Result<()> {
        message.insert(CHAN, chan);

        let options = options.unwrap_or_default();

        if let Some(label) = options.label {
            message.insert(LABEL, label);
        }

        if let Some(to) = options.to {
            message.insert(TO, to);
        }

        if options.ack {
            message.insert(ACK, Value::Null);
        }

        let id = if let Ok(id) = message.get_message_id(ID) {
            id.clone()
        } else {
            let id = MessageId::new();
            message.insert(ID, &id);
            id
        };

        let data = Crypto::encrypt_message(&self.inner.crypto, &message)?;

        if options.ack {
            let mut session = self.inner.session.lock().unwrap();
            if !session.is_connect() {
                return Err(io::Error::new(NotConnected, "Client::send").into())
            }

            session.stream.as_ref().unwrap().write(&data)?;

            let (tx, rx) = oneshot::<Result<()>>();
            session.sending.insert(id.clone(), tx);

            drop(session);

            let ret = rx.wait_timeout(options.timeout);

            if ret.is_err() {
                let mut session = self.inner.session.lock().unwrap();
                session.sending.remove(&id);
            }

            ret?
        } else {
            let session = self.inner.session.lock().unwrap();
            if !session.is_connect() {
                return Err(io::Error::new(NotConnected, "Client::send").into())
            }

            session.stream.as_ref().unwrap().write(&data)?;

            Ok(())
        }
    }

    pub fn call(
        &self,
        method: &str,
        mut message: Message,
        options: Option<CallOptions>
    ) -> Result<Message> {
        message.insert(CHAN, format!("CALL/{}", method));
        message.insert(SHARE, true);

        let options = options.unwrap_or_default();

        if let Some(label) = options.label {
            message.insert(LABEL, label);
        }

        let request_id = if let Ok(id) = message.get_message_id(REQUEST_ID) {
            id.clone()
        } else {
            let id = MessageId::new();
            message.insert(REQUEST_ID, &id);
            id
        };

        let data = Crypto::encrypt_message(&self.inner.crypto, &message)?;

        let mut session = self.inner.session.lock().unwrap();
        if !session.is_connect() {
            return Err(io::Error::new(NotConnected, "Client::send").into())
        }

        session.stream.as_ref().unwrap().write(&data)?;

        let (tx, rx) = oneshot();
        session.calling.insert(request_id.clone(), tx);

        drop(session);

        let ret = rx.wait_timeout(options.timeout);

        if ret.is_err() {
            let mut session = self.inner.session.lock().unwrap();
            session.calling.remove(&request_id);
        }

        ret?
    }

    pub fn recv(
        &self,
        chan: &str,
        options: Option<RecvOptions>
    ) -> Result<Recv> {
        let options = options.unwrap_or_default();
        let (tx, rx) = channel();
        let (ack_tx, ack_rx) = oneshot::<Result<()>>();
        let id = self.inner.id.fetch_add(1, Ordering::SeqCst);

        {
            let mut session = self.inner.session.lock().unwrap();

            let mut labels_set = HashSet::new();

            if chan == UNKNOWN {
                ack_tx.send(Ok(()));
            } else {
                if let Some(labels) = options.label {
                    for label in labels {
                        labels_set.insert(label.to_string());
                    }
                }

                session.attach(&self.inner.crypto, id, &chan, &mut labels_set, ack_tx)?;
            }

            // chans2
            let set = session.chans2.entry(chan.to_string()).or_insert_with(HashSet::new);
            set.insert(id);

            // recvs
            session.recvs.insert(id, (chan.to_string(), tx, labels_set));
        }

        let ret = ack_rx.wait_timeout(options.timeout);

        let ret = ret.map(|_| {
            Recv {
                client: self.clone(),
                id,
                chan: chan.to_string(),
                recv: rx
            }
        }).map_err(|e| e.into());

        if ret.is_err() {
            let mut session = self.inner.session.lock().unwrap();

            if let Some(recv) = session.recvs.remove(&id) {
                session.detach(&self.inner.crypto, id, recv.0, recv.2)?;
            }
        }

        ret
    }

    pub fn add_handle(
        &self,
        method: &str,
        handle: impl Fn(Message) -> Message + Sync + Send + 'static,
        options: Option<AddOptions>
    ) -> Result<u32> {
        let options = options.unwrap_or_default();
        let (tx, rx) = oneshot::<Result<()>>();
        let id = self.inner.id.fetch_add(1, Ordering::SeqCst);

        let req_chan = format!("CALL/{}", method);

        {
            let mut session = self.inner.session.lock().unwrap();

            let mut labels_set = HashSet::new();

            if let Some(labels) = options.label {
                for label in labels {
                    labels_set.insert(label.to_string());
                }
            }

            session.attach(&self.inner.crypto, id, &req_chan, &mut labels_set, tx)?;

            // chans2
            let set = session.chans2.entry(req_chan.clone()).or_insert_with(HashSet::new);
            set.insert(id);

            // recvs2
            session.recvs2.insert(id, (req_chan, labels_set));

            // handles
            session.handles.insert(id, Arc::new(Box::new(handle)));
        }

        let ret = rx.wait_timeout(options.timeout);

        let ret = ret.map(|_| {
            id
        }).map_err(|e| e.into());

        if ret.is_err() {
            let mut session = self.inner.session.lock().unwrap();

            if let Some(recv) = session.recvs2.remove(&id) {
                session.detach(&self.inner.crypto, id, recv.0, recv.1)?;
            }

            session.handles.remove(&id);
        }

        ret
    }

    pub fn remove_handle(&self, id: u32) -> Result<()> {
        let mut session = self.inner.session.lock().unwrap();

        if let Some(recv) = session.recvs2.remove(&id) {
            session.detach(&self.inner.crypto, id, recv.0, recv.1)?;
        }

        session.handles.remove(&id);

        Ok(())
    }

    fn heartbeat(self) -> thread::JoinHandle<()> {
        thread::Builder::new().name("heartbeat".to_string()).spawn(move || {
            let client = self;

            macro_rules! sleep {
                () => {
                    thread::sleep(Duration::from_secs(1));
                };
            }

            let run = || {
                while client.inner.run.load(Ordering::Relaxed) {
                    if !client.inner.session.lock().unwrap().is_connect() {
                        let tcp_stream = TcpStream::connect_timeout(&client.inner.options.addr, Duration::from_secs(10));
                        let tcp_stream = match tcp_stream {
                            Err(err) => {
                                log::error!("TcpStream::connect_timeout: {:?}", err);
                                sleep!();
                                continue
                            }
                            Ok(tcp_stream) => tcp_stream
                        };

                        let tcp_stream2 = match tcp_stream.try_clone() {
                            Err(err) => {
                                log::error!("tcp_stream.try_clone: {:?}", err);
                                sleep!();
                                return
                            }
                            Ok(tcp_stream) => tcp_stream
                        };

                        let net_stream = Stream(tcp_stream);

                        // handshake
                        if let Err(err) = net_stream.handshake(&client.inner.options.crypto_options) {
                            log::error!("net_stream.handshake: {:?}", err);
                            sleep!();
                            continue
                        }

                        // ping
                        if let Err(err) = net_stream.ping(&client.inner.crypto) {
                            log::error!("net_stream.ping: {:?}", err);
                            sleep!();
                            continue
                        }

                        // auth
                        match net_stream.auth(&client.inner.crypto, client.inner.options.auth_message.clone()) {
                            Err(err) => {
                                log::error!("net_stream.auth: {:?}", err);
                                sleep!();
                                continue
                            }
                            Ok(message) => {
                                if let Some(code) = ErrorCode::has_error(&message) {
                                    if code != ErrorCode::OK {
                                        log::error!("auth: {:?}", code);
                                        sleep!();
                                        continue
                                    }
                                } else {
                                    log::error!("ErrorCode::has_error: UnsupportedFormat");
                                    sleep!();
                                    continue
                                }
                            }
                        }

                        {
                            let mut session = client.inner.session.lock().unwrap();
                            session.stream = Some(net_stream);

                            // attach
                            for (chan, set) in &session.chans {
                                let labels: Vec<String> = set.iter().map(|s| s.to_string()).collect();

                                let message = msg!{
                                    CHAN: ATTACH,
                                    VALUE: chan,
                                    LABEL: labels
                                };

                                let data = Crypto::encrypt_message(&client.inner.crypto, &message);

                                match data {
                                    Err(err) => {
                                        log::error!("client.encrypt: {:?}", err);
                                        sleep!();
                                        continue
                                    }
                                    Ok(data) => {
                                        if let Err(err) = session.stream.as_ref().unwrap()
                                            .write(&data) {
                                            log::error!("write(&data): {:?}", err);
                                            sleep!();
                                            continue
                                        }
                                    }
                                }
                            }
                        }

                        let client2 = client.clone();

                        client2.net_read(Stream(tcp_stream2));
                    } else {
                        sleep!();
                    }
                }
            };

            run();

            client.stop();

            log::trace!("net read thread exit");
        }).unwrap()
    }

    fn net_read(self, net_stream: Stream) {
        thread::Builder::new().name("net read".to_string()).spawn(move || {
            let client = self;
            let run = || {
                net_stream.0.set_read_timeout(Some(Duration::from_secs(1))).unwrap();

                while client.inner.run.load(Ordering::Relaxed) {
                    match net_stream.read() {
                        Ok(data) => {
                            let message = match Crypto::decrypt_message(&client.inner.crypto, data) {
                                Err(err) => {
                                    log::error!("client.decrypt: {:?}", err);
                                    return
                                }
                                Ok(message) => message
                            };

                            if let Err(err) = client.dispatch_message(message) {
                                log::error!("client.dispatch_message: {:?}", err);
                                return
                            }
                        }
                        Err(err) => {
                            if let TimedOut | WouldBlock = err.kind() {
                                continue;
                            } else {
                                log::error!("net_stream.read: {:?}", err);
                                return
                            }
                        }
                    }
                }
            };

            run();

            let mut session = client.inner.session.lock().unwrap();
            session.stream = None;

            log::trace!("net read thread exit");

        }).unwrap();
    }

    fn dispatch_message(&self, message: Message) -> Result<()> {
        if let Ok(chan) = message.get_str(CHAN) {
            if chan.starts_with('_') {
                match chan {
                    ATTACH => {
                        let ok = if let Ok(ok) = message.get_i32(OK) {
                            ok
                        } else {
                            return Err(io::Error::new(InvalidData, "ATTACH => message.get_i32(OK)").into())
                        };

                        let attach_id = if let Ok(attach_id) = message.get_u32(ATTACH_ID) {
                            attach_id
                        } else {
                            return Ok(())
                        };

                        let mut session = self.inner.session.lock().unwrap();

                        if let Some(attaching_tx) = session.attaching.remove(&attach_id) {
                            if ok == 0 {
                                attaching_tx.send(Ok(()));
                            } else {
                                attaching_tx.send(Err(ErrorCode::from_i32(ok).into()));

                                if let Some(recv) = session.recvs.remove(&attach_id) {
                                    session.detach_no_send(attach_id, recv.0, recv.2);
                                }

                                if let Some(recv) = session.recvs2.remove(&attach_id) {
                                    session.detach_no_send(attach_id, recv.0, recv.1);
                                }
                            }
                        }
                    }
                    _ => ()
                }
            } else {
                self.handle_message(message)?;
            }
        }

        Ok(())
    }

    fn handle_message(&self, message: Message) -> Result<()> {
        let mut session = self.inner.session.lock().unwrap();

        if let Ok(ok) = message.get_i32(OK) {
            if let Ok(request_id) = message.get_message_id(REQUEST_ID) {
                if let Some(sending_tx) = session.calling.remove(request_id) {
                    sending_tx.send(Err(ErrorCode::from_i32(ok).into()));
                }
            } else if let Ok(message_id) = message.get_message_id(ID) {
                if let Some(sending_tx) = session.sending.remove(message_id) {
                    if ok == 0 {
                        sending_tx.send(Ok(()));
                    } else {
                        sending_tx.send(Err(ErrorCode::from_i32(ok).into()));
                    }
                }
            } else {
                return Err(io::Error::new(InvalidData, "message.get_message_id(ID)").into());
            }

            return Ok(())
        }

        let chan = message.get_str(CHAN).map(|s| s.to_string()).expect("handle_message:message.get_str(CHAN)");

        if chan == RECV {
            if let Ok(request_id) = message.get_message_id(REQUEST_ID) {
                if let Some(tx) = session.calling.remove(request_id) {
                    if tx.is_needed() {
                        tx.send(Ok(message));
                    }
                }
            }

            return Ok(())
        }

        if let Some(ids) = session.chans2.get(&chan) {
            let mut labels = HashSet::new();

            if let Some(label) = message.get(LABEL) {
                if let Some(label) = label.as_str() {
                    labels.insert(label.to_string());
                } else if let Some(label_array) = label.as_array() {
                    for v in label_array {
                        if let Some(v) = v.as_str() {
                            labels.insert(v.to_string());
                        }
                    }
                }
            }

            for id in ids {
                if let Some((_, tx, label2)) = session.recvs.get(id) {
                    if !labels.is_empty() && (&labels & label2).is_empty() {
                        continue;
                    }

                    let _ = tx.send(message.clone());
                }

                if let Some((_, label2)) = session.recvs2.get(id) {
                    if !labels.is_empty() && (&labels & label2).is_empty() {
                        continue;
                    }

                    if let Ok(from_id) = message.get_message_id(FROM) {
                        if let Ok(request_id) = message.get_message_id(REQUEST_ID) {
                            if let Some(handle) = session.handles.get(id) {
                                self.inner.worker_queue.push(WorkerQueueMessage {
                                    from_id: from_id.clone(),
                                    req_id: request_id.clone(),
                                    req_message: message.clone(),
                                    handle: handle.clone()
                                })
                            }
                        }
                    }
                }
            }

            return Ok(())
        }

        if let Some(ids) = session.chans2.get(UNKNOWN) {
            for id in ids {
                if let Some((_, tx, _)) = session.recvs.get(id) {          
                    let _ = tx.send(message.clone());
                }
            }
        }

        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            self.inner.run.store(false, Ordering::Relaxed);
        }
    }
}

#[derive(Debug)]
pub struct Recv {
    pub client: Client,
    pub id: u32,
    pub chan: String,
    pub recv: Receiver<Message>
}

impl Recv {
    pub fn recv(&self) -> Option<Message> {
        self.recv.recv().ok()
    }

    pub fn recv_timeout(&self, timeout: Duration) -> std::result::Result<Message, RecvTimeoutError> {
        self.recv.recv_timeout(timeout)
    }
}

impl Iterator for Recv {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        self.recv.recv().ok()
    }
}

impl Drop for Recv {
    fn drop(&mut self) {
        let mut session = self.client.inner.session.lock().unwrap();

        if let Some(recv) = session.recvs.remove(&self.id) {
            let _ = session.detach(&self.client.inner.crypto, self.id, recv.0, recv.2);
        }
    }
}
