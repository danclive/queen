use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::thread;

use crate::stream::{Stream, StreamTx};
use crate::nson::{msg, Message, MessageId};
use crate::dict::*;
use crate::util::oneshot::{channel, Sender};
use crate::error::{Result, ErrorCode, Error};

pub struct StreamExt {
    session: Arc<Mutex<StreamExtSession>>
}

struct StreamExtSession {
    stream_tx: StreamTx<Message>,
    auth: bool,
    sending: HashMap<MessageId, Sender<Result<Message>>>,
    #[allow(clippy::type_complexity)]
    handle: Option<Arc<Box<dyn Fn(String, Message) + Sync + Send + 'static>>>,
    on_close: Option<Arc<Box<dyn Fn() + Sync + Send + 'static>>>
}

impl StreamExt {
    pub fn new(stream: Stream<Message>) -> Self {
        let (stream_tx, stream_rx) = stream.split();

        let session = StreamExtSession {
            stream_tx,
            auth: false,
            sending: HashMap::new(),
            handle: None,
            on_close: None
        };

        let stream_ext = StreamExt {
            session: Arc::new(Mutex::new(session))
        };

        let stream_ext2 = stream_ext.clone();
        thread::Builder::new().name("stream_ext".to_string()).spawn(move || {
            loop {
                match stream_rx.wait(Some(Duration::from_secs(10))) {
                    Ok(message) => {
                        if let Ok(chan) = message.get_str(CHAN) {
                            if let Ok(ok) = message.get_i32(OK) {
                                if let Ok(id) = message.get_message_id(ID) {
                                    let mut session = stream_ext2.session.lock().unwrap();
                                    
                                    if let Some(sending_tx) = session.sending.remove(id) {
                                        if ok == 0 {
                                            if chan == AUTH {
                                                session.auth = true;
                                            }

                                            let _ = sending_tx.send(Ok(message));
                                        } else {
                                            let _ = sending_tx.send(Err(ErrorCode::from_i32(ok).into()));
                                        }
                                    }
                                }
                            } else {
                                let session = stream_ext2.session.lock().unwrap();

                                let handle = if let Some(handle) = &session.handle {
                                    handle.clone()   
                                } else {
                                    continue;
                                };

                                drop(session);

                                handle(chan.to_string(), message);
                            }
                        }
                    }
                    Err(err) => {
                        if matches!(err, Error::Empty(_) | Error::TimedOut(_)) {
                            continue;
                        }

                        log::error!("stream_ext: {}, attr: {}", err, stream_rx.attr());

                        let mut session = stream_ext2.session.lock().unwrap();
                        session.sending.clear();

                        drop(session);

                        stream_ext2.close();

                        return
                    }
                }
            }
        }).unwrap();

        stream_ext
    }

    pub fn auth(&self, mut message: Message) -> Result<Message> {
        message.insert(CHAN, AUTH);
        self._send(message, None)
    }

    pub fn is_auth(&self) -> bool {
        let session = self.session.lock().unwrap();
        session.auth
    }

    pub fn ping(&self, mut message: Message) -> Result<Message> {
        message.insert(CHAN, PING);
        self._send(message, None)
    }

    pub fn attach(&self, chan: &str, label: Option<Vec<String>>) -> Result<Message> {
        if !self.is_auth() {
            return Err(ErrorCode::Unauthorized.into())
        }

        let mut message = msg!{
            CHAN: ATTACH,
            VALUE: chan
        };

        if let Some(label) = label {
            message.insert(LABEL, label);
        }

        self._send(message, None)
    }

    pub fn detach(&self, chan: &str, label: Option<Vec<String>>) -> Result<Message> {
        if !self.is_auth() {
            return Err(ErrorCode::Unauthorized.into())
        }

        let mut message = msg!{
            CHAN: DETACH,
            VALUE: chan
        };

        if let Some(label) = label {
            message.insert(LABEL, label);
        }

        self._send(message, None)
    }

    pub fn query(&self, mut message: Message) -> Result<Message> {
        if !self.is_auth() {
            return Err(ErrorCode::Unauthorized.into())
        }

        message.insert(CHAN, QUERY);
        self._send(message, None)
    }

    pub fn mine(&self, mut message: Message) -> Result<Message> {
        message.insert(CHAN, MINE);
        self._send(message, None)
    }

    pub fn custom(&self, mut message: Message) -> Result<Message> {
        if !self.is_auth() {
            return Err(ErrorCode::Unauthorized.into())
        }

        message.insert(CHAN, CUSTOM);
        self._send(message, None)
    }

    pub fn client_kill(&self, mut message: Message) -> Result<Message> {
        if !self.is_auth() {
            return Err(ErrorCode::Unauthorized.into())
        }

        message.insert(CHAN, CLIENT_KILL);
        self._send(message, None)
    }

    pub fn send(
        &self,
        chan: &str,
        mut message: Message,
        label: Option<Vec<String>>,
        to: Option<Vec<MessageId>>,
        timeout: Option<Duration>
    ) -> Result<()> {
        if !self.is_auth() {
            return Err(ErrorCode::Unauthorized.into())
        }

        message.insert(CHAN, chan);

        if let Some(label) = label {
            message.insert(LABEL, label);
        }

        if let Some(to) = to {
            message.insert(TO, to);
        }

        if let Some(timeout) = timeout {
            message.insert(ACK, true);

            self._send(message, Some(timeout))?;
        } else {
            self._send_no_ack(message)?;
        }

        Ok(())
    }

    pub fn recv(&self, handle: impl Fn(String, Message) + Sync + Send + 'static) {
        let mut session = self.session.lock().unwrap();
        session.handle = Some(Arc::new(Box::new(handle)));
    }

    pub fn close(&self) {
        let mut session = self.session.lock().unwrap();
        session.stream_tx.close();

        let on_close = session.on_close.take();

        drop(session);

        if let Some(on_close) = on_close {
            on_close()
        }
    }

    pub fn is_close(&self) -> bool {
        let session = self.session.lock().unwrap();
        session.stream_tx.is_close()
    }

    pub fn on_close(&self, callback: impl Fn() + Sync + Send + 'static) {
        let mut session = self.session.lock().unwrap();
        session.on_close = Some(Arc::new(Box::new(callback)));
    }

    fn _send(&self, mut message: Message, timeout: Option<Duration>) -> Result<Message> {
        let id = if let Ok(id) = message.get_message_id(ID) {
            id.clone()
        } else {
            let id = MessageId::new();
            message.insert(ID, &id);
            id
        };

        let (tx, mut rx) = channel::<Result<Message>>()?;

        {
            let mut session = self.session.lock().unwrap();

            if session.stream_tx.is_close() {
                return Err(Error::NotConnected("StreamExt::_send".to_string()))
            }

            session.sending.insert(id.clone(), tx);

            while session.stream_tx.is_full() {
                thread::sleep(Duration::from_millis(10));
            }

            session.stream_tx.send(&mut Some(message))?;
        }

        let timeout = timeout.unwrap_or_else(|| Duration::from_secs(60));

        rx.wait(Some(timeout)).map_err(|err| {
            let mut session = self.session.lock().unwrap();
            session.sending.remove(&id);

            err
        })?;

        rx.try_recv().map_err(|err| {
            let mut session = self.session.lock().unwrap();
            session.sending.remove(&id);

            err
        })?
    }

    fn _send_no_ack(&self, message: Message) -> Result<()> {
        let session = self.session.lock().unwrap();

        if session.stream_tx.is_close() {
            return Err(Error::NotConnected("StreamExt::_send_no_ack".to_string()))
        }

        while session.stream_tx.is_full() {
            thread::sleep(Duration::from_millis(10));
        }

        Ok(session.stream_tx.send(&mut Some(message))?)
    }
}

impl Clone for StreamExt {
    fn clone(&self) -> Self {
        StreamExt {
            session: self.session.clone()
        }
    }
}
