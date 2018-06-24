use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::io;
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::io::ErrorKind::ConnectionAborted;
use std::collections::HashMap;

use queen_io::message_queue::MessagesQueue;
use nson;

use protocol::{Message, OpCode, ContentType};
use commom::Connect;

use self::backend::Backend;

pub mod backend;

type SubScribeHandle = Fn(String, u8, Vec<u8>) + Send + Sync + 'static;
type ResponseHandle = Fn(String, u8, Vec<u8>) -> io::Result<(u8, Vec<u8>)> + Send + Sync + 'static;

#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>
}

pub struct ClientInner {
    message_id: AtomicUsize,
    msg_queue: MessagesQueue<(usize, Message)>,
    task_queue: MessagesQueue<(Message, Option<Sender<Message>>)>,
    subscribe_handles: RwLock<HashMap<String, Box<SubScribeHandle>>>,
    response_handle: RwLock<Option<Box<ResponseHandle>>>
}

impl Client {
    pub fn new(addr: &str) -> io::Result<Client> {

        let mut backend = Backend::new(addr)?;

        let client = Client {
            inner: Arc::new(ClientInner {
                message_id: ATOMIC_USIZE_INIT,
                msg_queue: backend.msg_queue().clone(),
                task_queue: backend.task_queue().clone(),
                subscribe_handles: RwLock::new(HashMap::new()),
                response_handle: RwLock::new(None)
            })
        };

        thread::Builder::new().name("client backend".to_owned()).spawn(move || {
            let a = backend.run();
            println!("{:?}", a);
        }).unwrap();


        Ok(client)
    }

    pub fn get_message_id(&self) -> u32 {
        self.inner.message_id.fetch_add(1, Ordering::SeqCst) as u32
    }

    pub fn send(&self, message: Message) -> io::Result<Message> {
        let (tx, rx) = channel();

        self.inner.task_queue.push((message, Some(tx.clone()))).unwrap();

        match rx.recv() {
            Ok(message) => Ok(message),
            Err(err) => {
                return Err(io::Error::new(ConnectionAborted, err).into())
            }
        }
    }

    pub fn run(&self) -> io::Result<()> {
        loop {
            let (_, message) = self.inner.msg_queue.pop();

            let message_id = message.message_id;
            let message_opcode = message.opcode;
            let origin = message.origin;
            let topic = message.topic.clone();

            if message_opcode == OpCode::REQUEST {
                let response_handle = self.inner.response_handle.read().unwrap();

                let return_message = if let Some(ref response_handle) = *response_handle {
                    let (content_type, data) = response_handle(topic.clone(), message.content_type, message.body)?;

                    let mut response_message = Message::new();
                    response_message.message_id = message_id;
                    response_message.opcode = OpCode::RESPONSE;
                    response_message.origin = origin;
                    response_message.topic = topic;
                    response_message.content_type = content_type;
                    response_message.body = data;

                    response_message

                } else {

                    let mut error_message = Message::new();
                    error_message.message_id = message_id;
                    error_message.opcode = OpCode::ERROR;
                    error_message.origin = origin;
                    error_message.topic = topic;
                    error_message.content_type = ContentType::TEXT.bits();
                    error_message.body = "The method doesn't exist!".as_bytes().to_vec();

                    error_message
                };

                self.inner.task_queue.push((return_message, None)).unwrap();
            } else
            if message_opcode == OpCode::PUBLISH {
                let subscribe_handles = self.inner.subscribe_handles.read().unwrap();

                if let Some(ref subscribe_handle) = subscribe_handles.get(&topic) {
                    subscribe_handle(topic.clone(), message.content_type, message.body);

                    let mut puback_message = Message::new();
                    puback_message.message_id = message_id;
                    puback_message.opcode = OpCode::PUBACK;
                    puback_message.topic = topic;

                    self.inner.task_queue.push((puback_message, None)).unwrap();

                }
            }
        }
    }

    pub fn connect(&self, username: &str, password: &str, methods: Vec<String>) -> io::Result<()> {
        let connect = Connect {
            username: username.to_owned(),
            password: password.to_owned(),
            methods: methods
        };

        let body = nson::encode::to_vec(&connect).unwrap();

        let mut connect_message = Message::new();
        connect_message.message_id = self.get_message_id();
        connect_message.opcode = OpCode::CONNECT;
        connect_message.content_type = ContentType::NSON.bits();
        connect_message.body = body;

        let return_message = self.send(connect_message)?;

        if return_message.opcode != OpCode::CONNACK {
            unimplemented!()
        }

        Ok(())
    }

    pub fn ping(&self) -> io::Result<()> {
        let mut ping_message = Message::new();
        ping_message.message_id = self.get_message_id();
        ping_message.opcode = OpCode::PING;

        let pong_message = self.send(ping_message)?;

        if pong_message.opcode != OpCode::PONG {
            unimplemented!()
        }

        Ok(())
    }

    pub fn request(&self, method: &str, content_type: u8, data: Vec<u8>) -> io::Result<(u8, Vec<u8>)>{
        let mut request_message = Message::new();
        request_message.message_id = self.get_message_id();
        request_message.topic = method.to_owned();
        request_message.content_type = content_type;
        request_message.body = data;

        let response_message = self.send(request_message)?;

        if response_message.opcode != OpCode::RESPONSE {
            unimplemented!()
        }

        Ok((response_message.content_type, response_message.body))
    }

    pub fn response<H>(&self, handle: H) -> io::Result<()>
        where H: Fn(String, u8, Vec<u8>) -> io::Result<(u8, Vec<u8>)> + Send + Sync + 'static
    {
        let mut response_handle = self.inner.response_handle.write().unwrap();

        *response_handle = Some(Box::new(handle));

        Ok(())
    }

    pub fn subscribe<H>(&self, topic: &str, handle: H)-> io::Result<()>
        where H: Fn(String, u8, Vec<u8>) + Send + Sync + 'static
    {
        let mut subscribe_message = Message::new();
        subscribe_message.message_id = self.get_message_id();
        subscribe_message.opcode = OpCode::SUBSCRIBE;
        subscribe_message.topic = topic.to_owned();

        let suback_message = self.send(subscribe_message)?;

        if suback_message.opcode != OpCode::SUBACK {
            unimplemented!()
        }

        let mut subscribe_handles = self.inner.subscribe_handles.write().unwrap();

        subscribe_handles.insert(topic.to_owned(), Box::new(handle));

        Ok(())
    }

    pub fn unsubscribe(&self, topic: &str) -> io::Result<()> {
        let mut unsubscribe_message = Message::new();
        unsubscribe_message.message_id = self.get_message_id();
        unsubscribe_message.opcode = OpCode::UNSUBSCRIBE;
        unsubscribe_message.topic = topic.to_owned();

        let unsuback_message = self.send(unsubscribe_message)?;

        if unsuback_message.opcode != OpCode::UNSUBACK {
            unimplemented!()
        }

        Ok(())
    }

    pub fn publish(&self, topic: &str, content_type: u8, data: Vec<u8>) -> io::Result<()> {
        let mut publish_message = Message::new();
        publish_message.message_id = self.get_message_id();
        publish_message.opcode = OpCode::PUBLISH;
        publish_message.topic = topic.to_owned();
        publish_message.content_type = content_type;
        publish_message.body = data;

        let puback_message = self.send(publish_message)?;

        if puback_message.opcode != OpCode::PUBACK {
            unimplemented!()
        }

        Ok(())
    }
}
