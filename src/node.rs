use std::io::ErrorKind::{WouldBlock, Interrupted};
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::thread;
use std::time::Duration;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};
use std::str::FromStr;

use queen_io::{
    epoll::{Epoll, Events, Token, Ready, EpollOpt},
    queue::mpsc::Queue,
    net::tcp::{TcpListener, TcpStream}
};

use rand::{SeedableRng, seq::SliceRandom, rngs::SmallRng};

use nson::{Message, MessageId};

use crate::Socket;
use crate::Wire;
use crate::net::{NetWork, Packet, Codec, KeepAlive};
use crate::crypto::{Crypto, Method};
use crate::dict::*;
use crate::util::message::read_block;
use crate::error::{Result, Error, Code};

pub use hook::{Hook, NonHook};

mod hook;

pub trait Connector: Send + 'static {
    fn connect(
        &self,
        attr: Message,
        capacity: Option<usize>,
        timeout: Option<Duration>
    ) -> Result<Wire<Message>>;

    fn running(&self) -> bool;
}

impl Connector for Socket {
    fn connect(
        &self,
        attr: Message,
        capacity: Option<usize>,
        timeout: Option<Duration>
    ) -> Result<Wire<Message>> {
        self.connect(attr, capacity, timeout)
    }

    fn running(&self) -> bool {
        self.running()
    }
}

pub struct Node<C: Codec> {
    #[allow(clippy::rc_buffer)]
    queues: Arc<Vec<Queue<Packet<C>>>>,
    run: Arc<AtomicBool>
}

impl<C: Codec> Node<C> {
    pub fn new(
        connector: impl Connector,
        worker_num: usize,
        addrs: Vec<SocketAddr>,
        keep_alive: KeepAlive,
        hook: impl Hook
    ) -> Result<Self> {
        let mut queues = Vec::new();

        for _ in 0..worker_num {
            let queue: Queue<Packet<C>> = Queue::new()?;
            queues.push(queue);
        }

        let node = Self {
            queues: Arc::new(queues),
            run: Arc::new(AtomicBool::new(true))
        };

        let mut inner: Inner<C, _> = Inner::new(
            node.clone(),
            connector,
            addrs,
            keep_alive,
            hook
        )?;

        thread::Builder::new().name("node".to_string()).spawn(move || {
            inner.run().unwrap()
        }).unwrap();

        Ok(node)
    }

    #[inline]
    pub fn stop(&self) {
        self.run.store(false, Ordering::Relaxed);

        for queue in self.queues.iter() {
            queue.push(Packet::Close);
        }
    }

    #[inline]
    pub fn running(&self) -> bool {
        self.run.load(Ordering::Relaxed)
    }
}

struct Inner<C: Codec, H: Hook> {
    node: Node<C>,
    connector: Box<dyn Connector>,
    epoll: Epoll,
    events: Events,
    listens: Vec<TcpListener>,
    rand: SmallRng,
    hook: H,
}

impl<C: Codec, H: Hook> Inner<C, H> {
    fn new(
        node: Node<C>,
        connector: impl Connector,
        addrs: Vec<SocketAddr>,
        keep_alive: KeepAlive,
        hook: H
    ) -> Result<Self> {
        let mut listens = Vec::new();

        for addr in addrs {
            listens.push(TcpListener::bind(addr)?);
        }

        for queue in node.queues.iter() {
            let mut net_work = NetWork::<C>::new(queue.clone(), keep_alive.clone())?;

            let run2 = node.run.clone();

            thread::Builder::new().name("node_net".to_string()).spawn(move || {
                let ret = net_work.run();
                if ret.is_err() {
                    log::error!("net thread exit: {:?}", ret);
                } else {
                    log::debug!("net thread exit");
                }

                run2.store(false, Ordering::Relaxed);
            }).unwrap();
        }

        Ok(Self {
            node,
            connector: Box::new(connector),
            epoll: Epoll::new()?,
            events: Events::with_capacity(16),
            listens,
            hook,
            rand: SmallRng::from_entropy()
        })
    }

    #[inline]
    fn running(&self) -> bool {
        self.node.run.load(Ordering::Relaxed)
    }

    pub fn run(&mut self) -> Result<()> {
        for (id, listen) in self.listens.iter().enumerate() {
            self.epoll.add(&listen.as_raw_fd(), Token(id), Ready::readable(), EpollOpt::edge())?;
        }

        while self.running() && self.connector.running() {
            let size = match self.epoll.wait(&mut self.events, None) {
                Ok(size) => size,
                Err(err) => {
                    if err.kind() == Interrupted {
                        continue;
                    } else {
                        return Err(err.into())
                    }
                }
            };

            for i in 0..size {
                let event = self.events.get(i).unwrap();
                let token = event.token();

                if let Some(listen) = self.listens.get(token.0) {
                    loop {
                        let (mut stream, addr) = match listen.accept() {
                            Ok(stream) => stream,
                            Err(err) => {
                                if err.kind() == WouldBlock {
                                    break;
                                } else {
                                    return Err(err.into())
                                }
                            }
                        };

                        if !self.hook.accept(&mut stream) {
                            continue;
                        }

                        stream.set_nodelay(true)?;
                        // 握手开始
                        stream.set_nonblocking(false)?;
                        // 连接成功后，5秒内收不到握手消息应当断开
                        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
                        stream.set_write_timeout(Some(Duration::from_secs(5)))?;

                        let (wire, codec, crypto) = match Self::hand(&self.hook, &*self.connector, &mut stream, &addr) {
                            Ok(ret) => ret,
                            Err(err) => {
                                log::debug!("{}", err);
                                continue;
                            }
                        };

                        stream.set_nonblocking(true)?;
                        stream.set_read_timeout(None)?;
                        stream.set_write_timeout(None)?;
                        // 握手结束

                        if let Some(queue) = self.node.queues.choose(&mut self.rand) {
                            queue.push(Packet::NewConn {
                                wire,
                                stream,
                                codec,
                                crypto
                            })
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn hand(
        hook: &H,
        connector: &dyn Connector,
        stream: &mut TcpStream,
        addr: &SocketAddr
    ) -> Result<(Wire<Message>, C, Option<Crypto>)> {
        let mut codec = C::new();

        // 握手时的消息，不能超过 2048 字节
        let bytes = read_block(stream, Some(2048))?;
        let mut message = codec.decode(&None, bytes)?;

        let chan = match message.get_str(CHAN) {
            Ok(chan) => chan,
            Err(_) => {
                #[cfg(debug_assertions)]
                {
                    Code::CannotGetChanField.set(&mut message);
                    let _ = Self::send(&mut codec, stream, message);
                }

                return Err(Error::ErrorCode(Code::CannotGetChanField))
            }
        };

        if chan != HAND {
            #[cfg(debug_assertions)]
            {
                Code::UnsupportedChan.set(&mut message);
                let _ = Self::send(&mut codec, stream, message);
            }

            return Err(Error::ErrorCode(Code::UnsupportedChan))
        }

        // SLOT_ID
        let slot_id = if let Some(slot_id) = message.get(SLOT_ID) {
            if let Some(slot_id) = slot_id.as_message_id() {
                *slot_id
            } else {
                #[cfg(debug_assertions)]
                {
                    Code::InvalidSlotIdFieldType.set(&mut message);
                    let _ = Self::send(&mut codec, stream, message);
                }

                return Err(Error::ErrorCode(Code::InvalidSlotIdFieldType));
            }
        } else {
            let slot_id = MessageId::new();
            message.insert(SLOT_ID, slot_id);
            slot_id
        };

        // 握手消息是可以修改的，修改后的消息会发回客户端，因此可以携带自定义数据
        // 但是对于一些握手必备的属性，请谨慎修改，比如加密方式（METHOD）
        if !hook.start(slot_id, &mut message) {
            #[cfg(debug_assertions)]
            {
                Code::AuthenticationFailed.set(&mut message);
                let _ = Self::send(&mut codec, stream, message);
            }

            return Err(Error::ErrorCode(Code::AuthenticationFailed));
        }

        if !hook.enable_secure() {
            // 没有开启加密

            // 但是要注意，握手消息是没有加密的，不能传递敏感数据
            let mut attr = message.clone();

            attr.remove(CHAN);
            attr.insert(ADDR, addr.to_string());

            let wire = connector.connect(attr, None, None)?;

            // 这里可以修改 Wire 的属性
            hook.finish(slot_id, &mut message, &wire);

            Code::Ok.set(&mut message);

            // 握手消息发回
            Self::send(&mut codec, stream, message)?;

            return Ok((wire, codec, None))
        }

        if let Ok(method) = message.get_str(METHOD) {
            let method = if let Ok(method) = Method::from_str(method) {
                method
            } else {
                #[cfg(debug_assertions)]
                {
                    Code::UnsupportedFormat.set(&mut message);
                    let _ = Self::send(&mut codec, stream, message);
                }

                return Err(Error::ErrorCode(Code::UnsupportedFormat));
            };

            // 握手消息是可以修改的，修改后的消息会发回客户端，因此可以携带自定义数据
            // 但是对于一些握手必备的属性，请谨慎修改
            // 这里需要根据传递的自定义数据，返回一个加密密钥
            // let secret = hook.access(&mut message).ok_or_else(|| Error::ErrorCode(Code::PermissionDenied))?;
            let secret = match hook.access(slot_id, &mut message) {
                Some(s) => s,
                None => {
                    #[cfg(debug_assertions)]
                    {
                        Code::PermissionDenied.set(&mut message);
                        let _ = Self::send(&mut codec, stream, message);
                    }

                    return Err(Error::ErrorCode(Code::PermissionDenied))
                }
            };

            // 但是要注意，握手消息是没有加密的，不能传递敏感数据
            let mut attr = message.clone();

            attr.remove(CHAN);
            attr.insert(ADDR, addr.to_string());

            let wire = connector.connect(attr, None, None)?;

            // 这里可以修改 Wire 的属性
            hook.finish(slot_id, &mut message, &wire);

            Code::Ok.set(&mut message);

            // 握手消息发回
            Self::send(&mut codec, stream, message)?;

            let crypto = Crypto::new(&method, secret.as_bytes());

            return Ok((wire, codec, Some(crypto)))
        }

        #[cfg(debug_assertions)]
        {
            Code::PermissionDenied.set(&mut message);
            let _ = Self::send(&mut codec, stream, message);
        }

        Err(Error::ErrorCode(Code::PermissionDenied))
    }

    fn send(codec: &mut impl Codec, stream: &mut TcpStream, message: Message) -> Result<()> {
        let bytes = codec.encode(&None, message)?;
        stream.write_all(&bytes)?;

        Ok(())
    }
}

impl<C: Codec> Clone for Node<C> {
    fn clone(&self) -> Self {
        Self {
            queues: self.queues.clone(),
            run: self.run.clone()
        }
    }
}

impl<C: Codec> Drop for Node<C> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.run) <= 2 + self.queues.len() {
            self.stop()
        }
    }
}
