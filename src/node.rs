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

use nson::{msg, Message};

use crate::Socket;
use crate::Wire;
use crate::net::{NetWork, Packet, Codec};
use crate::net::tcp_ext::TcpExt;
use crate::crypto::{Crypto, Method};
use crate::dict::*;
use crate::util::message::read_block;
use crate::error::{Result, Error, Code};

pub use hook::{Hook, NonHook};

mod hook;

pub struct Node<C: Codec> {
    queues: Arc<Vec<Queue<Packet<C>>>>,
    run: Arc<AtomicBool>
}

impl<C: Codec> Node<C> {
    pub fn new<H: Hook + Send + 'static>(
        socket: Socket,
        works: usize,
        addrs: Vec<SocketAddr>,
        hook: H
    ) -> Result<Self> {
        let mut queues = Vec::new();

        for _ in 0..works {
            let queue: Queue<Packet<C>> = Queue::new()?;
            queues.push(queue.clone());
        }

        let node = Self {
            queues: Arc::new(queues),
            run: Arc::new(AtomicBool::new(true))
        };

        let mut inner: Inner<C, H> = Inner::new(node.clone(), socket, works, addrs, hook)?;

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
    pub fn is_run(&self) -> bool {
        self.run.load(Ordering::Relaxed)
    }
}

struct Inner<C: Codec, H: Hook> {
    node: Node<C>,
    socket: Socket,
    epoll: Epoll,
    events: Events,
    listens: Vec<TcpListener>,
    rand: SmallRng,
    hook: H,
    tcp_keep_alive: bool,
    tcp_keep_idle: u32,
    tcp_keep_intvl: u32,
    tcp_keep_cnt: u32
}

impl<C: Codec, H: Hook> Inner<C, H> {
    fn new(
        node: Node<C>,
        socket: Socket,
        _works: usize,
        addrs: Vec<SocketAddr>,
        hook: H
    ) -> Result<Self> {
        let mut listens = Vec::new();

        for addr in addrs {
            listens.push(TcpListener::bind(addr)?);
        }

        for queue in node.queues.iter() {
            let mut net_work = NetWork::<C>::new(queue.clone())?;

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
            socket,
            epoll: Epoll::new()?,
            events: Events::with_capacity(16),
            listens,
            hook,
            rand: SmallRng::from_entropy(),
            tcp_keep_alive: true,
            tcp_keep_idle: 30,
            tcp_keep_intvl: 5,
            tcp_keep_cnt: 3
        })
    }

    #[inline]
    fn is_run(&self) -> bool {
        self.node.run.load(Ordering::Relaxed)
    }

    pub fn run(&mut self) -> Result<()> {
        for (id, listen) in self.listens.iter().enumerate() {
            self.epoll.add(&listen.as_raw_fd(), Token(id), Ready::readable(), EpollOpt::edge())?;
        }

        while self.is_run() && self.socket.is_run() {
            let size = match self.epoll.wait(&mut self.events, Some(Duration::from_secs(10))) {
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

                        let (wire, codec, crypto) = match Self::hand(&self.hook, &self.socket, &mut stream, &addr) {
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

                        // 开启keepalive属性
                        stream.set_keep_alive(self.tcp_keep_alive)?;
                        // 如该连接在30秒内没有任何数据往来,则进行探测
                        stream.set_keep_idle(self.tcp_keep_idle as i32)?;
                        // 探测时发包的时间间隔为5秒
                        stream.set_keep_intvl(self.tcp_keep_intvl as i32)?;
                        // 探测尝试的次数.如果第1次探测包就收到响应了,则后2次的不再发
                        stream.set_keep_cnt(self.tcp_keep_cnt as i32)?;

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
        socket: &Socket,
        stream: &mut TcpStream,
        addr: &SocketAddr
    ) -> Result<(Wire<Message>, C, Option<Crypto>)> {
        let mut codec = C::new();

        // 握手时的消息，不能超过 1024 字节
        let bytes = read_block(stream, Some(1024))?;
        let mut message = codec.decode(&None, bytes)?;

        let chan = match message.get_str(CHAN) {
            Ok(chan) => chan,
            Err(_) => {
                return Err(Error::InvalidData("message.get_str(CHAN)".to_string()))
            }
        };

        if chan != HAND {
            return Err(Error::InvalidData("chan != HAND".to_string()))
        }

        // 握手消息是可以修改的，修改后的消息会发回客户端，因此可以携带自定义数据
        // 但是对于一些握手必备的属性，请谨慎修改，比如加密方式（METHOD）
        if !hook.start(&mut message) {
            return Err(Error::PermissionDenied("hook.hand".to_string()));
        }

        if !hook.enable_secure() {
            // 没有开启加密

            // 这里会将原始的握手消息传入。
            // 但是要注意，握手消息是没有加密的，不能传递敏感数据
            let mut origin = message.clone();
            // 去除一些冗余信息
            origin.remove(CHAN);
            origin.remove(ADDR);
            origin.remove(SECURE);

            let attr = msg!{
                ADDR: addr.to_string(),
                SECURE: false,
                ORIGIN: origin
            };

            let wire = socket.connect(attr, None, Some(Duration::from_secs(10)))?;

            // 这里可以修改 Wire 的属性
            hook.finish(&mut message, &wire);

            Code::Ok.set(&mut message);

            let bytes = codec.encode(&None, message)?;

            stream.write_all(&bytes)?;

            return Ok((wire, codec, None))
        }

        if let Ok(method) = message.get_str(METHOD) {
            let method = if let Ok(method) = Method::from_str(method) {
                method
            } else {
                return Err(Error::InvalidData("Method::from_str(hand)".to_string()))
            };

            // 握手消息是可以修改的，修改后的消息会发回客户端，因此可以携带自定义数据
            // 但是对于一些握手必备的属性，请谨慎修改
            // 这里需要根据传递的自定义数据，返回一个加密密钥
            let secret = hook.access(&mut message).ok_or_else(|| Error::PermissionDenied("hook.access".to_string()))?;

            // 这里会将原始的握手消息传入。
            // 但是要注意，握手消息是没有加密的，不能传递敏感数据
            let mut origin = message.clone();
            // 去除一些冗余信息
            origin.remove(CHAN);
            origin.remove(ADDR);
            origin.remove(SECURE);

            let attr = msg!{
                ADDR: addr.to_string(),
                SECURE: true,
                ORIGIN: origin
            };

            let wire = socket.connect(attr, None, Some(Duration::from_secs(10)))?;

            // 这里可以修改 Wire 的属性
            hook.finish(&mut message, &wire);

            Code::Ok.set(&mut message);

            // 握手消息发回
            let bytes = codec.encode(&None, message)?;

            stream.write_all(&bytes)?;

            let crypto = Crypto::new(&method, secret.as_bytes());

            return Ok((wire, codec, Some(crypto)))
        }

        Err(Error::InvalidData(format!("{}", message)))
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
        if Arc::strong_count(&self.run) - self.queues.len() <= 2 {
            self.stop()
        }
    }
}
