use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};
use std::thread;
use std::net::ToSocketAddrs;
use std::time::Duration;
use std::io::Write;

use queen_io::tcp::TcpStream;
use queen_io::queue::mpsc::Queue;

use crate::net::{NetWork, Packet, CryptoOptions, Codec};
use crate::net::tcp_ext::TcpExt;
use crate::Wire;
use crate::crypto::Crypto;
use crate::dict::*;
use crate::nson::{msg, Message};
use crate::error::{Result, Error};
use crate::util::message::read_block;

#[derive(Clone)]
pub struct Port<C: Codec> {
    queue: Queue<Packet<C>>,
    pub tcp_keep_alive: bool,
    pub tcp_keep_idle: u32,
    pub tcp_keep_intvl: u32,
    pub tcp_keep_cnt: u32,
    run: Arc<AtomicBool>
}

impl<C: Codec> Port<C> {
    pub fn new() -> Result<Self> {
        let port = Port {
            queue: Queue::new()?,
            tcp_keep_alive: true,
            tcp_keep_idle: 30,
            tcp_keep_intvl: 5,
            tcp_keep_cnt: 3,
            run: Arc::new(AtomicBool::new(true))
        };

        let mut net_work = NetWork::<C>::new(port.queue.clone(), port.run.clone())?;

        thread::Builder::new().name("port_net".to_string()).spawn(move || {
            let ret = net_work.run();
            if ret.is_err() {
                log::error!("net thread exit: {:?}", ret);
            } else {
                log::debug!("net thread exit");
            }

            net_work.run.store(false, Ordering::Relaxed);
        }).unwrap();

        Ok(port)
    }

    pub fn stop(&self) {
        self.run.store(false, Ordering::Relaxed);
    }

    pub fn is_run(&self) -> bool {
        self.run.load(Ordering::Relaxed)
    }

    pub fn connect<A: ToSocketAddrs>(
        &self,
        addr: A,
        attr: Message,
        crypto_options: Option<CryptoOptions>,
        capacity: Option<usize>
    ) -> Result<Wire<Message>> {
        let mut stream = TcpStream::connect(addr)?;

        // 握手开始
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;

        let mut hand = msg!{
            CHAN: HAND
        };

        let mut attr2 = msg!{
            ADDR: stream.peer_addr()?.to_string(),
            SECURE: false
        };

        let crypto = crypto_options.map(|options| {
            hand.insert(METHOD, options.method.as_str());
            hand.insert(ACCESS, &options.access);

            attr2.insert(SECURE, true);
            attr2.insert(ACCESS, &options.access);
            attr2.insert(SECURE, &options.secret);

            Crypto::new(&options.method, options.secret.as_bytes())
        });

        let mut codec = C::new();

        let bytes = codec.encode(&None, hand)?;

        stream.write_all(&bytes)?;

        // 握手时的消息，不能超过 1024 字节
        let bytes = read_block(&mut stream, Some(1024))?;
        let message = codec.decode(&None, bytes)?;

        if message.get_i32(OK) == Ok(0) {
            stream.set_nonblocking(true)?;
            stream.set_read_timeout(None)?;
            stream.set_write_timeout(None)?;
            // 握手结束

            stream.set_keep_alive(self.tcp_keep_alive)?;
            stream.set_keep_idle(self.tcp_keep_idle as i32)?;
            stream.set_keep_intvl(self.tcp_keep_intvl as i32)?;
            stream.set_keep_cnt(self.tcp_keep_cnt as i32)?;

            attr2.extend(attr);

            let (wire1, wire2) = Wire::pipe(capacity.unwrap_or(64), attr2)?;

            self.queue.push(Packet::NewConn {
                wire: wire1,
                stream,
                codec,
                crypto
            });

            return Ok(wire2)
        }

        Err(Error::InvalidInput(format!("{}", message)))
    }
}
