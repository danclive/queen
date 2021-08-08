use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};
use std::thread;
use std::net::ToSocketAddrs;
use std::time::Duration;
use std::io::Write;

use queen_io::net::tcp::TcpStream;
use queen_io::queue::mpsc::Queue;

use nson::Message;

use crate::net::{NetWork, Packet, CryptoOptions, Codec, KeepAlive};
use crate::Wire;
use crate::crypto::Crypto;
use crate::dict::*;
use crate::error::{Result, Error, Code};
use crate::util::message::read_block;

#[derive(Clone)]
pub struct Port<C: Codec> {
    inner: Arc<PortInner<C>>
}

struct PortInner<C: Codec> {
    queue: Queue<Packet<C>>,
    run: AtomicBool,
    keep_alive: KeepAlive
}

impl<C: Codec> Port<C> {
    pub fn new(keep_alive: KeepAlive) -> Result<Self> {
        let port = Port {
            inner: Arc::new(PortInner {
                queue: Queue::new()?,
                run: AtomicBool::new(true),
                keep_alive
            })
        };

        let mut net_work = NetWork::<C>::new(
            port.inner.queue.clone(),
            port.inner.keep_alive.clone()
        )?;

        let inner = port.inner.clone();

        thread::Builder::new().name("port_net".to_string()).spawn(move || {
            let ret = net_work.run();
            if ret.is_err() {
                log::error!("net thread exit: {:?}", ret);
            } else {
                log::debug!("net thread exit");
            }

            inner.run.store(false, Ordering::Relaxed);
        }).unwrap();

        Ok(port)
    }

    pub fn stop(&self) {
        self.inner.run.store(false, Ordering::Relaxed);
        self.inner.queue.push(Packet::Close);
    }

    pub fn running(&self) -> bool {
        self.inner.run.load(Ordering::Relaxed)
    }

    pub fn connect<A: ToSocketAddrs>(
        &self,
        addr: A,
        mut attr: Message,
        crypto_options: Option<CryptoOptions>,
        capacity: Option<usize>
    ) -> Result<Wire<Message>> {

        if !self.running() {
            return Err(Error::ConnectionAborted("port is not run!".to_string()))
        }

        let mut stream = TcpStream::connect(addr)?;

        stream.set_nodelay(true)?;
        // 握手开始
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;

        attr.insert(CHAN, HAND);
        attr.insert(ADDR, stream.peer_addr()?.to_string());
        attr.insert(SECURE, false);

        let crypto = crypto_options.map(|options| {
            attr.insert(SECURE, true);
            attr.insert(METHOD, options.method.as_str());

            Crypto::new(&options.method, options.secret.as_bytes())
        });

        let mut codec = C::new();

        let bytes = codec.encode(&None, attr)?;

        stream.write_all(&bytes)?;

        // 握手时的消息，不能超过 1024 字节
        let bytes = read_block(&mut stream, Some(1024))?;
        let mut message = codec.decode(&None, bytes)?;

        if let Some(code) = Code::get(&message) {
            if code == Code::Ok {
                message.remove(CHAN);
                message.remove(CODE);

                stream.set_nonblocking(true)?;
                stream.set_read_timeout(None)?;
                stream.set_write_timeout(None)?;
                // 握手结束

                // 握手消息可以被对端修改，这里将修改后的出入，以便能够携带一些自定义数据
                let (wire1, wire2) = Wire::pipe(capacity.unwrap_or(64), message)?;

                self.inner.queue.push(Packet::NewConn {
                    wire: wire1,
                    stream,
                    codec,
                    crypto
                });

                return Ok(wire2)
            } else {
                return Err(Error::ErrorCode(code))
            }
        }

        Err(Error::InvalidData(format!("{}", message)))
    }
}

impl<C: Codec> Drop for Port<C> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) <= 2 {
            self.stop()
        }
    }
}
