use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};

use std::thread;
use std::net::ToSocketAddrs;

use crate::net::{NetWork, Packet, CryptoOptions};
use crate::net::tcp_ext::TcpExt;
use crate::Stream;
use crate::dict::*;
use crate::nson::{msg, Message};
use crate::error::Result;

use queen_io::tcp::TcpStream;
use queen_io::queue::mpsc::Queue;

#[derive(Clone)]
pub struct Port {
    queue: Queue<Packet>,
    pub tcp_keep_alive: bool,
    pub tcp_keep_idle: u32,
    pub tcp_keep_intvl: u32,
    pub tcp_keep_cnt: u32,
    run: Arc<AtomicBool>
}

impl Port {
    pub fn new() -> Result<Port> {
        let port = Port {
            queue: Queue::new()?,
            tcp_keep_alive: true,
            tcp_keep_idle: 30,
            tcp_keep_intvl: 5,
            tcp_keep_cnt: 3,
            run: Arc::new(AtomicBool::new(true))
        };

        let mut net_work = NetWork::new(port.queue.clone(), port.run.clone())?;

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
    ) -> Result<Stream<Message>> {
        let socket = TcpStream::connect(addr)?;

        socket.set_keep_alive(self.tcp_keep_alive)?;
        socket.set_keep_idle(self.tcp_keep_idle as i32)?;
        socket.set_keep_intvl(self.tcp_keep_intvl as i32)?;
        socket.set_keep_cnt(self.tcp_keep_cnt as i32)?;

        let mut attr2 = msg!{
            ADDR: socket.peer_addr()?.to_string(),
            SECURE: crypto_options.is_some()
        };

        attr2.extend(attr);

        let (stream1, stream2) = Stream::pipe(capacity.unwrap_or(64), attr2).unwrap();

        self.queue.push(Packet::NewConn {
            net_stream: socket,
            stream: stream1,
            options: crypto_options
        });

        Ok(stream2)
    }
}
