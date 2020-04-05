use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};

use std::thread;
use std::net::ToSocketAddrs;

use crate::net::{NetWork, Packet, CryptoOptions};
use crate::Stream;
use crate::dict::*;
use crate::nson::{msg, Message};
use crate::error::Result;

use queen_io::tcp::TcpStream;
use queen_io::queue::mpsc::Queue;

#[derive(Clone)]
pub struct Port {
    queue: Queue<Packet>,
    run: Arc<AtomicBool>
}

impl Port {
    pub fn new() -> Result<Port> {
        let port = Port {
            queue: Queue::new()?,
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
        }).unwrap();

        Ok(port)
    }

    pub fn stop(&mut self) {
        self.run.store(false, Ordering::Relaxed);
    }

    pub fn is_run(&self) -> bool {
        self.run.load(Ordering::Relaxed)
    }

    pub fn connect<A: ToSocketAddrs>(
        &self,
        addr: A,
        crypto_options: Option<CryptoOptions>,
        capacity: Option<usize>
    ) -> Result<Stream<Message>> {
        let net_stream = TcpStream::connect(addr)?;

        let attr = msg!{
            ADDR: net_stream.peer_addr()?.to_string()
        };

        let (stream1, stream2) = Stream::pipe(capacity.unwrap_or(64), attr).unwrap();

        self.queue.push(Packet::NewConn {
            net_stream,
            stream: stream1,
            options: crypto_options
        });

        Ok(stream2)
    }
}
