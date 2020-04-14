use std::io::ErrorKind::WouldBlock;
use std::os::unix::io::AsRawFd;
use std::thread;
use std::time::Duration;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};

use queen_io::{
    epoll::{Epoll, Events, Token, Ready, EpollOpt},
    queue::mpsc::Queue,
    tcp::TcpListener
};

use rand::{SeedableRng, seq::SliceRandom, rngs::SmallRng};

use nson::msg;

use crate::Queen;
use crate::net::{NetWork, Packet, AccessFn};
use crate::net::tcp_ext::TcpExt;
use crate::dict::*;
use crate::error::Result;

pub struct Node {
    queen: Queen,
    epoll: Epoll,
    events: Events,
    queues: Vec<Queue<Packet>>,
    listens: Vec<TcpListener>,
    rand: SmallRng,
    access_fn: Option<AccessFn>,
    pub tcp_keep_alive: bool,
    pub tcp_keep_idle: u32,
    pub tcp_keep_intvl: u32,
    pub tcp_keep_cnt: u32,
    pub run: Arc<AtomicBool>
}

impl Node {
    pub fn new(
        queen: Queen,
        works: usize,
        addrs: Vec<SocketAddr>
    ) -> Result<Node> {
        let run = Arc::new(AtomicBool::new(true));

        let mut listens = Vec::new();

        for addr in addrs {
            listens.push(TcpListener::bind(addr)?);
        }

        let mut queues = Vec::new();

        for _ in 0..works {
            let queue: Queue<Packet> = Queue::new()?;

            queues.push(queue.clone());

            let mut net_work = NetWork::new(queue, run.clone())?;

            thread::Builder::new().name("node_net".to_string()).spawn(move || {
                let ret = net_work.run();
                if ret.is_err() {
                    log::error!("net thread exit: {:?}", ret);
                } else {
                    log::debug!("net thread exit");
                }

                net_work.run.store(false, Ordering::Relaxed);
            }).unwrap();
        }

        Ok(Node {
            queen,
            epoll: Epoll::new()?,
            events: Events::with_capacity(16),
            queues,
            listens,
            rand: SmallRng::from_entropy(),
            access_fn: None,
            tcp_keep_alive: true,
            tcp_keep_idle: 30,
            tcp_keep_intvl: 5,
            tcp_keep_cnt: 3,
            run
        })
    }

    pub fn set_access_fn<F>(&mut self, f: F)
        where F: Fn(String) -> Option<String> + Send + Sync + 'static
    {
        self.access_fn = Some(Arc::new(Box::new(f)))
    }

    pub fn stop(&self) {
        self.run.store(false, Ordering::Relaxed);
    }

    pub fn is_run(&self) -> bool {
        self.run.load(Ordering::Relaxed)
    }

    pub fn run(&mut self) -> Result<()> {
        for (id, listen) in self.listens.iter().enumerate() {
            self.epoll.add(&listen.as_raw_fd(), Token(id), Ready::readable(), EpollOpt::edge())?;
        }

        while self.run.load(Ordering::Relaxed) && self.queen.is_run() {
            let size = self.epoll.wait(&mut self.events, Some(Duration::from_secs(10)))?;

            for i in 0..size {
                let event = self.events.get(i).unwrap();
                let token = event.token();

                if let Some(listen) = self.listens.get(token.0) {
                    loop {
                        let (socket, addr) = match listen.accept() {
                            Ok(socket) => socket,
                            Err(err) => {
                                if let WouldBlock = err.kind() {
                                    break;
                                } else {
                                    return Err(err.into())
                                }
                            }
                        };

                        let attr = msg!{
                            ADDR: addr.to_string(),
                            SECURE: self.access_fn.is_some()
                        };

                        socket.set_keep_alive(self.tcp_keep_alive)?;
                        socket.set_keep_idle(self.tcp_keep_idle as i32)?;
                        socket.set_keep_intvl(self.tcp_keep_intvl as i32)?;
                        socket.set_keep_cnt(self.tcp_keep_cnt as i32)?;

                        match self.queen.connect(attr, None, None) {
                            Ok(stream) => {
                                if let Some(queue) = self.queues.choose(&mut self.rand) {
                                    queue.push(Packet::NewServ {
                                        stream,
                                        net_stream: socket,
                                        access_fn: self.access_fn.clone()
                                    })
                                }
                            },
                            Err(err) => {
                                log::error!("queen.connect: {:?}", err);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.run.store(false, Ordering::Relaxed);
    }
}
