use std::time::Duration;
use std::thread;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};

use std::io::ErrorKind::Interrupted;

use queen_io::{
    epoll::{Epoll, Events, Token, Ready, EpollOpt},
    queue::mpsc::Queue
};

use nson::{
    Message,
    message_id::MessageId
};

use crate::Wire;
use crate::error::{Result, Error, RecvError, Code};

pub use hook::{Hook, NonHook};
pub use switch::Switch;
pub use slot::Slot;

mod hook;
mod switch;
mod slot;

#[derive(Clone)]
pub struct Socket {
    inner: Arc<Inner>
}

struct Inner {
    id: MessageId,
    queue: Queue<Packet>,
    run: AtomicBool
}

impl Socket {
    pub fn new(id: MessageId, hook: impl Hook) -> Result<Self> {
        let queue = Queue::new()?;

        let socket = Socket {
            inner: Arc::new(Inner {
                id,
                queue: queue.clone(),
                run: AtomicBool::new(true)
            })
        };

        let mut main_loop = MainLoop::new(
            id,
            queue,
            hook,
        )?;

        let socket2 = socket.clone();
        thread::Builder::new().name("socket".to_string()).spawn(move || {
            let ret = main_loop.run();
            if ret.is_err() {
                log::error!("socket loop exit: {:?}", ret);
            } else {
                log::trace!("socket loop exit");
            }

            socket2.inner.run.store(false, Ordering::Relaxed);

            main_loop.hook.stop(&main_loop.switch);
        }).unwrap();

        Ok(socket)
    }

    pub fn id(&self) -> &MessageId {
        &self.inner.id
    }

    pub fn stop(&self) {
        self.inner.run.store(false, Ordering::Relaxed);
        self.inner.queue.push(Packet::Close);
    }

    pub fn running(&self) -> bool {
        self.inner.run.load(Ordering::Relaxed)
    }

    pub fn connect(
        &self,
        slot_id: MessageId,
        root: bool,
        attr: Message,
        capacity: Option<usize>,
        timeout: Option<Duration>
    ) -> Result<Wire<Message>> {
        let (wire1, wire2) = Wire::pipe(capacity.unwrap_or(64), attr)?;

        let packet = Packet::NewSlot(slot_id, root, wire1);

        self.inner.queue.push(packet);

        let ret = wire2.wait(Some(timeout.unwrap_or_else(|| Duration::from_secs(10))))?;

        if let Some(code) = Code::get(&ret) {
            if code != Code::Ok {
                return Err(Error::ErrorCode(code))
            }
        } else {
            unreachable!()
        }

        Ok(wire2)
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) <= 2 {
            self.stop()
        }
    }
}

struct MainLoop<H> {
    epoll: Epoll,
    events: Events,
    queue: Queue<Packet>,
    hook: H,
    switch: Switch
}

enum Packet {
    NewSlot(MessageId, bool, Wire<Message>),
    Close
}

impl<H: Hook> MainLoop<H> {
    const QUEUE_TOKEN: Token = Token(usize::max_value());

    fn new(socket_id: MessageId, queue: Queue<Packet>, hook: H) -> Result<MainLoop<H>> {
        Ok(MainLoop {
            epoll: Epoll::new()?,
            events: Events::with_capacity(1024),
            queue,
            hook,
            switch: Switch::new(socket_id)
        })
    }

    fn run(&mut self) -> Result<()> {
        self.epoll.add(&self.queue, Self::QUEUE_TOKEN, Ready::readable(), EpollOpt::level())?;

        loop {
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

                match token {
                    Self::QUEUE_TOKEN => {
                        if let Some(packet) = self.queue.pop() {
                            match packet {
                                Packet::NewSlot(id, root, wire) => {
                                    self.switch.add_slot(&self.epoll, &self.hook, id, root, wire)?;
                                }
                                Packet::Close => {
                                    return Ok(())
                                }
                            }
                        }
                    }
                    _ => {
                        let token = token.0;
                        if let Some(slot) = self.switch.slots.get(token) {
                            match slot.wire.recv() {
                                Ok(message) => {
                                    self.switch.recv_message(&self.epoll, &self.hook, token, message)?;
                                }
                                Err(err) => {
                                    if !matches!(err, RecvError::Empty) {
                                        self.switch.del_slot(&self.epoll, &self.hook, token)?;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
