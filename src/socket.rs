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
use crate::error::{Result, Error, RecvError};

pub use hook::Hook;
pub use switch::Switch;
pub use slot::{Slot, SlotModify};

mod hook;
mod switch;
mod slot;

#[derive(Clone)]
pub struct Socket {
    queue: Queue<Packet>,
    run: Arc<AtomicBool>
}

impl Socket {
    pub fn new<H: Hook>(
        id: MessageId,
        hook: H
    ) -> Result<Self> {
        let queue = Queue::new()?;
        let run = Arc::new(AtomicBool::new(true));

        let socket = Socket {
            queue: queue.clone(),
            run: run.clone()
        };

        let mut inner = QueenInner::new(
            id,
            queue,
            hook,
            run
        )?;

        thread::Builder::new().name("socket".to_string()).spawn(move || {
            let ret = inner.run();
            if ret.is_err() {
                log::error!("socket thread exit: {:?}", ret);
            } else {
                log::trace!("socket thread exit");
            }

            inner.run.store(false, Ordering::Relaxed);
            inner.hook.stop(&inner.switch);
        }).unwrap();

        Ok(socket)
    }

    pub fn stop(&self) {
        self.run.store(false, Ordering::Relaxed);
    }

    pub fn is_run(&self) -> bool {
        self.run.load(Ordering::Relaxed)
    }

    pub fn connect(
        &self,
        attr: Message,
        capacity: Option<usize>,
        timeout: Option<Duration>
    ) -> Result<Wire<Message>> {
        let (wire1, wire2) = Wire::pipe(capacity.unwrap_or(64), attr)?;

        let packet = Packet::NewSlot(wire1);

        self.queue.push(packet);

        let ret = wire2.wait(Some(timeout.unwrap_or(Duration::from_secs(10))));

        if let Err(err) = ret {
            return Err(Error::ConnectionRefused(err.to_string()))
        }

        Ok(wire2)
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        if Arc::strong_count(&self.run) == 1 {
            self.run.store(false, Ordering::Relaxed);
        }
    }
}

struct QueenInner<H> {
    epoll: Epoll,
    events: Events,
    queue: Queue<Packet>,
    hook: H,
    switch: Switch,
    run: Arc<AtomicBool>
}

enum Packet {
    NewSlot(Wire<Message>)
}

impl<H: Hook> QueenInner<H> {
    const QUEUE_TOKEN: Token = Token(usize::max_value());

    fn new(
        id: MessageId,
        queue: Queue<Packet>,
        hook: H,
        run: Arc<AtomicBool>
    ) -> Result<QueenInner<H>> {
        Ok(QueenInner {
            epoll: Epoll::new()?,
            events: Events::with_capacity(1024),
            queue,
            hook,
            switch: Switch::new(id),
            run
        })
    }

    fn run(&mut self) -> Result<()> {
        self.epoll.add(&self.queue, Self::QUEUE_TOKEN, Ready::readable(), EpollOpt::level())?;

        while self.run.load(Ordering::Relaxed) {
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

                if event.token() == Self::QUEUE_TOKEN {
                    self.dispatch_queue()?;
                } else {
                    self.dispatch_conn(event.token().0)?;
                }
            }
        }

        Ok(())
    }

    fn dispatch_queue(&mut self) -> Result<()> {
        if let Some(packet) = self.queue.pop() {
            match packet {
                Packet::NewSlot(wire) => {
                    self.switch.add_slot(&self.epoll, &self.hook, wire)?;
                }
            }
        }

        Ok(())
    }

    fn dispatch_conn(&mut self, token: usize) -> Result<()> {
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

        Ok(())
    }
}

impl<H> Drop for QueenInner<H> {
    fn drop(&mut self) {
        self.run.store(false, Ordering::Relaxed);
    }
}
