use std::io;
use std::io::ErrorKind::ConnectionAborted;
use std::collections::HashMap;
use std::sync::mpsc::Sender;

use queen_io::*;
use queen_io::tcp::TcpStream;
use queen_io::message_queue::MessagesQueue;

use protocol::{Message, OpCode};
use service::connection::Connection;
use util::both_queue::BothQueue;

const SOCK: Token = Token(1);
const MSG: Token = Token(2);
const TASK: Token = Token(3);
//const TIME: Token = Token(4);

pub struct Backend {
    poll: Poll,
    events: Events,
    //addr: String,
    conn: Connection,
    tasks: HashMap<u32, Sender<Message>>,
    msg_queue: BothQueue<(usize, Message)>,
    task_queue: MessagesQueue<(Message, Option<Sender<Message>>)>
}

impl Backend {
    pub fn new(addr: &str) -> io::Result<Backend> {
        let socket = TcpStream::connect(addr)?;
        socket.set_nodelay(true)?;

        let conn = Connection::new(socket, SOCK)?;

        let backend = Backend {
            poll: Poll::new()?,
            events: Events::with_capacity(128),
            //addr: addr.to_owned(),
            conn: conn,
            tasks: HashMap::new(),
            msg_queue: BothQueue::new(256)?,
            task_queue: MessagesQueue::with_capacity(256)?
        };

        backend.conn.register_insterest(&backend.poll);

        backend.poll.register(&backend.msg_queue.tx, MSG, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;
        backend.poll.register(&backend.task_queue, TASK, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;

        Ok(backend)
    }

    pub fn msg_queue(&self) -> &MessagesQueue<(usize, Message)> {
        &self.msg_queue.rx
    }

    pub fn task_queue(&self) -> &MessagesQueue<(Message, Option<Sender<Message>>)> {
        &self.task_queue
    }

    fn connect_handle(&mut self, event: Event) -> io::Result<()> {
        if event.readiness().is_hup() || event.readiness().is_error() {
            return Err(io::Error::new(ConnectionAborted, "").into())
        }

        if event.readiness().is_readable() {
            self.conn.reader(&self.poll, &self.msg_queue)?;
        }

        if event.readiness().is_writable() {
            self.conn.writer(&self.poll)?;
        }

        Ok(())
    }

    fn msg_handle(&mut self) -> io::Result<()> {
        loop {
            let msg = match self.msg_queue.tx.try_pop()? {
                Some(msg) => msg,
                None => break
            };

            let message_id = msg.1.message_id;

            if let Some(send) = self.tasks.remove(&message_id) {
                send.send(msg.1).unwrap();
            } else {
                self.msg_queue.rx.push(msg)?;
            }
        }

        self.poll.register(&self.msg_queue.tx, MSG, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;

        Ok(())
    }

    fn task_handle(&mut self) -> io::Result<()> {
        loop {
            let task = match self.task_queue.try_pop()? {
                Some(task) => task,
                None => break
            };

            if task.0.opcode != OpCode::RESPONSE && task.0.opcode != OpCode::PUBACK {
                if let Some(send) = task.1 {
                    let message_id = task.0.message_id;
                    self.tasks.insert(message_id, send);
                }
            }

            self.conn.set_message(&self.poll, task.0)?;
        }

        Ok(())
    }

    pub fn run(&mut self) -> io::Result<()> {
        loop {
            let size = self.poll.poll(&mut self.events, None)?;

            for i in 0..size {
                let event = self.events.get(i).unwrap();

                match event.token() {
                    SOCK => {
                        self.connect_handle(event)?;
                    }
                    MSG => {
                        self.msg_handle()?;
                    }
                    TASK => {
                        self.task_handle()?;
                    }
                    _ => ()
                }
            }
        }
    }
}
