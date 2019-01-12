use std::sync::atomic::AtomicIsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::thread;
use std::io;
use std::fmt;

use bsonrs::doc;
use queen_io::plus::block_queue::BlockQueue;
use queen_io::plus::mpms_queue::Queue;

use crate::control::Control;
use crate::Message;

#[derive(Clone)]
pub struct Queen {
    inner: Arc<InnerQueen>
}

struct InnerQueen {
    queue: BlockQueue<(String, Message)>,
    control_i: Queue<(Message)>,
    handles: Mutex<HashMap<String, Vec<(i32, Arc<Box<dyn (Fn(Context)) + Send + Sync + 'static>>)>>>,
    next_id: AtomicIsize
}

pub struct Context<'a> {
    pub queen: &'a Queen,
    pub id: i32,
    pub event: String,
    pub message: Message
}

impl Queen {
    pub fn new() -> io::Result<Queen> {
        let control = Control::new()?;

        let queue_i = control.queue_i.clone();
        let queue_o = control.queue_o.clone();

        thread::Builder::new().name("control".into()).spawn(move || {
            let mut control = control;
            let _ = control.run();
        }).unwrap();

        let queue = Queen {
            inner: Arc::new(InnerQueen {
                queue: queue_o,
                control_i: queue_i,
                handles: Mutex::new(HashMap::new()),
                next_id: AtomicIsize::new(0)
            })
        };

        Ok(queue)
    }

    pub fn on(&self, event: &str, handle: impl (Fn(Context)) + Send + Sync + 'static) -> i32 {
        let mut handles = self.inner.handles.lock().unwrap();
        let id = self.inner.next_id.fetch_add(1, SeqCst) as i32;

        let vector = handles.entry(event.to_owned()).or_insert(vec![]);
        vector.push((id, Arc::new(Box::new(handle))));

        if event.starts_with("pub:") || event.starts_with("sys:") {
            self.inner.control_i.push(doc!{"event": "sys:attach", "v": event}).unwrap();
        }

        id
    }

    pub fn off(&self, id: i32) -> Option<i32> {
        let mut handles = self.inner.handles.lock().unwrap();
        for (event, vector) in handles.iter_mut() {
            if let Some(position) = vector.iter().position(|(x, _)| x == &id) {
                vector.remove(position);

                if event.starts_with("pub:") || event.starts_with("sys:") {
                    self.inner.control_i.push(doc!{"event": "sys:detach", "v": event}).unwrap();
                }

                return Some(id)
            }
        }

        None
    }

    pub fn emit(&self, event: &str, message: Message) {
        if event.starts_with("pub:") || event.starts_with("sys:") {
            let mut message = message;
            message.insert("event", event);
            self.inner.control_i.push(message).unwrap();
        } else {
            self.inner.queue.push((event.to_string(), message.clone()));
        }
    }

    pub fn run(&self, worker_size: usize, block: bool) {
        let mut threads = Vec::new();

        for _ in 0..worker_size {
            let that = self.clone();
            threads.push(thread::Builder::new().name("worker".into()).spawn(move || {
                loop {
                    let (event, message) = that.inner.queue.pop();

                    let mut handles2 = Vec::new();

                    {
                        let handles = that.inner.handles.lock().unwrap();
                        if let Some(vector) = handles.get(&event) {
                            for (id, handle) in vector {
                                handles2.push((*id, handle.clone()));
                            }
                        } else {
                            continue;
                        }
                    }

                    for (id, handle) in handles2 {
                        let context = Context {
                            queen: &that,
                            id,
                            event: event.clone(),
                            message: message.clone()
                        };
                        handle(context);
                    }
                }
            }).unwrap());
        }

        if block {
            for thread in threads {
                thread.join().unwrap();
            }
        }
    }
}

impl<'a> fmt::Debug for Context<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Context {{ id: {}, event: {}, message: {:?} }}", self.id, self.event, self.message)
    }
}

impl<'a> fmt::Display for Context<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Context {{ id: {}, event: {}, message: {} }}", self.id, self.event, self.message)
    }
}
