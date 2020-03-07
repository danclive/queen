use std::sync::{Arc, RwLock, Mutex};
use std::collections::{HashMap, BinaryHeap};
use std::thread;
use std::io;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::cmp::Ordering;

use nson::{Value, Message};
use queen_io::plus::block_queue::BlockQueue;

#[derive(Clone)]
pub struct EventBus {
    inner: Arc<InnerEventBus>
}

struct InnerEventBus {
    queue: BlockQueue<(String, Message)>,
    handles: RwLock<Handles>,
    timer: Timer
}

struct Handles {
    next_id: usize,
    callbacks: HashMap<String, Vec<(usize, Arc<HandleFn>)>>,
    index: HashMap<usize, String>
}

type HandleFn = dyn Fn(Context) + Send + Sync + 'static;

pub struct Context<'a> {
    pub queen: &'a EventBus,
    pub id: usize,
    pub event: String,
    pub message: Message
}

impl EventBus {
    pub fn new() -> io::Result<EventBus> {
        let event_bus = EventBus {
            inner: Arc::new(InnerEventBus {
                queue: BlockQueue::with_capacity(1024),
                handles: RwLock::new(Handles {
                    next_id: 0,
                    callbacks: HashMap::new(),
                    index: HashMap::new()
                }),
                timer: Timer::new()
            })
        };

        let event_bus2 = event_bus.clone();

        event_bus.inner.timer.run(event_bus2);

        Ok(event_bus)
    }

    pub fn on(&self, event: &str, handle: impl Fn(Context) + Send + Sync + 'static) -> usize {
        let mut handles = self.inner.handles.write().unwrap();
        let id = handles.next_id;
        handles.next_id += 1;

        let vector = handles.callbacks.entry(event.to_owned()).or_insert_with(|| vec![]);
        vector.push((id, Arc::new(handle)));

        handles.index.insert(id, event.to_string());

        id
    }

    pub fn off(&self, id: usize) -> bool {
        let mut handles = self.inner.handles.write().unwrap();

        if let Some(event) = handles.index.get(&id).map(|e| e.clone()) {
            if let Some(vector) = handles.callbacks.get_mut(&event) {
                if let Some(position) = vector.iter().position(|(x, _)| x == &id) {
                    vector.remove(position);
                    handles.index.remove(&id);

                    return true
                }
            }
        }

        false
    }

    pub fn emit(&self, event: &str, message: Message) {
        let mut message = message;

        if let Some(Value::I32(delay)) = message.remove("_delay") {
            self.inner.timer.push((event.to_owned(), message), delay);
        } else {
            self.push(event, message);
        }
    }

    pub fn push(&self, event: &str, message: Message) {
        self.inner.queue.push((event.to_string(), message));
    }

    pub fn run(&self, worker_size: usize, block: bool) {
        let mut threads = Vec::new();

        for _ in 0..worker_size {
            let that = self.clone();
            threads.push(thread::Builder::new().name("worker".into()).spawn(move || {
                loop {
                    let (event, message) = that.inner.queue.pop();
                    let handles2 = {
                        let handles = that.inner.handles.read().unwrap();
                        if let Some(vector) = handles.callbacks.get(&event) {
                            vector.clone()
                        } else {
                            continue;
                        }
                    };

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

#[derive(Clone, Debug)]
pub struct Task {
    pub data: (String, Message),
    pub time: Duration,
}

impl PartialOrd for Task {
    fn partial_cmp(&self, other: &Task) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Task {
    fn cmp(&self, other: &Task) -> Ordering {
        match self.time.cmp(&other.time) {
            Ordering::Equal => Ordering::Equal,
            Ordering::Greater => Ordering::Less,
            Ordering::Less => Ordering::Greater
        }
    }
}

impl PartialEq for Task {
    fn eq(&self, other: &Task) -> bool {
        self.time == other.time
    }
}

impl Eq for Task {}

#[derive(Default)]
pub struct Timer {
    thread_handle: RwLock<Option<thread::JoinHandle<()>>>,
    tasks: Arc<Mutex<BinaryHeap<Task>>>
}

impl Timer {
    pub fn new() -> Timer {
        let tasks: Arc<Mutex<BinaryHeap<Task>>> = Arc::new(Mutex::new(BinaryHeap::new()));

        Timer {
            thread_handle: RwLock::new(None),
            tasks
        }
    }

    pub fn push(&self, data: (String, Message), delay: i32) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let time = Duration::from_millis(delay as u64) + now;

        let mut tasks = self.tasks.lock().unwrap();
        tasks.push(Task {data, time});

        let thread_handle = self.thread_handle.read().unwrap();
        if let Some(t) = thread_handle.as_ref() { t.thread().unpark() }
    }

    pub fn run(&self, event_bus: EventBus) {

        let tasks2 = self.tasks.clone();

        let thread_handle = thread::Builder::new().name("timer".to_owned()).spawn(move || {
            let tasks = tasks2;
            let event_bus = event_bus;

            loop {
                let mut sleep_duration = Duration::from_secs(60);

                loop {
                    let mut tasks = tasks.lock().unwrap();

                    if let Some(ref task) = tasks.peek() {
                        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

                        if task.time > now {
                            sleep_duration = task.time - now;
                            break;
                        } else if let Some(task) = tasks.pop() {
                            let (event, message) = task.data;
                            event_bus.emit(&event, message);
                        }
                    } else {
                        break;
                    }
                }

                thread::park_timeout(sleep_duration);
            }
        }).unwrap();

        let mut t = self.thread_handle.write().unwrap();
        *t = Some(thread_handle);
    }
}
