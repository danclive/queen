use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex};
use std::fmt;
use std::thread;
use std::sync::atomic::AtomicIsize;
use std::sync::atomic::Ordering::SeqCst;

use nson::Value;
use queen_io::plus::block_queue::BlockQueue;

#[derive(Clone)]
pub struct DataBus {
    inner: Arc<InnerDataBus>
}

pub struct Context<'a> {
    pub center: &'a DataBus,
    pub id: i32,
    pub key: &'a str,
    pub old_value: Option<Value>,
    pub value: Value
}

struct InnerDataBus {
    queue: BlockQueue<(String, Option<Value>, Value, bool, bool)>,
    map: Mutex<HashMap<String, Value>>,
    handles: Handles,
    all: RwLock<Vec<(i32, Arc<AllFn>)>>,
    next_id: AtomicIsize
}

type Handles = RwLock<HashMap<String, Vec<(i32, Arc<HandleFn>)>>>;
type HandleFn = dyn Fn(Context) + Send + Sync + 'static;
type AllFn = dyn Fn(Context) + Send + Sync + 'static;

impl Default for DataBus {
    fn default() -> Self {
        Self::new()
    }
}

impl DataBus {
    pub fn new() -> DataBus {
        DataBus {
            inner: Arc::new(InnerDataBus {
                queue: BlockQueue::with_capacity(4 * 1000),
                map: Mutex::new(HashMap::new()),
                handles: RwLock::new(HashMap::new()),
                all: RwLock::new(Vec::new()),
                next_id: AtomicIsize::new(0)
            })
        }
    }

    pub fn insert(&self, key: &str, value: Value) -> Option<Value> {
        let result = {
            let mut map = self.inner.map.lock().unwrap();
            map.insert(key.to_owned(), value.clone())
        };

        let mut skip = false;

        if let Some(ref result) = result {
            if *result == value {
                skip = true
            }
        }

        self.inner.queue.push((key.to_owned(), result.clone(), value, skip, false));

        result
    }

    pub fn insert_skip_all(&self, key: &str, value: Value) -> Option<Value> {
        let result = {
            let mut map = self.inner.map.lock().unwrap();
            map.insert(key.to_owned(), value.clone())
        };

        let mut skip = false;

        if let Some(ref result) = result {
            if *result == value {
                skip = true
            }
        }

        self.inner.queue.push((key.to_owned(), result.clone(), value, skip, true));

        result
    }

    pub fn set(&self, key: &str, value: Value) -> Option<Value> {
        let mut map = self.inner.map.lock().unwrap();
        map.insert(key.to_owned(), value)
    }

    pub fn get(&self, key: &str) -> Option<Value>{
        let map = self.inner.map.lock().unwrap();
        map.get(key).cloned()
    }

    pub fn remove(&self, key: &str) -> Option<Value> {
        let mut map = self.inner.map.lock().unwrap();
        map.remove(key)
    }

    pub fn on(&self, key: &str, handle: impl Fn(Context) + Send + Sync + 'static) -> i32 {
        let mut handles = self.inner.handles.write().unwrap();
        let id = self.inner.next_id.fetch_add(1, SeqCst) as i32;

        let vector = handles.entry(key.to_owned()).or_insert_with(|| vec![]);
        vector.push((id, Arc::new(handle)));

        id
    }

    pub fn all(&self, handle: impl Fn(Context) + Send + Sync + 'static) -> i32 {
        let mut handles = self.inner.all.write().unwrap();
        let id = self.inner.next_id.fetch_add(1, SeqCst) as i32;

        handles.push((id, Arc::new(handle)));

        id
    }

    pub fn off(&self, id: i32) -> bool {
        {
            let mut handles = self.inner.handles.write().unwrap();
            for (_, vector) in handles.iter_mut() {
                if let Some(position) = vector.iter().position(|(x, _)| x == &id) {
                    vector.remove(position);

                    return true
                }
            }
        }

        {
            let mut handles = self.inner.all.write().unwrap();
            if let Some(position) = handles.iter().position(|(x, _)| x == &id) {
                handles.remove(position);

                return true
            }
        }

        false
    }

    pub fn run(&self, worker_size: usize, block: bool) {
        let mut threads = Vec::new();

        for _ in 0..worker_size {
            let that = self.clone();
            threads.push(thread::Builder::new().name("worker".into()).spawn(move || {
                loop {
                    let (key, old_value, value, skip, skip_all) = that.inner.queue.pop();
                    let mut handles2 = Vec::new();

                    if !skip_all {
                        let handles = that.inner.all.read().unwrap();
                        handles2.extend_from_slice(&handles);
                    }

                    if !skip {
                        let handles = that.inner.handles.read().unwrap();
                        if let Some(vector) = handles.get(&key) {
                            handles2.extend_from_slice(vector);
                        }
                    }

                    for (id, handle) in handles2 {
                        let context = Context {
                            center: &that,
                            id,
                            key: &key,
                            old_value: old_value.clone(),
                            value: value.clone()
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
        write!(f, "Context {{ key: {}, old_value: {:?}, value: {:?} }}", self.key, self.old_value, self.value)
    }
}

impl<'a> fmt::Display for Context<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Context {{ key: {}, old_value: {:?}, value: {:?} }}", self.key, self.old_value, self.value)
    }
}
